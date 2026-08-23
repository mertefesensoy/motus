#include "motus/transport/backends/InMemoryTransport.hpp"

#include "motus/Logger.hpp"

#include <algorithm>
#include <deque>
#include <map>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>

namespace motus {
namespace transport {
namespace {

/**
 * The process-wide broker.
 *
 * Deliberately a singleton with no reset: see the class comment in InMemoryTransport.hpp. The
 * conformance suite must run the identical scenario under identical conditions on every
 * backend, and the AMQP backend shares a live queue.
 */
class Broker
{
public:
    static Broker &instance()
    {
        static Broker broker;
        return broker;
    }

    // Pattern matching is motus::transport::routeMatches, defined on the seam rather than here.
    // What `Routing::Pattern` means is part of the CONTRACT: a backend that matched differently
    // would still pass a contract that never said what matching means, and two backends
    // silently disagreeing about routing is a production-only defect.

    /**
     * Declare a topic, or verify that an existing declaration agrees.
     *
     * Mismatched properties are an ERROR here, exactly as AMQP answers a conflicting
     * redeclaration with 406 PRECONDITION_FAILED. Being permissive would make this backend
     * quietly more forgiving than the real one, so a bug that breaks production would pass its
     * conformance run -- which is the one thing a reference implementation must never do.
     */
    void ensureTopic(const Topic &topic, const TopicOptions &options)
    {
        const std::lock_guard<std::mutex> lock(_mutex);

        // A group is this backend's exchange. Redeclaring one with a different routing kind is
        // an error for the same reason the property mismatch below is: a real broker answers
        // 406 PRECONDITION_FAILED, and a reference implementation that is more forgiving than
        // the real one lets a bug pass its conformance run.
        if (!options.group.empty())
        {
            auto [groupIt, groupInserted] = _groups.try_emplace(options.group, options.routing);
            if (!groupInserted && groupIt->second != options.routing)
            {
                throw std::runtime_error(
                    "cannot declare group '" + options.group +
                    "': it already exists with a different routing kind (every declarer must "
                    "agree on Exact/Pattern/Broadcast)");
            }
        }

        auto [it, inserted] = _topics.try_emplace(topic);
        if (inserted)
        {
            it->second.options = options;
            it->second.binding = options.route.empty() ? topic : options.route;
            return;
        }

        const TopicOptions &existing = it->second.options;
        if (existing.durable          != options.durable ||
            existing.shared           != options.shared  ||
            existing.deleteWhenUnused != options.deleteWhenUnused ||
            existing.deadLetter       != options.deadLetter ||
            existing.maxLength        != options.maxLength ||
            existing.overflow         != options.overflow)
        {
            throw std::runtime_error(
                "cannot declare topic '" + topic + "': it already exists with different "
                "properties (a PRECONDITION_FAILED here means an existing destination was "
                "declared with different properties -- durable/shared/deleteWhenUnused/"
                "deadLetter/maxLength/overflow must match everywhere)");
        }
    }

    /**
     * Publish, routing through the topic's group if it has one.
     *
     * The un-grouped path is unchanged and still delivers straight to the named destination,
     * which is what every caller predating groups gets.
     *
     * The grouped path is where this backend earns its keep. A message goes to the GROUP with a
     * routing key, and every destination whose binding matches receives a copy -- so a message
     * can reach several destinations, or NONE. Silently reaching none is exactly what a real
     * broker does with an unmatched routing key, and modelling it faithfully is the point: a
     * backend that quietly delivered anyway would pass the contract while production dropped
     * the message.
     */
    bool publish(const Topic &topic, std::string body, const std::string &routeOverride = {})
    {
        const std::lock_guard<std::mutex> lock(_mutex);

        const auto origin = _topics.find(topic);
        const std::string group = (origin == _topics.end()) ? std::string{}
                                                            : origin->second.options.group;

        if (group.empty())
        {
            auto &state = _topics[topic];
            return enqueueLocked(topic, state, Held{++_sequence, std::move(body), false});
        }

        const Routing routing = _groups[group];
        // A per-message route overrides the one this destination was declared with. Without
        // this the key is always the publisher's own binding, so it necessarily matches itself
        // and every routing test would pass for the wrong reason.
        const std::string key = routeOverride.empty() ? origin->second.binding : routeOverride;

        bool accepted = true;
        for (auto &[name, state] : _topics)
        {
            if (state.options.group != group) continue;

            const bool matches =
                (routing == Routing::Broadcast) ? true
                : (routing == Routing::Exact)   ? (state.binding == key)
                                                : routeMatches(state.binding, key);
            if (matches)
                accepted = enqueueLocked(name, state, Held{++_sequence, body, false}) && accepted;
        }
        return accepted;
    }

    /// Take the next message, if any. FIFO.
    bool take(const Topic &topic, Held &out)
    {
        const std::lock_guard<std::mutex> lock(_mutex);

        auto it = _topics.find(topic);
        if (it == _topics.end() || it->second.messages.empty()) return false;

        out = std::move(it->second.messages.front());
        it->second.messages.pop_front();
        return true;
    }

    /// Put a message back for another attempt, flagged as redelivered.
    ///
    /// Pushed to the FRONT, so an unacknowledged message returns to where it was rather than
    /// behind everything published since. That is what preserves FIFO for a single consumer
    /// across a worker restart -- ordering that survives only while nothing goes wrong is not
    /// ordering worth relying on.
    void requeue(const Topic &topic, Held message)
    {
        message.redelivered = true;
        const std::lock_guard<std::mutex> lock(_mutex);
        _topics[topic].messages.push_front(std::move(message));
    }

    /// Preserve a rejected message in the source topic's forensic destination. The destination
    /// is provisioned alongside the source, so finding it missing is an invariant violation.
    void deadLetter(const Topic &source, Held message)
    {
        const std::lock_guard<std::mutex> lock(_mutex);
        deadLetterLocked(source, std::move(message));
    }

    std::uint32_t depth(const Topic &topic)
    {
        const std::lock_guard<std::mutex> lock(_mutex);
        auto it = _topics.find(topic);
        return (it == _topics.end())
                   ? 0u
                   : static_cast<std::uint32_t>(it->second.messages.size());
    }

    /// Discard everything WAITING at a topic; returns how many went.
    ///
    /// Deliveries already handed to a consumer and not yet resolved are deliberately untouched,
    /// matching AMQP's Queue.Purge: purging removes what is queued, not what is outstanding.
    /// Emptying those too would silently break the crash-safety guarantee that an unresolved
    /// delivery comes back when the transport closes.
    std::uint32_t purge(const Topic &topic)
    {
        const std::lock_guard<std::mutex> lock(_mutex);
        auto it = _topics.find(topic);
        if (it == _topics.end()) return 0u;

        const auto removed = static_cast<std::uint32_t>(it->second.messages.size());
        it->second.messages.clear();
        return removed;
    }

private:
    /** Apply maxLength/overflow while `_mutex` is held; false means the new publish was refused. */
    template <typename State>
    bool enqueueLocked(const Topic &topic, State &state, Held message)
    {
        if (state.options.maxLength > 0 &&
            state.messages.size() >= state.options.maxLength)
        {
            switch (state.options.overflow)
            {
            case OverflowPolicy::DropHead:
            {
                Held evicted = std::move(state.messages.front());
                state.messages.pop_front();
                if (state.options.deadLetter) deadLetterLocked(topic, std::move(evicted));
                break;
            }
            case OverflowPolicy::RejectPublish:
                return false;
            case OverflowPolicy::RejectPublishDlx:
                if (state.options.deadLetter) deadLetterLocked(topic, std::move(message));
                return false;
            }
        }

        state.messages.push_back(std::move(message));
        return true;
    }

    /// Core of deadLetter(), assuming the caller already holds `_mutex`. Split out so
    /// publish()'s own overflow handling (enqueueLocked, above) can dead-letter an evicted
    /// message without recursively re-locking a non-recursive std::mutex it is already holding
    /// -- deadLetter() itself still takes the lock and simply forwards here.
    void deadLetterLocked(const Topic &source, Held message)
    {
        const auto sourceIt = _topics.find(source);
        if (sourceIt == _topics.end() || !sourceIt->second.options.deadLetter) return;

        const Topic forensic = deadLetterTopic(source);
        auto target = _topics.find(forensic);
        if (target == _topics.end())
            throw std::logic_error("dead-letter destination '" + forensic +
                                   "' was not provisioned with source '" + source + "'");

        message.sequence    = ++_sequence;
        message.redelivered = false;
        enqueueLocked(forensic, target->second, std::move(message));
    }

    struct TopicState
    {
        std::deque<Held> messages;
        TopicOptions     options;
        /// The key this destination is bound with. Defaults to the topic name, matching what
        /// AMQP's default exchange does implicitly.
        std::string      binding;
    };

    std::mutex                   _mutex;
    std::map<Topic, TopicState>  _topics;
    /// Declared groups and the routing kind each was declared with, so a conflicting
    /// redeclaration can be refused the way a broker refuses it.
    std::map<std::string, Routing> _groups;
    std::uint64_t                _sequence = 0;
};

} // namespace

// ---------------------------------------------------------------------------

InMemoryTransport::~InMemoryTransport()
{
    try { returnOutstanding(); } catch (...) {}
}

Capabilities InMemoryTransport::capabilities() const
{
    Capabilities caps;
    caps.name                  = "inmemory";

    // FALSE, and honestly so. This is memory: nothing here survives the process, let alone a
    // restart. A Producer with default options therefore REFUSES this backend, which is the
    // capability model working as designed on its first customer -- and is exactly the check
    // that would have caught the predecessor system's transient-messages defect.
    caps.durableDelivery       = false;

    caps.publisherConfirms     = true;   // send() completes only once the message is stored
    caps.manualAcknowledgement = true;
    caps.redeliveryFlag        = true;
    caps.deadLetter            = true;
    // TRUE, with full fidelity: exchanges, bindings and AMQP-style * / # patterns. A backend
    // that declared this false would let every routing scenario skip, and the contract would
    // pass while proving nothing -- which is precisely why this backend models the real one
    // rather than the subset that is convenient.
    caps.routingGroups         = true;
    caps.fifoPerTopic          = true;
    caps.competingConsumers    = true;
    caps.topicDepth            = true;
    caps.purge                 = true;
    caps.requeue               = true;

    // TRUE, with full fidelity, for the same reason routingGroups is: a reference backend that
    // declared this false would let the deferred-acknowledgement conformance scenario skip, and
    // this backend's entire purpose is to prove ITransport is genuinely middleware-neutral --
    // which it cannot do for a concept it opts out of.
    caps.deferredAcknowledgement = true;

    // TRUE, with full fidelity for all OverflowPolicy values (Broker::enqueueLocked), the same
    // reason routingGroups and deferredAcknowledgement above are true rather than skipped -- a
    // reference backend that opted out would let its conformance scenarios prove nothing.
    caps.boundedQueue           = true;
    return caps;
}

void InMemoryTransport::connect(const Endpoint &, std::chrono::milliseconds)
{
    // There is nothing to reach. The endpoint is accepted and ignored, which is itself a useful
    // property of the seam: a caller cannot tell the difference from the call site.
    _connected = true;
}

void InMemoryTransport::close(std::chrono::milliseconds)
{
    returnOutstanding();
    _connected = false;
    _subscribed = false;
    _handler = {};
    _topic.clear();
}

bool InMemoryTransport::ready() const
{
    return _connected;
}

void InMemoryTransport::ensureTopic(const Topic &topic,
                                    const TopicOptions &options,
                                    std::chrono::milliseconds)
{
    if (!_connected) throw std::logic_error("ensureTopic() called before connect()");

    if (options.deadLetter)
    {
        const TopicOptions forensic = forensicTopicOptions(topic);
        Broker::instance().ensureTopic(deadLetterTopic(topic), forensic);
    }
    Broker::instance().ensureTopic(topic, options);
}

std::uint32_t InMemoryTransport::topicDepth(const Topic &topic, std::chrono::milliseconds)
{
    if (!_connected) throw std::logic_error("topicDepth() called before connect()");
    return Broker::instance().depth(topic);
}

std::uint32_t InMemoryTransport::purgeTopic(const Topic &topic, std::chrono::milliseconds)
{
    if (!_connected) throw std::logic_error("purgeTopic() called before connect()");

    // Full fidelity on purpose. A backend that declined to implement this -- or declared the
    // capability false -- would skip the conformance scenario and prove nothing, which is the
    // failure mode the whole contract exists to prevent.
    return Broker::instance().purge(topic);
}

bool InMemoryTransport::send(const Topic &topic,
                             const std::string &payload,
                             const SendOptions &options)
{
    if (!_connected) throw std::logic_error("send() called before connect()");

    if (options.durable)
        throw std::logic_error(
            "backend 'inmemory' cannot deliver durable messages "
            "(Capabilities::durableDelivery is false); check the capability before requesting it");

    // Same rule as the AMQP backend: a per-message route overrides the declared one, but
    // only inside a group. Ungrouped, the route IS the destination name.
    const bool accepted = Broker::instance().publish(topic, payload, options.route);
    ++_stats.sent;
    _stats.bytesSent += payload.size();
    return accepted;
}

void InMemoryTransport::subscribe(const Topic &topic,
                                  DeliveryHandler handler,
                                  std::chrono::milliseconds)
{
    if (!_connected) throw std::logic_error("subscribe() called before connect()");
    if (!handler)    throw std::invalid_argument("subscribe() requires a callable handler");
    if (_subscribed)
        throw std::logic_error("subscribe() called twice on one transport; use one transport per "
                               "topic instead of overwriting the active subscription to '" +
                               _topic + "'");

    _topic   = topic;
    _handler = std::move(handler);
    _subscribed = true;
}

bool InMemoryTransport::pump(std::chrono::milliseconds slice)
{
    if (!_connected) return false;
    if (!_handler)   { std::this_thread::sleep_for(slice); return true; }

    const auto deadline = std::chrono::steady_clock::now() + slice;

    while (std::chrono::steady_clock::now() < deadline)
    {
        if (!_outstanding.empty())
        {
            // An earlier delivery on THIS transport is still unresolved -- ordinarily a
            // vanishingly short window, but Disposition::Deferred can hold it open indefinitely
            // pending settle(). Real AMQP enforces exactly this via setQos(1): the broker never
            // PUSHES a second delivery while the first is unacked (proven empirically, not just
            // read from the client library, by the design doc step-0 spike). A backend that let
            // a second message through here would be an easier test than the real one, which
            // defeats this class's entire reason to exist -- see the header comment.
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            continue;
        }

        Held message;
        if (!Broker::instance().take(_topic, message))
        {
            // Nothing waiting. Sleep briefly rather than spinning, but stay well inside the
            // caller's slice so a bounded poll stays bounded.
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            continue;
        }

        Delivery delivery;
        delivery.body        = message.body;
        delivery.redelivered = message.redelivered;
        delivery.tag         = message.sequence;

        ++_stats.received;
        _stats.bytesReceived += message.body.size();

        // Outstanding from the instant it leaves the topic until it is resolved. If the handler
        // throws, the catch below returns it -- a subscriber must never take work with it.
        _outstanding.emplace(message.sequence, message);

        Disposition disposition;
        try
        {
            disposition = _handler(delivery);
        }
        catch (...)
        {
            // Contract: an uncooperative failure resolves nothing. The delivery is NOT returned
            // here -- it stays outstanding until close(), which is what AMQP does with an
            // unacknowledged message and what makes "a worker that dies gives its work back"
            // mean the same thing on both backends.
            //
            // Returning it immediately would be subtly wrong: a still-running subscriber could
            // be handed the same message again before it has gone away, which no real broker
            // does.
            ++_stats.abandoned;
            throw;
        }

        if (disposition == Disposition::Deferred)
        {
            // Responsibility for this delivery has transferred to whoever holds `delivery.tag`.
            // It stays in _outstanding (crash-safety, and the take-loop guard above) until
            // settle() resolves it. Nothing here is a broker action yet, so no Stats counter
            // moves -- Stats must reflect what this transport actually DID.
            return true;
        }

        _outstanding.erase(message.sequence);   // resolved synchronously; done with the bookkeeping copy

        switch (disposition)
        {
            case Disposition::Ack:
                ++_stats.acksPerformed;
                break;

            case Disposition::Requeue:
                Broker::instance().requeue(_topic, message);
                ++_stats.requeuesPerformed;
                break;

            case Disposition::Reject:
                Broker::instance().deadLetter(_topic, std::move(message));
                ++_stats.rejectsPerformed;
                break;

            case Disposition::Deferred:
                break;   // unreachable -- handled above before this switch
        }

        // One message per pump call, mirroring prefetch=1: competing consumers must be able to
        // interleave, and a single pump draining an entire topic would make that untestable.
        return true;
    }

    return true;
}

void InMemoryTransport::settle(std::uint64_t tag, Disposition disposition)
{
    if (!_connected) throw std::logic_error("settle() called before connect()");

    // Checked before touching _outstanding: Deferred-into-Deferred is a caller error (settling a
    // delivery into the very state it is already being settled OUT of), not an unknown-tag
    // error, and the two deserve different diagnostics.
    if (disposition == Disposition::Deferred)
        throw std::invalid_argument(
            "settle() cannot be called with Disposition::Deferred; a delivery already deferred "
            "must be settled with Ack, Reject or Requeue");

    const auto it = _outstanding.find(tag);
    if (it == _outstanding.end())
        throw std::logic_error(
            "settle() called for tag " + std::to_string(tag) + ", which is not an outstanding "
            "deferred delivery on this transport (already settled, or never deferred)");

    Held message = std::move(it->second);
    _outstanding.erase(it);

    switch (disposition)
    {
        case Disposition::Ack:
            ++_stats.acksPerformed;
            break;

        case Disposition::Requeue:
            Broker::instance().requeue(_topic, message);
            ++_stats.requeuesPerformed;
            break;

        case Disposition::Reject:
            Broker::instance().deadLetter(_topic, std::move(message));
            ++_stats.rejectsPerformed;
            break;

        case Disposition::Deferred:
            break;   // unreachable -- rejected above before the lookup
    }
}

void InMemoryTransport::returnOutstanding()
{
    // The crash-safety guarantee, and the whole reason manual acknowledgement exists: a
    // subscriber that goes away does not take its work with it. Reached when a handler threw
    // and the transport is then closed or destroyed -- exactly what a killed worker looks like.
    if (_outstanding.empty()) return;

    MOTUS_LOG_WARN("inmemory transport closing with " << _outstanding.size()
                  << " unresolved delivery(ies); returning them for redelivery");

    // Reverse order, so the earliest abandoned message ends up at the front and FIFO survives
    // the crash. Ordering that holds only while nothing goes wrong is not ordering. _outstanding
    // is keyed by sequence number, which was assigned in arrival order, so iterating it highest-
    // to-lowest is the same reverse-chronological order the old push_back/rbegin vector gave.
    for (auto it = _outstanding.rbegin(); it != _outstanding.rend(); ++it)
        Broker::instance().requeue(_topic, it->second);

    _outstanding.clear();
}

Stats InMemoryTransport::stats() const
{
    return _stats;
}

} // namespace transport
} // namespace motus
