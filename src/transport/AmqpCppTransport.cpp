#include "motus/transport/backends/AmqpCppTransport.hpp"

#include "motus/Logger.hpp"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <utility>

namespace motus {
namespace transport {
namespace {

/*
 * Neutral Routing -> AMQP exchange type.
 *
 * This mapping is the whole reason ITransport does not simply carry the string "topic". The
 * seam states INTENT -- exact, pattern, broadcast -- and each backend spells that in its own
 * vocabulary, so a DDS backend can be added without the interface having learned any AMQP.
 *
 * Pattern is the default because it is what motus's convention uses: topic-type exchanges with
 * the topic name as the routing key, which makes the dotted topic names (TOPIC.Nav.Position)
 * meaningful -- AMQP routes those on dot-separated segments, so `TOPIC.Nav.*` works for free.
 *
 * AMQP-CPP takes an ENUM here, not the type string every AMQP tutorial shows -- and its own
 * default is `fanout`, which would silently broadcast to every bound queue if the argument were
 * ever omitted. Both are worth naming: the string form does not compile, and the default is the
 * one value that fails by delivering too much rather than too little.
 */
AMQP::ExchangeType amqpExchangeType(Routing routing)
{
    switch (routing)
    {
    case Routing::Exact:     return AMQP::direct;
    case Routing::Pattern:   return AMQP::topic;
    case Routing::Broadcast: return AMQP::fanout;
    }
    return AMQP::topic;
}

} // namespace

AmqpCppTransport::~AmqpCppTransport()
{
    // Order matters and the destructor must not propagate: ~Channel writes a Channel.Close
    // frame through the connection, and a throw from here reaches a noexcept boundary.
    try
    {
        _channel.reset();
        _connection.reset();
    }
    catch (...) {}
}

Capabilities AmqpCppTransport::capabilities() const
{
    Capabilities caps;
    caps.name                   = "amqpcpp";
    caps.durableDelivery        = true;   // send() publishes with delivery mode 2
    caps.publisherConfirms      = false;  // confirmSelect not implemented -- declared, not glossed
    caps.manualAcknowledgement  = true;
    caps.redeliveryFlag         = true;
    caps.deadLetter             = true;   // broker-native DLX preserves body plus x-death time
    caps.routingGroups          = true;   // exchanges, bindings and topic patterns
    caps.fifoPerTopic           = true;
    caps.competingConsumers     = true;
    caps.topicDepth             = true;   // Queue.DeclareOk carries the message count
    caps.purge                  = true;   // Queue.Purge, and it reports what it removed
    caps.requeue                = true;   // Basic.Reject with the requeue flag
    caps.reconnect              = true;   // see setReconnectPolicy; OFF unless asked for

    // TRUE, proven rather than assumed: holding a delivery tag across a pump() boundary was
    // treated as plausible-and-unverified until a live-broker spike held a tag past
    // onReceived's return, across an intervening pump(), and acked it from a call outside any
    // AMQP-CPP callback frame -- the broker settled it correctly. The conformance suite pins
    // that behaviour permanently (DeliveryConformanceTest).
    caps.deferredAcknowledgement = true;

    caps.boundedQueue            = true;   // x-max-length + all three x-overflow policies
    return caps;
}

void AmqpCppTransport::openConnection(std::chrono::milliseconds timeout)
{
    const std::uint16_t port = (_endpoint.port != 0) ? _endpoint.port : 5672;

    _connection = std::make_unique<AmqpConnection>(
        _endpoint.host, port, _endpoint.user, _endpoint.password, _endpoint.realm);

    _connection->connect(timeout);

    // The channel cannot exist before the connection is ready: constructing it emits a
    // Channel.Open frame immediately.
    _channel = std::make_unique<AMQP::Channel>(&_connection->connection());
}

void AmqpCppTransport::connect(const Endpoint &endpoint, std::chrono::milliseconds timeout)
{
    _endpoint = endpoint;   // remembered so reconnection targets the same peer
    openConnection(timeout);
}

void AmqpCppTransport::setReconnectPolicy(const ReconnectPolicy &policy)
{
    _policy         = policy;
    _failedAttempts = 0;
    _nextAttempt    = std::chrono::steady_clock::time_point{};

    MOTUS_LOG_INFO("reconnection " << (policy.enabled ? "enabled" : "disabled")
                  << " (maxAttempts=" << policy.maxAttempts
                  << ", initialDelay=" << policy.initialDelay.count() << "ms"
                  << ", maxDelay=" << policy.maxDelay.count() << "ms)");
}

bool AmqpCppTransport::serviceReconnect()
{
    if (!_policy.enabled) return false;

    if (_failedAttempts >= _policy.maxAttempts)
    {
        MOTUS_LOG_ERROR("giving up: " << _failedAttempts << " consecutive reconnection attempts "
                       "to " << _endpoint.host << " failed. A worker retrying forever against a "
                       "broker that will never answer looks healthy while doing nothing.");
        return false;
    }

    const auto now = std::chrono::steady_clock::now();
    if (now < _nextAttempt) return true;   // still backing off; not dead, just between connections

    ++_failedAttempts;
    MOTUS_LOG_WARN("connection lost; reconnection attempt " << _failedAttempts
                  << " of " << _policy.maxAttempts);

    try
    {
        // Order matters: the channel writes a Channel.Close frame through the connection as it
        // dies, so it must go first -- and both are already unusable.
        _channel.reset();
        _connection.reset();

        // Same rule as _subscribed/_topicDeclared, stated in the comment below: tags from the
        // dead connection are meaningless on the new one. A stale entry here would make a later
        // settle() for a NEW delivery that happens to reuse a small tag number look like it
        // settled an already-dead one, or -- worse -- silently succeed against nothing.
        _outstandingDeferred.clear();

        openConnection(std::chrono::seconds(5));

        // Restore exactly what was established before, not a guess. A reconnected transport
        // that forgot its topic options would redeclare with defaults and earn a 406.
        if (_topicDeclared) declare(_topic, _topicOptions, std::chrono::seconds(5));
        if (_subscribed)    startConsuming(std::chrono::seconds(5));

        _failedAttempts = 0;
        ++_stats.reconnects;

        // Anything in flight when the old connection died was returned to the broker and may
        // already have gone to another worker. Its delivery tags are meaningless here and are
        // never acknowledged -- the abandonment clause, and why this is safe to add at all.
        MOTUS_LOG_INFO("reconnected to " << _endpoint.host << " (" << _stats.reconnects
                      << " total); any in-flight deliveries were returned to the broker");
        return true;
    }
    catch (const std::exception &e)
    {
        const auto delay = std::min(
            _policy.maxDelay,
            std::chrono::milliseconds(_policy.initialDelay.count() << (_failedAttempts - 1)));

        _nextAttempt = std::chrono::steady_clock::now() + delay;
        MOTUS_LOG_WARN("reconnection attempt " << _failedAttempts << " failed: " << e.what()
                      << "; next attempt in " << delay.count() << "ms");
        return true;
    }
}

void AmqpCppTransport::close(std::chrono::milliseconds timeout)
{
    // Whatever is still deferred and unsettled is abandoned here, exactly like an unacked
    // delivery: never resolved, so the broker returns it on its own once this connection is
    // gone. That is Test 3 of the step-0 spike, now pinned in the conformance suite.
    _outstandingDeferred.clear();

    if (_channel) _channel.reset();
    if (_connection) _connection->close(timeout);
    _subscribed = false;
    _handler = {};
}

bool AmqpCppTransport::ready() const
{
    return _connection && _connection->ready();
}

AmqpConnection &AmqpCppTransport::connection()
{
    if (!_connection) throw std::logic_error("connection() called before connect()");
    return *_connection;
}

std::uint32_t AmqpCppTransport::declare(const Topic &topic,
                                        const TopicOptions &options,
                                        std::chrono::milliseconds timeout)
{
    if (!_channel) throw std::logic_error("ensureTopic() called before connect()");

    // Provision the forensic destination before attaching it to the source. One queue per
    // source prevents an inspector for one topic from disturbing another topic's evidence.
    // Recursion terminates because the dead-letter queue explicitly has deadLetter=false.
    if (options.deadLetter)
    {
        const TopicOptions forensic = forensicTopicOptions(topic);
        declare(deadLetterTopic(topic), forensic, timeout);
    }

    // Translate neutral options into AMQP flags. `shared` is the inverse of AMQP's exclusive,
    // which is the one that reads backwards and is worth doing in one obvious place: an
    // exclusive queue admits a single consumer, so competing consumers would fail outright.
    int flags = 0;
    if (options.durable)          flags |= AMQP::durable;
    if (!options.shared)          flags |= AMQP::exclusive;
    if (options.deleteWhenUnused) flags |= AMQP::autodelete;

    // Callback state is heap-allocated and captured by value. AMQP-CPP holds these in its
    // Deferred objects, which can outlive this function -- a late onError firing against
    // captured stack locals would be a use-after-free, and has been one here before.
    auto ready   = std::make_shared<bool>(false);
    auto waiting = std::make_shared<std::uint32_t>(0);
    auto failure = std::make_shared<std::string>();

    // A routing group is an EXCHANGE, declared before the queue that binds into it. An empty
    // group means AMQP's default exchange, which needs no declaration and routes by queue
    // name -- exactly what this transport did before groups existed, so every pre-existing
    // call site is unaffected.
    if (!options.group.empty())
    {
        auto exchangeReady = std::make_shared<bool>(false);

        _channel->declareExchange(options.group, amqpExchangeType(options.routing),
                                  options.durable ? AMQP::durable : 0)
            .onSuccess([exchangeReady] { *exchangeReady = true; })
            .onError([failure](const char *message)
            {
                *failure = (message != nullptr) ? message : "unknown error";
            });

        _connection->pumpUntil(
            [exchangeReady, failure] { return *exchangeReady || !failure->empty(); }, timeout);

        if (!failure->empty())
        {
            // A 406 here means the exchange exists with a DIFFERENT type. That is its own
            // failure mode, distinct from the queue-property mismatch below, and the raw
            // broker text names neither.
            throw std::runtime_error(
                "cannot declare group '" + options.group + "': " + *failure +
                " (a PRECONDITION_FAILED here means the exchange already exists with a"
                " different type; every declarer must agree on Exact/Pattern/Broadcast)");
        }
        if (!*exchangeReady)
            throw std::runtime_error("timed out declaring group '" + options.group + "'");
    }

    AMQP::Table arguments;
    if (options.deadLetter)
    {
        arguments.set("x-dead-letter-exchange", kDeadLetterExchange);
        arguments.set("x-dead-letter-routing-key", topic);
    }
    if (options.maxLength > 0)
    {
        arguments.set("x-max-length", static_cast<std::int32_t>(options.maxLength));
        arguments.set("x-overflow", overflowPolicyName(options.overflow));
    }

    _channel->declareQueue(topic, flags, arguments)
        .onSuccess([ready, waiting](const std::string &, uint32_t messageCount, uint32_t)
        {
            *waiting = messageCount;
            *ready   = true;
        })
        .onError([failure](const char *message)
        {
            *failure = (message != nullptr) ? message : "unknown error";
        });

    _connection->pumpUntil([ready, failure] { return *ready || !failure->empty(); }, timeout);

    if (!failure->empty())
    {
        MOTUS_LOG_ERROR("declaration of topic '" << topic << "' rejected: " << *failure);
        // A 406 here almost always means two declarations disagree about durability,
        // exclusivity or auto-delete. Say so, because the raw broker text does not.
        throw std::runtime_error(
            "cannot declare topic '" + topic + "': " + *failure +
            " (a PRECONDITION_FAILED here means an existing destination was declared with"
            " different properties -- durable/shared/deleteWhenUnused/deadLetter/maxLength/overflow "
            "must match "
            "everywhere; changing queue properties requires the documented delete-and-recreate "
            "migration)");
    }
    if (!*ready)
        throw std::runtime_error("timed out declaring topic '" + topic + "'");

    // Bind the queue into the group. Without this the exchange exists, the queue exists, and
    // published messages are SILENTLY DISCARDED -- a broker drops anything matching no
    // binding and tells the publisher nothing. That is the quietest failure mode in AMQP,
    // and it looks exactly like a consumer that is not running.
    if (!options.group.empty())
    {
        const std::string key = options.route.empty() ? topic : options.route;

        auto bound = std::make_shared<bool>(false);
        _channel->bindQueue(options.group, topic, key)
            .onSuccess([bound] { *bound = true; })
            .onError([failure](const char *message)
            {
                *failure = (message != nullptr) ? message : "unknown error";
            });

        _connection->pumpUntil([bound, failure] { return *bound || !failure->empty(); }, timeout);

        if (!failure->empty())
            throw std::runtime_error("cannot bind topic '" + topic + "' into group '" +
                                     options.group + "': " + *failure);
        if (!*bound)
            throw std::runtime_error("timed out binding topic '" + topic + "'");

        // Remembered so send() can publish through the group. A publisher states the routing
        // once, at declaration, rather than repeating it on every message -- and a topic never
        // declared here keeps the default-exchange behaviour, which is a safe fallback rather
        // than a silent misroute.
        _routes[topic] = RouteBinding{options.group, key};
    }

    return *waiting;
}

void AmqpCppTransport::ensureTopic(const Topic &topic,
                                   const TopicOptions &options,
                                   std::chrono::milliseconds timeout)
{
    const std::uint32_t waiting = declare(topic, options, timeout);

    // Remembered so a reconnection redeclares identically. Redeclaring with defaults instead
    // would disagree with the original on durability and earn a 406 PRECONDITION_FAILED.
    _topic         = topic;
    _topicOptions  = options;
    _topicDeclared = true;

    // Debug, not info: provisioning an application's topology declares one destination per
    // topic -- dozens in a real system -- and dozens of identical lines bury whatever the
    // caller was actually reporting. The caller owns the useful one-line summary.
    MOTUS_LOG_DEBUG("topic '" << topic << "' ready, holding " << waiting << " message(s)");
}

std::uint32_t AmqpCppTransport::topicDepth(const Topic &topic, std::chrono::milliseconds timeout)
{
    if (!_channel) throw std::logic_error("topicDepth() called before connect()");

    // Passive is load-bearing now that a source queue and its forensic queue intentionally have
    // different arguments. A normal redeclaration would have to guess which kind `topic` is and
    // could either create a DLQ-of-a-DLQ or earn a 406 from the broker.
    auto ready   = std::make_shared<bool>(false);
    auto waiting = std::make_shared<std::uint32_t>(0);
    auto failure = std::make_shared<std::string>();

    _channel->declareQueue(topic, AMQP::passive)
        .onSuccess([ready, waiting](const std::string &, uint32_t messageCount, uint32_t)
        {
            *waiting = messageCount;
            *ready   = true;
        })
        .onError([failure](const char *message)
        {
            *failure = (message != nullptr) ? message : "unknown error";
        });

    _connection->pumpUntil([ready, failure] { return *ready || !failure->empty(); }, timeout);

    if (!failure->empty())
        throw std::runtime_error("cannot inspect topic '" + topic + "': " + *failure);
    if (!*ready)
        throw std::runtime_error("timed out inspecting topic '" + topic + "'");
    return *waiting;
}

std::uint32_t AmqpCppTransport::purgeTopic(const Topic &topic, std::chrono::milliseconds timeout)
{
    if (!_channel) throw std::logic_error("purgeTopic() called before connect()");

    // shared_ptr state captured BY VALUE, never stack locals by reference: AMQP-CPP stores these
    // callbacks and can fire them after this function has returned, which is precisely what
    // happens when an error arrives late -- when the system is already misbehaving.
    auto removed = std::make_shared<std::uint32_t>(0);
    auto done    = std::make_shared<bool>(false);
    auto failure = std::make_shared<std::string>();

    _channel->purgeQueue(topic)
        .onSuccess([removed, done](std::uint32_t count) { *removed = count; *done = true; })
        .onError([failure](const char *message)
        {
            *failure = (message != nullptr) ? message : "unknown error";
        });

    _connection->pumpUntil([done, failure] { return *done || !failure->empty(); }, timeout);

    if (!failure->empty())
        throw std::runtime_error("cannot purge topic '" + topic + "': " + *failure);
    if (!*done)
        throw std::runtime_error("timed out purging topic '" + topic + "'");

    MOTUS_LOG_DEBUG("purged topic '" << topic << "', discarding " << *removed << " message(s)");
    return *removed;
}

bool AmqpCppTransport::send(const Topic &topic,
                            const std::string &payload,
                            const SendOptions &options)
{
    if (!_channel) throw std::logic_error("send() called before connect()");

    if (options.confirm)
        throw std::logic_error(
            "backend 'amqpcpp' does not support publisher confirms "
            "(Capabilities::publisherConfirms is false); check the capability before requesting it");

    // A durable QUEUE and a persistent MESSAGE are separate mechanisms and both are needed to
    // survive a restart. Channel::publish(exchange, key, string) -- the overload every AMQP-CPP
    // example uses -- builds an Envelope with no properties at all, hence no delivery_mode,
    // which AMQP defaults to 1 (transient). That was the state of this project for its whole
    // first phase and nothing reported it.
    //
    // Envelope stores a pointer and a length rather than copying, so `payload` must outlive
    // this call. It does: it is the caller's argument.
    AMQP::Envelope envelope(payload.data(), payload.size());
    if (options.durable) envelope.setDeliveryMode(2);

    // Publish through the group this topic was declared into, if it was. The empty exchange is
    // AMQP's default, which routes by queue name -- the behaviour every call site had before
    // groups existed, and the fallback for a topic this transport never declared.
    std::string exchange;
    std::string routingKey = topic;
    if (const auto it = _routes.find(topic); it != _routes.end())
    {
        exchange   = it->second.group;
        routingKey = it->second.key;
    }

    // A per-message override wins over the declared route. Only meaningful inside a group: on
    // the default exchange the routing key IS the destination name, so overriding it would
    // quietly send the message somewhere else entirely rather than route it differently.
    if (!options.route.empty() && !exchange.empty())
        routingKey = options.route;

    const bool accepted = _channel->publish(exchange, routingKey, envelope);

    if (accepted)
    {
        ++_stats.sent;
        MOTUS_LOG_DEBUG("sent #" << _stats.sent << " to topic '" << topic << "' ("
                       << payload.size() << " byte(s), "
                       << (options.durable ? "persistent" : "transient") << ")");
    }
    else
    {
        MOTUS_LOG_WARN("channel refused a send to topic '" << topic << "'");
    }

    return accepted;
}

void AmqpCppTransport::subscribe(const Topic &topic,
                                 DeliveryHandler handler,
                                 std::chrono::milliseconds timeout)
{
    if (!_channel) throw std::logic_error("subscribe() called before connect()");
    if (!handler)  throw std::invalid_argument("subscribe() requires a callable handler");
    if (_subscribed)
        throw std::logic_error("subscribe() called twice on one transport; use one transport per "
                               "topic instead of overwriting the active subscription to '" +
                               _topic + "'");

    _handler = std::move(handler);
    _topic   = topic;

    startConsuming(timeout);
    _subscribed = true;
}

void AmqpCppTransport::startConsuming(std::chrono::milliseconds timeout)
{
    const Topic &topic = _topic;

    // One unacknowledged message at a time. Without this the broker pushes the whole queue at
    // once and a crash mid-processing loses every prefetched job.
    //
    // DO NOT "OPTIMISE" THIS. Deferred-acknowledgement timing depends on THIS EXACT VALUE
    // being 1: it is what makes "one delivery in flight per subscription" true, which is what
    // lets a deferred delivery serialize its topic until it is settled. Raising this prefetch
    // for throughput silently changes acknowledgement behaviour for every consumer that uses
    // Disposition::Deferred -- a connection between this line and behaviour far above the seam
    // that nothing else here will tell you about.
    _channel->setQos(1);

    auto consuming = std::make_shared<bool>(false);
    auto failure   = std::make_shared<std::string>();

    _channel->consume(topic)
        .onSuccess([consuming](const std::string &) { *consuming = true; })
        .onReceived([this](const AMQP::Message &message, uint64_t deliveryTag, bool redelivered)
        {
            // Capturing `this` is safe: _channel is destroyed before _handler (reverse
            // declaration order), so no callback can fire against a dead handler.
            Delivery delivery;
            delivery.body        = std::string(message.body(),
                                               static_cast<std::size_t>(message.bodySize()));
            delivery.redelivered = redelivered;
            delivery.tag         = deliveryTag;

            ++_stats.received;

            Disposition disposition;
            try
            {
                disposition = _handler(delivery);
            }
            catch (...)
            {
                // Contract: an uncooperative failure resolves nothing. No ack, no reject -- the
                // delivery stays unacknowledged, so the broker returns it when this connection
                // closes. That is the crash-safety guarantee manual acknowledgement exists for.
                _handlerFailure = std::current_exception();
                ++_stats.abandoned;
                return;
            }

            switch (disposition)
            {
                case Disposition::Ack:
                    _channel->ack(deliveryTag);
                    ++_stats.acksPerformed;
                    break;

                case Disposition::Requeue:
                    // AMQP::requeue is the whole difference between "try again" and "destroy
                    // it". Passing 0 here instead would silently turn every transient failure
                    // into permanent data loss.
                    _channel->reject(deliveryTag, AMQP::requeue);
                    ++_stats.requeuesPerformed;
                    break;

                case Disposition::Reject:
                    _channel->reject(deliveryTag);   // flags = 0 -> no requeue
                    ++_stats.rejectsPerformed;
                    break;

                case Disposition::Deferred:
                    // Deliberately NOT acked, rejected or requeued here. Responsibility for this
                    // tag has transferred to whoever holds it; settle() resolves it later, from
                    // outside this callback. Stats do not move yet either -- Stats::acksPerformed
                    // etc. must reflect what the transport actually DID, and nothing has been
                    // done to the broker about this delivery yet.
                    _outstandingDeferred.insert(deliveryTag);
                    break;
            }
        })
        .onError([failure](const char *message)
        {
            *failure = (message != nullptr) ? message : "unknown error";
        });

    _connection->pumpUntil([consuming, failure] { return *consuming || !failure->empty(); },
                           timeout);

    if (!failure->empty())
        throw std::runtime_error("failed to subscribe to '" + topic + "': " + *failure);
    if (!*consuming)
        throw std::runtime_error("timed out subscribing to '" + topic + "'");

    MOTUS_LOG_INFO("subscribed to '" << topic << "' with prefetch=1");
}

void AmqpCppTransport::settle(std::uint64_t tag, Disposition disposition)
{
    if (!_channel) throw std::logic_error("settle() called before connect()");

    // Checked before touching _outstandingDeferred: Deferred-into-Deferred is a caller error
    // (settling a delivery into the very state it is already being settled OUT of), not a
    // "this tag is unknown" error, and the two deserve different diagnostics.
    if (disposition == Disposition::Deferred)
        throw std::invalid_argument(
            "settle() cannot be called with Disposition::Deferred; a delivery already deferred "
            "must be settled with Ack, Reject or Requeue");

    const auto it = _outstandingDeferred.find(tag);
    if (it == _outstandingDeferred.end())
        throw std::logic_error(
            "settle() called for tag " + std::to_string(tag) + ", which is not an outstanding "
            "deferred delivery on this transport (already settled, never deferred, or from a "
            "connection that has since been replaced)");
    _outstandingDeferred.erase(it);

    switch (disposition)
    {
        case Disposition::Ack:
            _channel->ack(tag);
            ++_stats.acksPerformed;
            break;

        case Disposition::Requeue:
            _channel->reject(tag, AMQP::requeue);
            ++_stats.requeuesPerformed;
            break;

        case Disposition::Reject:
            _channel->reject(tag);   // flags = 0 -> no requeue
            ++_stats.rejectsPerformed;
            break;

        case Disposition::Deferred:
            break;   // unreachable -- rejected above before the lookup
    }
}

bool AmqpCppTransport::pump(std::chrono::milliseconds slice)
{
    if (!_connection || _connection->closed())
    {
        // Without a policy this is the end, exactly as before. With one, the transport is not
        // dead -- it is between connections -- so pump() keeps returning true while it tries.
        if (!_policy.enabled) return false;
        return serviceReconnect();
    }

    // Predicate never satisfied on purpose: run the socket for the whole slice so inbound
    // deliveries arrive and outbound frames leave.
    _connection->pumpUntil([] { return false; }, slice);

    // Rethrow a handler failure here, outside AMQP-CPP's parser, where it can be caught.
    if (_handlerFailure)
    {
        const std::exception_ptr failure = _handlerFailure;
        _handlerFailure = nullptr;
        std::rethrow_exception(failure);
    }

    // The close is usually detected HERE, by this pump rather than before it -- a peer going
    // away is only discoverable by doing I/O. Returning false at this point would report the
    // transport dead on the very call that noticed the loss, before reconnection had been
    // offered a single attempt, and a caller whose loop stops on a false return would never
    // call again. The restart test caught exactly that.
    if (_connection->closed() && _policy.enabled)
        return serviceReconnect();

    return !_connection->closed();
}

Stats AmqpCppTransport::stats() const
{
    Stats snapshot = _stats;
    if (_connection)
    {
        snapshot.bytesSent     = _connection->bytesSent();
        snapshot.bytesReceived = _connection->bytesReceived();
    }
    return snapshot;
}

} // namespace transport
} // namespace motus
