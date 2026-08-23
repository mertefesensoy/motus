#include "motus/Consumer.hpp"

#include "motus/Logger.hpp"

#include <stdexcept>
#include <utility>

namespace motus {
namespace {

/**
 * Decode one body into a ByteMessage, or decide its fate without any handler running.
 *
 * Payload-level failures are decided here, before the handler ever runs, and are always
 * permanent: an oversized body, a missing or malformed envelope, or an unrecognised
 * format_version is a deterministic property of the bytes. Retrying one fails identically and
 * would spin the worker forever, so these are rejected without requeue. Dead-lettering, where
 * the destination was declared with it, preserves the payload for inspection.
 *
 * Returns true when `message` is usable; false with `failure` filled in otherwise.
 */
bool decode(const std::string &body, ByteMessage &message, DispatchResult &failure)
{
    if (body.size() > ByteMessage::MAX_PAYLOAD_BYTES)
    {
        failure = {Disposition::Reject,
                   "message body is " + std::to_string(body.size()) + " bytes, over the " +
                       std::to_string(ByteMessage::MAX_PAYLOAD_BYTES) + " byte limit"};
        return false;
    }

    try
    {
        message = ByteMessage::deserialize(body);
        return true;
    }
    catch (const std::exception &e)
    {
        failure = {Disposition::Reject, e.what()};
        return false;
    }
}

/// Bounded-retry classification shared by both handler shapes. A transient failure gets
/// exactly one more attempt, bounded by the middleware's own redelivered flag.
DispatchResult classifyTransient(const TransientFailure &e, bool redelivered)
{
    if (redelivered)
        return {Disposition::Reject,
                std::string("transient failure persisted across a redelivery, treating as "
                            "permanent: ") + e.what()};
    return {Disposition::Requeue, e.what()};
}

} // namespace

DispatchResult dispatch(const std::string &body, const MessageHandler &handler, bool redelivered)
{
    return dispatch(body, Handlers{handler, {}}, redelivered);
}

DispatchResult dispatch(const std::string &body, const Handlers &handlers, bool redelivered,
                        SettlementHandle deliveryTag)
{
    ByteMessage    message;
    DispatchResult failure{Disposition::Reject, {}};
    if (!decode(body, message, failure)) return failure;

    if (handlers.onDeferred)
    {
        try
        {
            // Whatever the handler returns IS the decision -- including Disposition::Deferred.
            // Responsibility for the delivery has transferred to the handler the instant this
            // call returns without throwing.
            return {handlers.onDeferred(message, deliveryTag), {}};
        }
        catch (const TransientFailure &e)
        {
            // This means the handler could not even ACCEPT the message (e.g. an internal queue
            // at its bound), not that accepted work later failed. Nothing was deferred, so the
            // normal bounded-retry policy applies exactly as it does below.
            return classifyTransient(e, redelivered);
        }
        catch (const std::exception &e)
        {
            return {Disposition::Reject, e.what()};
        }
    }

    if (!handlers.onMessage)
        return {Disposition::Reject, "message understood, but no handler is configured"};

    try
    {
        handlers.onMessage(message);
        return {Disposition::Ack, {}};
    }
    catch (const TransientFailure &e)
    {
        // The handler says this would plausibly succeed later -- a full disk, a locked output
        // directory, an unavailable downstream. Rejecting it would destroy real work for a
        // reason that may already have cleared. Bounded to exactly one retry: an unbounded
        // requeue on a failure that is not actually transient recreates the infinite spin this
        // whole policy exists to prevent, and a handler cannot always tell the difference.
        return classifyTransient(e, redelivered);
    }
    catch (const std::exception &e)
    {
        // Anything else the handler throws is permanent by default. The conservative direction:
        // a handler that wants a retry must ask for one explicitly.
        return {Disposition::Reject, e.what()};
    }
}

Consumer::Consumer(transport::ITransport &transport, MessageHandler handler)
    : Consumer(transport, Handlers{std::move(handler), {}})
{}

Consumer::Consumer(transport::ITransport &transport, Handlers handlers)
    : _transport(transport)
    , _handlers(std::move(handlers))
{
    if (!_handlers.onMessage && !_handlers.onDeferred)
        throw std::invalid_argument("Consumer requires at least one callable handler");

    const transport::Capabilities caps = transport.capabilities();

    // Without manual acknowledgement the middleware acks on delivery, before the handler has
    // run. The reject-without-requeue policy would then be unenforceable and an unprocessed
    // message would be gone -- so this is refused at startup rather than degraded silently.
    if (!caps.manualAcknowledgement)
        throw std::runtime_error(
            std::string("transport backend '") + caps.name +
            "' does not support manual acknowledgement, so a message would be acknowledged "
            "before it is processed. Poison-message rejection and redelivery after a worker "
            "crash both depend on it.");

    // Same shape of refusal, for the same reason, one level more specific: a deferred handler
    // that ever returns Disposition::Deferred against a backend that cannot honour it would
    // either crash inside settle() or -- if the backend degraded silently -- behave exactly
    // like an immediate ack, silently defeating ack-on-completion. Refused here, before any
    // message ever reaches the handler.
    if (_handlers.onDeferred && !caps.deferredAcknowledgement)
        throw std::runtime_error(
            std::string("transport backend '") + caps.name +
            "' does not support deferred acknowledgement (Capabilities::deferredAcknowledgement "
            "is false), but a deferred handler was configured.");
}

void Consumer::start(const std::string &topic,
                     std::chrono::milliseconds timeout,
                     const transport::TopicOptions &options,
                     bool allowAsymmetric)
{
    // Producer and consumer both declare, on purpose, so either can start first -- and AMQP
    // rejects a redeclaration whose properties differ. checkTopicOptions enforces that
    // symmetry from ONE place shared with Producer::declareTopic, because the same rule
    // written twice is the same rule drifting.
    transport::checkTopicOptions(_transport.capabilities(), options, allowAsymmetric);

    _transport.ensureTopic(topic, options, timeout);

    _transport.subscribe(
        topic,
        [this, topic, deadLetter = options.deadLetter](const transport::Delivery &delivery)
            -> Disposition
        {
            MOTUS_LOG_DEBUG("received tag=" << delivery.tag << " (" << delivery.body.size()
                              << " byte(s))"
                              << (delivery.redelivered ? " [redelivered]" : ""));

            DispatchResult result =
                dispatch(delivery.body, _handlers, delivery.redelivered, delivery.tag);

            // A backend that cannot requeue must not silently swallow the distinction. Degrade
            // to a rejection, but say so loudly and name the backend: the message is about to
            // be destroyed for a reason that was explicitly reported as temporary.
            if (result.disposition == Disposition::Requeue &&
                !_transport.capabilities().requeue)
            {
                MOTUS_LOG_ERROR("transport backend '" << _transport.capabilities().name
                                  << "' cannot requeue; a transient failure is being rejected: "
                                  << result.error);
                result.disposition = Disposition::Reject;
            }

            switch (result.disposition)
            {
                case Disposition::Ack:
                    ++_acked;
                    MOTUS_LOG_DEBUG("acking tag=" << delivery.tag << " (" << _acked
                                      << " total)");
                    break;

                case Disposition::Requeue:
                    ++_requeued;
                    // Warn: nothing is lost, but a worker requeueing steadily is a symptom.
                    MOTUS_LOG_WARN("requeueing tag=" << delivery.tag
                                     << " after a transient failure: " << result.error
                                     << " (" << _requeued << " total)");
                    break;

                case Disposition::Reject:
                    ++_rejected;
                    if (deadLetter && _transport.capabilities().deadLetter)
                        MOTUS_LOG_WARN("dead-lettering tag=" << delivery.tag << " from '"
                                         << topic << "' into '"
                                         << transport::deadLetterTopic(topic) << "': "
                                         << result.error << " (" << _rejected << " total)");
                    else
                        MOTUS_LOG_WARN("rejecting tag=" << delivery.tag
                                         << " without requeue; no forensic destination was "
                                            "requested: " << result.error << " (" << _rejected
                                         << " total)");
                    break;

                case Disposition::Deferred:
                    ++_deferred;
                    // Deliberately NOT acted on here -- that is the entire point.
                    // Responsibility for tag=delivery.tag now belongs to whatever the deferred
                    // handler handed it to; this Consumer's own SettlementQueue (if attached)
                    // is drained in poll(), on this same thread, whenever that responsibility
                    // comes back.
                    MOTUS_LOG_DEBUG("deferring tag=" << delivery.tag << " from '" << topic
                                      << "' (" << _deferred << " total)");
                    break;
            }

            return result.disposition;
        },
        timeout);
}

bool Consumer::poll(std::chrono::milliseconds slice)
{
    if (_settlements)
    {
        // Drained BEFORE pump(): a completion that arrived while this thread was elsewhere
        // should reach the broker on the very next opportunity, not wait behind a full slice
        // of pump() first. This is the one place a Consumer calls ITransport::settle() -- on
        // behalf of work that finished on a different thread entirely.
        SettlementHandle handle{};
        Disposition disposition{};
        while (_settlements->tryPop(handle, disposition))
        {
            _transport.settle(handle, disposition);

            switch (disposition)
            {
                case Disposition::Ack:      ++_acked;    break;
                case Disposition::Requeue:  ++_requeued; break;
                case Disposition::Reject:   ++_rejected; break;
                case Disposition::Deferred: break;   // never pushed; SettlementQueue is a
                                                      // hand-off for the FINAL disposition only
            }
        }
    }

    return _transport.pump(slice);
}

} // namespace motus
