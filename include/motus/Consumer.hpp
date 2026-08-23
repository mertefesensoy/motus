#pragma once

#include "motus/ByteMessage.hpp"
#include "motus/transport/ITransport.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>

namespace motus {

/**
 * What to do with a received message. Supplied by the caller rather than hard-coded, so the
 * consumer's control flow can be tested without any middleware.
 *
 * The handler receives the payload **contents**, already carried in the message -- never a
 * reference to something it must go and fetch. Carrying data rather than references is what
 * keeps a worker free of hidden shared-filesystem or shared-service assumptions.
 *
 * Contract: throwing signals that the message could not be processed. The exception's what()
 * is reported and the message is rejected.
 */
using MessageHandler = std::function<void(const ByteMessage &)>;

/// Same underlying value as transport::Delivery::tag. Named separately here because at this
/// layer it means something more specific: the one thing a DeferredMessageHandler must hold on
/// to in order to resolve the delivery later, via ITransport::settle().
using SettlementHandle = std::uint64_t;

/**
 * Full manual control over a delivery's outcome, including Disposition::Deferred.
 *
 * Supersedes MessageHandler when set: dispatch() records EXACTLY the Disposition this returns,
 * rather than inferring Ack-on-return / Reject-on-throw the way MessageHandler's void return
 * does. A handler that returns Disposition::Deferred has taken responsibility for eventually
 * resolving `handle` -- typically by handing it to something the owning consumer thread drains
 * and settles later (see SettlementQueue below), since only that thread may touch the
 * transport.
 *
 * May still throw TransientFailure or any std::exception exactly like MessageHandler does; that
 * is unrelated to Deferred and means "I could not even ACCEPT this message", decided before any
 * responsibility ever transferred.
 */
using DeferredMessageHandler = std::function<Disposition(const ByteMessage &, SettlementHandle)>;

/** The handlers a Consumer can drive. Either may be omitted, but not both. */
struct Handlers
{
    MessageHandler onMessage;

    /// Takes priority over onMessage when set. The two are not meant to be used together on one
    /// Consumer; onDeferred existing at all is what lets a single handler decide, per message,
    /// between ack-on-return behaviour and Disposition::Deferred -- the ack-on-COMPLETION
    /// pattern a work-executing consumer needs so that a crash between "accepted the order" and
    /// "the work actually ran" is at-least-once rather than silent loss.
    DeferredMessageHandler onDeferred;
};

/**
 * Thread-safe hand-off for a delivery that was returned as Disposition::Deferred.
 *
 * ANY thread may push() -- this is how a worker thread reports that deferred work finished.
 * Only the ONE thread that owns the matching transport may tryPop(), and only from
 * Consumer::poll(), which is what actually calls ITransport::settle() with what comes out.
 * Workers never touch the transport; the work and the settlement necessarily happen on
 * different threads, and this queue is the boundary between them.
 *
 * One instance per topic that might defer. A topic that never uses onDeferred never needs one
 * attached, and Consumer::poll() is a no-op with respect to settlement when none is.
 */
class SettlementQueue
{
public:
    void push(SettlementHandle handle, Disposition disposition)
    {
        const std::lock_guard<std::mutex> lock(_mutex);
        _pending.emplace_back(handle, disposition);
    }

    /// Returns false once nothing is pending. Call in a loop to drain everything at once.
    bool tryPop(SettlementHandle &handle, Disposition &disposition)
    {
        const std::lock_guard<std::mutex> lock(_mutex);
        if (_pending.empty()) return false;
        std::tie(handle, disposition) = _pending.front();
        _pending.pop_front();
        return true;
    }

private:
    std::mutex _mutex;
    std::deque<std::pair<SettlementHandle, Disposition>> _pending;
};

/**
 * Thrown by a handler to say "I could not process this **now**".
 *
 * The distinction this type carries is one only the handler can make, so it is the handler
 * that makes it. Everything else a handler throws is treated as permanent and the message is
 * discarded.
 *
 * Throw this for a full disk, a locked output directory, an unavailable downstream service --
 * anything that would plausibly succeed on a later attempt. Do NOT throw it for a payload that
 * is simply wrong: that will never improve, and requeueing it forever is the poison-message
 * spin this library's rejection policy exists to prevent.
 *
 * Retry is bounded to one attempt -- see dispatch().
 */
class TransientFailure : public std::runtime_error
{
public:
    using std::runtime_error::runtime_error;
};

// Disposition lives in motus/transport/ITransport.hpp -- it is the vocabulary of the seam,
// decided here and carried out by the transport.

struct DispatchResult
{
    Disposition disposition;
    std::string error;   ///< empty when disposition == Ack
};

/**
 * Decide what to do with one raw message body. Pure with respect to the network: it performs no
 * middleware calls and owns no transport, which is what makes the poison-message policy
 * testable in isolation.
 *
 * ---------------------------------------------------------------------------
 * The policy, in full
 * ---------------------------------------------------------------------------
 *   oversized body, malformed envelope, unrecognised format_version -> Reject
 *   handler throws TransientFailure, first delivery                 -> Requeue
 *   handler throws TransientFailure, already redelivered            -> Reject
 *   handler throws anything else                                    -> Reject
 *   handler returns                                                 -> Ack
 *   handlers.onDeferred is set                                      -> exactly what IT returns,
 *                                                                      including Deferred
 *
 * Rejection always means *without requeue*. Every payload-level failure is deterministic, so a
 * retry fails identically and would spin the worker forever on one bad message.
 *
 * `redelivered` is what bounds the retry. A transient failure gets exactly one more attempt:
 * if the message comes back already flagged as redelivered and fails again, it is rejected.
 * That needs no retry counter, no timer and no state anywhere -- the middleware already tracks
 * the one bit required, and this is what makes surfacing that flag load-bearing rather than
 * merely informative.
 *
 * Defaulted to false so a caller that cannot supply it (and a unit suite that tests policy
 * rather than delivery) behaves conservatively: transient failures are then treated as
 * permanent.
 *
 * `deliveryTag` is meaningless unless handlers.onDeferred is set -- it exists only to be
 * threaded through to that handler, which needs the raw transport tag to eventually call
 * ITransport::settle(). Defaulted to 0 for every other caller, none of which look at it.
 *
 * This function deliberately lives ABOVE the transport seam and is shared by every backend. If
 * the poison-message policy lived inside backends, each new library would re-implement it and
 * one of them would get it wrong -- silently, until a malformed message arrived.
 */
DispatchResult dispatch(const std::string &body,
                        const MessageHandler &handler,
                        bool redelivered = false);

/** Decode the envelope exactly once and route to whichever handler is configured. */
DispatchResult dispatch(const std::string &body,
                        const Handlers &handlers,
                        bool redelivered = false,
                        SettlementHandle deliveryTag = 0);

/**
 * Consumes messages from a topic and feeds each to its configured handler.
 *
 * Borrows a transport that must outlive it. Not thread-safe; drive it from one thread via
 * poll().
 */
class Consumer
{
public:
    /// `transport` must already be connected and must outlive this object.
    Consumer(transport::ITransport &transport, MessageHandler handler);
    Consumer(transport::ITransport &transport, Handlers handlers);

    /**
     * Declare the topic and begin consuming. Blocks until the middleware confirms the
     * subscription, or `timeout` elapses.
     *
     * Declared with the same options as Producer::declareTopic, by design, so either can start
     * first. AMQP rejects a redeclaration whose properties differ with a 406
     * PRECONDITION_FAILED that closes the channel -- the two must stay in step, which is why
     * both state the options explicitly rather than relying on a backend's defaults.
     *
     * `options` must carry the SAME group and route the producer declared, or this consumer
     * binds somewhere the messages never arrive.
     *
     * Throws std::runtime_error if the topic cannot be declared or consumption cannot start.
     */
    void start(const std::string &topic,
               std::chrono::milliseconds timeout = std::chrono::seconds(5),
               const transport::TopicOptions &options = {},
               bool allowAsymmetric = false);

    /// Service the transport for up to `slice`, dispatching whatever arrives.
    /// Returns false once the middleware connection is gone.
    ///
    /// If a SettlementQueue is attached, this ALSO drains it first, calling
    /// transport::ITransport::settle() for everything pending -- the one place this Consumer
    /// touches the transport on behalf of work that finished on a different thread entirely
    /// (only the thread that owns a transport may call into it).
    bool poll(std::chrono::milliseconds slice = std::chrono::seconds(30));

    /// Drain and settle every delivery `queue` accumulates, once per poll() call, for as long
    /// as this Consumer lives. Call before start(); at most one queue per Consumer, matching
    /// the one-transport-per-thread model everywhere else in this library. Optional -- a
    /// Consumer with no onDeferred handler never needs one.
    void attachSettlementQueue(SettlementQueue &queue) { _settlements = &queue; }

    /// Messages successfully processed and acked since construction.
    ///
    /// Counts what this Consumer DECIDED. The transport separately reports what it actually
    /// PERFORMED (Stats::acksPerformed). The duplication is deliberate: a conformance scenario
    /// asserts the two agree, which detects a backend that accepts a Reject and then silently
    /// fails to send it -- silent message loss that neither counter alone could reveal.
    std::size_t acked() const { return _acked; }
    /// Messages rejected since construction. See acked() for why this is duplicated.
    std::size_t rejected() const { return _rejected; }
    /// Messages returned for another attempt after a transient handler failure.
    std::size_t requeued() const { return _requeued; }
    /// Deliveries returned Disposition::Deferred since construction. Like the other three, a
    /// running total, not a live "currently outstanding" count -- ask the attached
    /// SettlementQueue's owner for that if it matters.
    std::size_t deferred() const { return _deferred; }

private:
    transport::ITransport &_transport;
    Handlers               _handlers;
    std::size_t            _acked    = 0;
    std::size_t            _rejected = 0;
    std::size_t            _requeued = 0;
    std::size_t            _deferred = 0;
    SettlementQueue       *_settlements = nullptr;
};

} // namespace motus
