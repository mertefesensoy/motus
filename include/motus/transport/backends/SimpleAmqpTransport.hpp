#pragma once

// The SimpleAmqpClient backend -- the second real transport, and the one that actually tests
// whether ITransport is an abstraction or a description of AMQP-CPP.
//
// ---------------------------------------------------------------------------
// Why this backend matters more than "a second option"
// ---------------------------------------------------------------------------
// Everything before it was validated against AMQP-CPP and against an in-memory backend written
// by the same hand to fit the same interface. SimpleAmqpClient was written by someone else, on
// the opposite model: AMQP-CPP is asynchronous with deferred callbacks driven by our own pump,
// while this is blocking and owns its own I/O. If the seam can absorb that difference it can
// absorb most things; if it could not, the abstraction was a guess.
//
// It absorbs it. `bool BasicConsumeMessage(tag, envelope, timeout_ms)` is a genuine bounded
// receive -- true with a message, false on timeout -- which is ITransport::pump() verbatim with
// no adaptation layer. That was verified against a live broker before this file was written.
//
// ---------------------------------------------------------------------------
// Two things this library does that the contract has to work around
// ---------------------------------------------------------------------------
// 1. Its default arguments are actively dangerous. BasicConsume defaults to no_ack = true
//    (auto-acknowledge, which would destroy the rejection policy AND the redelivery guarantee)
//    and exclusive = true (which makes competing consumers fail outright). DeclareQueue
//    defaults to durable=false, exclusive=true, auto_delete=true -- the exact opposite of
//    motus's destination on all three, which would be a 406 PRECONDITION_FAILED. Every call
//    below passes its arguments explicitly. None of them relies on a default.
//
// 2. Channel::Create has NO connect timeout, and neither does OpenOpts. ITransport::connect
//    promises a bounded wait (NFR-1), so connect() pre-flights a plain TCP connection with our
//    own deadline and only then hands the endpoint to the library. See the note on connect().
//
// Verified against SimpleAmqpClient pinned at cf44cc8. The newest RELEASE, v2.5.1, cannot be
// built on MSYS2/UCRT64 at all: static is a hard FATAL_ERROR ("cannot be built as a static
// library on Win32") and shared links with every symbol undefined. The Windows fixes exist
// only on master.

#include "motus/transport/ITransport.hpp"

#include <SimpleAmqpClient/SimpleAmqpClient.h>

#include <chrono>
#include <cstdint>
#include <exception>
#include <string>

namespace motus {
namespace transport {

/**
 * ITransport over SimpleAmqpClient (which sits on rabbitmq-c).
 *
 * Single-threaded, like every transport here: one instance driven by one thread.
 *
 * Capability notes, each deliberate:
 *   - publisherConfirms is FALSE and cannot be otherwise. SimpleAmqpClient's Channel exposes no
 *     confirm API at all -- verified, not assumed. Declaring it false means SendOptions::confirm
 *     is refused at startup rather than silently ignored.
 *   - deadLetter is TRUE, matching the AMQP-CPP backend's native DLX and per-topic forensic
 *     queue. A pre-1.4.1 source still needs the same queue-property migration.
 *   - boundedQueue is TRUE: queue field tables carry x-max-length and all x-overflow policies,
 *     including the reject-publish policy used by forensic destinations.
 *   - reconnect is TRUE. Restoring state here is the same three steps as the other backend --
 *     reopen, re-declare with the STORED options, re-consume with the stored handler.
 */
class SimpleAmqpTransport final : public ITransport
{
public:
    SimpleAmqpTransport() = default;
    ~SimpleAmqpTransport() override;

    Capabilities capabilities() const override;

    void connect(const Endpoint &endpoint, std::chrono::milliseconds timeout) override;
    void close(std::chrono::milliseconds timeout) override;
    bool ready() const override;

    void ensureTopic(const Topic &topic,
                     const TopicOptions &options,
                     std::chrono::milliseconds timeout) override;

    std::uint32_t topicDepth(const Topic &topic, std::chrono::milliseconds timeout) override;
    std::uint32_t purgeTopic(const Topic &topic, std::chrono::milliseconds timeout) override;

    bool send(const Topic &topic,
              const std::string &payload,
              const SendOptions &options) override;

    void subscribe(const Topic &topic,
                   DeliveryHandler handler,
                   std::chrono::milliseconds timeout) override;

    /// Always throws: Capabilities::deferredAcknowledgement is false on this backend (see
    /// capabilities()). Consumer refuses at construction before a handler could ever return
    /// Disposition::Deferred against this transport, so reaching this method at all means that
    /// guard was bypassed -- itself worth failing loudly for.
    void settle(std::uint64_t tag, Disposition disposition) override;

    void setReconnectPolicy(const ReconnectPolicy &policy) override;

    bool pump(std::chrono::milliseconds slice) override;

    Stats stats() const override;

private:
    /// Open the channel against `_endpoint`, pre-flighting reachability with a bounded TCP
    /// connect first. Shared by connect() and reconnection.
    void openChannel(std::chrono::milliseconds timeout);

    /// Register the stored handler on the stored topic. Shared by subscribe() and reconnection.
    void startConsuming();

    /// One reconnection attempt, if the policy allows and enough backoff has elapsed.
    /// At most one attempt per pump() call, with the backoff expressed as "not before this
    /// instant" rather than a sleep -- sleeping inside pump() would make a call the caller
    /// bounded to 250ms take twenty seconds.
    bool serviceReconnect();

    AmqpClient::Channel::ptr_t _channel;
    std::string                _consumerTag;

    // Everything needed to rebuild this transport's state on a new connection, stored as it is
    // established so reconnection restores what was asked for rather than a guess.
    Endpoint        _endpoint;
    ReconnectPolicy _policy;
    Topic           _topic;
    TopicOptions    _topicOptions;
    bool            _topicDeclared = false;
    bool            _subscribed    = false;
    bool            _connected     = false;

    unsigned                              _failedAttempts = 0;
    std::chrono::steady_clock::time_point _nextAttempt{};

    DeliveryHandler _handler;
    Stats           _stats;
};

} // namespace transport
} // namespace motus
