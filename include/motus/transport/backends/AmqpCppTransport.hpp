#pragma once

// The AMQP-CPP backend: motus's v1 transport, now behind the seam.
//
// This wraps motus::AmqpConnection rather than replacing it. That class is the hardest-won code
// in the project -- a hand-written AMQP transport that works on Windows, where AMQP-CPP's own
// Boost.Asio integration cannot even compile -- and every AMQP conformance run still exercises
// it. What changes is its status: it stops being the system's messaging layer and becomes one
// backend's implementation detail.

#include "motus/AmqpConnection.hpp"
#include "motus/transport/ITransport.hpp"

#include <amqpcpp.h>

#include <exception>
#include <map>
#include <memory>
#include <string>
#include <unordered_set>

namespace motus {
namespace transport {

/**
 * ITransport over AMQP-CPP.
 *
 * Owns an AmqpConnection and one AMQP::Channel. Single-threaded, like everything else here.
 *
 * Capability notes, all of them deliberate:
 *   - durableDelivery is TRUE: send() publishes with delivery mode 2. It was false in practice
 *     for an entire phase of the predecessor system without anybody knowing, which is why the
 *     capability exists.
 *   - publisherConfirms is FALSE: confirmSelect is not implemented, so send() returning true
 *     means the frame reached the socket and nothing more. Declared rather than glossed, so
 *     SendOptions::confirm is refused at startup instead of silently ignored.
 *   - deadLetter is TRUE: every source has a broker-native DLX and per-topic forensic queue.
 *     Existing pre-1.4.1 queues still require deletion/recreation because AMQP queue arguments
 *     are immutable -- a migration, not a run-time switch.
 *   - boundedQueue is TRUE: queue declarations carry x-max-length and all x-overflow policies,
 *     including reject-publish for the automatically provisioned forensic queues.
 */
class AmqpCppTransport final : public ITransport
{
public:
    AmqpCppTransport() = default;
    ~AmqpCppTransport() override;

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

    void settle(std::uint64_t tag, Disposition disposition) override;

    void setReconnectPolicy(const ReconnectPolicy &policy) override;

    bool pump(std::chrono::milliseconds slice) override;

    Stats stats() const override;

    /**
     * The underlying connection.
     *
     * AMQP-ONLY ESCAPE HATCH. Exists so the twelve AMQP-specific integration scenarios -- the
     * ones asserting delivery_mode, prefetch_count and broker-side connection counts through
     * RabbitMQ's management API -- can keep reaching the real connection. Those assertions have
     * no meaning for a non-AMQP backend and are not part of the portable contract.
     *
     * Nothing in motus_core outside this class may use it. Reaching for it from shared code
     * would put AMQP semantics back above the seam and undo the entire point.
     *
     * Throws std::logic_error before connect().
     */
    AmqpConnection &connection();

private:
    /// Where a declared topic publishes to. Empty group means AMQP's default exchange.
    struct RouteBinding
    {
        std::string group;   ///< the exchange
        std::string key;     ///< the routing key the queue was bound with
    };

    /**
     * Routing recorded at declaration, so send() need not be told again.
     *
     * A publisher states its routing once, in ensureTopic, which is where the exchange and
     * binding are created and therefore the only place the three can be kept consistent.
     * A topic absent from this map falls back to the default exchange -- the behaviour that
     * predates groups, and a safer fallback than guessing.
     */
    std::map<Topic, RouteBinding> _routes;

    /// Declare a queue and report its depth. Shared by ensureTopic and topicDepth, which differ
    /// only in what they do with the answer.
    std::uint32_t declare(const Topic &topic,
                          const TopicOptions &options,
                          std::chrono::milliseconds timeout);

    /// Open the socket and channel against `_endpoint`. Shared by connect() and reconnection.
    void openConnection(std::chrono::milliseconds timeout);

    /// Register the stored handler on the stored topic. Shared by subscribe() and reconnection.
    void startConsuming(std::chrono::milliseconds timeout);

    /**
     * One reconnection attempt, if the policy allows and enough backoff has elapsed.
     *
     * Deliberately at most ONE attempt per pump() call, with the backoff expressed as "not
     * before this instant" rather than a sleep. Sleeping inside pump() would make a call the
     * caller asked to bound to 250ms take twenty seconds, which breaks the one invariant this
     * transport has always held: every wait is bounded.
     *
     * Returns false only once attempts are exhausted -- the permanent-failure signal pump()
     * turns into a false return.
     */
    bool serviceReconnect();

    // Declaration order is load-bearing: _channel must be destroyed before _connection, since
    // AMQP::Channel writes a Channel.Close frame through the connection as it dies.
    std::unique_ptr<AmqpConnection> _connection;
    std::unique_ptr<AMQP::Channel>  _channel;

    DeliveryHandler _handler;
    Stats           _stats;

    /// Delivery tags currently held via Disposition::Deferred: seen and neither acked, rejected
    /// nor requeued yet. Guards settle() against a tag that was never deferred, already settled,
    /// or belongs to a connection that no longer exists (cleared on close()/reconnection, since
    /// old tags are meaningless on a new channel -- the same rule serviceReconnect() already
    /// applies to _subscribed/_topicDeclared).
    std::unordered_set<std::uint64_t> _outstandingDeferred;

    // Everything needed to rebuild this transport's state on a new connection. Stored as it is
    // established, so reconnection restores exactly what was asked for rather than a guess.
    Endpoint        _endpoint;
    ReconnectPolicy _policy;
    Topic           _topic;
    TopicOptions    _topicOptions;
    bool            _topicDeclared = false;
    bool            _subscribed    = false;

    unsigned                              _failedAttempts = 0;
    std::chrono::steady_clock::time_point _nextAttempt{};

    /// Set when the handler throws. AMQP-CPP invokes the handler from inside its frame parser,
    /// and an exception escaping through there takes the same route that already terminated
    /// this process once. Caught, stashed here, and rethrown from pump() once parsing is done.
    std::exception_ptr _handlerFailure;
};

} // namespace transport
} // namespace motus
