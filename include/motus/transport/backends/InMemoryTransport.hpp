#pragma once

// A transport with no middleware behind it at all.
//
// ---------------------------------------------------------------------------
// Why this exists, and why it is not a toy
// ---------------------------------------------------------------------------
// An abstraction with one implementation is a guess. The cheapest way to find out whether
// ITransport is genuinely middleware-neutral or merely AMQP wearing a different vocabulary is
// to implement something that is not AMQP at all -- if a concept cannot be honoured here, it
// leaked in from the library it was supposed to hide.
//
// It is held to the SAME contract as the real backends: it must pass every portable conformance
// scenario, not a reduced set. A backend that declares few capabilities passes trivially and
// proves nothing, which would make the whole conformance bar decorative.
//
// ---------------------------------------------------------------------------
// TEST BUILDS ONLY
// ---------------------------------------------------------------------------
// MOTUS_WITH_INMEMORY follows MOTUS_BUILD_TESTS, so a release binary cannot select this
// transport. That is deliberate: --transport inmemory would run, connect, publish, report
// success, and lose every message the moment the process exits. A transport that silently
// discards everything must not be reachable by a typo on a production command line.
//
// Its declared capabilities say so honestly -- durableDelivery is false -- which means a
// Producer constructed with default options REFUSES to use it. Callers that genuinely accept
// loss must say so with SendOptions{false}, out loud, at the call site.

#include "motus/transport/ITransport.hpp"

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace motus {
namespace transport {

/// One message sitting in, or taken from, a topic.
struct Held
{
    std::uint64_t sequence    = 0;
    std::string   body;
    bool          redelivered = false;
};

/**
 * ITransport over a process-wide in-memory broker.
 *
 * Unlike every other object in motus this is internally mutex-protected. The competing-consumer
 * conformance scenarios drive two transports from two threads against one shared topic, and the
 * shared state here is ours rather than a third-party library's -- so there is no foreign
 * threading model to respect, and guarding it is straightforward rather than dangerous.
 *
 * State is process-wide and deliberately NOT reset between tests. The conformance suite must
 * run the identical scenario under identical conditions on every backend, and the AMQP backend
 * shares a live queue with demos; a backend handed a clean slate would be running an easier
 * test and could pass where the real one fails. Scenarios match on their own unique marker.
 */
class InMemoryTransport final : public ITransport
{
public:
    InMemoryTransport() = default;
    ~InMemoryTransport() override;

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

    bool pump(std::chrono::milliseconds slice) override;

    Stats stats() const override;

private:
    /// Return anything taken but not yet resolved to its topic, flagged as redelivered.
    /// Called on close and from the destructor: a subscriber that goes away mid-job must not
    /// take its work with it, which is the guarantee manual acknowledgement exists to provide.
    void returnOutstanding();

    bool        _connected = false;
    bool        _subscribed = false;
    Topic       _topic;
    DeliveryHandler _handler;

    /// Deliveries taken from the topic but not yet resolved -- kept whole, so they can be put
    /// back. Non-empty while a handler is running, after one threw, or while a Deferred delivery
    /// awaits settle().
    ///
    /// Keyed by Held::sequence (== Delivery::tag) rather than held as a plain list: settle() must
    /// look one up by that value, and Disposition::Deferred means more than one entry can now be
    /// outstanding at once in principle (pump()'s take-loop refuses a new one while any remains,
    /// mirroring real AMQP's setQos(1) -- see pump()'s comment -- so in practice this holds at
    /// most one, but the type does not assume that).
    std::map<std::uint64_t, Held> _outstanding;

    Stats _stats;
};

} // namespace transport
} // namespace motus
