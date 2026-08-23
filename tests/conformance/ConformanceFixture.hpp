#pragma once

// The portable transport contract.
//
// ---------------------------------------------------------------------------
// What this directory is
// ---------------------------------------------------------------------------
// Everything here runs against EVERY transport backend this build contains. It is the
// specification a new backend is measured against: implement ITransport, declare capabilities
// honestly, run this suite, and either pass or state in the capability struct why a scenario
// does not apply.
//
// That is the difference between "two libraries that both happen to work" and an abstraction
// with a contract. It is also what makes the work reviewable -- a directory someone can point
// at and say "this is what any motus transport must do".
//
// Scenarios that assert through RabbitMQ's management API -- delivery_mode, prefetch_count,
// broker-side connection counts, forced-close injection, trace hex dumps, heartbeat idle --
// have no meaning for a non-AMQP backend and stay in tests/integration/.
//
// ---------------------------------------------------------------------------
// Two runs, two ctest entries
// ---------------------------------------------------------------------------
//   conformance.inmemory   needs nothing. Labelled unit, so the contract is checkable on any
//                          machine, in CI, and anywhere the broker is down.
//   conformance.amqpcpp    needs a live broker. Labelled integration.
//
// The in-memory run is the valuable half of that split: until now, "does a message survive a
// round trip, get rejected without requeue, and recover" was a question only RabbitMQ could
// answer.

#include "motus/ByteMessage.hpp"
#include "motus/Consumer.hpp"
#include "motus/Producer.hpp"
#include "motus/transport/Factory.hpp"

#include <gtest/gtest.h>

#include "support/TestTopic.hpp"

#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>

namespace motus_test {

using namespace std::chrono_literals;

/// True when `name` is set to anything other than "0".
inline bool envDemands(const char *name)
{
    const char *value = std::getenv(name);
    return value != nullptr && std::string(value) != "0";
}

/// Backends that need something outside this process to be running.
///
/// The distinction drives the skip policy: a scenario cannot meaningfully skip on a backend
/// that has no external dependency, so conformance.inmemory must be incapable of a quiet pass.
inline bool needsExternalMiddleware(motus::transport::Backend backend)
{
    return backend != motus::transport::Backend::InMemory;
}

/// Marker unique to this process AND this call.
///
/// Every backend here keeps state between scenarios -- the AMQP queue is shared with live
/// demos, and the in-memory registry is deliberately never reset so that both run under
/// identical conditions. Scenarios therefore match on their own marker and ignore everything
/// else, rather than assuming an empty destination.
inline std::string uniqueMarker(const std::string &prefix = "conf")
{
    static unsigned long long sequence = 0;
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    return prefix + "-" + std::to_string(now) + "-" + std::to_string(++sequence);
}

/// First line of a payload -- the discriminator roundTrip() and the scenarios match on.
inline std::string markerOf(const motus::ByteMessage &message)
{
    return message.payload.substr(0, message.payload.find('\n'));
}

/// A message whose payload leads with `marker` on its own line, followed by `content`.
inline motus::ByteMessage markedMessage(const std::string &marker,
                                          const std::string &content = {})
{
    motus::ByteMessage message;
    message.payload = marker + "\n" + content;
    return message;
}

/// Everything after the payload's first line -- the `content` half of markedMessage().
inline std::string contentOf(const motus::ByteMessage &message)
{
    const std::size_t newline = message.payload.find('\n');
    return newline == std::string::npos ? std::string() : message.payload.substr(newline + 1);
}

/**
 * Base fixture, parameterized over every backend in this build.
 *
 * Adding a backend to CMake automatically runs the entire contract against it. That is the
 * property that makes this a bar rather than a description: a new transport cannot be declared
 * finished without facing the same scenarios as the one already shipping.
 */
class ConformanceTest : public ::testing::TestWithParam<motus::transport::Backend>
{
protected:
    void SetUp() override
    {
        try
        {
            _transport = connect();
        }
        catch (const std::exception &e)
        {
            const std::string reason =
                std::string("backend '") + backendName() + "' could not connect -- " + e.what();

            // A backend with no external dependency has no excuse. If InMemory cannot connect,
            // something is genuinely broken and skipping would hide it behind a green bar --
            // and ctest renders every skip as "Passed".
            if (!needsExternalMiddleware(GetParam()))
                FAIL() << reason
                       << "\n\nThis backend needs no external middleware, so a connect failure "
                          "is a defect, not an environment problem.";

            if (envDemands("MOTUS_REQUIRE_BROKER"))
                FAIL() << reason
                       << "\n\nMOTUS_REQUIRE_BROKER is set, so this is a failure rather than a "
                          "skip. Start the broker, or -- if it IS running -- check that nothing "
                          "(a policy, a sandbox, a container boundary) is denying this binary "
                          "socket access.";

            GTEST_SKIP() << reason;
        }
    }

    void TearDown() override
    {
        if (_transport) _transport->close(2s);
    }

    /// The fixture's own connected transport.
    motus::transport::ITransport &transport() { return *_transport; }

    motus::transport::Capabilities caps() const { return _transport->capabilities(); }

    const char *backendName() const { return motus::transport::backendName(GetParam()); }

    /**
     * Another connected transport of the same backend.
     *
     * Needed by every scenario that models more than one process: a separate worker, competing
     * consumers, a publisher that goes away.
     *
     * Deliberately has no skip path. SetUp() has already established that this backend is
     * reachable, so a second connection failing is a defect rather than an environment problem
     * -- and letting it throw makes the test fail with the reason attached, which is what you
     * want. Skipping here would hide a backend that cannot support two connections at once.
     */
    std::unique_ptr<motus::transport::ITransport> newTransport() { return connect(); }

    /**
     * Send options this backend can actually honour.
     *
     * Derived from capabilities rather than fixed, so each backend runs the configuration it
     * would ship with: AMQP-CPP publishes persistently, which is its production path and where
     * this project's data-loss defect lived, while the in-memory backend publishes the only way
     * it can.
     *
     * The consequence is worth stating plainly rather than leaving implied: a green run means
     * "each backend passed in its own configuration", NOT "both passed byte-identical tests".
     * Pinning both to non-durable would make them identical at the cost of never exercising
     * AMQP's shipping path from inside the contract.
     */
    motus::transport::SendOptions producerOptions() const
    {
        motus::transport::SendOptions options;
        options.durable = caps().durableDelivery;
        options.confirm = false;   // no backend declares publisherConfirms yet
        return options;
    }

    /**
     * Publish `message` and consume it back on a SEPARATE transport, matching on the marker at
     * the head of its payload (markerOf). Returns null if it never arrived within `budget`.
     *
     * The separate transport is not incidental: a real worker is a different process, and
     * reusing one connection could mask a bug that only appears across them.
     *
     * Matching is on the payload's FIRST LINE only, deliberately, so a caller can distinguish
     * "arrived but corrupted" (marker matched, rest of the payload did not) from "never came"
     * (null). With large payloads those have completely different causes, and the distinction
     * is the first thing worth knowing when one fails.
     */
    std::unique_ptr<motus::ByteMessage> roundTrip(const motus::ByteMessage &message,
                                                    std::chrono::seconds budget = 20s)
    {
        {
            motus::Producer producer(transport(), producerOptions());
            producer.declareTopic(motus_test::testTopic());
            if (!producer.publish(motus_test::testTopic(), message)) return nullptr;
            producer.flush(1s);
        }

        auto worker = newTransport();
        auto received = std::make_shared<std::unique_ptr<motus::ByteMessage>>();
        const std::string marker = markerOf(message);

        motus::Consumer consumer(*worker, [received, marker](const motus::ByteMessage &task)
        {
            if (markerOf(task) == marker)
                *received = std::make_unique<motus::ByteMessage>(task);
        });
        consumer.start(motus_test::testTopic());

        const auto deadline = std::chrono::steady_clock::now() + budget;
        while (!*received && std::chrono::steady_clock::now() < deadline)
            consumer.poll(250ms);

        worker->close(2s);
        return std::move(*received);
    }

private:
    /// Construct and connect. Throws on failure; SetUp() decides whether that is a skip.
    std::unique_ptr<motus::transport::ITransport> connect()
    {
        auto transport = motus::transport::makeTransport(GetParam());

        motus::transport::Endpoint endpoint;
        endpoint.host = "127.0.0.1";   // by IP: skips the resolver, which locked-down networks break

        transport->connect(endpoint, 5s);
        return transport;
    }

    std::unique_ptr<motus::transport::ITransport> _transport;
};

/// Names the parameterized instance after the backend, so ctest and gtest output read
/// `Backends/ConformanceTest.SomeScenario/amqpcpp` rather than `/0`.
struct BackendName
{
    template <typename ParamType>
    std::string operator()(const ::testing::TestParamInfo<ParamType> &info) const
    {
        return motus::transport::backendName(info.param);
    }
};

} // namespace motus_test

/**
 * Skip a scenario the backend cannot support -- unless MOTUS_REQUIRE_CAPS demands otherwise.
 *
 * Follows the MOTUS_REQUIRE_BROKER precedent, adopted after being burned twice: ctest renders
 * every skip as `Passed`, so a capability skip is information, not an excuse. Setting the
 * variable turns "this backend cannot do it" into a failure, which is what you want when the
 * point of the run is to prove a backend is complete.
 *
 * The message names both the backend and the capability, because "skipped" without either is
 * indistinguishable from a suite that quietly stopped testing anything.
 */
#define MOTUS_REQUIRE_CAPABILITY_OR_SKIP(capabilityField)                                       \
    do {                                                                                       \
        if (!caps().capabilityField)                                                           \
        {                                                                                      \
            const std::string reason =                                                         \
                std::string("backend '") + backendName() + "' declares " #capabilityField      \
                " = false, so this scenario does not apply to it";                             \
                                                                                               \
            if (::motus_test::envDemands("MOTUS_REQUIRE_CAPS"))                                  \
                FAIL() << reason                                                                \
                       << "\n\nMOTUS_REQUIRE_CAPS is set, so a missing capability is a failure " \
                          "rather than a skip.";                                                \
            GTEST_SKIP() << reason;                                                            \
        }                                                                                      \
    } while (false)
