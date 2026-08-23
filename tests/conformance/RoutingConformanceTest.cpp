/*
 * Routing groups: the portable contract.
 *
 * Every backend that declares `routingGroups` must behave identically here. Until these
 * existed, routing was proven only by unit tests of a pure matching function -- nothing
 * asserted that a message actually flows through a real group and binding, on any backend.
 *
 * THE IMPORTANT SCENARIO IS THE NEGATIVE ONE. A backend that delivered to every destination
 * regardless of binding would pass any test that only checks arrival, and that is exactly what
 * a fanout exchange created where a topic one was intended looks like. Over-delivery on a
 * message bus carrying operational data is worse than under-delivery: under-delivery is
 * noticed.
 *
 * Driven through ITransport directly rather than Producer/Consumer, because routing IS a
 * transport property and a failure here should point at the backend. One scenario at the end
 * goes through the full stack, so the layers above are not left unexercised.
 *
 * ISOLATION: every scenario derives its group, destinations and routes from uniqueMarker().
 * The in-memory broker is a process-wide singleton with no reset, and the AMQP backend shares
 * a live broker across runs -- so a fixed name would leak state between scenarios and between
 * CI runs, which is the kind of flakiness that gets a suite ignored.
 */
#include "ConformanceFixture.hpp"

#include "motus/ByteMessage.hpp"
#include "motus/Consumer.hpp"
#include "motus/Producer.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <exception>
#include <string>
#include <vector>

namespace {

using namespace std::chrono_literals;
using motus::transport::Delivery;
using motus::transport::Routing;
using motus::transport::SendOptions;
using motus::transport::TopicOptions;
using motus_test::ConformanceTest;
using motus_test::uniqueMarker;

// Its own suite name so a ctest failure says "routing" rather than pointing at the whole
// contract. The per-backend ctest entries filter on the parameter suffix, so a new suite is
// picked up automatically -- nothing to register, and therefore nothing to forget.
using RoutingConformanceTest = ConformanceTest;

// Destinations are named at the call site, never carried on the message type -- which is what
// lets every scenario here use a destination unique to the run. A predecessor design carried
// the destination as a compile-time constant on the wire type, and this file is one of the
// places that showed why that could not stand.

TopicOptions groupedAs(const std::string &group, const std::string &route, Routing routing)
{
    TopicOptions options;                 // durable/shared/deleteWhenUnused stay at defaults,
    options.group   = group;              // which is what keeps producer and consumer symmetric
    options.route   = route;
    options.routing = routing;
    return options;
}

/// Drain whatever is waiting on `topic` into `received`, for up to `budget`.
void collect(motus::transport::ITransport &transport,
             const std::string &topic,
             std::vector<std::string> &received,
             std::chrono::milliseconds budget)
{
    transport.subscribe(topic,
                        [&received](const Delivery &delivery) {
                            received.push_back(delivery.body);
                            return motus::Disposition::Ack;
                        },
                        2s);

    const auto deadline = std::chrono::steady_clock::now() + budget;
    while (std::chrono::steady_clock::now() < deadline)
        transport.pump(100ms);
}

// ------------------------------------------------------------------------------------------
// Delivery
// ------------------------------------------------------------------------------------------

TEST_P(RoutingConformanceTest, AMessageReachesADestinationBoundWithAMatchingRoute)
{
    MOTUS_REQUIRE_CAPABILITY_OR_SKIP(routingGroups);

    const std::string marker = uniqueMarker("route-hit");
    const std::string group  = marker + ".ex";
    const std::string dest   = marker + ".q";
    const std::string route  = "TEST." + marker + ".alpha";

    transport().ensureTopic(dest, groupedAs(group, route, Routing::Pattern), 5s);

    SendOptions send = producerOptions();
    ASSERT_TRUE(transport().send(dest, "payload-" + marker, send));
    transport().pump(300ms);

    auto worker = newTransport();
    worker->ensureTopic(dest, groupedAs(group, route, Routing::Pattern), 5s);

    std::vector<std::string> received;
    collect(*worker, dest, received, 3s);

    ASSERT_EQ(received.size(), 1u) << "a message published into a group must reach the "
                                      "destination bound with its exact route";
    EXPECT_EQ(received.front(), "payload-" + marker);

    worker->close(2s);
}

// ------------------------------------------------------------------------------------------
// Non-delivery -- the scenario that catches an over-delivering backend
// ------------------------------------------------------------------------------------------

TEST_P(RoutingConformanceTest, AMessageDoesNotReachADestinationBoundWithADifferentRoute)
{
    MOTUS_REQUIRE_CAPABILITY_OR_SKIP(routingGroups);

    const std::string marker = uniqueMarker("route-miss");
    const std::string group  = marker + ".ex";
    const std::string wanted = marker + ".wanted";
    const std::string other  = marker + ".other";

    // Two destinations in ONE group, bound with routes that cannot both match.
    transport().ensureTopic(wanted, groupedAs(group, "TEST." + marker + ".alpha",
                                              Routing::Pattern), 5s);
    transport().ensureTopic(other,  groupedAs(group, "TEST." + marker + ".beta",
                                              Routing::Pattern), 5s);

    // Published with the FIRST destination's route.
    ASSERT_TRUE(transport().send(wanted, "only-for-alpha", producerOptions()));
    transport().pump(300ms);

    auto worker = newTransport();
    worker->ensureTopic(other, groupedAs(group, "TEST." + marker + ".beta", Routing::Pattern), 5s);

    std::vector<std::string> received;
    collect(*worker, other, received, 2s);

    EXPECT_TRUE(received.empty())
        << "a destination bound with a non-matching route received " << received.size()
        << " message(s). A backend that delivers regardless of binding passes every test that "
           "only checks arrival -- which is what a fanout created where a topic was intended "
           "looks like.";

    worker->close(2s);
}

// ------------------------------------------------------------------------------------------
// Wildcards
// ------------------------------------------------------------------------------------------

TEST_P(RoutingConformanceTest, AWildcardBindingReceivesAMatchingRoute)
{
    MOTUS_REQUIRE_CAPABILITY_OR_SKIP(routingGroups);

    const std::string marker = uniqueMarker("route-star");
    const std::string group  = marker + ".ex";
    const std::string dest   = marker + ".q";

    // Bound with `*`, which matches exactly one word -- the reason dotted route names exist.
    transport().ensureTopic(dest, groupedAs(group, "TEST." + marker + ".*", Routing::Pattern), 5s);

    SendOptions send = producerOptions();
    send.route = "TEST." + marker + ".Position";   // one word where the wildcard is
    ASSERT_TRUE(transport().send(dest, "wildcard-hit", send));
    transport().pump(300ms);

    auto worker = newTransport();
    worker->ensureTopic(dest, groupedAs(group, "TEST." + marker + ".*", Routing::Pattern), 5s);

    std::vector<std::string> received;
    collect(*worker, dest, received, 3s);

    ASSERT_EQ(received.size(), 1u) << "`*` must match exactly one word";
    EXPECT_EQ(received.front(), "wildcard-hit");

    worker->close(2s);
}

TEST_P(RoutingConformanceTest, AWildcardBindingRejectsARouteWithTooManyWords)
{
    MOTUS_REQUIRE_CAPABILITY_OR_SKIP(routingGroups);

    const std::string marker = uniqueMarker("route-star-miss");
    const std::string group  = marker + ".ex";
    const std::string dest   = marker + ".q";

    transport().ensureTopic(dest, groupedAs(group, "TEST." + marker + ".*", Routing::Pattern), 5s);

    SendOptions send = producerOptions();
    send.route = "TEST." + marker + ".Nav.Position";   // TWO words where `*` allows one
    transport().send(dest, "should-not-arrive", send);
    transport().pump(300ms);

    auto worker = newTransport();
    worker->ensureTopic(dest, groupedAs(group, "TEST." + marker + ".*", Routing::Pattern), 5s);

    std::vector<std::string> received;
    collect(*worker, dest, received, 2s);

    std::string got;
    for (const std::string &body : received) got += "[" + body + "]";
    EXPECT_TRUE(received.empty())
        << "`*` matches exactly one word, so a two-word tail must not match. This is the "
           "distinction between * and #, and treating them alike is the commonest routing bug. "
        << "Received " << received.size() << ": " << got;

    worker->close(2s);
}

TEST_P(RoutingConformanceTest, AHashBindingReceivesAnyNumberOfTrailingWords)
{
    MOTUS_REQUIRE_CAPABILITY_OR_SKIP(routingGroups);

    const std::string marker = uniqueMarker("route-hash");
    const std::string group  = marker + ".ex";
    const std::string dest   = marker + ".q";

    transport().ensureTopic(dest, groupedAs(group, "TEST." + marker + ".#", Routing::Pattern), 5s);

    SendOptions send = producerOptions();
    send.route = "TEST." + marker + ".Sensor.Raw.Feed";   // three words
    ASSERT_TRUE(transport().send(dest, "hash-hit", send));
    transport().pump(300ms);

    auto worker = newTransport();
    worker->ensureTopic(dest, groupedAs(group, "TEST." + marker + ".#", Routing::Pattern), 5s);

    std::vector<std::string> received;
    collect(*worker, dest, received, 3s);

    ASSERT_EQ(received.size(), 1u) << "`#` must match zero or more words";
    EXPECT_EQ(received.front(), "hash-hit");

    worker->close(2s);
}

// ------------------------------------------------------------------------------------------
// Conflicting declaration
// ------------------------------------------------------------------------------------------

TEST_P(RoutingConformanceTest, RedeclaringAGroupWithADifferentRoutingKindIsRefused)
{
    MOTUS_REQUIRE_CAPABILITY_OR_SKIP(routingGroups);

    const std::string marker = uniqueMarker("route-conflict");
    const std::string group  = marker + ".ex";

    transport().ensureTopic(marker + ".a", groupedAs(group, "r.a", Routing::Pattern), 5s);

    // AMQP answers this with 406 PRECONDITION_FAILED. A backend that accepted it silently
    // would be more forgiving than the real one, so a bug that breaks production would pass
    // its conformance run -- the one thing a reference implementation must never do.
    auto second = newTransport();
    EXPECT_THROW(second->ensureTopic(marker + ".b",
                                     groupedAs(group, "r.b", Routing::Broadcast), 5s),
                 std::exception)
        << "redeclaring a group with a different routing kind must be refused";

    second->close(2s);
}

// ------------------------------------------------------------------------------------------
// The whole stack
// ------------------------------------------------------------------------------------------

TEST_P(RoutingConformanceTest, AGroupedTopicWorksThroughProducerAndConsumer)
{
    MOTUS_REQUIRE_CAPABILITY_OR_SKIP(routingGroups);

    const std::string marker = uniqueMarker("route-stack");
    const std::string group  = marker + ".ex";
    // Unique per run, so nothing routes across runs even though queues persist on a real broker,
    // and so this scenario cannot disturb any other that shares a destination.
    const std::string dest   = marker + ".q";
    const TopicOptions options = groupedAs(group, "TEST." + marker, Routing::Pattern);

    const motus::ByteMessage message = motus_test::markedMessage(marker, "b");

    {
        motus::Producer producer(transport(), producerOptions());
        producer.declareTopic(dest, 5s, options);
        ASSERT_TRUE(producer.publish(dest, message));
        producer.flush(500ms);
    }

    auto workerTransport = newTransport();
    motus::ByteMessage seen;
    bool arrived = false;

    motus::Consumer consumer(*workerTransport, [&](const motus::ByteMessage &task) {
        seen    = task;
        arrived = true;
    });
    consumer.start(dest, 5s, options);

    const auto deadline = std::chrono::steady_clock::now() + 3s;
    while (!arrived && std::chrono::steady_clock::now() < deadline)
        consumer.poll(100ms);

    ASSERT_TRUE(arrived) << "a grouped topic must work through the full Producer/Consumer stack, "
                            "not only through ITransport";
    EXPECT_EQ(motus_test::markerOf(seen), marker);

    workerTransport->close(2s);
}

INSTANTIATE_TEST_SUITE_P(Backends,
                         RoutingConformanceTest,
                         ::testing::ValuesIn(motus::transport::availableBackends()),
                         motus_test::BackendName());

} // namespace
