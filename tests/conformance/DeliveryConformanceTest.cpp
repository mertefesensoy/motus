// Delivery semantics, topic declaration, wire compatibility and lifetime -- the portable half.
//
// Everything here runs against every backend. See ConformanceFixture.hpp for what belongs in
// this directory and what stays AMQP-specific.

#include "ConformanceFixture.hpp"

#include "motus/ByteMessage.hpp"
#include "motus/Consumer.hpp"
#include "motus/Producer.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

using namespace std::chrono_literals;
using motus_test::ConformanceTest;
using motus_test::contentOf;
using motus_test::markedMessage;
using motus_test::markerOf;
using motus_test::uniqueMarker;

using DeliveryConformanceTest = ConformanceTest;

motus::ByteMessage messageWith(const std::string &marker, const std::string &content = "payload")
{
    return markedMessage(marker, content);
}

// ---------------------------------------------------------------------------
// Connection and topic declaration
// ---------------------------------------------------------------------------

/**
 * A connected transport reports itself usable.
 *
 * `ready()` means usable RIGHT NOW, not "was ready once" -- a distinction that cost a real
 * defect: the flag was latched by the handshake and never cleared, so a connection the broker
 * had dropped kept answering true. Any caller using it as a health check would publish into a
 * dead socket and be told it succeeded.
 */
TEST_P(DeliveryConformanceTest, AConnectedTransportReportsItselfReady)
{
    EXPECT_TRUE(transport().ready())
        << "backend '" << backendName() << "' connected but does not report ready()";
}

/**
 * Declaring a topic twice with the same options must succeed; the second call is a no-op.
 *
 * Idempotency is not a nicety here. Producer AND Consumer both declare, by design, so that
 * either can start first. AMQP rejects a redeclaration whose properties differ with a 406
 * PRECONDITION_FAILED that closes the channel, and the in-memory backend deliberately mirrors
 * that rather than being quietly more forgiving -- a reference backend more permissive than
 * production would let a bug that breaks production pass its conformance run.
 *
 * Production symptom if this regresses: a worker that dies on startup only when the producer
 * ran first. Order-dependent and miserable to reproduce.
 */
TEST_P(DeliveryConformanceTest, DeclaringATopicTwiceWithTheSameOptionsIsIdempotent)
{
    const motus::transport::TopicOptions options;

    EXPECT_NO_THROW(transport().ensureTopic(motus_test::testTopic(), options, 5s));
    EXPECT_NO_THROW(transport().ensureTopic(motus_test::testTopic(), options, 5s))
        << "an identical redeclaration was rejected, so Producer and Consumer cannot both "
           "declare and whichever starts second will fail";
}

// ---------------------------------------------------------------------------
// Crash safety
// ---------------------------------------------------------------------------

/**
 * A delivery whose handler THREW must not be resolved, and must come back when the transport
 * closes -- flagged as a redelivery.
 *
 * This is "unacknowledged work comes back when a worker dies", the entire reason manual
 * acknowledgement exists. It was previously asserted twice, in two files with two bodies: an
 * AMQP version in tests/integration/ and an in-memory version in tests/unit/. Two bodies
 * asserting one clause is how contracts drift, so it lives here now and runs against both.
 *
 * The distinction from Disposition::Requeue is the point and is not academic. Requeue is a
 * decision -- "I understand this and cannot process it right now". A throw is the ABSENCE of a
 * decision. Asserting one while claiming the other would be a green test verifying something
 * other than its own name.
 *
 * Note the timing assertion: the message must NOT be back while the subscriber is still alive.
 * Returning it immediately would let a still-running worker be handed the message it just
 * crashed on, which no real broker does -- the in-memory backend originally did exactly that
 * and was corrected.
 *
 * Production symptom if this regresses: killing a worker mid-job loses that message silently.
 * The publisher reported success, the queue is empty, no output exists, and nothing anywhere
 * records that a job vanished.
 */
TEST_P(DeliveryConformanceTest, DeliveryAbandonedByAThrowingHandlerIsReturnedForRedelivery)
{
    const std::string marker = uniqueMarker("abandon");

    {
        motus::Producer producer(transport(), producerOptions());
        producer.declareTopic(motus_test::testTopic());
        ASSERT_TRUE(producer.publish(motus_test::testTopic(), messageWith(marker)));
        producer.flush(1s);
    }

    // A worker that takes the message and then breaks. It may see leftovers from other
    // scenarios first, so it keeps going until it has thrown on ours.
    {
        auto dying = newTransport();
        auto sawOurs = std::make_shared<bool>(false);

        dying->subscribe(motus_test::testTopic(),
            [sawOurs, marker](const motus::transport::Delivery &delivery) -> motus::Disposition
            {
                if (delivery.body.find(marker) == std::string::npos)
                    return motus::Disposition::Reject;   // not ours; do not leave it lying around

                *sawOurs = true;
                throw std::runtime_error("worker died mid-job");
            }, 5s);

        const auto deadline = std::chrono::steady_clock::now() + 15s;
        bool threw = false;
        while (!threw && std::chrono::steady_clock::now() < deadline)
        {
            try { dying->pump(250ms); }
            catch (const std::runtime_error &) { threw = true; }
        }

        ASSERT_TRUE(*sawOurs) << "the message never reached the worker that was to abandon it";
        EXPECT_TRUE(threw)
            << "the handler's failure was not reported out of pump(); it was swallowed, and a "
               "crashing worker would look healthy";
        EXPECT_EQ(dying->stats().abandoned, 1u) << "the abandonment was not counted";

        dying->close(2s);   // unresolved deliveries are returned here
    }

    auto successor = newTransport();
    auto redelivered = std::make_shared<int>(-1);

    successor->subscribe(motus_test::testTopic(),
        [redelivered, marker](const motus::transport::Delivery &delivery) -> motus::Disposition
        {
            if (delivery.body.find(marker) != std::string::npos)
                *redelivered = delivery.redelivered ? 1 : 0;
            return motus::Disposition::Ack;
        }, 5s);

    const auto deadline = std::chrono::steady_clock::now() + 15s;
    while (*redelivered < 0 && std::chrono::steady_clock::now() < deadline)
        successor->pump(250ms);

    successor->close(2s);

    ASSERT_NE(*redelivered, -1)
        << "the abandoned message was never returned to a successor; the work is lost";
    EXPECT_EQ(*redelivered, 1)
        << "the message came back without the redelivered flag, so a worker cannot tell it may "
           "be repeating work it half-completed";
}

// ---------------------------------------------------------------------------
// At-least-once
// ---------------------------------------------------------------------------

/**
 * The same message published twice reaches the handler twice. There is no deduplication
 * anywhere in motus.
 *
 * This asserts the true current behaviour, not a wished-for one, and it exists to make a
 * contract explicit for every application above the seam: **handlers must be idempotent.**
 * Processing the same payload twice has to be harmless. If a handler ever appends, increments
 * a counter, or refuses to overwrite, an ordinary middleware hiccup corrupts its output.
 *
 * Production symptom if the assumption is violated later: duplicated or corrupted output
 * appearing only under redelivery -- that is, only when something else already went wrong.
 */
TEST_P(DeliveryConformanceTest, TheSameMessagePublishedTwiceReachesTheHandlerTwice)
{
    const std::string marker = uniqueMarker("dedup");
    const motus::ByteMessage message = messageWith(marker);

    {
        motus::Producer producer(transport(), producerOptions());
        producer.declareTopic(motus_test::testTopic());
        ASSERT_TRUE(producer.publish(motus_test::testTopic(), message));
        ASSERT_TRUE(producer.publish(motus_test::testTopic(), message));
        producer.flush(1s);
    }

    auto worker = newTransport();
    auto seen = std::make_shared<std::size_t>(0);

    motus::Consumer consumer(*worker, [seen, marker](const motus::ByteMessage &task)
    {
        if (markerOf(task) == marker) ++*seen;
    });
    ASSERT_NO_THROW(consumer.start(motus_test::testTopic()));

    const auto deadline = std::chrono::steady_clock::now() + 15s;
    while (*seen < 2 && std::chrono::steady_clock::now() < deadline)
        consumer.poll(250ms);

    worker->close(2s);

    EXPECT_EQ(*seen, 2u)
        << "two identical publishes produced " << *seen << " handler invocation(s). motus "
           "performs no deduplication by design; if this reports 1, something is silently "
           "dropping messages, which is far worse than processing one twice.";
}

// ---------------------------------------------------------------------------
// Concurrent producers
// ---------------------------------------------------------------------------

/**
 * Several producers publishing at once, each on its own transport, must all succeed and lose
 * nothing -- verified by CONSUMING them, not by counting accepted publishes. A send returning
 * true means the payload reached the middleware, nothing more.
 *
 * The realistic trigger is not deliberate scaling: it is two engineers running the same
 * publishing tool against the same broker at the same moment.
 *
 * Production symptom if this regresses: publishes silently refused under concurrent load,
 * reported only as a WARN and a false return the caller does not treat as fatal -- so the
 * operator sees a successful-looking run that submitted fewer messages than it claimed.
 */
TEST_P(DeliveryConformanceTest, ConcurrentProducersOnSeparateTransportsAllDeliverTheirMessages)
{
    const std::string marker = uniqueMarker("multiprod");
    // `static` is load-bearing, not decoration. A plain `constexpr` local used inside the
    // lambda below compiles under GCC -- it is not odr-used, so no capture is required -- but
    // MSVC rejects it with C3493 ("cannot be implicitly captured because no default capture
    // mode has been specified"). Making it static gives it storage outside the enclosing
    // frame, so no compiler considers it a capture at all. Found by the MSVC CI leg; this is
    // precisely the class of GCC-only assumption NFR-3 exists to catch.
    static constexpr std::size_t kThreads   = 3;
    static constexpr std::size_t kPerThread = 6;

    std::vector<std::string> errors(kThreads);
    std::vector<std::thread>  threads;

    for (std::size_t t = 0; t < kThreads; ++t)
    {
        threads.emplace_back([this, &errors, marker, t]
        {
            try
            {
                // One transport per thread. The documented concurrency model, obeyed by the
                // test that exercises it.
                auto own = newTransport();
                motus::Producer producer(*own, producerOptions());
                producer.declareTopic(motus_test::testTopic());

                for (std::size_t i = 0; i < kPerThread; ++i)
                {
                    const std::string field = "t" + std::to_string(t) + "-m" + std::to_string(i);
                    if (!producer.publish(motus_test::testTopic(), messageWith(marker, field)))
                        throw std::runtime_error("publish refused");
                }
                producer.flush(1s);
                own->close(2s);
            }
            catch (const std::exception &e)
            {
                errors[t] = e.what();
            }
        });
    }
    for (auto &thread : threads) thread.join();

    for (std::size_t t = 0; t < kThreads; ++t)
        ASSERT_TRUE(errors[t].empty()) << "producer thread " << t << ": " << errors[t];

    auto worker = newTransport();
    auto received = std::make_shared<std::set<std::string>>();

    motus::Consumer consumer(*worker, [received, marker](const motus::ByteMessage &task)
    {
        if (markerOf(task) == marker) received->insert(contentOf(task));
    });
    ASSERT_NO_THROW(consumer.start(motus_test::testTopic()));

    const std::size_t expected = kThreads * kPerThread;
    const auto deadline = std::chrono::steady_clock::now() + 25s;
    while (received->size() < expected && std::chrono::steady_clock::now() < deadline)
        consumer.poll(250ms);

    worker->close(2s);

    EXPECT_EQ(received->size(), expected)
        << kThreads << " concurrent producers published " << expected << " messages but only "
        << received->size() << " arrived";
}

// ---------------------------------------------------------------------------
// Wire compatibility
// ---------------------------------------------------------------------------

/**
 * A deterministic rejection leaves the exact original bytes in a topic-specific forensic
 * destination instead of destroying them. The AMQP-specific suite separately verifies the
 * broker's x-death timestamp and source metadata; this portable clause proves the seam-level
 * WHAT invariant for every backend.
 *
 * Production symptom if this regresses: a malformed or twice-failed transient message vanishes
 * without the exact bytes operators need to reproduce it, or stays in front of newer work.
 */
TEST_P(DeliveryConformanceTest, RejectedPayloadIsPreservedInItsDeadLetterDestination)
{
    MOTUS_REQUIRE_CAPABILITY_OR_SKIP(deadLetter);

    const std::string marker = uniqueMarker("dead-letter");
    const std::string source = "motus.conformance." + marker;
    const std::string body   = "{ deliberately malformed: " + marker;

    transport().ensureTopic(source, {}, 5s);
    ASSERT_TRUE(transport().send(source, body, producerOptions()));

    auto worker = newTransport();
    motus::Consumer consumer(*worker, [](const motus::ByteMessage &) {});
    ASSERT_NO_THROW(consumer.start(source));

    const auto rejectedDeadline = std::chrono::steady_clock::now() + 15s;
    while (consumer.rejected() == 0 && std::chrono::steady_clock::now() < rejectedDeadline)
        consumer.poll(250ms);
    ASSERT_EQ(consumer.rejected(), 1u) << "the malformed payload was not rejected";
    worker->close(2s);

    EXPECT_EQ(transport().topicDepth(source, 5s), 0u)
        << "the rejected payload still blocks the source topic";
    ASSERT_EQ(transport().topicDepth(motus::transport::deadLetterTopic(source), 5s), 1u)
        << "the rejection disappeared instead of becoming forensic evidence";

    auto inspector = newTransport();
    auto recovered = std::make_shared<std::string>();
    inspector->subscribe(motus::transport::deadLetterTopic(source),
        [recovered](const motus::transport::Delivery &delivery)
        {
            *recovered = delivery.body;
            return motus::Disposition::Ack;
        }, 5s);

    const auto inspectDeadline = std::chrono::steady_clock::now() + 15s;
    while (recovered->empty() && std::chrono::steady_clock::now() < inspectDeadline)
        inspector->pump(250ms);
    inspector->close(2s);

    EXPECT_EQ(*recovered, body)
        << "dead-lettering changed the bytes needed to reproduce the failure";
}

/**
 * A stale-version message left in the destination with no consumer attached must still be
 * refused by a worker that starts later.
 *
 * This is the deployed-fleet case the versioning comment describes, and it is a different
 * situation from a bad message arriving while a consumer is already running: the publisher is
 * gone entirely. That is the real production sequence -- publish from an old snapshot today,
 * start a new worker tomorrow.
 *
 * Production symptom if this regresses: a destination drained after an upgrade quietly
 * misinterprets everything submitted before it.
 */
TEST_P(DeliveryConformanceTest, StaleVersionLeftInTheDestinationIsRejectedByAFreshConsumer)
{
    const std::string marker = uniqueMarker("stale-v0");

    // An envelope from a build whose FORMAT_VERSION this build has never carried. Durable
    // queues are exactly where such messages live: published before an upgrade, consumed
    // after it, with the publisher long gone.
    //
    // Rejecting it loudly is the property that makes an in-place version bump safe at all --
    // under a new-type-on-a-new-destination design these messages would simply be orphaned,
    // and nothing would ever report them.
    const std::string stale = "motus:0:" + marker + "\nrows published by an old snapshot\n";

    transport().ensureTopic(motus_test::testTopic(), {}, 5s);
    ASSERT_TRUE(transport().send(motus_test::testTopic(), stale,
                                 producerOptions()));

    // The publishing transport goes away entirely: the message now exists only in the
    // middleware, as it would after the publishing machine went home.
    transport().close(2s);

    auto fresh = newTransport();
    auto seen = std::make_shared<std::vector<std::string>>();

    motus::Consumer consumer(*fresh, [seen](const motus::ByteMessage &task)
    {
        seen->push_back(markerOf(task));
    });
    ASSERT_NO_THROW(consumer.start(motus_test::testTopic()));

    const auto deadline = std::chrono::steady_clock::now() + 15s;
    while (consumer.rejected() == 0 && std::chrono::steady_clock::now() < deadline)
        consumer.poll(250ms);

    const std::size_t rejected = consumer.rejected();
    fresh->close(2s);

    EXPECT_EQ(std::count(seen->begin(), seen->end(), marker), 0)
        << "a stale-version message that had been sitting in the destination was accepted";
    EXPECT_GE(rejected, 1u)
        << "a stale-version message was neither accepted nor rejected -- it is still there, and "
           "will be redelivered forever";
}

/**
 * An envelope carrying no version field at all must be rejected, not treated as the current
 * version.
 *
 * The body opens with the correct `motus:` magic and then simply stops declaring a version
 * -- the shape a truncated or hand-assembled envelope takes. Defaulting a missing version to
 * the current one is the single most tempting piece of leniency available here, and it would
 * defeat the whole mechanism: the payloads that most need rejecting are precisely the ones
 * that fail to declare themselves.
 */
TEST_P(DeliveryConformanceTest, PayloadWithNoFormatVersionAtAllIsRejected)
{
    const std::string marker = uniqueMarker("noversion");

    const std::string legacy = "motus:" + marker;   // magic, then no version field at all

    transport().ensureTopic(motus_test::testTopic(), {}, 5s);
    ASSERT_TRUE(transport().send(motus_test::testTopic(), legacy,
                                 producerOptions()));

    auto worker = newTransport();
    auto seen = std::make_shared<std::vector<std::string>>();

    motus::Consumer consumer(*worker, [seen](const motus::ByteMessage &task)
    {
        seen->push_back(markerOf(task));
    });
    ASSERT_NO_THROW(consumer.start(motus_test::testTopic()));

    const auto deadline = std::chrono::steady_clock::now() + 15s;
    while (consumer.rejected() == 0 && std::chrono::steady_clock::now() < deadline)
        consumer.poll(250ms);

    const std::size_t rejected = consumer.rejected();
    worker->close(2s);

    EXPECT_EQ(std::count(seen->begin(), seen->end(), marker), 0)
        << "a payload with no format_version was accepted as if it were the current version";
    EXPECT_GE(rejected, 1u) << "a payload with no format_version was not rejected";
}

// ---------------------------------------------------------------------------
// Lifetime and observability
// ---------------------------------------------------------------------------

/**
 * Closing a transport while a Producer still holds it must be clean, and the Producer must then
 * be destroyed without incident.
 *
 * This ordering happens on every real shutdown path: the transport closes, then the Producer
 * goes out of scope and its channel destructor runs against something already closed.
 *
 * The REVERSE order -- destroying the transport while a Producer is still alive -- is undefined
 * behaviour and is deliberately NOT tested: Producer documents that the transport must outlive
 * it, and a test provoking the use-after-free would either crash the suite or pass by luck.
 * That is a documentation-enforced invariant, not a testable one.
 *
 * Production symptom if this regresses: an application crashes on exit after a successful run,
 * making working submissions look like failed ones.
 */
TEST_P(DeliveryConformanceTest, ClosingATransportThatStillHasALiveProducerIsClean)
{
    auto owner = newTransport();

    motus::Producer producer(*owner, producerOptions());
    ASSERT_NO_THROW(producer.declareTopic(motus_test::testTopic()));
    ASSERT_TRUE(producer.publish(motus_test::testTopic(), messageWith(uniqueMarker("lifetime"))));

    EXPECT_NO_THROW(owner->close(2s)) << "closing a transport with a live Producer threw";

    // The Producer is destroyed here, after the transport has closed. Reaching the end of the
    // test at all is the assertion.
    SUCCEED();
}

/**
 * bytesSent must account for at least the payload that was published.
 *
 * A LOWER bound only, and deliberately so. The AMQP backend adds method and header framing on
 * top of the payload; the in-memory backend counts the payload exactly. A tight upper bound is
 * meaningful for the first and vacuous for the second, so it stays in the AMQP-specific suite
 * where it can actually catch a double-counting bug. What every backend must guarantee is that
 * the counter is real rather than decorative.
 *
 * Production symptom if this regresses: the one diagnostic available in a locked-down
 * environment -- no debugger, no packet capture, no profiler -- reports plausible-looking
 * traffic that never happened.
 */
TEST_P(DeliveryConformanceTest, BytesSentAccountsForAtLeastThePublishedPayload)
{
    const motus::ByteMessage message = messageWith(uniqueMarker("bytes"));
    const std::size_t payloadSize = message.serialize().size();

    motus::Producer producer(transport(), producerOptions());
    producer.declareTopic(motus_test::testTopic());

    const std::uint64_t before = transport().stats().bytesSent;
    ASSERT_TRUE(producer.publish(motus_test::testTopic(), message));
    producer.flush(1s);
    const std::uint64_t sent = transport().stats().bytesSent - before;

    EXPECT_GE(sent, payloadSize)
        << "publishing a " << payloadSize << " byte payload accounted for only " << sent
        << " byte(s) on backend '" << backendName() << "'; the counter cannot be believed";
}

// ---------------------------------------------------------------------------
// Deferred acknowledgement
// ---------------------------------------------------------------------------

/**
 * A handler that returns Disposition::Deferred must NOT have the delivery resolved until a
 * later, separate call to settle() names it -- and settle() must actually resolve it.
 *
 * This is the permanent, backend-parameterized counterpart to a one-time hand spike that
 * proved, against a live broker, that a delivery tag survives past onReceived's return and
 * settles correctly from outside that callback. This test pins the same property so a
 * regression is caught by ctest rather than rediscovered by hand. Skipped on any backend that
 * declares deferredAcknowledgement = false.
 *
 * Production symptom if this regresses: a consumer that acks critical work on COMPLETION would
 * ack -- or fail to ack -- at the wrong moment, silently trading away its at-least-once
 * guarantee.
 */
TEST_P(DeliveryConformanceTest, ADeferredDeliveryIsNotResolvedUntilSettled)
{
    MOTUS_REQUIRE_CAPABILITY_OR_SKIP(deferredAcknowledgement);

    const std::string marker = uniqueMarker("defer-settle");
    {
        motus::Producer producer(transport(), producerOptions());
        producer.declareTopic(motus_test::testTopic());
        ASSERT_TRUE(producer.publish(motus_test::testTopic(), messageWith(marker)));
        producer.flush(1s);
    }

    auto worker = newTransport();
    auto handle = std::make_shared<std::uint64_t>(0);
    auto received = std::make_shared<bool>(false);

    worker->subscribe(motus_test::testTopic(),
        [handle, received, marker](const motus::transport::Delivery &delivery) -> motus::Disposition
        {
            if (delivery.body.find(marker) == std::string::npos)
                return motus::Disposition::Reject;   // not ours; do not leave it lying around
            *handle = delivery.tag;
            *received = true;
            return motus::Disposition::Deferred;
        }, 5s);

    const auto deadline = std::chrono::steady_clock::now() + 15s;
    while (!*received && std::chrono::steady_clock::now() < deadline)
        worker->pump(250ms);
    ASSERT_TRUE(*received) << "the message never reached the deferring handler";

    // Settle it, late, from a call that is not inside any subscribe() callback -- the exact
    // pattern the step-0 spike exists to validate.
    worker->settle(*handle, motus::Disposition::Ack);
    worker->pump(200ms);   // flush the settlement where the backend needs an extra pump
    worker->close(2s);

    // A successor must NOT see this message again: it was genuinely acked, just late.
    auto successor = newTransport();
    auto sawIt = std::make_shared<bool>(false);
    successor->subscribe(motus_test::testTopic(),
        [sawIt, marker](const motus::transport::Delivery &delivery) -> motus::Disposition
        {
            if (delivery.body.find(marker) != std::string::npos) *sawIt = true;
            return motus::Disposition::Ack;
        }, 5s);
    successor->pump(500ms);
    successor->close(2s);

    EXPECT_FALSE(*sawIt) << "a deferred delivery settled with Ack came back anyway on backend '"
                         << backendName() << "'; settle() did not actually resolve it";
}

/**
 * A deferred delivery that is NEVER settled must come back for redelivery once the transport
 * that deferred it closes -- exactly like a delivery a throwing handler abandoned
 * (DeliveryAbandonedByAThrowingHandlerIsReturnedForRedelivery above). This is the crash-safety
 * half of Disposition::Deferred: a process that dies between "accepted the order" and "settled
 * it" must not have silently destroyed the order.
 *
 * Production symptom if this regresses: a worker crashing mid-execution of critical deferred
 * work loses that order permanently instead of it being redelivered -- turning an
 * at-least-once guarantee into at-most-once exactly where it matters most.
 */
TEST_P(DeliveryConformanceTest, ADeferredDeliveryNeverSettledIsReturnedForRedelivery)
{
    MOTUS_REQUIRE_CAPABILITY_OR_SKIP(deferredAcknowledgement);

    const std::string marker = uniqueMarker("defer-abandon");
    {
        motus::Producer producer(transport(), producerOptions());
        producer.declareTopic(motus_test::testTopic());
        ASSERT_TRUE(producer.publish(motus_test::testTopic(), messageWith(marker)));
        producer.flush(1s);
    }

    {
        auto worker = newTransport();
        auto received = std::make_shared<bool>(false);
        worker->subscribe(motus_test::testTopic(),
            [received, marker](const motus::transport::Delivery &delivery) -> motus::Disposition
            {
                if (delivery.body.find(marker) == std::string::npos)
                    return motus::Disposition::Reject;
                *received = true;
                return motus::Disposition::Deferred;   // and deliberately never settled
            }, 5s);

        const auto deadline = std::chrono::steady_clock::now() + 15s;
        while (!*received && std::chrono::steady_clock::now() < deadline)
            worker->pump(250ms);
        ASSERT_TRUE(*received) << "the message never reached the deferring handler";

        worker->close(2s);   // never settled -- must come back, exactly like a thrown handler
    }

    auto successor = newTransport();
    auto redelivered = std::make_shared<int>(-1);
    successor->subscribe(motus_test::testTopic(),
        [redelivered, marker](const motus::transport::Delivery &delivery) -> motus::Disposition
        {
            if (delivery.body.find(marker) != std::string::npos)
                *redelivered = delivery.redelivered ? 1 : 0;
            return motus::Disposition::Ack;
        }, 5s);

    const auto deadline = std::chrono::steady_clock::now() + 15s;
    while (*redelivered < 0 && std::chrono::steady_clock::now() < deadline)
        successor->pump(250ms);
    successor->close(2s);

    ASSERT_NE(*redelivered, -1)
        << "a deferred delivery that was NEVER settled was not returned on backend '"
        << backendName() << "'; the work is lost, exactly the failure decision 10's "
           "at-least-once guarantee exists to prevent";
    EXPECT_EQ(*redelivered, 1)
        << "it came back without the redelivered flag, so a worker cannot tell it may be "
           "repeating work it half-completed";
}

// ---------------------------------------------------------------------------
// Bounded destinations (TopicOptions::maxLength)
// ---------------------------------------------------------------------------

/**
 * A destination declared with TopicOptions::maxLength genuinely drops the OLDEST message once
 * that many are waiting, admitting the newest -- the live-state-stream requirement that a slow
 * or absent consumer can never grow an unbounded backlog on the broker, and can never
 * backpressure the publisher either: every send() past the bound must still report success.
 *
 * Also proves drop-HEAD specifically, not merely "some" eviction: the three messages that
 * survive must be the three newest, not the three oldest or an unspecified three, or a backend
 * technically satisfying "depth stays at 3" could still be silently discarding new work instead
 * of aging out old work -- the opposite of what a lossy-but-live telemetry stream needs.
 *
 * Skipped on any backend that declares boundedQueue = false.
 *
 * Production symptom if this regresses: a disconnected viewer lets its telemetry queue grow
 * without limit until the broker runs out of memory -- the failure bounded queues exist to
 * rule out structurally, not by operational discipline.
 */
TEST_P(DeliveryConformanceTest, ABoundedDestinationDropsTheOldestMessageOnOverflow)
{
    MOTUS_REQUIRE_CAPABILITY_OR_SKIP(boundedQueue);

    const std::string marker = uniqueMarker("bounded");
    const std::string topic  = "motus.conformance.bounded." + marker;

    motus::transport::TopicOptions options;
    options.maxLength  = 3;
    options.deadLetter = false;   // isolate the property under test from dead-lettering

    transport().ensureTopic(topic, options, 5s);

    for (int i = 0; i < 5; ++i)
    {
        const std::string body = marker + "-" + std::to_string(i);
        ASSERT_TRUE(transport().send(topic, body, producerOptions()))
            << "send() refused message " << i << " on a bounded destination on backend '"
            << backendName() << "'; drop-on-overflow must never backpressure the publisher";
    }

    // x-max-length enforcement is not necessarily synchronous with the very next operation on
    // this channel -- observed directly against a live RabbitMQ 4.3.4 broker: a topicDepth()
    // issued immediately after the 5th send() can report a transient, LOWER depth (the queue
    // process's own mailbox has not caught up to the channel's frame-processing order) before
    // settling at the true bounded value a moment later. Poll rather than assert once, exactly
    // like every other eventually-consistent assertion in this suite -- and exactly the caveat
    // any snapshot-on-connect design needs to hold in mind: what a consumer reads RIGHT NOW
    // and what the middleware has actually settled to can briefly disagree.
    std::uint32_t depth = 0;
    const auto depthDeadline = std::chrono::steady_clock::now() + 10s;
    while (std::chrono::steady_clock::now() < depthDeadline)
    {
        depth = transport().topicDepth(topic, 5s);
        if (depth == 3u) break;
        std::this_thread::sleep_for(50ms);
    }
    ASSERT_EQ(depth, 3u)
        << "backend '" << backendName() << "' did not settle at maxLength=3 within 10s";

    auto worker = newTransport();
    auto seen = std::make_shared<std::vector<std::string>>();
    worker->subscribe(topic,
        [seen](const motus::transport::Delivery &delivery) -> motus::Disposition
        {
            seen->push_back(delivery.body);
            return motus::Disposition::Ack;
        }, 5s);

    const auto deadline = std::chrono::steady_clock::now() + 15s;
    while (seen->size() < 3 && std::chrono::steady_clock::now() < deadline)
        worker->pump(250ms);
    worker->close(2s);

    ASSERT_EQ(seen->size(), 3u)
        << "expected exactly the 3 surviving messages on backend '" << backendName() << "'";
    EXPECT_EQ((*seen)[0], marker + "-2") << "oldest surviving message is not the 3rd published";
    EXPECT_EQ((*seen)[1], marker + "-3");
    EXPECT_EQ((*seen)[2], marker + "-4") << "newest published message did not survive";
}

/**
 * A destination combining `maxLength` with `deadLetter=true` (the default) preserves EVICTED
 * messages in the same forensic destination a REJECTED message would land in -- verified against
 * real broker behaviour first: RabbitMQ dead-letters a drop-head eviction with `x-death
 * reason=maxlen` when the queue has a DLX configured, confirmed via the management API before
 * any code here changed. `InMemoryTransport` now replicates that rather than silently discarding
 * the evicted body, closing a cross-backend fidelity gap the sibling scenario above deliberately
 * left open by setting `deadLetter=false` to isolate the bound itself.
 *
 * Also proves EVICTION ORDER lands in the forensic queue the same way it left the live one:
 * oldest-evicted-first, not an unspecified order -- a forensic trail that reordered evidence
 * would be as misleading as one that dropped it.
 *
 * Skipped on any backend that declares boundedQueue = false.
 *
 * Production symptom if this regresses: an operator inspecting a telemetry destination's
 * forensic queue after a burst overload sees nothing, or the wrong messages, for exactly the
 * data a post-incident review would need most.
 */
TEST_P(DeliveryConformanceTest, ABoundedDestinationDeadLettersItsOverflowEvictions)
{
    MOTUS_REQUIRE_CAPABILITY_OR_SKIP(boundedQueue);

    const std::string marker = uniqueMarker("bounded-dlx");
    const std::string topic  = "motus.conformance.bounded-dlx." + marker;

    motus::transport::TopicOptions options;
    options.maxLength = 2;
    // deadLetter left at its default (true) -- deliberately, this scenario is about exactly the
    // interaction ABoundedDestinationDropsTheOldestMessageOnOverflow excludes.

    transport().ensureTopic(topic, options, 5s);

    for (int i = 0; i < 4; ++i)
    {
        const std::string body = marker + "-" + std::to_string(i);
        ASSERT_TRUE(transport().send(topic, body, producerOptions()));
    }

    // Same eventual-consistency caveat as the sibling scenario: poll rather than assert once.
    const std::string deadTopic = motus::transport::deadLetterTopic(topic);
    std::uint32_t deadDepth = 0;
    const auto depthDeadline = std::chrono::steady_clock::now() + 10s;
    while (std::chrono::steady_clock::now() < depthDeadline)
    {
        deadDepth = transport().topicDepth(deadTopic, 5s);
        if (deadDepth == 2u) break;
        std::this_thread::sleep_for(50ms);
    }
    ASSERT_EQ(deadDepth, 2u)
        << "backend '" << backendName() << "' did not dead-letter its 2 overflow evictions";

    auto inspector = newTransport();
    auto recovered = std::make_shared<std::vector<std::string>>();
    inspector->subscribe(deadTopic,
        [recovered](const motus::transport::Delivery &delivery) -> motus::Disposition
        {
            recovered->push_back(delivery.body);
            return motus::Disposition::Ack;
        }, 5s);

    const auto collectDeadline = std::chrono::steady_clock::now() + 15s;
    while (recovered->size() < 2 && std::chrono::steady_clock::now() < collectDeadline)
        inspector->pump(250ms);
    inspector->close(2s);

    ASSERT_EQ(recovered->size(), 2u)
        << "expected exactly the 2 evicted messages in the forensic destination on backend '"
        << backendName() << "'";
    EXPECT_EQ((*recovered)[0], marker + "-0") << "eviction order was not oldest-first";
    EXPECT_EQ((*recovered)[1], marker + "-1");

    EXPECT_EQ(transport().topicDepth(topic, 5s), 2u)
        << "the live destination itself no longer holds exactly the 2 surviving messages";
}

/**
 * A source's automatically provisioned forensic destination is bounded independently from the
 * source and preserves the FIRST failure sequence. Once full it rejects newer dead-letter
 * republishes rather than dropping the root-cause prefix for later cascade noise.
 */
TEST_P(DeliveryConformanceTest, AForensicDestinationPreservesItsFirstMessagesAndRejectsOverflow)
{
    MOTUS_REQUIRE_CAPABILITY_OR_SKIP(deadLetter);
    MOTUS_REQUIRE_CAPABILITY_OR_SKIP(boundedQueue);

    static constexpr std::uint32_t kExpectedForensicLimit =
        motus::transport::kForensicMaxLength;
    const std::string marker = uniqueMarker("forensic-bound");
    const std::string source = "motus.conformance.forensic-bound." + marker;
    const std::string forensic = motus::transport::deadLetterTopic(source);

    transport().ensureTopic(source, {}, 5s);
    for (std::uint32_t i = 0; i <= kExpectedForensicLimit; ++i)
        ASSERT_TRUE(transport().send(source, marker + "-" + std::to_string(i), producerOptions()));

    auto rejected = std::make_shared<std::uint32_t>(0);
    auto rejector = newTransport();
    rejector->subscribe(source,
        [rejected](const motus::transport::Delivery &) -> motus::Disposition
        {
            ++*rejected;
            return motus::Disposition::Reject;
        }, 5s);

    const auto rejectDeadline = std::chrono::steady_clock::now() + 30s;
    while (*rejected <= kExpectedForensicLimit &&
           std::chrono::steady_clock::now() < rejectDeadline)
        rejector->pump(50ms);
    rejector->close(2s);
    ASSERT_EQ(*rejected, kExpectedForensicLimit + 1u)
        << "the source messages were not all rejected into the forensic path";

    std::uint32_t forensicDepth = 0;
    const auto depthDeadline = std::chrono::steady_clock::now() + 10s;
    while (std::chrono::steady_clock::now() < depthDeadline)
    {
        forensicDepth = transport().topicDepth(forensic, 5s);
        if (forensicDepth == kExpectedForensicLimit) break;
        std::this_thread::sleep_for(50ms);
    }
    ASSERT_EQ(forensicDepth, kExpectedForensicLimit)
        << "the automatically declared forensic destination is not independently bounded";

    auto first = std::make_shared<std::string>();
    auto inspector = newTransport();
    const bool canHoldFirst = caps().deferredAcknowledgement;
    inspector->subscribe(forensic,
        [first, canHoldFirst](const motus::transport::Delivery &delivery) -> motus::Disposition
        {
            // AMQP-CPP can dispatch a broker-dependent batch during one pump(), so hold its
            // first delivery unresolved behind prefetch=1. SimpleAmqpClient's blocking receive
            // returns one envelope per pump and honestly lacks deferred settlement, so Ack is
            // the portable fallback there while the first-body guard remains deterministic.
            if (first->empty()) *first = delivery.body;
            return canHoldFirst ? motus::Disposition::Deferred : motus::Disposition::Ack;
        }, 5s);

    const auto inspectDeadline = std::chrono::steady_clock::now() + 5s;
    while (first->empty() && std::chrono::steady_clock::now() < inspectDeadline)
        inspector->pump(50ms);
    inspector->close(2s);

    EXPECT_EQ(*first, marker + "-0")
        << "forensic overflow discarded the first failure instead of later cascade noise";
    transport().purgeTopic(forensic, 5s);
}

INSTANTIATE_TEST_SUITE_P(Backends,
                         DeliveryConformanceTest,
                         ::testing::ValuesIn(motus::transport::availableBackends()),
                         motus_test::BackendName());

} // namespace
