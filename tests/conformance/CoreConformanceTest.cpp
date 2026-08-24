// The portable transport contract -- first batch.
//
// Every scenario here runs against every backend in the build. See ConformanceFixture.hpp for
// what belongs in this directory and what stays AMQP-specific.

#include "ConformanceFixture.hpp"

#include "motus/ByteMessage.hpp"
#include "motus/Consumer.hpp"
#include "motus/Producer.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <set>
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

motus::ByteMessage messageWith(const std::string &marker, const std::string &content = "payload")
{
    return markedMessage(marker, content);
}

// ---------------------------------------------------------------------------
// The floor
// ---------------------------------------------------------------------------

/**
 * Some capabilities are not optional. A backend declaring any of these false is not a motus
 * transport, whatever else it does.
 *
 * Without a floor, "conformant" would be a claim with no content: a backend declaring
 * everything false would skip almost every scenario and report green, and defaults are what
 * people actually see. This scenario is what makes the word mean something.
 *
 * The three required ones, and why each is structural rather than a nicety:
 *
 *   manualAcknowledgement  without it the middleware acks on delivery, before the handler has
 *                          run. Reject-without-requeue becomes unenforceable and an unprocessed
 *                          message is simply gone.
 *   fifoPerTopic           sequences whose later items depend on earlier ones are the normal
 *                          case, not the exception. Nothing downstream can repair ordering
 *                          that was never there.
 *   redeliveryFlag         the only signal a worker gets that it may be repeating work -- and
 *                          the bit dispatch() uses to bound a transient-failure retry to one
 *                          attempt. Without it that retry is unbounded, which is the
 *                          poison-message spin the rejection policy exists to prevent.
 *
 * Everything else -- durability, publisher confirms, dead-lettering, topic depth, requeue --
 * is genuinely optional and is capability-gated per scenario.
 *
 * Production symptom if this regresses: a backend is added, declares little, passes its
 * conformance run, and is deployed. The gaps surface as lost or duplicated messages under
 * conditions nobody tested, because nothing ever asserted the backend could do the job.
 */
TEST_P(ConformanceTest, MandatoryCapabilitiesAreDeclared)
{
    EXPECT_TRUE(caps().manualAcknowledgement)
        << "backend '" << backendName() << "' cannot acknowledge manually, so a message would "
           "be acknowledged before it is processed";

    EXPECT_TRUE(caps().fifoPerTopic)
        << "backend '" << backendName() << "' does not preserve order for a single consumer";

    EXPECT_TRUE(caps().redeliveryFlag)
        << "backend '" << backendName() << "' cannot distinguish a redelivery, so a transient-"
           "failure retry could not be bounded and would spin forever";

    EXPECT_STRNE(caps().name, "unknown")
        << "backend '" << backendName() << "' does not name itself in its capabilities, so no "
           "diagnostic anywhere can say which transport was involved";
}

// ---------------------------------------------------------------------------
// Delivery
// ---------------------------------------------------------------------------

/**
 * A published message must arrive at a SEPARATE consumer, byte-identical.
 *
 * The separate transport matters: a real worker is a different process, and reusing one
 * connection could mask a bug that only appears across them.
 *
 * Production symptom if this regresses: payloads are submitted and silently never arrive, or
 * arrive corrupted and are processed as something subtly different -- the worst failure a
 * messaging layer can have.
 */
TEST_P(ConformanceTest, PublishedMessageIsDeliveredIntactToASeparateConsumer)
{
    const std::string marker = uniqueMarker("roundtrip");
    const motus::ByteMessage sent = messageWith(marker);

    {
        motus::Producer producer(transport(), producerOptions());
        producer.declareTopic(motus_test::testTopic());
        ASSERT_TRUE(producer.publish(motus_test::testTopic(), sent));
        producer.flush();
    }

    auto worker = newTransport();
    ASSERT_NE(worker, nullptr);

    auto received = std::make_shared<std::unique_ptr<motus::ByteMessage>>();
    motus::Consumer consumer(*worker, [received, marker](const motus::ByteMessage &task)
    {
        if (markerOf(task) == marker)
            *received = std::make_unique<motus::ByteMessage>(task);
    });
    ASSERT_NO_THROW(consumer.start(motus_test::testTopic()));

    const auto deadline = std::chrono::steady_clock::now() + 15s;
    while (!*received && std::chrono::steady_clock::now() < deadline)
        consumer.poll(250ms);

    worker->close(2s);

    ASSERT_NE(*received, nullptr) << "the published message never arrived";
    EXPECT_EQ(**received, sent) << "the message arrived but was corrupted in transit";
}

/**
 * An undecodable message must be rejected without requeue, and the worker must keep serving.
 *
 * The second half is the requirement. Proving the bad message was rejected is easy; proving the
 * worker RECOVERED is what matters, because a consumer that rejects correctly and then stops
 * consuming has failed just as badly.
 *
 * This is also the scenario that proves the poison-message policy lives above the seam: the
 * same dispatch() runs on every backend, through completely different code below it.
 *
 * Production symptom if this regresses: one malformed message from a foreign or out-of-date
 * publisher halts the pipeline. Throughput goes to zero and nothing behind it is processed.
 */
TEST_P(ConformanceTest, MalformedMessageIsRejectedAndTheWorkerKeepsServing)
{
    const std::string marker = uniqueMarker("poison");

    // Bypassing the message layer on purpose: a foreign publisher that has never heard of the
    // motus envelope is exactly how an undecodable payload reaches a real queue, and our own
    // serializer could never produce one.
    ASSERT_TRUE(transport().send(motus_test::testTopic(), "no envelope here at all",
                                 producerOptions()));

    {
        motus::Producer producer(transport(), producerOptions());
        producer.declareTopic(motus_test::testTopic());
        ASSERT_TRUE(producer.publish(motus_test::testTopic(), messageWith(marker)));
        producer.flush();
    }

    auto worker = newTransport();
    ASSERT_NE(worker, nullptr);

    auto sawGood = std::make_shared<bool>(false);
    motus::Consumer consumer(*worker, [sawGood, marker](const motus::ByteMessage &task)
    {
        if (markerOf(task) == marker) *sawGood = true;
    });
    ASSERT_NO_THROW(consumer.start(motus_test::testTopic()));

    const auto deadline = std::chrono::steady_clock::now() + 15s;
    while (!*sawGood && std::chrono::steady_clock::now() < deadline)
        consumer.poll(250ms);

    const std::size_t rejected = consumer.rejected();
    worker->close(2s);

    EXPECT_GE(rejected, 1u) << "the malformed message was not rejected";
    EXPECT_TRUE(*sawGood) << "the worker did not recover: the good message never arrived";
}

/**
 * A message declaring a wire format version we do not understand must never reach the handler.
 *
 * Note what is asserted beyond "it was rejected": the payload is a perfectly well-formed
 * motus envelope in every respect except its version number. A consumer ignoring
 * format_version would decode it happily and hand the handler bytes whose layout means
 * something different from what they appear to mean -- far more dangerous than a decode error,
 * because nothing reports a problem.
 *
 * Production symptom if this regresses: after a wire-format change, a machine still running an
 * old snapshot publishes messages a newer consumer silently misreads. Everything keeps
 * running.
 */
TEST_P(ConformanceTest, MessageWithAnUnrecognisedFormatVersionNeverReachesTheHandler)
{
    const std::string foreign = uniqueMarker("v99");
    const std::string good    = uniqueMarker("v1");

    const std::string fromTheFuture =
        "motus:99:" + foreign + "\n// emitted by a publisher this build has never heard of\n";

    ASSERT_TRUE(transport().send(motus_test::testTopic(), fromTheFuture,
                                 producerOptions()));

    {
        motus::Producer producer(transport(), producerOptions());
        producer.declareTopic(motus_test::testTopic());
        ASSERT_TRUE(producer.publish(motus_test::testTopic(), messageWith(good)));
        producer.flush();
    }

    auto worker = newTransport();
    ASSERT_NE(worker, nullptr);

    auto seen = std::make_shared<std::vector<std::string>>();
    motus::Consumer consumer(*worker, [seen](const motus::ByteMessage &task)
    {
        seen->push_back(markerOf(task));
    });
    ASSERT_NO_THROW(consumer.start(motus_test::testTopic()));

    const auto deadline = std::chrono::steady_clock::now() + 15s;
    bool sawGood = false;
    while (!sawGood && std::chrono::steady_clock::now() < deadline)
    {
        consumer.poll(250ms);
        for (const auto &target : *seen)
            if (target == good) { sawGood = true; break; }
    }

    worker->close(2s);

    EXPECT_EQ(std::count(seen->begin(), seen->end(), foreign), 0)
        << "a message declaring format_version 99 was handed to the handler; the version check "
           "is not being applied";
    EXPECT_TRUE(sawGood) << "the worker stopped serving after the unrecognised message";
}

// ---------------------------------------------------------------------------
// Ordering
// ---------------------------------------------------------------------------

/**
 * A single consumer must see messages in publication order.
 *
 * Scope matters and is asserted nowhere else: this holds for ONE consumer. With competing
 * consumers it does not, and must not be relied on.
 *
 * Production symptom if this regresses: intermittent, unreproducible failures where a message
 * references something an earlier message was supposed to have established -- appearing only
 * under load.
 */
TEST_P(ConformanceTest, MessageOrderIsPreservedForASingleConsumer)
{
    MOTUS_REQUIRE_CAPABILITY_OR_SKIP(fifoPerTopic);

    const std::string marker = uniqueMarker("order");
    static constexpr std::size_t kCount = 10;

    {
        motus::Producer producer(transport(), producerOptions());
        producer.declareTopic(motus_test::testTopic());
        for (std::size_t i = 0; i < kCount; ++i)
            ASSERT_TRUE(producer.publish(motus_test::testTopic(),
                                         messageWith(marker, "field-" + std::to_string(i))))
                << "publish " << i << " was refused";
        producer.flush();
    }

    auto worker = newTransport();
    ASSERT_NE(worker, nullptr);

    auto order = std::make_shared<std::vector<std::string>>();
    motus::Consumer consumer(*worker, [order, marker](const motus::ByteMessage &task)
    {
        if (markerOf(task) == marker) order->push_back(contentOf(task));
    });
    ASSERT_NO_THROW(consumer.start(motus_test::testTopic()));

    const auto deadline = std::chrono::steady_clock::now() + 20s;
    while (order->size() < kCount && std::chrono::steady_clock::now() < deadline)
        consumer.poll(250ms);

    worker->close(2s);

    ASSERT_EQ(order->size(), kCount) << "only " << order->size() << " of " << kCount << " arrived";

    std::vector<std::string> expected;
    for (std::size_t i = 0; i < kCount; ++i) expected.push_back("field-" + std::to_string(i));

    EXPECT_EQ(*order, expected) << "messages arrived out of publication order";
}

// ---------------------------------------------------------------------------
// Competing consumers
// ---------------------------------------------------------------------------

namespace {

/// Everything one competing consumer produces. Assertions run on the main thread after the
/// join: GoogleTest's fatal assertions return from the enclosing function, which in a thread
/// body means abandoning the thread rather than failing the test.
struct WorkerResult
{
    std::vector<std::string> fields;
    std::string              error;
};

} // namespace

/**
 * With two workers on one topic, every message must reach exactly one of them -- and both
 * workers must actually receive work.
 *
 * The exactly-once half is the correctness guarantee. The both-received-work half is a
 * deliberate extra: it catches a backend where competing consumers silently degrade to a single
 * active consumer, which is not hypothetical -- SimpleAmqpClient's BasicConsume defaults to
 * exclusive=true, and taking that default would produce exactly this failure.
 *
 * The handler is deliberately slow. With prefetch=1 and instant processing, whichever worker
 * reaches the destination first can drain it before the other is scheduled, and asserting a
 * distribution would then be flaky rather than meaningful. A handler that takes real time makes
 * the consumers genuinely compete, which is the condition the assertion is about. The delay is
 * the mechanism under test, not padding.
 *
 * Fairness beyond "both got something" is NOT asserted, because no middleware promises it: a
 * lopsided split is valid behaviour and a future maintainer should not "strengthen" this into
 * something no backend guarantees.
 *
 * Production symptom if this regresses: with two workers running, either every message is
 * processed twice or half are never processed at all -- appearing only after someone scales
 * beyond one worker.
 */
TEST_P(ConformanceTest, EachMessageReachesExactlyOneOfTwoCompetingConsumersAndBothGetWork)
{
    MOTUS_REQUIRE_CAPABILITY_OR_SKIP(competingConsumers);

    const std::string marker = uniqueMarker("compete");
    static constexpr std::size_t kCount = 12;

    {
        motus::Producer producer(transport(), producerOptions());
        producer.declareTopic(motus_test::testTopic());
        for (std::size_t i = 0; i < kCount; ++i)
            ASSERT_TRUE(producer.publish(motus_test::testTopic(),
                                         messageWith(marker, "f" + std::to_string(i))));
        producer.flush(500ms);
    }

    std::atomic<std::size_t> progress{0};
    WorkerResult first, second;

    const auto body = [&](WorkerResult *result)
    {
        try
        {
            auto worker = motus::transport::makeTransport(GetParam());
            motus::transport::Endpoint endpoint;
            endpoint.host = "127.0.0.1";
            worker->connect(endpoint, 5s);

            motus::Consumer consumer(*worker, [&, result](const motus::ByteMessage &task)
            {
                if (markerOf(task) != marker) return;
                // Slow on purpose -- see the comment above.
                std::this_thread::sleep_for(40ms);
                result->fields.push_back(contentOf(task));
                progress.fetch_add(1);
            });
            consumer.start(motus_test::testTopic());

            const auto deadline = std::chrono::steady_clock::now() + 25s;
            while (progress.load() < kCount && std::chrono::steady_clock::now() < deadline)
                consumer.poll(200ms);

            worker->close(2s);
        }
        catch (const std::exception &e)
        {
            result->error = e.what();
        }
    };

    std::thread a(body, &first);
    std::thread b(body, &second);
    a.join();
    b.join();

    ASSERT_TRUE(first.error.empty())  << "consumer A failed: " << first.error;
    ASSERT_TRUE(second.error.empty()) << "consumer B failed: " << second.error;

    std::vector<std::string> all = first.fields;
    all.insert(all.end(), second.fields.begin(), second.fields.end());
    const std::set<std::string> unique(all.begin(), all.end());

    EXPECT_EQ(all.size(), unique.size())
        << "a message was delivered to both consumers: " << all.size() << " deliveries but "
        << unique.size() << " distinct messages. Competing consumers are duplicating work.";

    EXPECT_EQ(unique.size(), kCount)
        << "only " << unique.size() << " of " << kCount << " messages were received across both "
           "consumers (A got " << first.fields.size() << ", B got " << second.fields.size()
        << "). Messages are being lost between workers.";

    EXPECT_GT(first.fields.size(), 0u)
        << "consumer A received nothing; the second consumer never became active, which is what "
           "an exclusive subscription looks like";
    EXPECT_GT(second.fields.size(), 0u)
        << "consumer B received nothing; the second consumer never became active, which is what "
           "an exclusive subscription looks like";
}

// ---------------------------------------------------------------------------
// Observability
// ---------------------------------------------------------------------------

TEST_P(ConformanceTest, OneTransportRefusesASecondActiveSubscription)
{
    auto worker = newTransport();
    ASSERT_NE(worker, nullptr);
    worker->ensureTopic(motus_test::testTopic(), {}, 5s);
    ASSERT_NO_THROW(worker->subscribe(
        motus_test::testTopic(), [](const motus::transport::Delivery &) {
            return motus::Disposition::Ack;
        }, 5s));

    EXPECT_THROW(worker->subscribe(
        motus_test::testTopic(), [](const motus::transport::Delivery &) {
            return motus::Disposition::Ack;
        }, 5s), std::logic_error)
        << "a second subscription silently replaced the first; every one-transport-per-topic "
           "pooling scheme depends on this rule being enforced";
    worker->close(2s);
}

/**
 * The Consumer's counters must agree with what the transport actually performed.
 *
 * These two numbers are produced on opposite sides of the seam: Consumer counts what it
 * DECIDED, the transport counts what it PERFORMED. The redundancy is deliberate, and this is
 * the scenario it exists for -- it detects a backend that accepts a Reject and then silently
 * fails to send it. That is silent message loss, and no single counter could reveal it: both
 * sides would report the same comfortable number.
 *
 * Production symptom if this regresses: a worker discarding messages still reports a rising ack
 * count and zero rejections, so the loss is invisible until somebody notices output is
 * missing.
 */
TEST_P(ConformanceTest, ConsumerCountersAgreeWithWhatTheTransportPerformed)
{
    const std::string marker = uniqueMarker("counters");
    static constexpr std::size_t kGood = 4;

    {
        motus::Producer producer(transport(), producerOptions());
        producer.declareTopic(motus_test::testTopic());
        for (std::size_t i = 0; i < kGood; ++i)
            ASSERT_TRUE(producer.publish(motus_test::testTopic(), messageWith(marker)));
        // One undecodable payload, so the rejected counters are exercised too.
        ASSERT_TRUE(transport().send(motus_test::testTopic(), "}{", producerOptions()));
        producer.flush();
    }

    auto worker = newTransport();
    ASSERT_NE(worker, nullptr);

    auto handled = std::make_shared<std::size_t>(0);
    motus::Consumer consumer(*worker, [handled](const motus::ByteMessage &) { ++*handled; });
    ASSERT_NO_THROW(consumer.start(motus_test::testTopic()));

    const auto deadline = std::chrono::steady_clock::now() + 20s;
    while (*handled < kGood && std::chrono::steady_clock::now() < deadline)
        consumer.poll(250ms);

    const motus::transport::Stats stats = worker->stats();
    const std::size_t acked    = consumer.acked();
    const std::size_t rejected = consumer.rejected();
    worker->close(2s);

    ASSERT_GE(*handled, kGood) << "only " << *handled << " of " << kGood << " messages arrived";

    EXPECT_EQ(acked, stats.acksPerformed)
        << "the Consumer decided " << acked << " acknowledgement(s) but backend '"
        << backendName() << "' performed " << stats.acksPerformed
        << ". One of the two is not telling the truth about what happened to the messages.";

    EXPECT_EQ(rejected, stats.rejectsPerformed)
        << "the Consumer decided " << rejected << " rejection(s) but backend '" << backendName()
        << "' performed " << stats.rejectsPerformed
        << ". A rejection that is decided but never sent leaves the message unresolved.";

    EXPECT_EQ(acked, *handled) << "acked() disagrees with the number of handler invocations";
}

// ---------------------------------------------------------------------------
// Purge
// ---------------------------------------------------------------------------

/**
 * Purging a destination discards what is waiting there, and reports how much it discarded.
 *
 * A destination is durable and therefore ACCUMULATES: publishing the same message twice leaves
 * two copies behind, so depths climb every run. That is correct behaviour and it is also why an
 * explicit way to empty one has to exist -- otherwise the only remedies are deleting the queue
 * (losing its properties and bindings) or draining it through a consumer (slow, and it invents a
 * consumer nobody wanted).
 *
 * Production symptom if this regresses: a purge reports destinations emptied while they still
 * hold a previous run's messages, so a worker receives yesterday's payloads alongside today's
 * and has no way to tell them apart -- both are valid, both decode, and the older one may
 * arrive second.
 */
TEST_P(ConformanceTest, PurgingADestinationDiscardsWhatIsWaitingAndReportsHowMany)
{
    MOTUS_REQUIRE_CAPABILITY_OR_SKIP(purge);

    const std::string marker = uniqueMarker("purge");
    const std::string topic  = marker + ".q";

    {
        motus::Producer producer(transport(), producerOptions());
        producer.declareTopic(topic);
        for (int i = 0; i < 3; ++i)
            ASSERT_TRUE(producer.publish(topic, messageWith(marker + "-" + std::to_string(i))));
        producer.flush(1s);
    }

    // Asserted before purging, so a failure distinguishes "purge did nothing" from "nothing was
    // there to purge" -- which are different defects with the same visible symptom.
    ASSERT_EQ(transport().topicDepth(topic, 5s), 3u)
        << "the three published messages never reached the destination, so this scenario would "
           "pass for the wrong reason";

    const std::uint32_t discarded = transport().purgeTopic(topic, 5s);

    EXPECT_EQ(discarded, 3u)
        << "purge reported " << discarded << " message(s) discarded, not 3";
    EXPECT_EQ(transport().topicDepth(topic, 5s), 0u)
        << "the destination still holds messages after being purged";
}

/**
 * Purging an empty destination is not an error, and reports zero.
 *
 * A pre-run purge sweep runs unconditionally, so the very first run of an application purges
 * destinations that have never held anything. Throwing there would make the sweep unusable
 * exactly when it is least needed.
 */
TEST_P(ConformanceTest, PurgingAnEmptyDestinationSucceedsAndReportsZero)
{
    MOTUS_REQUIRE_CAPABILITY_OR_SKIP(purge);

    const std::string topic = uniqueMarker("purge-empty") + ".q";

    motus::Producer producer(transport(), producerOptions());
    producer.declareTopic(topic);

    std::uint32_t discarded = 1;
    ASSERT_NO_THROW(discarded = transport().purgeTopic(topic, 5s));
    EXPECT_EQ(discarded, 0u);
}

INSTANTIATE_TEST_SUITE_P(Backends,
                         ConformanceTest,
                         ::testing::ValuesIn(motus::transport::availableBackends()),
                         motus_test::BackendName());

} // namespace
