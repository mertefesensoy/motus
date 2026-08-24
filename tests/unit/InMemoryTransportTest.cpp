// The in-memory transport, and through it the transport seam itself.
//
// These run in the UNIT suite because they need no broker, no socket and no filesystem -- which
// is itself the point being demonstrated. Until now, "does a message survive a round trip"
// could only be asked with RabbitMQ running. It can now be asked anywhere.
//
// Scope: this file tests the backend's own mechanics and the capability checks that guard it.
// The portable contract -- the scenarios every backend must satisfy -- is the conformance
// suite's job, and is where this backend gets held to the same bar as AMQP-CPP.
//
// Note on isolation: most tests here use a private topic, because the subject is the backend's
// mechanics rather than the contract and a private topic keeps a failure legible. That is NOT
// the conformance suite's arrangement, where the in-memory backend deliberately keeps state
// across tests exactly as the shared broker queue does, so that both backends run the identical
// scenario under identical conditions. The round-trip test below uses the shared test topic and
// a marker, mirroring the conformance arrangement on purpose.

#include "motus/ByteMessage.hpp"
#include "motus/Consumer.hpp"
#include "motus/Producer.hpp"
#include "support/TestTopic.hpp"

#include "motus/transport/backends/InMemoryTransport.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>

namespace {

using namespace std::chrono_literals;
using motus::transport::InMemoryTransport;
using motus::transport::SendOptions;

/// Unique per call, so tests cannot interfere through the process-wide registry.
std::string uniqueTopic(const std::string &prefix)
{
    static int sequence = 0;
    return "motus.test." + prefix + "." + std::to_string(++sequence);
}

/// A message whose payload leads with `marker` on its own line -- the same discriminator
/// arrangement the conformance suite uses.
motus::ByteMessage sampleMessage(const std::string &marker)
{
    motus::ByteMessage message;
    message.payload = marker + "\ntrackId";
    return message;
}

std::string markerOf(const motus::ByteMessage &message)
{
    return message.payload.substr(0, message.payload.find('\n'));
}

/// A connected in-memory transport. Loss is acceptable here and is stated out loud, which is
/// exactly the friction the capability model is meant to create.
std::unique_ptr<InMemoryTransport> connected()
{
    auto transport = std::make_unique<InMemoryTransport>();
    transport->connect({}, 1s);
    return transport;
}

// `const`, not `constexpr`: SendOptions gained a std::string `route` and is no longer a literal
// type. Worth noting rather than silently editing -- a struct crossing the seam that can no
// longer live in constant storage is a real, if small, cost of the per-message route override.
const SendOptions kLossy = [] {
    SendOptions options;
    options.durable = false;
    return options;
}();

// ---------------------------------------------------------------------------
// The capability model, doing its job
// ---------------------------------------------------------------------------

TEST(InMemoryTransport, ConsumerRefusesAnEmptyHandlerSet)
{
    InMemoryTransport transport;
    EXPECT_THROW((motus::Consumer{transport, motus::Handlers{}}), std::invalid_argument);
}

/**
 * A Producer with default options must REFUSE the in-memory backend.
 *
 * This is the capability model's first real customer and the reason it exists. The system this
 * library was extracted from published messages transient onto a durable queue for an entire
 * phase and nothing reported it: the capability was absent and the system behaved exactly as
 * though it were present. Any broker restart would have destroyed every queued message in
 * silence.
 *
 * Production symptom if this regresses: a backend that cannot persist is accepted without
 * complaint, and the loss is discovered only after a restart has already happened.
 */
TEST(InMemoryTransport, ProducerRefusesABackendThatCannotDeliverDurably)
{
    auto transport = connected();

    ASSERT_FALSE(transport->capabilities().durableDelivery)
        << "precondition: the in-memory backend must not claim durability";

    // Braces, not parentheses: `motus::Producer(*transport)` is parsed as a declaration of a
    // variable named `transport`, constructs nothing, and the EXPECT_THROW silently passes
    // nothing. The most vexing parse, costing a green test that tested nothing.
    EXPECT_THROW({ motus::Producer producer{*transport}; }, std::runtime_error)
        << "a Producer requiring durable delivery accepted a backend that cannot provide it";
}

/**
 * ...and must accept it when the caller states explicitly that loss is acceptable.
 *
 * The check is a gate, not a ban. What matters is that accepting loss is a visible decision at
 * the call site rather than a silent default.
 */
TEST(InMemoryTransport, ProducerAcceptsANonDurableBackendWhenLossIsStatedExplicitly)
{
    auto transport = connected();
    EXPECT_NO_THROW((motus::Producer{*transport, kLossy}));
}

/**
 * Requesting a capability the backend lacks must fail loudly at the point of use, not degrade.
 *
 * Production symptom if this regresses: a caller asks for publisher confirms, gets none, and
 * treats "send returned true" as "the middleware stored it" -- which is precisely the
 * distinction confirms exist to make.
 */
TEST(InMemoryTransport, SendingDurablyThroughANonDurableBackendThrows)
{
    auto transport = connected();
    SendOptions durable;
    EXPECT_THROW(transport->send(uniqueTopic("durable"), "{}", durable),
                 std::logic_error);
}

// ---------------------------------------------------------------------------
// Delivery, with no middleware anywhere
// ---------------------------------------------------------------------------

/**
 * A message published and consumed through the seam must arrive intact, with no broker.
 *
 * This is the proof that ITransport is genuinely middleware-neutral rather than AMQP wearing a
 * different vocabulary. Every concept the message layer uses -- topics, options, dispatch,
 * acknowledgement -- is honoured by an implementation that has never opened a socket.
 */
TEST(InMemoryTransport, PublishedMessageIsDeliveredIntactWithNoBroker)
{
    // The SHARED test topic, not a private one, with a unique marker -- the same arrangement
    // the conformance suite uses against a shared broker queue, mirrored deliberately.
    const std::string marker = "inmem-roundtrip";

    auto transport = connected();
    motus::Producer producer(*transport, kLossy);
    producer.declareTopic(motus_test::testTopic());
    ASSERT_TRUE(producer.publish(motus_test::testTopic(), sampleMessage(marker)));

    auto received = std::make_shared<std::unique_ptr<motus::ByteMessage>>();
    motus::Consumer consumer(*transport, [received, marker](const motus::ByteMessage &task)
    {
        if (markerOf(task) == marker)
            *received = std::make_unique<motus::ByteMessage>(task);
    });
    consumer.start(motus_test::testTopic());

    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (!*received && std::chrono::steady_clock::now() < deadline)
        consumer.poll(50ms);

    ASSERT_NE(*received, nullptr) << "the message never arrived";
    EXPECT_EQ(**received, sampleMessage(marker)) << "the message arrived corrupted";
    EXPECT_EQ(consumer.acked(), 1u);
}

/**
 * A malformed payload must be rejected without requeue and must not stall the subscriber.
 *
 * The same poison-message policy as the AMQP path, running through entirely different code
 * below the seam. If this behaved differently here, the policy would have leaked into a
 * backend instead of living above the seam where every backend shares one implementation.
 */
TEST(InMemoryTransport, MalformedPayloadIsRejectedWithoutRequeueAndDoesNotStall)
{
    const std::string topic = uniqueTopic("poison");

    auto transport = connected();
    transport->ensureTopic(topic, {}, 1s);
    transport->send(topic, "not an envelope", kLossy);
    transport->send(topic, sampleMessage("after-poison").serialize(), kLossy);

    auto seen = std::make_shared<int>(0);
    motus::Consumer consumer(*transport, [seen](const motus::ByteMessage &) { ++*seen; });
    consumer.start(topic);

    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (*seen == 0 && std::chrono::steady_clock::now() < deadline)
        consumer.poll(50ms);

    EXPECT_EQ(consumer.rejected(), 1u) << "the malformed payload was not rejected";
    EXPECT_EQ(*seen, 1) << "the subscriber did not recover: the good message never arrived";
    EXPECT_EQ(transport->topicDepth(topic, 1s), 0u)
        << "the rejected message was requeued rather than discarded, which is the infinite "
           "spin that reject-without-requeue exists to prevent";
    EXPECT_EQ(transport->topicDepth(motus::transport::deadLetterTopic(topic), 1s), 1u)
        << "the malformed payload was not retained for forensic inspection";
}

// ---------------------------------------------------------------------------
// Disposition::Requeue, and the bound on it
// ---------------------------------------------------------------------------

/**
 * A transient handler failure must return the message for another attempt rather than destroy
 * it, and the retry must be flagged as a redelivery.
 *
 * Production symptom if this regresses: a full disk or a locked output directory destroys a
 * message permanently, for a reason that would have cleared in seconds. Before Disposition
 * gained a third value, every handler failure was treated as permanent -- correct while
 * handlers only printed what arrived, wrong the moment they do real work.
 */
TEST(InMemoryTransport, TransientHandlerFailureRequeuesTheMessageAndMarksTheRetry)
{
    const std::string topic = uniqueTopic("transient");

    auto transport = connected();
    transport->ensureTopic(topic, {}, 1s);
    transport->send(topic, sampleMessage("transient").serialize(), kLossy);

    auto attempts = std::make_shared<int>(0);
    motus::Consumer consumer(*transport, [attempts](const motus::ByteMessage &)
    {
        ++*attempts;
        if (*attempts == 1) throw motus::TransientFailure("output directory is locked");
        // Second attempt succeeds, as a transient failure that has cleared would.
    });
    consumer.start(topic);

    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (consumer.acked() == 0 && std::chrono::steady_clock::now() < deadline)
        consumer.poll(50ms);

    EXPECT_EQ(*attempts, 2) << "the message was not offered a second time";
    EXPECT_EQ(consumer.requeued(), 1u) << "the transient failure did not requeue";
    EXPECT_EQ(consumer.acked(), 1u) << "the retry did not succeed";
    EXPECT_EQ(consumer.rejected(), 0u) << "the message was destroyed despite being retryable";
}

/**
 * A transient failure that persists across the retry must be rejected, not requeued forever.
 *
 * This is the bound, and it is the reason the redelivered flag is load-bearing rather than
 * merely informative: one bit already tracked by the middleware replaces a retry counter, a
 * timer and any state of our own.
 *
 * Production symptom if this regresses: a handler that always throws TransientFailure -- a disk
 * that stays full, a downstream that never returns -- pins one message in an infinite
 * redelivery loop. Throughput goes to zero and nothing behind it is ever processed, which is
 * exactly the poison-message spin reject-without-requeue was introduced to prevent.
 */
TEST(InMemoryTransport, TransientFailureThatPersistsAcrossARedeliveryIsRejected)
{
    const std::string topic = uniqueTopic("persistent");

    auto transport = connected();
    transport->ensureTopic(topic, {}, 1s);
    transport->send(topic, sampleMessage("persistent").serialize(), kLossy);

    auto attempts = std::make_shared<int>(0);
    motus::Consumer consumer(*transport, [attempts](const motus::ByteMessage &)
    {
        ++*attempts;
        throw motus::TransientFailure("the disk is still full");
    });
    consumer.start(topic);

    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (consumer.rejected() == 0 && std::chrono::steady_clock::now() < deadline)
        consumer.poll(50ms);

    EXPECT_EQ(*attempts, 2) << "the retry was not bounded to exactly one extra attempt";
    EXPECT_EQ(consumer.requeued(), 1u);
    EXPECT_EQ(consumer.rejected(), 1u) << "a permanently failing message was not given up on";
    EXPECT_EQ(transport->topicDepth(topic, 1s), 0u)
        << "the message is still in the topic and will be redelivered forever";
    EXPECT_EQ(transport->topicDepth(motus::transport::deadLetterTopic(topic), 1s), 1u)
        << "the permanently failing retry was dropped instead of dead-lettered";
}

// ---------------------------------------------------------------------------
// Topic declaration
// ---------------------------------------------------------------------------

/**
 * Declaring a topic twice with different properties must fail, exactly as AMQP answers a
 * conflicting redeclaration with 406 PRECONDITION_FAILED.
 *
 * A reference backend that is quietly more forgiving than the real one is worse than no
 * reference backend: a bug that breaks production would pass its conformance run. This is also
 * the trap SimpleAmqpClient walks straight into, since its DeclareQueue defaults are the
 * opposite of motus's queue on all three properties.
 */
TEST(InMemoryTransport, RedeclaringATopicWithDifferentPropertiesIsRefused)
{
    const std::string topic = uniqueTopic("mismatch");

    auto transport = connected();
    motus::transport::TopicOptions defaults;
    ASSERT_NO_THROW(transport->ensureTopic(topic, defaults, 1s));
    EXPECT_NO_THROW(transport->ensureTopic(topic, defaults, 1s))
        << "an identical redeclaration must be idempotent";

    auto conflicting = defaults;
    conflicting.durable = false;
    EXPECT_THROW(transport->ensureTopic(topic, conflicting, 1s), std::runtime_error)
        << "a conflicting redeclaration was accepted; whichever process starts second would "
           "then behave differently from the first, order-dependently";
}

/**
 * The forensic-pressure alarm.
 *
 * Forensic queues use `OverflowPolicy::RejectPublish` deliberately: the FIRST failures are the
 * diagnostically valuable ones, so a flood of later duplicates must not push them out. The cost
 * is that a FULL forensic queue means the broker is refusing new dead letters right now, and
 * every failure after the cap leaves no trace. forensicPressure() is what reports that cost.
 *
 * The classification lives in the library rather than inside any status command because
 * anything in a `main.cpp` is unreachable from a test binary. An alarm nobody can test is an
 * alarm nobody has checked fires.
 */
TEST(ForensicPressure, ClassifiesDepthAgainstTheCapThatActuallyShips)
{
    using motus::transport::forensicPressure;
    using motus::transport::ForensicPressure;
    static constexpr std::uint32_t kCap = motus::transport::kForensicMaxLength;

    // The shipped cap, asserted rather than assumed: the boundaries below are only meaningful
    // relative to it, and a silent change to it would move them without failing anything.
    EXPECT_EQ(kCap, 2000u);

    EXPECT_EQ(forensicPressure(0), ForensicPressure::Normal);
    EXPECT_EQ(forensicPressure(1), ForensicPressure::Normal);
    EXPECT_EQ(forensicPressure(kCap / 2), ForensicPressure::Normal);

    // 90% is the NearCap boundary, and it is inclusive. Checked either side by ONE message, because
    // an off-by-one in a threshold is exactly the defect that makes an alarm fire late.
    EXPECT_EQ(forensicPressure(1799), ForensicPressure::Normal);
    EXPECT_EQ(forensicPressure(1800), ForensicPressure::NearCap);
    EXPECT_EQ(forensicPressure(1999), ForensicPressure::NearCap);

    // AtCap is where evidence is ACTUALLY being lost, so it must not be reported as a mere warning.
    EXPECT_EQ(forensicPressure(kCap), ForensicPressure::AtCap);
    EXPECT_EQ(forensicPressure(kCap + 1), ForensicPressure::AtCap);
    EXPECT_EQ(forensicPressure(4000), ForensicPressure::AtCap);
}

TEST(ForensicPressure, AnUnboundedQueueIsNeverAtCapacity)
{
    using motus::transport::forensicPressure;
    using motus::transport::ForensicPressure;

    // `maxLength == 0` means unbounded everywhere else in TopicOptions, and the two must agree --
    // reporting an unbounded queue as full would send an operator purging evidence for no reason.
    EXPECT_EQ(forensicPressure(0, 0), ForensicPressure::Normal);
    EXPECT_EQ(forensicPressure(1'000'000, 0), ForensicPressure::Normal);
}

/// A custom cap must move both boundaries with it, not just the upper one.
TEST(ForensicPressure, TheBoundariesScaleWithTheCapRatherThanBeingHardcoded)
{
    using motus::transport::forensicPressure;
    using motus::transport::ForensicPressure;

    EXPECT_EQ(forensicPressure(89, 100), ForensicPressure::Normal);
    EXPECT_EQ(forensicPressure(90, 100), ForensicPressure::NearCap);
    EXPECT_EQ(forensicPressure(100, 100), ForensicPressure::AtCap);

    EXPECT_EQ(forensicPressure(8, 10), ForensicPressure::Normal);
    EXPECT_EQ(forensicPressure(9, 10), ForensicPressure::NearCap);
    EXPECT_EQ(forensicPressure(10, 10), ForensicPressure::AtCap);
}

} // namespace
