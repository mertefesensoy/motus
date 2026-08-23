// Unit tests for the poison-message policy. No broker, no filesystem.
//
// motus::dispatch() decides ack vs reject for one raw message body. Getting this wrong is
// expensive in a way that is hard to spot: reject-with-requeue on a deterministic failure
// puts the worker in an infinite loop against the same message, and acking a message that
// was never processed loses work silently. Both are asserted here, along with the bounded
// transient-failure retry and the deferred hand-off.

#include "motus/ByteMessage.hpp"
#include "motus/Consumer.hpp"

#include <gtest/gtest.h>

#include <stdexcept>
#include <string>
#include <vector>

namespace {

/// Records what the handler was asked to do, so tests can assert on it.
struct HandlerSpy
{
    std::vector<motus::ByteMessage> received;
    bool                              shouldThrow  = false;
    std::string                       throwMessage = "handler failed";

    motus::MessageHandler asHandler()
    {
        return [this](const motus::ByteMessage &message)
        {
            received.push_back(message);
            if (shouldThrow) throw std::runtime_error(throwMessage);
        };
    }
};

const std::string kContent = "struct RadarTrack { std::uint32_t trackId{}; };\n";

std::string validPayload()
{
    motus::ByteMessage message;
    message.payload = kContent;
    return message.serialize();
}

TEST(Dispatch, ValidPayloadIsAcked)
{
    HandlerSpy spy;
    const auto result = motus::dispatch(validPayload(), spy.asHandler());

    EXPECT_EQ(result.disposition, motus::Disposition::Ack);
    EXPECT_TRUE(result.error.empty()) << "an acked message must carry no error";
}

TEST(Dispatch, ValidPayloadReachesTheHandlerIntact)
{
    HandlerSpy spy;
    motus::dispatch(validPayload(), spy.asHandler());

    ASSERT_EQ(spy.received.size(), 1u);

    // The substance: the handler receives the payload BYTES, with the envelope removed --
    // never something it would have to go and fetch.
    EXPECT_EQ(spy.received.front().payload, kContent);
    EXPECT_EQ(spy.received.front().byteCount(), kContent.size());
}

TEST(Dispatch, BodyWithoutAnEnvelopeIsRejectedAndNeverReachesTheHandler)
{
    HandlerSpy spy;
    const auto result = motus::dispatch("this is not an envelope", spy.asHandler());

    EXPECT_EQ(result.disposition, motus::Disposition::Reject);
    EXPECT_FALSE(result.error.empty()) << "a rejection must explain itself";
    EXPECT_TRUE(spy.received.empty()) << "the handler must not see an undecodable message";
}

TEST(Dispatch, EnvelopeWithNoVersionFieldIsRejected)
{
    HandlerSpy spy;
    const auto result = motus::dispatch("motus:payload-without-a-version", spy.asHandler());

    EXPECT_EQ(result.disposition, motus::Disposition::Reject);
    EXPECT_TRUE(spy.received.empty());
}

TEST(Dispatch, UnknownFormatVersionIsRejected)
{
    // A message from a newer publisher must be refused rather than misread. Without this,
    // a durable queue outliving a format change silently corrupts everything downstream.
    HandlerSpy spy;

    const std::string fromTheFuture =
        "motus:" + std::to_string(motus::ByteMessage::FORMAT_VERSION + 1) + ":payload";

    const auto result = motus::dispatch(fromTheFuture, spy.asHandler());

    EXPECT_EQ(result.disposition, motus::Disposition::Reject);
    EXPECT_TRUE(spy.received.empty()) << "the handler must not see a message it cannot interpret";
    EXPECT_NE(result.error.find("format_version"), std::string::npos)
        << "actual error was: " << result.error;
}

TEST(Dispatch, EmptyBodyIsRejected)
{
    HandlerSpy spy;
    EXPECT_EQ(motus::dispatch("", spy.asHandler()).disposition, motus::Disposition::Reject);
    EXPECT_TRUE(spy.received.empty());
}

TEST(Dispatch, OversizedBodyIsRejectedBeforeDecoding)
{
    HandlerSpy spy;
    const std::string huge(motus::ByteMessage::MAX_PAYLOAD_BYTES + 1, 'x');

    const auto result = motus::dispatch(huge, spy.asHandler());

    EXPECT_EQ(result.disposition, motus::Disposition::Reject);
    EXPECT_NE(result.error.find("byte limit"), std::string::npos)
        << "actual error was: " << result.error;
    EXPECT_TRUE(spy.received.empty());
}

TEST(Dispatch, HandlerFailureIsRejectedNotAcked)
{
    // The decodable-but-unprocessable case: a well-formed message the handler cannot process.
    // Acking here would silently drop real work.
    HandlerSpy spy;
    spy.shouldThrow  = true;
    spy.throwMessage = "unrecognised record layout in payload";

    const auto result = motus::dispatch(validPayload(), spy.asHandler());

    EXPECT_EQ(result.disposition, motus::Disposition::Reject);
    EXPECT_EQ(spy.received.size(), 1u) << "the handler should have been attempted";
}

TEST(Dispatch, HandlerFailureMessageIsPropagated)
{
    // The operator's only clue about why a message was dropped, so it must survive.
    HandlerSpy spy;
    spy.shouldThrow  = true;
    spy.throwMessage = "unrecognised record layout in payload";

    const auto result = motus::dispatch(validPayload(), spy.asHandler());

    EXPECT_NE(result.error.find("record layout"), std::string::npos)
        << "actual error was: " << result.error;
}

TEST(Dispatch, RepeatedFailuresStayDeterministic)
{
    // The property that justifies rejecting without requeue: the same poison message fails
    // identically every time, so a retry could only loop.
    HandlerSpy spy;

    for (int attempt = 0; attempt < 5; ++attempt)
        EXPECT_EQ(motus::dispatch("still not an envelope", spy.asHandler()).disposition,
                  motus::Disposition::Reject);

    EXPECT_TRUE(spy.received.empty());
}

// ---------------------------------------------------------------------------
// The bounded transient-failure retry
// ---------------------------------------------------------------------------

TEST(Dispatch, TransientFailureOnFirstDeliveryIsRequeued)
{
    const motus::MessageHandler handler = [](const motus::ByteMessage &)
    {
        throw motus::TransientFailure("downstream unavailable");
    };

    const auto result = motus::dispatch(validPayload(), handler, /*redelivered=*/false);

    EXPECT_EQ(result.disposition, motus::Disposition::Requeue)
        << "a transient failure on a first delivery deserves exactly one more attempt";
    EXPECT_NE(result.error.find("downstream unavailable"), std::string::npos);
}

TEST(Dispatch, TransientFailurePersistingAcrossARedeliveryIsRejected)
{
    // The bound. The middleware's own redelivered flag is the one bit of state required, which
    // is why no retry counter exists anywhere.
    const motus::MessageHandler handler = [](const motus::ByteMessage &)
    {
        throw motus::TransientFailure("downstream still unavailable");
    };

    const auto result = motus::dispatch(validPayload(), handler, /*redelivered=*/true);

    EXPECT_EQ(result.disposition, motus::Disposition::Reject)
        << "an unbounded requeue on a failure that is not actually transient recreates the "
           "infinite spin the policy exists to prevent";
}

// ---------------------------------------------------------------------------
// The deferred hand-off
// ---------------------------------------------------------------------------

TEST(Dispatch, DeferredHandlerReturnValueIsRecordedVerbatim)
{
    motus::Handlers handlers;
    motus::SettlementHandle seenTag = 0;
    handlers.onDeferred = [&seenTag](const motus::ByteMessage &,
                                     motus::SettlementHandle tag) -> motus::Disposition
    {
        seenTag = tag;
        return motus::Disposition::Deferred;
    };

    const auto result = motus::dispatch(validPayload(), handlers, false, /*deliveryTag=*/42);

    EXPECT_EQ(result.disposition, motus::Disposition::Deferred)
        << "dispatch() must record exactly what a deferred handler returned";
    EXPECT_EQ(seenTag, 42u) << "the raw transport tag must reach the deferred handler, or it "
                               "has no way to settle the delivery later";
}

TEST(Dispatch, DeferredHandlerTakesPriorityOverThePlainHandler)
{
    motus::Handlers handlers;
    bool plainRan = false, deferredRan = false;
    handlers.onMessage  = [&plainRan](const motus::ByteMessage &) { plainRan = true; };
    handlers.onDeferred = [&deferredRan](const motus::ByteMessage &,
                                         motus::SettlementHandle) -> motus::Disposition
    {
        deferredRan = true;
        return motus::Disposition::Ack;
    };

    motus::dispatch(validPayload(), handlers);

    EXPECT_TRUE(deferredRan);
    EXPECT_FALSE(plainRan) << "one delivery must reach exactly one handler";
}

TEST(Dispatch, NoConfiguredHandlerRejects)
{
    const auto result = motus::dispatch(validPayload(), motus::Handlers{});

    EXPECT_EQ(result.disposition, motus::Disposition::Reject);
    EXPECT_NE(result.error.find("no handler"), std::string::npos)
        << "actual error was: " << result.error;
}

} // namespace
