// The wire envelope, exhaustively. No broker, no transport -- serialize() and deserialize()
// are pure functions over bytes, which is what makes every edge here testable in microseconds.
//
// The envelope carries the two properties the policy layer depends on: an explicit
// format_version that is rejected rather than guessed at, and a hard payload bound. Both are
// asserted from both directions.

#include "motus/ByteMessage.hpp"

#include <gtest/gtest.h>

#include <stdexcept>
#include <string>

namespace {

using motus::ByteMessage;

// ---------------------------------------------------------------------------
// Round trips
// ---------------------------------------------------------------------------

TEST(ByteMessage, RoundTripsOrdinaryText)
{
    ByteMessage message;
    message.payload = "an ordinary payload\nwith two lines";

    const ByteMessage back = ByteMessage::deserialize(message.serialize());
    EXPECT_EQ(back, message);
    EXPECT_EQ(back.byteCount(), message.payload.size());
}

TEST(ByteMessage, RoundTripsAnEmptyPayload)
{
    // A marker-only or heartbeat-shaped message is legitimate; emptiness must not be conflated
    // with malformedness.
    ByteMessage message;

    const ByteMessage back = ByteMessage::deserialize(message.serialize());
    EXPECT_EQ(back.byteCount(), 0u);
    EXPECT_EQ(back, message);
}

TEST(ByteMessage, RoundTripsEveryByteValue)
{
    // The reason the envelope is not JSON: arbitrary bytes -- NUL, newline, invalid UTF-8 --
    // must survive without base64 and without corruption.
    ByteMessage message;
    for (int value = 0; value < 256; ++value)
        message.payload.push_back(static_cast<char>(value));

    const ByteMessage back = ByteMessage::deserialize(message.serialize());
    EXPECT_EQ(back, message);
    EXPECT_EQ(back.byteCount(), 256u);
}

TEST(ByteMessage, RoundTripsAPayloadContainingAnEnvelopeLookalike)
{
    // The envelope is a PREFIX, never a scan: bytes that look like an envelope inside the
    // payload must arrive verbatim.
    ByteMessage message;
    message.payload = "motus:99:this is payload, not an envelope";

    const ByteMessage back = ByteMessage::deserialize(message.serialize());
    EXPECT_EQ(back, message);
}

TEST(ByteMessage, SerializeEmitsTheDocumentedPrefix)
{
    ByteMessage message;
    message.payload = "x";

    const std::string wire = message.serialize();
    EXPECT_EQ(wire, ByteMessage::envelopePrefix() + "x");
    EXPECT_EQ(ByteMessage::envelopePrefix(),
              "motus:" + std::to_string(ByteMessage::FORMAT_VERSION) + ":");
}

// ---------------------------------------------------------------------------
// Rejection: the version check
// ---------------------------------------------------------------------------

TEST(ByteMessage, DeserializeRejectsAMissingMagic)
{
    EXPECT_THROW(ByteMessage::deserialize("no envelope at all"), std::runtime_error);
    EXPECT_THROW(ByteMessage::deserialize(""), std::runtime_error);
    EXPECT_THROW(ByteMessage::deserialize("msgsea"), std::runtime_error);
}

TEST(ByteMessage, DeserializeRejectsAMissingVersionField)
{
    // Magic present, then no version -- the shape a truncated envelope takes.
    EXPECT_THROW(ByteMessage::deserialize("motus:"), std::runtime_error);
    EXPECT_THROW(ByteMessage::deserialize("motus:payload"), std::runtime_error);
    EXPECT_THROW(ByteMessage::deserialize("motus::payload"), std::runtime_error);
}

TEST(ByteMessage, DeserializeRejectsANonNumericVersion)
{
    EXPECT_THROW(ByteMessage::deserialize("motus:one:payload"), std::runtime_error);
    EXPECT_THROW(ByteMessage::deserialize("motus:1a:payload"), std::runtime_error);
    EXPECT_THROW(ByteMessage::deserialize("motus:-1:payload"), std::runtime_error);
}

TEST(ByteMessage, DeserializeRejectsEveryOtherVersionNamingBoth)
{
    // The message must name BOTH versions, so the operator can tell an old publisher from a
    // corrupted frame without a debugger.
    try
    {
        ByteMessage::deserialize("motus:99:payload");
        FAIL() << "format_version 99 was accepted";
    }
    catch (const std::runtime_error &e)
    {
        const std::string what = e.what();
        EXPECT_NE(what.find("99"), std::string::npos) << "actual: " << what;
        EXPECT_NE(what.find(std::to_string(ByteMessage::FORMAT_VERSION)), std::string::npos)
            << "actual: " << what;
    }

    EXPECT_THROW(ByteMessage::deserialize("motus:0:payload"), std::runtime_error)
        << "version 0 predates every real build and must be rejected, not defaulted";
}

TEST(ByteMessage, DeserializeRejectsAnAbsurdlyLongVersionField)
{
    EXPECT_THROW(ByteMessage::deserialize("motus:99999999999999999999:x"),
                 std::runtime_error);
}

// ---------------------------------------------------------------------------
// The payload bound, from both directions
// ---------------------------------------------------------------------------

TEST(ByteMessage, SerializeRefusesAnOversizedPayload)
{
    ByteMessage message;
    message.payload.assign(ByteMessage::MAX_PAYLOAD_BYTES, 'x');   // over once the prefix lands

    EXPECT_THROW(message.serialize(), std::length_error)
        << "the bound is on the ENCODED size; a payload of exactly MAX_PAYLOAD_BYTES plus the "
           "prefix must be refused";
}

TEST(ByteMessage, SerializeAcceptsAPayloadJustUnderTheBound)
{
    ByteMessage message;
    message.payload.assign(
        ByteMessage::MAX_PAYLOAD_BYTES - ByteMessage::envelopePrefix().size(), 'x');

    std::string wire;
    ASSERT_NO_THROW(wire = message.serialize());
    EXPECT_EQ(wire.size(), ByteMessage::MAX_PAYLOAD_BYTES);
}

TEST(ByteMessage, DeserializeRefusesAnOversizedBody)
{
    const std::string wire =
        ByteMessage::envelopePrefix() + std::string(ByteMessage::MAX_PAYLOAD_BYTES, 'x');

    EXPECT_THROW(ByteMessage::deserialize(wire), std::length_error);
}

} // namespace
