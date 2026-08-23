// Boundaries and payload fidelity -- the portable half.
//
// Every scenario here runs against every backend. Payload fidelity is a guarantee any transport
// must give: a payload containing a quote, a newline, a NUL or non-ASCII text has to arrive as
// it left, whatever middleware carried it. That is exactly the kind of thing a hand-written
// serializer or a naive byte pipe gets wrong, and it is cheap to check.
//
// The two heavy scenarios -- a 3,000-line payload and a payload just under the 4 MiB bound --
// are here too, deliberately. Against AMQP-CPP they exercise frame splitting and the buffered
// re-parse in pumpOnce(); against the in-memory backend they exercise nothing about framing,
// because it has no frames. They still earn their place: a backend that mishandles megabyte
// payloads for its own reasons -- an allocation bug, a copy that truncates -- fails here, and
// the cost is a few seconds.
//
// The trade-off was accepted knowingly: `ctest -L unit` grows by those seconds, and a fast set
// that people stop running casually would undo the point of having one. If that ever happens,
// these two are the first candidates to move back to AMQP-only.

#include "ConformanceFixture.hpp"

#include "motus/ByteMessage.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

using namespace std::chrono_literals;
using motus_test::ConformanceTest;
using motus_test::contentOf;
using motus_test::markedMessage;
using motus_test::uniqueMarker;

using ScaleConformanceTest = ConformanceTest;

// ---------------------------------------------------------------------------
// Synthetic payload generation
// ---------------------------------------------------------------------------

/// One deterministic, realistically shaped line: `columns` tab-separated fields whose content
/// varies by BOTH row and column, so truncation, reordering or duplication cannot cancel out
/// and pass a size-only check.
std::string syntheticLine(std::uint32_t row, std::uint16_t columns)
{
    std::string line;
    for (std::uint16_t c = 0; c < columns; ++c)
    {
        if (c != 0) line += '\t';
        line += "r" + std::to_string(row) + "c" + std::to_string(c) + "-v" +
                std::to_string((row * 31u + c * 7u) % 1000u);
    }
    return line;
}

/// A message whose ENCODED size lands close to (and, when `atLeast`, never under) `target`
/// bytes, built from synthetic structured lines rather than a single repeated character --
/// a compressible, uniform payload is precisely the shape that lets a truncation bug hide.
motus::ByteMessage messageOfApproximateSize(std::size_t target, const std::string &marker)
{
    motus::ByteMessage message;
    message.payload = marker + "\n";

    const std::size_t overhead = motus::ByteMessage::envelopePrefix().size();
    for (std::uint32_t row = 0; message.payload.size() + overhead < target; ++row)
        message.payload += syntheticLine(row, 7) + "\n";

    return message;
}

// ---------------------------------------------------------------------------
// Payload fidelity
// ---------------------------------------------------------------------------

/**
 * Payloads containing quotes, backslashes, newlines, tabs, control bytes and non-ASCII text
 * must survive byte-identical.
 *
 * Each of these has a specific way to break: a quote or backslash breaks naive escaping, a
 * newline breaks anything line-oriented, and non-ASCII breaks anything assuming one byte is one
 * character. Non-ASCII content is not hypothetical for any real deployment.
 *
 * The envelope lookalike is the interesting one: a payload that CONTAINS the bytes
 * `motus:99:` must not confuse decoding, because the envelope is a prefix, never a scan.
 *
 * Production symptom if this regresses: a payload with a quoted value or accented text is
 * rejected as a poison message and discarded, and the operator sees only "rejected without
 * requeue" with no indication which bytes were responsible.
 */
TEST_P(ScaleConformanceTest, AwkwardPayloadContentSurvivesTheRoundTrip)
{
    const motus::ByteMessage message = markedMessage(
        uniqueMarker("awkward"),
        "// quote \" inside\n"
        "// backslash \\ and \\\" both\n"
        "// Turkish: ISIL izleme, ucus rotasi\n"
        "line one\nline two\r\nline three\n"
        "col\tcol\tcol\n"
        + std::string("nul-free\x01\x02 control\n")
        + "an envelope lookalike inside the payload: motus:99:not-an-envelope\n");

    const auto arrived = roundTrip(message);

    ASSERT_NE(arrived, nullptr) << "the message never arrived";
    EXPECT_EQ(*arrived, message) << "awkward payload content was altered in transit";
}

/**
 * Arbitrary binary bytes -- every value 0x00 through 0xFF, NUL and newline included -- must
 * survive byte-identical.
 *
 * This is the property the envelope design exists for: a JSON envelope would force base64 or
 * corrupt exactly these payloads, and a transport or policy layer that quietly treats the body
 * as text (strlen anywhere, a codepage conversion, CRLF translation) fails here and nowhere
 * else.
 *
 * Production symptom if this regresses: text payloads work everywhere while protobuf, CBOR or
 * compressed payloads arrive corrupted -- and only sometimes, depending on their bytes.
 */
TEST_P(ScaleConformanceTest, ArbitraryBinaryBytesSurviveTheRoundTrip)
{
    std::string binary;
    for (int repeat = 0; repeat < 16; ++repeat)
        for (int value = 0; value < 256; ++value)
            binary.push_back(static_cast<char>(value));

    const motus::ByteMessage message = markedMessage(uniqueMarker("binary"), binary);

    const auto arrived = roundTrip(message);

    ASSERT_NE(arrived, nullptr) << "the binary message never arrived";
    ASSERT_EQ(arrived->byteCount(), message.byteCount())
        << "the binary payload changed size in transit";
    EXPECT_EQ(*arrived, message) << "binary bytes were altered in transit";
}

/**
 * A single very large payload must survive. The size bound is on the whole encoded message,
 * and nothing caps content below it.
 *
 * Production symptom if this regresses: the largest real payloads arrive truncated, while
 * every small test payload keeps working.
 */
TEST_P(ScaleConformanceTest, AVeryLargeSinglePayloadSurvivesTheRoundTrip)
{
    constexpr std::size_t kContentBytes = 1024u * 1024u;

    const motus::ByteMessage message =
        markedMessage(uniqueMarker("longfile"), std::string(kContentBytes, 'A'));

    const auto arrived = roundTrip(message, 45s);

    ASSERT_NE(arrived, nullptr) << "a message with a 1 MiB payload never arrived";
    EXPECT_EQ(arrived->byteCount(), message.byteCount())
        << "the payload arrived at " << arrived->byteCount() << " bytes, not "
        << message.byteCount();
}

/**
 * An empty content section must round-trip as a valid message rather than failing or being
 * mistaken for a malformed payload.
 *
 * Not hypothetical: producers legitimately emit heartbeat-shaped or marker-only messages whose
 * substance is their arrival, not their bytes.
 *
 * Production symptom if this regresses: an empty payload becomes a poison message that is
 * rejected and discarded, so the consumer silently misses an event rather than receiving an
 * empty one.
 */
TEST_P(ScaleConformanceTest, AnEmptyContentSectionRoundTripsAsZeroBytes)
{
    const motus::ByteMessage message = markedMessage(uniqueMarker("empty"));

    const auto arrived = roundTrip(message);

    ASSERT_NE(arrived, nullptr)
        << "an empty-content message was rejected as undeliverable rather than delivered";
    EXPECT_TRUE(contentOf(*arrived).empty());
    EXPECT_EQ(*arrived, message);
}

// ---------------------------------------------------------------------------
// The 4 MiB payload bound, from both sides
// ---------------------------------------------------------------------------

/**
 * A payload just under MAX_PAYLOAD_BYTES must survive the whole path.
 *
 * What this proves differs by backend, and that is worth being explicit about rather than
 * pretending the test means the same thing everywhere. Against AMQP-CPP it exercises frame
 * splitting and the buffered re-parse in pumpOnce(), which is quadratic in the worst case and
 * is the real risk. Against the in-memory backend there are no frames, so it proves only that
 * megabyte payloads are carried without truncation or an allocation failure -- a weaker claim,
 * but not a worthless one.
 *
 * Production symptom if this regresses: the largest real payloads -- the ones that matter most
 * -- fail or hang, while every small test payload keeps working.
 */
TEST_P(ScaleConformanceTest, PayloadJustUnderTheFourMebibyteBoundArrivesIntact)
{
    const motus::ByteMessage message = messageOfApproximateSize(
        motus::ByteMessage::MAX_PAYLOAD_BYTES - (128u * 1024u), uniqueMarker("under4m"));

    const std::size_t payloadSize = message.serialize().size();
    ASSERT_LT(payloadSize, motus::ByteMessage::MAX_PAYLOAD_BYTES)
        << "test setup produced an oversized payload; adjust the target size";
    ASSERT_GT(payloadSize, 3u * 1024u * 1024u)
        << "test setup produced a payload too small to exercise the bound";

    const auto started = std::chrono::steady_clock::now();
    const auto arrived = roundTrip(message, 60s);
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - started);

    ASSERT_NE(arrived, nullptr)
        << "a " << payloadSize << " byte payload never arrived (waited 60s)";

    EXPECT_EQ(arrived->byteCount(), message.byteCount())
        << "the payload arrived at a different size than it was sent with";
    EXPECT_EQ(*arrived, message) << "a large payload arrived corrupted";

    // Not an assertion on throughput -- no backend promises one -- but a multi-minute round trip
    // would point at the quadratic re-parse, and a number on the record beats a feeling.
    std::cout << "[ scale    ] " << backendName() << ": " << payloadSize
              << " byte round trip took " << elapsed.count() << " ms\n";
}

/**
 * A payload over the bound must be refused before anything reaches the transport.
 *
 * MAX_PAYLOAD_BYTES is a sanity bound, not a protocol limit -- "a multi-megabyte payload means
 * something unintended was read". The important half of that contract is that it fails loudly
 * and EARLY: a truncated payload would be processed as something subtly different, and a
 * partially written payload would corrupt the connection for every later message.
 *
 * The bytesSent assertion is the early half. Rejecting the payload is not enough if some of it
 * has already gone out.
 *
 * Production symptom if this regresses: pointing a producer at the wrong input produces either
 * silently truncated payloads, or a connection that dies mid-message and takes the whole
 * session with it.
 */
TEST_P(ScaleConformanceTest, PayloadOverTheFourMebibyteBoundIsRefusedBeforeReachingTheTransport)
{
    const motus::ByteMessage message = messageOfApproximateSize(
        motus::ByteMessage::MAX_PAYLOAD_BYTES + (256u * 1024u), uniqueMarker("over4m"));

    motus::Producer producer(transport(), producerOptions());
    producer.declareTopic(motus_test::testTopic());

    const std::uint64_t before = transport().stats().bytesSent;

    EXPECT_THROW(producer.publish(motus_test::testTopic(), message), std::length_error)
        << "an oversized payload was accepted for publishing";

    EXPECT_EQ(transport().stats().bytesSent, before)
        << "an oversized payload was rejected, but "
        << (transport().stats().bytesSent - before)
        << " byte(s) had already been written. A partial payload leaves the transport unusable "
           "for every subsequent message.";

    EXPECT_EQ(producer.published(), 0u) << "a refused publish was counted as published";
}

// ---------------------------------------------------------------------------
// A large structured payload, line for line
// ---------------------------------------------------------------------------

/**
 * A payload three orders of magnitude larger than a typical message must arrive reconstructed
 * line-for-line -- 3,000 rows of 7 tab-separated fields, every field distinct.
 *
 * The payload is generated rather than committed: a multi-megabyte fixture cannot be reviewed
 * in a diff, and a deterministic generator lets the receiving side regenerate the expectation
 * instead of trusting a copy.
 *
 * Production symptom if this regresses: a real bulk payload is delivered truncated or
 * corrupted in its middle rows. Everything still decodes, and the output is silently wrong --
 * the worst failure a messaging layer can have.
 */
TEST_P(ScaleConformanceTest, LargeGeneratedPayloadRoundTripsLineForLine)
{
    constexpr std::uint32_t kRows    = 3000;
    constexpr std::uint16_t kColumns = 7;

    std::vector<std::string> lines;
    lines.reserve(kRows);
    for (std::uint32_t r = 0; r < kRows; ++r)
        lines.push_back(syntheticLine(r, kColumns));

    motus::ByteMessage message = markedMessage(uniqueMarker("large"));
    for (const std::string &line : lines)
        message.payload += line + "\n";

    const auto arrived = roundTrip(message, 60s);

    ASSERT_NE(arrived, nullptr)
        << "a " << message.serialize().size() << " byte payload was published but never arrived";
    ASSERT_EQ(arrived->byteCount(), message.byteCount())
        << "the payload arrived at a different size than it was sent";

    // Name the first differing LINE rather than leaving a reader to diff 3,000 of them. A single
    // EXPECT_EQ on the whole content would print two megabytes on failure and say nothing.
    const std::string body = contentOf(*arrived);
    std::size_t offset = 0;
    for (std::uint32_t r = 0; r < kRows; ++r)
    {
        const std::string expected = lines[r] + "\n";
        ASSERT_EQ(body.compare(offset, expected.size(), expected), 0)
            << "first corrupted line is row " << (r + 1);
        offset += expected.size();
    }
}

INSTANTIATE_TEST_SUITE_P(Backends,
                         ScaleConformanceTest,
                         ::testing::ValuesIn(motus::transport::availableBackends()),
                         motus_test::BackendName());

} // namespace
