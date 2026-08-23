// Unit tests for the logger. No broker, no filesystem -- the sink is replaced with an
// in-memory capture, which is the reason the sink is injectable at all.
//
// Logging is easy to get subtly wrong in ways nobody notices until an incident: a level
// filter off by one hides the very messages being hunted, and an eagerly evaluated argument
// makes "disabled" trace logging expensive enough to change the system's behaviour.

#include "motus/Logger.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

namespace {

struct CapturedLine
{
    motus::LogLevel level;
    std::string    text;
};

/// Redirects the logger into a vector for the duration of a test and restores the previous
/// level afterwards, so tests cannot leak state into each other.
class LoggerTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        _previousLevel = motus::Logger::level();
        lines.clear();
        motus::Logger::flush();
        motus::Logger::setSink([this](motus::LogLevel level, const std::string &text)
        {
            lines.push_back({level, text});
        });
        motus::Logger::setLevel(motus::LogLevel::Trace);
    }

    void TearDown() override
    {
        motus::Logger::flush();
        motus::Logger::setSink({});   // restore the default stderr sink
        motus::Logger::setLevel(_previousLevel);
    }

    std::vector<CapturedLine> lines;

private:
    motus::LogLevel _previousLevel = motus::LogLevel::Info;
};

TEST_F(LoggerTest, EmitsAtOrAboveTheConfiguredLevel)
{
    motus::Logger::setLevel(motus::LogLevel::Warn);

    MOTUS_LOG_TRACE("trace");
    MOTUS_LOG_DEBUG("debug");
    MOTUS_LOG_INFO("info");
    MOTUS_LOG_WARN("warn");
    MOTUS_LOG_ERROR("error");
    motus::Logger::flush();

    ASSERT_EQ(lines.size(), 2u) << "only warn and error should survive a Warn threshold";
    EXPECT_EQ(lines[0].level, motus::LogLevel::Warn);
    EXPECT_EQ(lines[1].level, motus::LogLevel::Error);
}

TEST_F(LoggerTest, OffSuppressesEverythingIncludingErrors)
{
    motus::Logger::setLevel(motus::LogLevel::Off);

    MOTUS_LOG_ERROR("this must not appear");
    motus::Logger::flush();

    EXPECT_TRUE(lines.empty());
}

TEST_F(LoggerTest, TraceLevelEmitsEveryLevel)
{
    MOTUS_LOG_TRACE("a");
    MOTUS_LOG_ERROR("b");
    motus::Logger::flush();

    EXPECT_EQ(lines.size(), 2u);
}

TEST_F(LoggerTest, DisabledLevelDoesNotEvaluateItsArgument)
{
    // The point of the macro form. If a disabled MOTUS_LOG_TRACE still evaluated its
    // arguments, leaving wire-level logging in the hot path would cost a hex dump per frame
    // even with tracing switched off (NFR-2).
    motus::Logger::setLevel(motus::LogLevel::Error);

    int evaluations = 0;
    const auto countingCall = [&evaluations] { ++evaluations; return "payload"; };

    MOTUS_LOG_TRACE(countingCall());
    MOTUS_LOG_DEBUG(countingCall());
    MOTUS_LOG_INFO(countingCall());

    EXPECT_EQ(evaluations, 0) << "arguments were evaluated for a disabled level";

    MOTUS_LOG_ERROR(countingCall());
    EXPECT_EQ(evaluations, 1) << "an enabled level must evaluate its argument exactly once";
    motus::Logger::flush();
}

TEST_F(LoggerTest, LineCarriesLevelNameAndMessage)
{
    MOTUS_LOG_WARN("queue depth is " << 42);
    motus::Logger::flush();

    ASSERT_EQ(lines.size(), 1u);
    EXPECT_NE(lines[0].text.find("WARN"), std::string::npos);
    EXPECT_NE(lines[0].text.find("queue depth is 42"), std::string::npos)
        << "actual: " << lines[0].text;
}

TEST_F(LoggerTest, LineIsTimestamped)
{
    MOTUS_LOG_INFO("anything");
    motus::Logger::flush();

    ASSERT_EQ(lines.size(), 1u);
    // Millisecond resolution is what makes an AMQP exchange readable; assert the shape
    // rather than a value, since the clock moves.
    EXPECT_NE(lines[0].text.find("20"), std::string::npos) << "actual: " << lines[0].text;
    EXPECT_NE(lines[0].text.find('.'), std::string::npos)
        << "expected sub-second precision, got: " << lines[0].text;
}

TEST_F(LoggerTest, MacroDoesNotSwallowAFollowingElse)
{
    // The macro expands to an if/else. Written carelessly it would bind a caller's `else`
    // to its own `if`, silently inverting control flow at the call site.
    bool tookElseBranch = false;

    if (false)
        MOTUS_LOG_INFO("not taken");
    else
        tookElseBranch = true;

    EXPECT_TRUE(tookElseBranch);
}

TEST_F(LoggerTest, BlockedDiagnosticSinkNeverBlocksCallers)
{
    std::promise<void> enteredPromise;
    auto entered = enteredPromise.get_future();
    std::promise<void> releasePromise;
    auto release = releasePromise.get_future().share();
    std::atomic<bool> announced{false};

    motus::Logger::setSink([&](motus::LogLevel, const std::string &)
    {
        if (!announced.exchange(true)) enteredPromise.set_value();
        release.wait();
    });

    MOTUS_LOG_WARN("occupy the diagnostic worker");
    ASSERT_EQ(entered.wait_for(1s), std::future_status::ready);

    const std::size_t beforeDropped = motus::Logger::dropped();
    const auto before = std::chrono::steady_clock::now();
    for (int i = 0; i < 1200; ++i) MOTUS_LOG_WARN("queued warning " << i);
    const auto elapsed = std::chrono::steady_clock::now() - before;

    EXPECT_LT(elapsed, 1s)
        << "logging waited on a blocked sink and could pin a broker delivery";
    EXPECT_GT(motus::Logger::dropped(), beforeDropped)
        << "the bounded handoff must discard diagnostics instead of blocking application work";

    releasePromise.set_value();
    motus::Logger::flush();
}

TEST_F(LoggerTest, ParseLevelAcceptsKnownNamesCaseInsensitively)
{
    motus::LogLevel level{};

    EXPECT_TRUE(motus::Logger::parseLevel("trace", level));
    EXPECT_EQ(level, motus::LogLevel::Trace);

    EXPECT_TRUE(motus::Logger::parseLevel("DEBUG", level));
    EXPECT_EQ(level, motus::LogLevel::Debug);

    EXPECT_TRUE(motus::Logger::parseLevel("WaRn", level));
    EXPECT_EQ(level, motus::LogLevel::Warn);

    EXPECT_TRUE(motus::Logger::parseLevel("warning", level));
    EXPECT_EQ(level, motus::LogLevel::Warn);

    EXPECT_TRUE(motus::Logger::parseLevel("off", level));
    EXPECT_EQ(level, motus::LogLevel::Off);
}

TEST_F(LoggerTest, ParseLevelRejectsUnknownNamesWithoutModifyingTheOutput)
{
    motus::LogLevel level = motus::LogLevel::Error;

    EXPECT_FALSE(motus::Logger::parseLevel("verbose", level));
    EXPECT_FALSE(motus::Logger::parseLevel("", level));
    EXPECT_EQ(level, motus::LogLevel::Error) << "a failed parse must not clobber the target";
}

TEST_F(LoggerTest, HexDumpRendersOffsetsHexAndPrintableAscii)
{
    const std::string data = "AMQP\x00\x00\x09\x01";
    const std::string dump = motus::hexDump(data.data(), 8);

    EXPECT_NE(dump.find("41 4d 51 50"), std::string::npos) << dump;  // "AMQP"
    EXPECT_NE(dump.find("AMQP"), std::string::npos) << dump;         // ASCII column
    EXPECT_NE(dump.find("0000"), std::string::npos) << dump;         // offset column
}

TEST_F(LoggerTest, HexDumpTruncatesAndSaysSo)
{
    const std::string large(500, 'x');
    const std::string dump = motus::hexDump(large.data(), large.size(), 32);

    EXPECT_NE(dump.find("more byte(s)"), std::string::npos)
        << "truncation must be visible, or a reader will think they saw the whole frame";
    EXPECT_NE(dump.find("468"), std::string::npos) << dump;   // 500 - 32
}

TEST_F(LoggerTest, HexDumpHandlesEmptyAndNullInput)
{
    EXPECT_EQ(motus::hexDump(nullptr, 0), "(empty)");
    EXPECT_EQ(motus::hexDump("abc", 0), "(empty)");
}

} // namespace
