#include "motus/Logger.hpp"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <deque>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <thread>
#include <utility>

namespace motus {
namespace {

// Function-local statics rather than namespace-scope objects: this guarantees construction
// before first use regardless of translation-unit initialisation order. A logger that is
// itself a victim of the static initialisation order fiasco is worse than no logger.

std::atomic<LogLevel> &currentLevel()
{
    static std::atomic<LogLevel> level{LogLevel::Info};
    return level;
}

/// "2026-07-27 11:45:03.123" -- local time, millisecond resolution.
/// Millisecond resolution matters here: AMQP round-trips complete in single-digit
/// milliseconds, so second-granularity stamps would collapse a whole exchange onto one line.
std::string timestamp()
{
    using namespace std::chrono;

    const auto now  = system_clock::now();
    const auto time = system_clock::to_time_t(now);
    const auto ms   = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;

    std::tm tm{};
#ifdef _WIN32
    localtime_s(&tm, &time);
#else
    localtime_r(&time, &tm);
#endif

    std::ostringstream out;
    out << std::put_time(&tm, "%Y-%m-%d %H:%M:%S")
        << '.' << std::setfill('0') << std::setw(3) << ms.count();
    return out.str();
}

/**
 * One bounded handoff for every diagnostic record in the process.
 *
 * The state is intentionally process-lifetime storage. Its worker may be blocked forever in an
 * OS stderr write when the parent stops draining a pipe; destroying and joining that worker at
 * static teardown would turn a clean return from main into a hang. The operating system reclaims
 * the worker and this small allocation when the process exits.
 */
class AsyncLogSink
{
public:
    AsyncLogSink()
    {
        std::thread([this] { run(); }).detach();
    }

    void setSink(LogSink sink)
    {
        const std::lock_guard<std::mutex> lock(_sinkMutex);
        _sink = std::move(sink);
    }

    void enqueue(LogLevel level, std::string line) noexcept
    {
        try
        {
            const std::lock_guard<std::mutex> lock(_queueMutex);
            if (_pending.size() >= kMaxPendingLines ||
                line.size() > kMaxPendingBytes - std::min(_pendingBytes, kMaxPendingBytes))
            {
                _dropped.fetch_add(1, std::memory_order_relaxed);
                return;
            }

            _pendingBytes += line.size();
            _pending.push_back({level, std::move(line)});
        }
        catch (...)
        {
            _dropped.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        _changed.notify_one();
    }

    void flush()
    {
        std::unique_lock<std::mutex> lock(_queueMutex);
        _drained.wait(lock, [this] { return _pending.empty() && !_inFlight; });
    }

    std::size_t dropped() const
    {
        return _dropped.load(std::memory_order_relaxed);
    }

private:
    struct Record
    {
        LogLevel level;
        std::string line;
    };

    static constexpr std::size_t kMaxPendingLines = 1024;
    static constexpr std::size_t kMaxPendingBytes = 1024u * 1024u;

    static void writeToStderr(const std::string &line)
    {
        // This call is allowed to block: it runs only on the diagnostic worker. fwrite avoids
        // touching iostream's process-wide locks while the CLI writes its human-facing banners.
        const std::string complete = line + '\n';
        if (std::fwrite(complete.data(), 1, complete.size(), stderr) != complete.size() ||
            std::fflush(stderr) != 0)
            throw std::runtime_error("stderr write failed");
    }

    LogSink sinkSnapshot()
    {
        const std::lock_guard<std::mutex> lock(_sinkMutex);
        return _sink;
    }

    void run()
    {
        for (;;)
        {
            Record record;
            {
                std::unique_lock<std::mutex> lock(_queueMutex);
                _changed.wait(lock, [this] { return !_pending.empty(); });
                record = std::move(_pending.front());
                _pending.pop_front();
                _pendingBytes -= record.line.size();
                _inFlight = true;
            }

            try
            {
                LogSink sink = sinkSnapshot();
                if (sink) sink(record.level, record.line);
                else writeToStderr(record.line);
            }
            catch (...)
            {
                _dropped.fetch_add(1, std::memory_order_relaxed);
            }

            {
                const std::lock_guard<std::mutex> lock(_queueMutex);
                _inFlight = false;
                if (_pending.empty()) _drained.notify_all();
            }
        }
    }

    std::mutex _queueMutex;
    std::condition_variable _changed;
    std::condition_variable _drained;
    std::deque<Record> _pending;
    std::size_t _pendingBytes = 0;
    bool _inFlight = false;
    std::atomic<std::size_t> _dropped{0};

    std::mutex _sinkMutex;
    LogSink _sink;   // empty == use the default stderr sink
};

AsyncLogSink &asyncSink()
{
    static AsyncLogSink *sink = new AsyncLogSink;
    return *sink;
}

} // namespace

void Logger::setLevel(LogLevel level)
{
    currentLevel().store(level, std::memory_order_relaxed);
}

LogLevel Logger::level()
{
    return currentLevel().load(std::memory_order_relaxed);
}

bool Logger::enabled(LogLevel level)
{
    const LogLevel threshold = currentLevel().load(std::memory_order_relaxed);
    if (threshold == LogLevel::Off) return false;
    return static_cast<int>(level) >= static_cast<int>(threshold);
}

void Logger::setSink(LogSink sink)
{
    asyncSink().setSink(std::move(sink));
}

void Logger::write(LogLevel level, const std::string &message)
{
    std::ostringstream line;
    line << timestamp() << " [" << levelName(level) << "] " << message;

    asyncSink().enqueue(level, line.str());
}

void Logger::flush()
{
    asyncSink().flush();
}

std::size_t Logger::dropped()
{
    return asyncSink().dropped();
}

const char *Logger::levelName(LogLevel level)
{
    switch (level)
    {
        case LogLevel::Trace: return "TRACE";
        case LogLevel::Debug: return "DEBUG";
        case LogLevel::Info:  return "INFO ";
        case LogLevel::Warn:  return "WARN ";
        case LogLevel::Error: return "ERROR";
        case LogLevel::Off:   return "OFF  ";
    }
    return "?????";
}

bool Logger::parseLevel(const std::string &text, LogLevel &out)
{
    std::string lowered = text;
    std::transform(lowered.begin(), lowered.end(), lowered.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });

    if      (lowered == "trace") out = LogLevel::Trace;
    else if (lowered == "debug") out = LogLevel::Debug;
    else if (lowered == "info")  out = LogLevel::Info;
    else if (lowered == "warn" || lowered == "warning") out = LogLevel::Warn;
    else if (lowered == "error") out = LogLevel::Error;
    else if (lowered == "off" || lowered == "none")     out = LogLevel::Off;
    else return false;

    return true;
}

bool Logger::initFromEnvironment()
{
    const char *value = std::getenv("MOTUS_LOG_LEVEL");
    if (value == nullptr) return true;   // unset is not an error; the default stands

    LogLevel parsed{};
    if (!parseLevel(value, parsed)) return false;

    setLevel(parsed);
    return true;
}

LogRecord::~LogRecord()
{
    // Destructors must not propagate exceptions. A logger that can abort the process it is
    // meant to be diagnosing would be worse than one that occasionally drops a line.
    try { Logger::write(_level, _stream.str()); } catch (...) {}
}

std::string hexDump(const char *data, std::size_t size, std::size_t maxBytes)
{
    if (data == nullptr || size == 0) return "(empty)";

    const std::size_t shown   = std::min(size, maxBytes);
    static constexpr std::size_t kPerLine = 16;

    std::ostringstream out;
    out << std::hex << std::setfill('0');

    for (std::size_t offset = 0; offset < shown; offset += kPerLine)
    {
        const std::size_t lineEnd = std::min(offset + kPerLine, shown);

        out << "\n  " << std::setw(4) << offset << "  ";

        for (std::size_t i = offset; i < offset + kPerLine; ++i)
        {
            if (i < lineEnd)
                out << std::setw(2)
                    << static_cast<unsigned>(static_cast<unsigned char>(data[i])) << ' ';
            else
                out << "   ";           // pad so the ASCII column stays aligned
        }

        out << " |";
        for (std::size_t i = offset; i < lineEnd; ++i)
        {
            const auto c = static_cast<unsigned char>(data[i]);
            out << (std::isprint(c) ? static_cast<char>(c) : '.');
        }
        out << '|';
    }

    if (size > shown)
        out << std::dec << "\n  ... " << (size - shown) << " more byte(s)";

    return out.str();
}

} // namespace motus
