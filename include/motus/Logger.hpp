#pragma once

#include <cstddef>
#include <functional>
#include <sstream>
#include <string>

namespace motus {

/**
 * Severity levels, ordered. A logger set to Info emits Info, Warn and Error and discards
 * Debug and Trace.
 *
 *   Trace  wire-level detail: byte counts, hex dumps, individual AMQP frames.
 *          Verbose enough to slow the system down -- diagnostic use only.
 *   Debug  protocol events: queue declared, message published, ack sent.
 *   Info   lifecycle: connected, consuming, shutting down. The default.
 *   Warn   recoverable trouble: a rejected message, a retry.
 *   Error  the operation failed.
 *   Off    emit nothing.
 */
enum class LogLevel { Trace = 0, Debug, Info, Warn, Error, Off };

/// Receives fully formatted lines, including timestamp and level. Replaceable so that tests
/// can capture output and so a file or syslog sink can be added without touching call sites.
using LogSink = std::function<void(LogLevel, const std::string &)>;

/**
 * Process-wide logger.
 *
 * Static rather than injected because logging is a cross-cutting concern: threading a logger
 * reference through every constructor would distort interfaces that have nothing to do with
 * diagnostics. The cost is global state, which is mitigated by making the sink replaceable so
 * tests are not forced to read stderr.
 *
 * Thread-safe and non-blocking for callers. Complete records cross a bounded process-wide queue
 * to one sink thread, so a blocked diagnostic destination can never hold a broker delivery
 * unacknowledged. When that queue is full, newer diagnostic records are dropped rather than
 * applying backpressure to application work; dropped() exposes the cumulative count.
 */
class Logger
{
public:
    static void     setLevel(LogLevel level);
    static LogLevel level();

    /// True if `level` would currently be emitted. Cheap -- one atomic load, no formatting.
    static bool enabled(LogLevel level);

    /// Replace the destination. Passing an empty sink restores the default (stderr). The sink is
    /// invoked by the logger worker, never by the caller that emitted the record.
    static void setSink(LogSink sink);

    /// Emit a pre-formatted message. Prefer the MOTUS_LOG_* macros, which skip the work
    /// entirely when the level is disabled.
    static void write(LogLevel level, const std::string &message);

    /// Wait until all accepted records have reached the sink. Intended for deterministic tests
    /// and orderly tooling boundaries; production delivery paths must never call this.
    static void flush();

    /// Records discarded because the bounded diagnostic handoff was full or a sink failed.
    static std::size_t dropped();

    /// Parse "trace"/"debug"/"info"/"warn"/"error"/"off", case-insensitive.
    /// Returns false and leaves `out` untouched if unrecognised.
    static bool parseLevel(const std::string &text, LogLevel &out);

    /// Fixed-width name for formatting: "TRACE", "DEBUG", "INFO ", "WARN ", "ERROR".
    static const char *levelName(LogLevel level);

    /// Apply the MOTUS_LOG_LEVEL environment variable if present and valid.
    /// Returns false if the variable was set but not understood -- callers should treat that
    /// as a startup error rather than silently running at the default (NFR-1).
    static bool initFromEnvironment();
};

/**
 * Accumulates one line and emits it on destruction.
 *
 * Only constructed when the level is enabled, so a disabled MOTUS_LOG_TRACE costs a single
 * atomic comparison and never touches the heap -- which is what keeps trace logging
 * affordable to leave in the hot path (NFR-2).
 */
class LogRecord
{
public:
    explicit LogRecord(LogLevel level) : _level(level) {}
    ~LogRecord();

    LogRecord(const LogRecord &)            = delete;
    LogRecord &operator=(const LogRecord &) = delete;

    std::ostringstream &stream() { return _stream; }

private:
    LogLevel           _level;
    std::ostringstream _stream;
};

/**
 * Render bytes as offset + hex + printable ASCII, the standard layout for reading protocol
 * traffic. Truncates after `maxBytes` and says so, because a full AMQP frame can be large
 * enough to bury the part you are looking at.
 *
 * Intended for MOTUS_LOG_TRACE when diagnosing handshake or framing problems.
 */
std::string hexDump(const char *data, std::size_t size, std::size_t maxBytes = 64);

} // namespace motus

/**
 * Logging macros.
 *
 * The `if (!enabled) {} else` shape is deliberate: it guarantees the streaming expression is
 * never evaluated when the level is disabled, and the empty then-branch prevents the macro
 * from swallowing a following `else` at the call site.
 *
 *   MOTUS_LOG_INFO("connected to " << host << ':' << port);
 */
#define MOTUS_LOG_AT(level)                          \
    if (!::motus::Logger::enabled(level)) {} else    \
        ::motus::LogRecord(level).stream()

#define MOTUS_LOG_TRACE(expr) MOTUS_LOG_AT(::motus::LogLevel::Trace) << expr
#define MOTUS_LOG_DEBUG(expr) MOTUS_LOG_AT(::motus::LogLevel::Debug) << expr
#define MOTUS_LOG_INFO(expr)  MOTUS_LOG_AT(::motus::LogLevel::Info)  << expr
#define MOTUS_LOG_WARN(expr)  MOTUS_LOG_AT(::motus::LogLevel::Warn)  << expr
#define MOTUS_LOG_ERROR(expr) MOTUS_LOG_AT(::motus::LogLevel::Error) << expr
