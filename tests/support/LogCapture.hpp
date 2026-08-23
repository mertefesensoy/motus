#pragma once

// Capture the process-wide logger into a vector for the duration of one test.
//
// WHY A TEST ASSERTS ON LOG LINES AT ALL, which is otherwise a smell: the log
// stream is the ONLY way a human outside a test binary can watch the scheduler's lease,
// preemption and cancellation actually happen -- and, since the exception-logging round, the only
// signal at all that a task failed, because `Outcome::Failed` is deliberately not counted in
// `TierMetrics` (ExecMetrics.hpp). Observability nobody tests is observability that silently
// stops working, and it stops working in precisely the situation it was built for, because
// nothing else reads it. So a transcript is a contract here, not decoration.
//
// Lives in tests/support/ rather than inside one test file because two suites now need it
// (AgentPipelineTest's scheduler transcript, TaskTest's failure line) -- the same rule the rest
// of this directory follows: helpers usable by ANY suite live here.

#include "motus/Logger.hpp"

#include <algorithm>
#include <cstddef>
#include <mutex>
#include <string>
#include <vector>

namespace motus::testing {

/**
 * Scoped, not a gtest fixture: the tests that need this are plain `TEST`s sharing no fixture.
 * Construct one at the top of a test; the destructor restores the previous level and the default
 * stderr sink even if an assertion fails.
 *
 * `snapshot()` flushes BEFORE taking `_mutex`, never while holding it -- `Logger::flush()` waits
 * for the logger's own worker thread to drain, and that worker is what calls the sink below.
 * Flushing under the lock would deadlock.
 */
class LogCapture
{
public:
    explicit LogCapture(LogLevel level)
        : _previousLevel(Logger::level())
    {
        Logger::flush();
        Logger::setSink([this](LogLevel, const std::string &line)
        {
            std::lock_guard<std::mutex> lock(_mutex);
            _lines.push_back(line);
        });
        Logger::setLevel(level);
        _droppedBefore = Logger::dropped();
    }

    ~LogCapture()
    {
        Logger::flush();
        Logger::setSink({});          // restore the default stderr sink
        Logger::setLevel(_previousLevel);
    }

    LogCapture(const LogCapture &) = delete;
    LogCapture &operator=(const LogCapture &) = delete;

    std::vector<std::string> snapshot() const
    {
        Logger::flush();
        std::lock_guard<std::mutex> lock(_mutex);
        return _lines;
    }

    /// Diagnostics are dropped rather than blocking application work when the bounded handoff
    /// fills. An assertion over a stream that dropped records would fail for a reason
    /// that has nothing to do with the code under test, so asserting this is zero turns "these
    /// lines were emitted" into "these lines were emitted AND survived".
    std::size_t droppedDuringCapture() const { return Logger::dropped() - _droppedBefore; }

private:
    mutable std::mutex _mutex;
    std::vector<std::string> _lines;
    LogLevel _previousLevel;
    std::size_t _droppedBefore = 0;
};

/**
 * Index of the first line at or after `from` containing every fragment in `fragments`, or -1.
 *
 * Matching is by SUBSEQUENCE and by fragment, not by whole-line equality against a fixed
 * transcript, and both choices are deliberate. Where several pool workers and dogs interleave,
 * only one dog's own lines are deterministically ordered -- the rest land in unpredictable
 * positions. And every line carries a timestamp, so exact equality could never work anyway.
 */
inline int findLineFrom(const std::vector<std::string> &lines, std::size_t from,
                        const std::vector<std::string> &fragments)
{
    for (std::size_t i = from; i < lines.size(); ++i)
    {
        const bool all = std::all_of(fragments.begin(), fragments.end(),
                                     [&](const std::string &fragment)
                                     { return lines[i].find(fragment) != std::string::npos; });
        if (all) return static_cast<int>(i);
    }
    return -1;
}

/// Renders `fragments` for a failure message: `a | b | c`.
inline std::string describeFragments(const std::vector<std::string> &fragments)
{
    std::string joined;
    for (const std::string &fragment : fragments)
        joined += (joined.empty() ? "" : " | ") + fragment;
    return joined;
}

} // namespace motus::testing
