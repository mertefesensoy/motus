#pragma once

// Backend selection.
//
// Two-stage by design:
//
//   CMake decides what a build CONTAINS   (MOTUS_WITH_* options)
//   the operator decides what it USES     (--transport / MOTUS_TRANSPORT)
//
// The split exists because not every build environment can obtain every dependency --
// SimpleAmqpClient is not packaged everywhere and needs a FetchContent clone, which an
// air-gapped or credential-restricted environment cannot perform. A machine that cannot fetch
// a backend's library must still build, so backends are compiled in conditionally rather than
// assumed.
//
// The pay-off is that one binary can demonstrate two backends back to back, which is the
// strongest available evidence that the abstraction is real rather than decorative.

#include "motus/transport/Config.hpp"
#include "motus/transport/ITransport.hpp"

#include <memory>
#include <string>
#include <vector>

namespace motus {
namespace transport {

/// Every backend motus knows how to name. A value being listed here does NOT mean this build
/// contains it -- see availableBackends().
enum class Backend
{
    AmqpCpp,     ///< AMQP-CPP over the hand-written Windows transport. The v1 default.
    InMemory,    ///< No middleware at all. Test builds only; never durable.
    SimpleAmqp,  ///< SimpleAmqpClient over rabbitmq-c.
};

/// Lower-case name as it appears on the command line: "amqpcpp", "inmemory", "simpleamqp".
const char *backendName(Backend backend);

/// The backend used when nothing is specified. Always AmqpCpp: it is the production-grade
/// default, and a default that silently changed with build options would be a trap.
constexpr Backend defaultBackend() { return Backend::AmqpCpp; }

/// Backends this build actually contains, in a stable order. Never empty in a supported
/// configuration. The conformance suite instantiates itself from exactly this list, so adding
/// a backend to CMake automatically runs the whole contract against it.
std::vector<Backend> availableBackends();

/// True if this build contains `backend`.
bool isAvailable(Backend backend);

/// Parse a command-line or environment value. Returns false on an unrecognised name, leaving
/// `out` untouched -- callers treat that as a startup error rather than falling back to a
/// default, because a typo silently selecting the wrong transport is precisely the class of
/// failure this design exists to prevent.
bool parseBackend(const std::string &text, Backend &out);

/// Comma-separated available backend names, for --help text and diagnostics.
std::string availableBackendList();

/**
 * Construct a transport.
 *
 * Throws std::runtime_error if `backend` is not compiled into this build, and the message
 * names the CMake option that would enable it. Being told "rebuild with
 * -DMOTUS_WITH_SIMPLEAMQP=ON" is the difference between a five-second fix and an afternoon.
 *
 * The returned transport is not connected; call connect() on it.
 */
std::unique_ptr<ITransport> makeTransport(Backend backend);

} // namespace transport
} // namespace motus
