#include "motus/transport/Factory.hpp"

#include <stdexcept>

#ifdef MOTUS_WITH_AMQPCPP
#  include "motus/transport/backends/AmqpCppTransport.hpp"
#endif
#ifdef MOTUS_WITH_INMEMORY
#  include "motus/transport/backends/InMemoryTransport.hpp"
#endif
#ifdef MOTUS_WITH_SIMPLEAMQP
#  include "motus/transport/backends/SimpleAmqpTransport.hpp"
#endif

namespace motus {
namespace transport {
namespace {

/// The CMake option that would make a backend available, named in the diagnostic when it is
/// not. Being told exactly what to turn on is the difference between a five-second fix and an
/// afternoon of guessing -- the same reasoning that put the numeric code next to every socket
/// error message in this project.
const char *cmakeOptionFor(Backend backend)
{
    switch (backend)
    {
        case Backend::AmqpCpp:    return "MOTUS_WITH_AMQPCPP";
        case Backend::InMemory:   return "MOTUS_WITH_INMEMORY";
        case Backend::SimpleAmqp: return "MOTUS_WITH_SIMPLEAMQP";
    }
    return "MOTUS_WITH_UNKNOWN";
}

} // namespace

const char *backendName(Backend backend)
{
    switch (backend)
    {
        case Backend::AmqpCpp:    return "amqpcpp";
        case Backend::InMemory:   return "inmemory";
        case Backend::SimpleAmqp: return "simpleamqp";
    }
    return "unknown";
}

bool isAvailable(Backend backend)
{
    switch (backend)
    {
#ifdef MOTUS_WITH_AMQPCPP
        case Backend::AmqpCpp:    return true;
#endif
#ifdef MOTUS_WITH_INMEMORY
        case Backend::InMemory:   return true;
#endif
#ifdef MOTUS_WITH_SIMPLEAMQP
        case Backend::SimpleAmqp: return true;
#endif
        default: return false;
    }
}

std::vector<Backend> availableBackends()
{
    std::vector<Backend> available;
    for (const Backend candidate : {Backend::AmqpCpp, Backend::InMemory, Backend::SimpleAmqp})
        if (isAvailable(candidate)) available.push_back(candidate);
    return available;
}

bool parseBackend(const std::string &text, Backend &out)
{
    for (const Backend candidate : {Backend::AmqpCpp, Backend::InMemory, Backend::SimpleAmqp})
    {
        if (text == backendName(candidate))
        {
            out = candidate;
            return true;
        }
    }
    // Deliberately no fallback to the default. A typo silently selecting a different transport
    // is precisely the class of failure this design exists to prevent, so an unrecognised name
    // is a startup error for the caller to report.
    return false;
}

std::string availableBackendList()
{
    std::string list;
    for (const Backend backend : availableBackends())
    {
        if (!list.empty()) list += ", ";
        list += backendName(backend);
    }
    return list.empty() ? "(none)" : list;
}

std::unique_ptr<ITransport> makeTransport(Backend backend)
{
    switch (backend)
    {
#ifdef MOTUS_WITH_AMQPCPP
        case Backend::AmqpCpp:
            return std::make_unique<AmqpCppTransport>();
#endif
#ifdef MOTUS_WITH_INMEMORY
        case Backend::InMemory:
            return std::make_unique<InMemoryTransport>();
#endif
#ifdef MOTUS_WITH_SIMPLEAMQP
        case Backend::SimpleAmqp:
            return std::make_unique<SimpleAmqpTransport>();
#endif
        default:
            break;
    }

    throw std::runtime_error(
        std::string("transport backend '") + backendName(backend) +
        "' is not in this build -- reconfigure with -D" + cmakeOptionFor(backend) +
        "=ON (available here: " + availableBackendList() + ")");
}

} // namespace transport
} // namespace motus
