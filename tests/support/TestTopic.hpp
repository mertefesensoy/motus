#pragma once

#include <string>

/**
 * The destination the transport suites publish to.
 *
 * These suites need *a* destination, because what they test is the transport -- delivery,
 * acknowledgement, redelivery, ordering, reconnection -- and the payload and its address are
 * merely the vehicle. The name lives here, where it is visibly test scaffolding, rather than
 * anywhere it could read as production topology.
 *
 * Deliberately not a name any application is likely to use: a test destination that collides
 * with a real one would have the suites and a real run competing for the same queue on a
 * shared broker, and the failure would look like message loss.
 */
namespace motus_test {

inline const std::string &testTopic()
{
    static const std::string topic = "motus.test.transport";
    return topic;
}

} // namespace motus_test
