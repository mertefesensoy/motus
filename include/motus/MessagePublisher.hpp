#pragma once

// Not AMQP-specific -- it publishes through whichever transport it is handed, and a class
// named for one middleware that also serves another backend would be actively misleading.
//
// It survives as a distinct facade rather than folding into Producer so that stateless,
// one-shot publishing has a name of its own; Producer adds the capability checks and
// counters a long-lived publisher wants.

#include "motus/transport/ITransport.hpp"

#include <string>

namespace motus {

/**
 * Static publishing facade over the transport layer.
 *
 * Templated on the message type rather than accepting a polymorphic base, so publishing
 * resolves at compile time -- no virtual dispatch and no dynamic_cast on the serialization
 * path. The trade-off is that this must stay header-only and that a new message type
 * recompiles its callers, which is acceptable because wire types change rarely.
 *
 * Note precisely where the one virtual call is. serialize() is resolved statically; the single
 * indirection is ITransport::send(), immediately before a socket write or middleware call that
 * costs orders of magnitude more. Nothing on the serialization hot path is virtual.
 *
 * Requirements on MessageType (a compile-time contract, checked by use):
 *   - std::string serialize() const             -- wire payload
 *
 * That is the whole contract, and it deliberately includes no static constant naming the
 * destination. Two messages of the same C++ type can belong to different destinations on
 * different routing groups, so no constant on the type could express where one goes. The
 * caller names the destination. motus::ByteMessage satisfies the contract; so does any
 * application type that returns its wire bytes from serialize().
 *
 * Isolating the middleware behind this facade plus ITransport is what makes the underlying
 * client library genuinely swappable: replacing AMQP-CPP means writing one ITransport
 * implementation and touching nothing else.
 */
class MessagePublisher final
{
public:
    MessagePublisher() = delete;   // static facade; never instantiated

    /**
     * Publish one message to a named destination.
     *
     * `topic` is the destination, not a routing key. The transport resolves it to the group
     * and key it was declared into, and the caller never has to know that mapping. A
     * destination the transport never declared falls back to the default exchange, which
     * routes by name.
     *
     * Returns false if the transport refused the frame. A true return means the payload was
     * handed to the middleware, NOT that it was stored -- see Capabilities::publisherConfirms.
     *
     * Propagates whatever serialize() throws -- notably std::length_error when a payload
     * exceeds its size bound, which must not be silently swallowed.
     */
    template <typename MessageType>
    static bool publish(transport::ITransport &transport,
                        const std::string &topic,
                        const MessageType &message,
                        const transport::SendOptions &options = {})
    {
        const std::string payload = message.serialize();
        return transport.send(topic, payload, options);
    }
};

} // namespace motus
