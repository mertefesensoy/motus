#pragma once

#include "motus/Logger.hpp"
#include "motus/MessagePublisher.hpp"
#include "motus/transport/ITransport.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>

namespace motus {

/**
 * Publishes messages to a topic.
 *
 * Owns nothing: it borrows a transport that must outlive it. Not thread-safe -- transports are
 * single-threaded and no amount of atomic bookkeeping changes that. Scaling means one transport
 * per thread.
 *
 * Exists as a class rather than free functions so every publisher in an application and its
 * test suites publish through one code path -- and so that the capability check below happens
 * exactly once, for everybody.
 */
class Producer
{
public:
    /**
     * `transport` must already be connected and must outlive this object.
     *
     * `options` states what this producer requires of every send. Requirements are checked
     * against the transport's declared capabilities HERE, at construction, so a backend that
     * cannot deliver what was asked for fails at startup with a diagnostic naming the backend
     * and the capability.
     *
     * That check is the reason the capability model exists. The system this library was
     * extracted from published messages transient onto a durable queue for an entire phase:
     * the capability was absent, the system behaved exactly as though it were present, and any
     * broker restart would have destroyed every queued message in silence. A requirement that
     * is merely documented gets violated; one that is checked cannot be.
     */
    explicit Producer(transport::ITransport &transport,
                      transport::SendOptions options = {})
        : _transport(transport)
        , _options(options)
    {
        const transport::Capabilities caps = transport.capabilities();

        if (_options.durable && !caps.durableDelivery)
            throw std::runtime_error(
                std::string("transport backend '") + caps.name +
                "' cannot deliver durable messages, but this producer requires them. "
                "Messages would be lost on any middleware restart. Either select a backend "
                "that supports durability, or construct the Producer with SendOptions{false} "
                "to state explicitly that loss is acceptable.");

        if (_options.confirm && !caps.publisherConfirms)
            throw std::runtime_error(
                std::string("transport backend '") + caps.name +
                "' does not support publisher confirms, but this producer requires them. "
                "A send would report success once the frame left the socket, which is not the "
                "same as the middleware having stored it.");
    }

    /**
     * Declare the target topic and wait for the middleware to confirm.
     *
     * Durable and shared so the destination survives a restart and accumulates messages with no
     * consumer attached, and so several workers can compete for them.
     *
     * Note that destination durability is only HALF of surviving a restart: it keeps the
     * destination, not its contents. Message persistence is per-send, via
     * SendOptions::durable, and both are required. That distinction cost this library's
     * predecessor a silent data-loss path for an entire phase.
     *
     * Throws std::runtime_error if the middleware rejects the declaration or does not answer
     * within `timeout`. Returns the number of messages already waiting, or 0 if the backend
     * cannot report a depth -- check capabilities().topicDepth to tell those apart.
     */
    std::uint32_t declareTopic(const std::string &topic,
                               std::chrono::milliseconds timeout = std::chrono::seconds(5),
                               const transport::TopicOptions &options = {},
                               bool allowAsymmetric = false)
    {
        // `options` last, after `timeout`, purely so existing positional callers keep compiling.
        // Reading order would put it first; not breaking call sites wins.
        //
        // Checked here rather than in the constructor because the group is not known until now.
        // Every other capability this class requires IS checked at construction, and the
        // deviation is worth naming: a Producer can declare several topics, so its routing is a
        // property of a declaration rather than of the object.
        transport::checkTopicOptions(_transport.capabilities(), options, allowAsymmetric);

        _transport.ensureTopic(topic, options, timeout);

        if (!_transport.capabilities().topicDepth) return 0;
        return _transport.topicDepth(topic, timeout);
    }

    /**
     * Publish one message to a named destination.
     *
     * Templated so any message type satisfying the publisher contract works -- ByteMessage, or
     * any application type with a `std::string serialize() const`, with no change here.
     *
     * Returns false if the transport refused the frame. A true return means the payload reached
     * the middleware, NOT that it was stored: without publisher confirms, publishing is
     * fire-and-forget. Call flush() before tearing the transport down.
     *
     * Propagates whatever serialize() throws -- notably std::length_error when a payload
     * exceeds its size bound, which must not be silently swallowed.
     *
     * There is no defaulted destination, deliberately. Two messages of the same C++ type can
     * belong to different destinations on different routing groups, so no static constant on
     * the type could express where one goes. A default would have been a default for the tests
     * only, and it would have made the most dangerous mistake here the easiest one to make.
     *
     * `topic` is the DESTINATION, not a routing key. The transport resolves it to the group and
     * key it was declared into -- the caller never has to know that mapping, which is what
     * keeps declared topology in exactly one place.
     *
     * THE TRAP, stated because it is silent: a message whose routing key matches no binding is
     * DISCARDED by the middleware with no error to the publisher. Publishing to a destination
     * that was never declared does not fail, it evaporates. Declare the whole topology before
     * sending anything.
     */
    template <typename MessageType>
    bool publish(const std::string &topic, const MessageType &message)
    {
        return publishWith(topic, message, _options);
    }

    /// Service the transport briefly so outbound frames land and errors surface. Required for
    /// asynchronous backends and harmless for synchronous ones.
    void flush(std::chrono::milliseconds duration = std::chrono::milliseconds(300))
    {
        _transport.pump(duration);
    }

    /// Successful publish() calls since construction.
    std::size_t published() const { return _published; }

private:
    /// The one place a publish is counted and logged.
    template <typename MessageType>
    bool publishWith(const std::string            &topic,
                     const MessageType            &message,
                     const transport::SendOptions &options)
    {
        const bool accepted = MessagePublisher::publish(_transport, topic, message, options);

        if (accepted)
        {
            ++_published;
            MOTUS_LOG_DEBUG("published #" << _published << " topic='" << topic << "'");
        }
        else
        {
            // Not an exception: the caller decides whether one refused publish is fatal.
            MOTUS_LOG_WARN("transport refused publish to topic='" << topic << "'");
        }

        return accepted;
    }

    transport::ITransport  &_transport;
    transport::SendOptions  _options;
    std::size_t             _published = 0;
};

} // namespace motus
