#pragma once

#include <cstddef>
#include <stdexcept>
#include <string>

namespace motus {

/**
 * One opaque payload, carried on the wire inside a versioned envelope.
 *
 * ---------------------------------------------------------------------------
 * Why the envelope exists at all
 * ---------------------------------------------------------------------------
 * Two properties are architectural, and they are the whole reason this type is part of the
 * seam's policy layer rather than left to each application:
 *
 *   1. **An explicit format_version that is rejected rather than guessed at.** A consumer that
 *      does not understand a version must refuse the message loudly. Cheap to add early and
 *      impossible to retrofit once messages from an older publisher are already sitting in a
 *      durable queue -- the system this library was extracted from added it one wire change
 *      before that would have happened.
 *
 *   2. **A hard payload bound.** A multi-megabyte payload where kilobytes were expected means
 *      something unintended was read. Failing loudly at serialize() beats discovering it as a
 *      broker memory problem under load. Brokers tolerate far larger messages, so this is a
 *      sanity bound rather than a protocol limit.
 *
 * The envelope is deliberately NOT JSON. The payload is arbitrary bytes -- protobuf, JSON,
 * CBOR, a raw sensor frame -- and a JSON envelope would force base64 (a 33% size tax) or
 * corrupt non-UTF-8 payloads. A fixed ASCII prefix costs ten bytes and is binary-safe:
 *
 *     motus:<version>:<payload bytes...>
 *
 * Everything after the second colon is the payload, verbatim.
 */
struct ByteMessage
{
    /**
     * Wire format version, present in every envelope. Bump it when the envelope's shape
     * changes; a consumer built for one version rejects every other by name, in both
     * directions.
     */
    static constexpr int FORMAT_VERSION = 1;

    /**
     * Refuse to build or accept an encoded message larger than this.
     * Enforced in serialize() (std::length_error, so a publishing bug is caught in the process
     * that has the context to explain it) and again in deserialize() and dispatch(), where an
     * oversized body is a poison message and is rejected without requeue.
     */
    static constexpr std::size_t MAX_PAYLOAD_BYTES = 4u * 1024u * 1024u;

    /// The application's bytes. Opaque to this library: no structure is imposed or inspected.
    std::string payload;

    /// The exact prefix serialize() emits for this build's FORMAT_VERSION.
    static const std::string &envelopePrefix()
    {
        static const std::string prefix =
            std::string("motus:") + std::to_string(FORMAT_VERSION) + ":";
        return prefix;
    }

    /**
     * Encode to the wire form.
     *
     * @throws std::length_error when the encoded result would exceed MAX_PAYLOAD_BYTES.
     */
    std::string serialize() const
    {
        const std::string &prefix = envelopePrefix();

        if (prefix.size() + payload.size() > MAX_PAYLOAD_BYTES)
            throw std::length_error(
                "encoded message is " + std::to_string(prefix.size() + payload.size()) +
                " bytes, over the " + std::to_string(MAX_PAYLOAD_BYTES) + " byte limit");

        return prefix + payload;
    }

    /**
     * Reconstruct from a wire body.
     *
     * Throws std::runtime_error for a body that is not a motus envelope, carries a malformed
     * version field, or carries a version this build does not understand -- naming both
     * versions, so the operator can tell an old publisher from a corrupted frame. Throws
     * std::length_error for an oversized body. Callers above the seam treat any of these as a
     * poison message -- see motus::dispatch().
     */
    static ByteMessage deserialize(const std::string &wire)
    {
        if (wire.size() > MAX_PAYLOAD_BYTES)
            throw std::length_error(
                "message body is " + std::to_string(wire.size()) + " bytes, over the " +
                std::to_string(MAX_PAYLOAD_BYTES) + " byte limit");

        static const std::string magic = "motus:";
        if (wire.compare(0, magic.size(), magic) != 0)
            throw std::runtime_error("body is not a motus envelope (missing 'motus:' prefix)");

        const std::size_t versionBegin = magic.size();
        const std::size_t versionEnd   = wire.find(':', versionBegin);
        if (versionEnd == std::string::npos || versionEnd == versionBegin)
            throw std::runtime_error("malformed motus envelope: no version field");

        int version = 0;
        for (std::size_t i = versionBegin; i < versionEnd; ++i)
        {
            const char c = wire[i];
            if (c < '0' || c > '9')
                throw std::runtime_error("malformed motus envelope: non-numeric version field");
            if (version > 100000)
                throw std::runtime_error("malformed motus envelope: version field out of range");
            version = version * 10 + (c - '0');
        }

        if (version != FORMAT_VERSION)
            throw std::runtime_error(
                "unsupported format_version " + std::to_string(version) +
                " (this build understands " + std::to_string(FORMAT_VERSION) + ")");

        ByteMessage message;
        message.payload = wire.substr(versionEnd + 1);
        return message;
    }

    /// Size of the carried payload, in bytes.
    std::size_t byteCount() const { return payload.size(); }

    friend bool operator==(const ByteMessage &a, const ByteMessage &b)
    {
        return a.payload == b.payload;
    }
};

} // namespace motus
