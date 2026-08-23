#pragma once

// Boost.Asio pulls in <winsock2.h> on Windows and needs a target OS version chosen
// before it does. Must precede every Boost header in this translation unit.
#ifdef _WIN32
#  ifndef _WIN32_WINNT
#    define _WIN32_WINNT 0x0601   // Windows 7 or later
#  endif
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#endif

#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/write.hpp>

#include <amqpcpp.h>

#include "motus/Logger.hpp"
#include "motus/Version.hpp"

#include <array>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace motus {

namespace detail {

/**
 * Replace AMQP-CPP's library-only handshake identity with the motus build that owns it.
 *
 * RabbitMQ exposes these fields on its Connections page and through `/api/connections`.
 * `connection_name` becomes versionString(), or retains and annotates a name supplied by an
 * embedding application. This matters when manager, producer and already-running consumer
 * processes came from different bundle extractions: a manager banner proves nothing about the
 * worker processes that own the unacknowledged deliveries.
 */
inline void applyMotusClientProperties(AMQP::Table &client)
{
    const std::string existingName = client.get("connection_name");
    const std::string identity = versionString();

    client.set("product", "motus");
    client.set("version", kVersion);
    client.set("information", "https://github.com/mertefesensoy/motus");
    client.set("motus_commit", kCommit);
    client.set("motus_commit_date", kCommitDate);
    client.set("motus_provenance", kProvenance);
    client.set("connection_name", existingName.empty() ? identity
                                                        : existingName + " | " + identity);
}

} // namespace detail

/**
 * A blocking, Windows-compatible AMQP transport for AMQP-CPP.
 *
 * ---------------------------------------------------------------------------
 * Why this class exists
 * ---------------------------------------------------------------------------
 * AMQP-CPP ships event-loop integrations (libboostasio.h, libev.h, libuv.h) and a
 * TcpConnection family under include/amqpcpp/linux_tcp/. None of them build on
 * Windows: libboostasio.h is written against boost::asio::posix::stream_descriptor,
 * which does not exist outside POSIX. The header even says so itself. Because the
 * header is present in the MSYS2 package, the #include resolves and the failure only
 * appears deep inside the compile -- which is what makes this trap expensive.
 *
 * The library's *core* is transport-agnostic by design, however. AMQP::Connection
 * speaks protocol only: it hands outbound bytes to ConnectionHandler::onData() and
 * accepts inbound bytes through parse(). Supplying that byte pipe is the whole job,
 * and boost::asio::ip::tcp::socket is fully cross-platform. So this class implements
 * the handler directly and skips the POSIX-bound integrations entirely.
 *
 * ---------------------------------------------------------------------------
 * Concurrency model
 * ---------------------------------------------------------------------------
 * Single-threaded and synchronous. AMQP is asynchronous at the protocol level --
 * declareQueue() and consume() complete via callbacks -- so the socket must be
 * serviced for any callback to fire. Callers drive that explicitly with pumpUntil().
 * There is no background thread and no io_context::run() loop to reason about, which
 * keeps the control flow readable at the cost of not overlapping I/O with work.
 *
 * ---------------------------------------------------------------------------
 * Invariants
 * ---------------------------------------------------------------------------
 *  - connection() is valid only after connect() returns successfully.
 *  - Every wait is bounded. A broker that accepts the TCP connection and then goes
 *    quiet produces a timeout and a diagnostic, never an indefinite hang (NFR-1).
 *  - Failures throw std::runtime_error; nothing fails silently.
 */
class AmqpConnection final : public AMQP::ConnectionHandler
{
public:
    AmqpConnection(std::string host,
                   std::uint16_t port     = 5672,
                   std::string user       = "guest",
                   std::string password   = "guest",
                   std::string vhost      = "/")
        : _host(std::move(host))
        , _port(port)
        , _user(std::move(user))
        , _password(std::move(password))
        , _vhost(std::move(vhost))
        , _socket(_io)
    {}

    ~AmqpConnection() override
    {
        // A destructor must not propagate exceptions; best-effort teardown only.
        try { close(std::chrono::milliseconds(250)); } catch (...) {}
    }

    AmqpConnection(const AmqpConnection &)            = delete;
    AmqpConnection &operator=(const AmqpConnection &) = delete;

    /**
     * Open the socket and complete the AMQP handshake.
     * Throws std::runtime_error on resolve failure, refused connection, rejected
     * credentials, or handshake timeout.
     */
    void connect(std::chrono::milliseconds handshakeTimeout = std::chrono::seconds(10))
    {
        boost::system::error_code ec;

        // Try the host as a literal IP address before falling back to name resolution.
        //
        // Resolving even "localhost" is a DNS-client call on Windows, and on a locked-down
        // network that lookup can fail outright -- WSANO_RECOVERY, reported as
        // "a non-recoverable error occurred during a database lookup" -- while the loopback
        // interface itself is perfectly healthy and the broker is listening. An IP literal
        // requires no lookup, so the common case never touches the resolver at all.
        MOTUS_LOG_INFO("connecting to broker " << _host << ':' << _port
                                              << " vhost='" << _vhost << "' user='" << _user << "'");

        const auto literal = boost::asio::ip::make_address(_host, ec);

        if (!ec)
        {
            MOTUS_LOG_DEBUG("host is an IP literal; skipping name resolution");
            _socket.connect(boost::asio::ip::tcp::endpoint(literal, _port), ec);
        }
        else
        {
            MOTUS_LOG_DEBUG("host is not an IP literal; resolving '" << _host << "'");

            boost::asio::ip::tcp::resolver resolver(_io);
            const auto endpoints = resolver.resolve(_host, std::to_string(_port), ec);
            if (ec)
            {
                MOTUS_LOG_ERROR("name resolution failed for '" << _host << "': " << ec.message());
                throw std::runtime_error(
                    "cannot resolve '" + _host + "': " + ec.message() +
                    " (pass an IP literal such as 127.0.0.1 instead -- restrictive DNS policy"
                    " can break even localhost lookups)");
            }

            boost::asio::connect(_socket, endpoints, ec);
        }

        if (ec)
        {
            MOTUS_LOG_ERROR("TCP connect to " << _host << ':' << _port << " failed: " << ec.message()
                           << " (value=" << ec.value() << ")");
            throw std::runtime_error(
                "cannot reach broker at " + _host + ":" + std::to_string(_port) + " -- " + ec.message() +
                " (is rabbitmq-server.bat running?)");
        }

        MOTUS_LOG_DEBUG("TCP connected; starting AMQP handshake");

        _socket.set_option(boost::asio::ip::tcp::no_delay(true), ec); // best effort

        // Constructing the Connection immediately calls onData() with the AMQP protocol
        // header, so the socket has to be open first. This ordering is not optional.
        _connection = std::make_unique<AMQP::Connection>(
            this, AMQP::Login(_user, _password), _vhost);

        const bool settled = pumpUntil(
            [this] { return _ready || !_error.empty() || _closed; }, handshakeTimeout);

        if (!_error.empty())
            throw std::runtime_error("AMQP handshake rejected: " + _error +
                                     " (check credentials and vhost '" + _vhost + "')");
        if (_closed)
            throw std::runtime_error("broker closed the connection during handshake");
        if (!settled || !_ready)
            throw std::runtime_error("timed out waiting for AMQP handshake from " + _host);
    }

    /**
     * Service the socket until `predicate` is true or `timeout` elapses.
     * Returns the final value of the predicate -- false means it timed out or the
     * peer went away. This is what makes AMQP-CPP's callbacks fire.
     */
    bool pumpUntil(const std::function<bool()> &predicate, std::chrono::milliseconds timeout)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;

        while (!predicate())
        {
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline) break;

            const auto remaining =
                std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);

            if (!pumpOnce(remaining)) break;   // timeout, EOF, or closed
        }
        return predicate();
    }

    /// Graceful AMQP close followed by socket shutdown. Safe to call more than once.
    void close(std::chrono::milliseconds timeout = std::chrono::seconds(2))
    {
        if (_connection && !_closed)
        {
            _connection->close();
            pumpUntil([this] { return _closed; }, timeout);
        }

        boost::system::error_code ec;
        if (_socket.is_open())
        {
            _socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
            _socket.close(ec);
        }
    }

    /// Valid only after a successful connect().
    AMQP::Connection &connection()
    {
        if (!_connection) throw std::logic_error("connection() called before connect()");
        return *_connection;
    }

    /// True only while the connection is usable *right now*: the handshake completed and
    /// nothing has closed it since.
    ///
    /// _ready is latched by onReady() and never cleared, so returning it raw meant ready()
    /// kept answering true for a connection the broker had already dropped. Any caller using
    /// it as a health check -- the obvious reading of the name -- would publish into a dead
    /// socket and be told everything was fine.
    bool ready()  const { return _ready && !_closed; }

    /// True once the connection has ended, for any reason: our own close(), a broker-initiated
    /// close, EOF, or a failed socket operation. Latched -- there is no reconnection.
    bool closed() const { return _closed; }
    const std::string &error() const { return _error; }

    /// Cumulative bytes written to / read from the socket since construction.
    std::uint64_t bytesSent()     const { return _bytesSent; }
    std::uint64_t bytesReceived() const { return _bytesReceived; }

protected:
    // ---- AMQP::ConnectionHandler ------------------------------------------------

    void onProperties(AMQP::Connection *, const AMQP::Table &, AMQP::Table &client) override
    {
        detail::applyMotusClientProperties(client);
    }

    /**
     * AMQP-CPP hands us outbound protocol bytes here. Written synchronously: for the
     * frame sizes motus produces this cannot meaningfully block, and it keeps ordering
     * trivially correct without an outbound queue. A high-throughput publisher would
     * need to buffer here and flush from the pump instead.
     *
     * Throwing propagates out through parse() or the Connection constructor. That is
     * intentional -- a dead socket must not be mistaken for a delivered message.
     *
     * ---------------------------------------------------------------------------
     * ...but only while the connection is still believed to be alive
     * ---------------------------------------------------------------------------
     * AMQP-CPP calls onData() from DESTRUCTORS. AMQP::Channel emits a Channel.Close frame when
     * it is destroyed, and AMQP::Connection emits Connection.Close. Destructors are noexcept by
     * default, so a throw from there calls std::terminate immediately -- no stack trace, no log
     * line beyond `terminate called after throwing`, and no chance for a caller to report
     * anything useful.
     *
     * That is not hypothetical: it is exactly what happened when the broker force-closed a
     * connection. The read side threw, and while the stack unwound, ~Channel tried to write its
     * close frame down the dead socket and killed the process.
     *
     * So a write failure is only escalated to an exception while the connection is still
     * *believed live*. Once _closed is set -- by the read path, by onClosed(), or by an earlier
     * failed write -- teardown is best-effort and silent-but-logged. A publish on a healthy
     * connection still throws, which is the case the original contract was written for.
     */
    void onData(AMQP::Connection *, const char *data, std::size_t size) override
    {
        _bytesSent += size;
        MOTUS_LOG_TRACE("-> " << size << " byte(s) out (total " << _bytesSent << ")"
                             << hexDump(data, size));

        boost::system::error_code ec;
        boost::asio::write(_socket, boost::asio::buffer(data, size), ec);
        if (!ec) return;

        const bool alreadyDown = _closed;

        MOTUS_LOG_ERROR("socket write of " << size << " byte(s) failed: " << ec.message()
                       << " (value=" << ec.value() << ")");

        // Any failed write means the connection is finished. Recording that here is what makes
        // a subsequent destructor-driven write silent instead of fatal.
        _closed = true;

        if (alreadyDown)
        {
            MOTUS_LOG_DEBUG("write failed during teardown of an already-closed connection; "
                           "not throwing (a throw here would reach a destructor)");
            return;
        }

        throw std::runtime_error("socket write failed: " + ec.message());
    }

    void onReady(AMQP::Connection *) override
    {
        _ready = true;
        MOTUS_LOG_INFO("AMQP handshake complete; connection ready");
    }

    void onError(AMQP::Connection *, const char *message) override
    {
        _error = (message != nullptr) ? message : "unknown protocol error";
        MOTUS_LOG_ERROR("AMQP protocol error: " << _error);
    }

    void onClosed(AMQP::Connection *) override
    {
        _closed = true;
        MOTUS_LOG_INFO("AMQP connection closed by broker (sent " << _bytesSent
                      << " byte(s), received " << _bytesReceived << ")");
    }

private:
    /**
     * One read/parse cycle, bounded by `timeout`.
     * Returns false on timeout or clean EOF; true if bytes were read and parsed.
     *
     * Asio has no timeout on synchronous reads, so this issues an async read and lets
     * the io_context run only until the deadline. On expiry the outstanding read is
     * cancelled and the handler drained *before returning*, so the completion lambda's
     * captured references are never touched after this frame dies.
     */
    bool pumpOnce(std::chrono::milliseconds timeout)
    {
        bool                      completed = false;
        boost::system::error_code readEc;
        std::size_t               bytes = 0;

        _socket.async_read_some(
            boost::asio::buffer(_chunk),
            [&](const boost::system::error_code &ec, std::size_t transferred)
            {
                readEc    = ec;
                bytes     = transferred;
                completed = true;
            });

        _io.restart();
        _io.run_for(timeout);

        if (!completed)
        {
            boost::system::error_code ignored;
            _socket.cancel(ignored);
            _io.restart();
            _io.run();          // drain the cancelled handler while our frame is alive
            return false;
        }

        if (readEc)
        {
            // A peer that goes away is an ordinary end of the connection, not an error to
            // throw about -- and it arrives under several different error codes depending on
            // HOW it went away:
            //
            //   eof                  the peer shut down cleanly and we read the FIN
            //   operation_aborted    our own cancel(), from the timeout path above
            //   connection_reset     WSAECONNRESET (10054) -- the peer sent RST. This is what
            //                        `rabbitmqctl close_connection`, the management API's
            //                        DELETE /api/connections, and a broker restart all produce.
            //   connection_aborted   WSAECONNABORTED (10053) -- the local stack gave up
            //   broken_pipe          the write side died first
            //   not_connected        the socket was already torn down underneath us
            //
            // Only `eof` and `operation_aborted` were treated as a clean end originally, so a
            // broker-forced close threw std::runtime_error out of poll(). During the resulting
            // stack unwind, AMQP-CPP's Channel destructor emitted a Channel.Close frame, our
            // onData() failed to write it and threw again -- from a destructor -- and the
            // process called std::terminate. A routine broker restart crashed the worker.
            if (readEc == boost::asio::error::eof ||
                readEc == boost::asio::error::operation_aborted ||
                readEc == boost::asio::error::connection_reset ||
                readEc == boost::asio::error::connection_aborted ||
                readEc == boost::asio::error::broken_pipe ||
                readEc == boost::asio::error::not_connected)
            {
                MOTUS_LOG_INFO("connection ended: " << readEc.message()
                              << " (value=" << readEc.value() << ")");
                _closed = true;
                return false;
            }

            MOTUS_LOG_ERROR("socket read failed: " << readEc.message()
                           << " (value=" << readEc.value() << ")");
            _closed = true;   // unusable either way; see onData()
            throw std::runtime_error("socket read failed: " + readEc.message());
        }

        _bytesReceived += bytes;
        MOTUS_LOG_TRACE("<- " << bytes << " byte(s) in (total " << _bytesReceived << ")"
                             << hexDump(_chunk.data(), bytes));

        _inBuffer.insert(_inBuffer.end(), _chunk.data(), _chunk.data() + bytes);

        // parse() consumes only whole frames; a partial frame stays buffered for the
        // next cycle. Dropping the remainder here would silently corrupt the stream.
        const std::uint64_t consumed = _connection->parse(_inBuffer.data(), _inBuffer.size());
        if (consumed > 0)
            _inBuffer.erase(_inBuffer.begin(),
                            _inBuffer.begin() + static_cast<std::ptrdiff_t>(consumed));

        // A persistently non-empty buffer means frames are arriving fragmented -- normal --
        // but a buffer that never drains points at a framing bug, so it is worth seeing.
        MOTUS_LOG_TRACE("parsed " << consumed << " of " << (consumed + _inBuffer.size())
                       << " buffered byte(s); " << _inBuffer.size() << " awaiting more data");

        return true;
    }

    std::string   _host;
    std::uint16_t _port;
    std::string   _user;
    std::string   _password;
    std::string   _vhost;

    // Declaration order matters: _io must outlive and precede _socket.
    boost::asio::io_context        _io;
    boost::asio::ip::tcp::socket   _socket;

    std::array<char, 8192> _chunk{};
    std::vector<char>      _inBuffer;

    std::unique_ptr<AMQP::Connection> _connection;

    bool        _ready  = false;
    bool        _closed = false;
    std::string _error;

    // Cumulative wire totals. Cheap to maintain and the first thing worth checking when a
    // connection appears healthy but nothing is arriving.
    std::uint64_t _bytesSent     = 0;
    std::uint64_t _bytesReceived = 0;
};

} // namespace motus
