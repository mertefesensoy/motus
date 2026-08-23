// Boost.Asio pulls in <winsock2.h> on Windows and needs a target OS version chosen before it
// does. Must precede every Boost header in this translation unit.
#ifdef _WIN32
#  ifndef _WIN32_WINNT
#    define _WIN32_WINNT 0x0601
#  endif
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#endif

#include "motus/transport/backends/SimpleAmqpTransport.hpp"

#include "motus/Logger.hpp"

#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <algorithm>
#include <stdexcept>
#include <thread>
#include <utility>

namespace motus {
namespace transport {
namespace {

constexpr std::uint16_t kDefaultPort = 5672;

AmqpClient::Table queueArguments(const Topic &topic, const TopicOptions &options)
{
    AmqpClient::Table arguments;
    if (options.deadLetter)
    {
        arguments.insert(AmqpClient::TableEntry("x-dead-letter-exchange",
                                                 kDeadLetterExchange));
        arguments.insert(AmqpClient::TableEntry("x-dead-letter-routing-key", topic));
    }
    if (options.maxLength > 0)
    {
        arguments.insert(AmqpClient::TableEntry(
            "x-max-length", static_cast<boost::int32_t>(options.maxLength)));
        arguments.insert(AmqpClient::TableEntry(
            "x-overflow", std::string(overflowPolicyName(options.overflow))));
    }
    return arguments;
}

/**
 * Prove the endpoint is reachable within `timeout`, then throw the socket away.
 *
 * ---------------------------------------------------------------------------
 * Why this exists
 * ---------------------------------------------------------------------------
 * Channel::Create has no connect timeout, and neither does OpenOpts -- verified in the header,
 * not assumed. ITransport::connect promises a bounded wait (NFR-1), so the bound has to come
 * from somewhere else.
 *
 * A plain TCP connect with our own deadline covers the failures that actually happen: wrong
 * host, broker not running, port blocked by policy. Once it succeeds, Channel::Create connects
 * to an endpoint already proven reachable and returns promptly.
 *
 * HONEST LIMIT, stated rather than glossed: this does NOT bound the AMQP handshake. A broker
 * that accepts TCP and then goes silent would still hang inside Channel::Create. The AMQP-CPP
 * backend bounds that case because it drives the handshake itself; this one cannot without a
 * thread it could not cancel.
 */
void probeReachable(const std::string &host, std::uint16_t port,
                    std::chrono::milliseconds timeout)
{
    boost::asio::io_context io;
    boost::asio::ip::tcp::socket socket(io);
    boost::system::error_code connectEc;
    bool completed = false;

    boost::system::error_code parseEc;
    const auto literal = boost::asio::ip::make_address(host, parseEc);

    boost::asio::ip::tcp::endpoint endpoint;
    if (!parseEc)
    {
        // An IP literal needs no lookup. On a locked-down network even a "localhost"
        // resolution can fail outright while the loopback interface is perfectly healthy.
        endpoint = boost::asio::ip::tcp::endpoint(literal, port);
    }
    else
    {
        boost::asio::ip::tcp::resolver resolver(io);
        boost::system::error_code resolveEc;
        const auto endpoints = resolver.resolve(host, std::to_string(port), resolveEc);
        if (resolveEc || endpoints.empty())
            throw std::runtime_error(
                "cannot resolve '" + host + "': " + resolveEc.message() +
                " (pass an IP literal such as 127.0.0.1 instead -- restrictive DNS policy can"
                " break even localhost lookups)");
        endpoint = *endpoints.begin();
    }

    socket.async_connect(endpoint,
                         [&](const boost::system::error_code &ec)
                         {
                             connectEc = ec;
                             completed = true;
                         });

    io.restart();
    io.run_for(timeout);

    if (!completed)
    {
        // Cancel and DRAIN before returning: the completion lambda captures locals in this
        // frame, and letting it fire after the frame dies is a use-after-free.
        boost::system::error_code ignored;
        socket.cancel(ignored);
        io.restart();
        io.run();
        throw std::runtime_error(
            "timed out after " + std::to_string(timeout.count()) + "ms connecting to " + host +
            ":" + std::to_string(port));
    }

    if (connectEc)
        throw std::runtime_error(
            "cannot reach broker at " + host + ":" + std::to_string(port) + " -- " +
            connectEc.message() + " (value=" + std::to_string(connectEc.value()) +
            "; is rabbitmq-server.bat running?)");

    boost::system::error_code ignored;
    socket.close(ignored);
}

} // namespace

// ---------------------------------------------------------------------------

SimpleAmqpTransport::~SimpleAmqpTransport()
{
    // A destructor must not propagate. Anything still unacknowledged is returned to the broker
    // when the channel drops, which is the abandonment clause and is what we want here.
    try { _channel.reset(); } catch (...) {}
}

Capabilities SimpleAmqpTransport::capabilities() const
{
    Capabilities caps;
    caps.name                   = "simpleamqp";
    caps.durableDelivery        = true;   // BasicMessage::DeliveryMode(dm_persistent)
    caps.publisherConfirms      = false;  // Channel exposes no confirm API at all -- verified
    caps.manualAcknowledgement  = true;   // BasicConsume with no_ack = false
    caps.redeliveryFlag         = true;   // Envelope::Redelivered()
    caps.deadLetter             = true;   // broker-native DLX preserves body plus x-death time
    // FALSE, and declared rather than left to the default. SimpleAmqpClient can declare
    // exchanges and bindings perfectly well; this backend simply has not been taught to, and
    // it is not built in CI, so writing that code here would mean shipping something never
    // compiled -- the exact situation that has cost this project the most time.
    //
    // Declaring it false is not a shrug: Producer refuses at construction, naming this backend
    // and this capability, the moment anyone asks for a group. Loud beats silent.
    caps.routingGroups          = false;
    caps.fifoPerTopic           = true;
    caps.competingConsumers     = true;   // BasicConsume with exclusive = false
    caps.topicDepth             = true;   // DeclareQueueWithCounts
    caps.purge                  = true;   // PurgeQueue (returns void; count read beforehand)
    caps.requeue                = true;   // BasicReject with requeue = true
    caps.reconnect              = true;

    // FALSE, and declared rather than left to the default, same reasoning as routingGroups
    // above: this backend has not been taught to hold a delivery tag past its receive callback
    // and settle it later, it is not built in CI (MOTUS_WITH_SIMPLEAMQP is OFF by default), and
    // the design doc's step-0 spike that proved this works at all was run against AMQP-CPP, not
    // SimpleAmqpClient -- claiming the capability here would be a claim nothing has verified.
    // Consumer refuses at construction the moment a deferred handler is configured against a
    // backend that says false, before any message ever reaches settle().
    caps.deferredAcknowledgement = false;

    // TRUE: DeclareQueue's field table carries x-max-length and every x-overflow policy. This is
    // compiled in the optional SimpleAmqpClient verification build even though that backend is
    // disabled in the default build.
    caps.boundedQueue            = true;
    return caps;
}

void SimpleAmqpTransport::settle(std::uint64_t, Disposition)
{
    throw std::logic_error(
        "backend 'simpleamqp' does not support deferred acknowledgement "
        "(Capabilities::deferredAcknowledgement is false); check the capability before "
        "returning Disposition::Deferred against it");
}

void SimpleAmqpTransport::openChannel(std::chrono::milliseconds timeout)
{
    const std::uint16_t port = (_endpoint.port != 0) ? _endpoint.port : kDefaultPort;

    probeReachable(_endpoint.host, port, timeout);

    _channel = AmqpClient::Channel::Create(_endpoint.host,
                                           static_cast<int>(port),
                                           _endpoint.user,
                                           _endpoint.password,
                                           _endpoint.realm);
    _connected = true;
}

void SimpleAmqpTransport::connect(const Endpoint &endpoint, std::chrono::milliseconds timeout)
{
    _endpoint = endpoint;   // remembered so reconnection targets the same peer
    openChannel(timeout);

    MOTUS_LOG_INFO("simpleamqp connected to " << _endpoint.host << ':' << _endpoint.port
                  << " vhost='" << _endpoint.realm << "'");
}

void SimpleAmqpTransport::close(std::chrono::milliseconds)
{
    // Best-effort throughout. Every shutdown path in the project calls close() without a
    // try/catch, and the peer may already be gone -- which is precisely when this runs.
    if (_channel && _subscribed && !_consumerTag.empty())
    {
        try { _channel->BasicCancel(_consumerTag); }
        catch (const std::exception &e)
        {
            MOTUS_LOG_DEBUG("simpleamqp BasicCancel during close: " << e.what());
        }
    }

    try { _channel.reset(); } catch (...) {}
    _connected = false;
    _subscribed = false;
    _consumerTag.clear();
    _handler = {};
}

bool SimpleAmqpTransport::ready() const
{
    // Usable RIGHT NOW, not "was ready once".
    return _connected && static_cast<bool>(_channel);
}

void SimpleAmqpTransport::ensureTopic(const Topic &topic,
                                      const TopicOptions &options,
                                      std::chrono::milliseconds)
{
    if (!_channel) throw std::logic_error("ensureTopic() called before connect()");

    if (options.deadLetter)
    {
        _channel->DeclareExchange(kDeadLetterExchange,
                                  AmqpClient::Channel::EXCHANGE_TYPE_DIRECT,
                                  /*passive*/ false,
                                  /*durable*/ true,
                                  /*auto_delete*/ false);

        const Topic forensicTopic = deadLetterTopic(topic);
        const TopicOptions forensic = forensicTopicOptions(topic);
        _channel->DeclareQueue(forensicTopic,
                               /*passive*/ false,
                               /*durable*/ forensic.durable,
                               /*exclusive*/ !forensic.shared,
                               /*auto_delete*/ forensic.deleteWhenUnused,
                               queueArguments(forensicTopic, forensic));
        _channel->BindQueue(forensicTopic, kDeadLetterExchange, topic);
    }

    const AmqpClient::Table arguments = queueArguments(topic, options);

    // EVERY argument explicit. SimpleAmqpClient's DeclareQueue defaults are durable=false,
    // exclusive=true, auto_delete=true -- the exact opposite of motus's destination on all
    // three, which against the queue AMQP-CPP created is a 406 PRECONDITION_FAILED that closes
    // the channel and kills whichever process started second.
    _channel->DeclareQueue(topic,
                           /*passive*/     false,
                           /*durable*/     options.durable,
                           /*exclusive*/   !options.shared,
                           /*auto_delete*/ options.deleteWhenUnused,
                           arguments);

    _topic         = topic;
    _topicOptions  = options;
    _topicDeclared = true;

    MOTUS_LOG_INFO("simpleamqp topic '" << topic << "' ready");
}

std::uint32_t SimpleAmqpTransport::topicDepth(const Topic &topic, std::chrono::milliseconds)
{
    if (!_channel) throw std::logic_error("topicDepth() called before connect()");

    // Source and forensic queues intentionally have different arguments. Reconstruct the
    // declaration kind from the reserved forensic prefix so a depth read cannot create a
    // DLQ-of-a-DLQ or conflict with the queue it is inspecting.
    const bool isForensic = topic.rfind(kDeadLetterPrefix, 0) == 0;
    const std::string source = isForensic
        ? topic.substr(std::char_traits<char>::length(kDeadLetterPrefix))
        : topic;
    const TopicOptions options = isForensic
        ? forensicTopicOptions(source)
        : ((_topicDeclared && topic == _topic) ? _topicOptions : TopicOptions{});
    const AmqpClient::Table arguments = queueArguments(topic, options);

    boost::uint32_t messages = 0;
    boost::uint32_t consumers = 0;
    _channel->DeclareQueueWithCounts(topic, messages, consumers,
                                     /*passive*/     false,
                                     /*durable*/     options.durable,
                                     /*exclusive*/   !options.shared,
                                     /*auto_delete*/ options.deleteWhenUnused,
                                     arguments);
    return static_cast<std::uint32_t>(messages);
}

std::uint32_t SimpleAmqpTransport::purgeTopic(const Topic &topic, std::chrono::milliseconds)
{
    if (!_channel) throw std::logic_error("purgeTopic() called before connect()");

    // PurgeQueue returns void, so the count has to be read BEFORE the purge rather than from it
    // -- unlike AMQP-CPP, whose purgeQueue reports what it removed. The number is therefore a
    // reading taken a moment earlier, not a receipt: a message arriving between the two calls is
    // purged but not counted. Stated because the two backends' counts are subtly different
    // promises, and the conformance contract only asserts the destination ends up empty.
    const std::uint32_t before = topicDepth(topic, std::chrono::seconds(5));
    _channel->PurgeQueue(topic);
    return before;
}

bool SimpleAmqpTransport::send(const Topic &topic,
                               const std::string &payload,
                               const SendOptions &options)
{
    if (!_channel) throw std::logic_error("send() called before connect()");

    if (options.confirm)
        throw std::logic_error(
            "backend 'simpleamqp' does not support publisher confirms "
            "(Capabilities::publisherConfirms is false); SimpleAmqpClient's Channel exposes no "
            "confirm API at all. Check the capability before requesting it.");

    AmqpClient::BasicMessage::ptr_t message = AmqpClient::BasicMessage::Create(payload);

    // A durable destination and a persistent message are separate mechanisms and both are
    // needed to survive a restart. Without this the message is transient, which is the exact
    // data-loss defect this project already shipped once.
    if (options.durable)
        message->DeliveryMode(AmqpClient::BasicMessage::dm_persistent);

    // Empty exchange = the default exchange, which routes to the destination whose name equals
    // the routing key. Same arrangement as the AMQP-CPP backend.
    _channel->BasicPublish("", topic, message);

    ++_stats.sent;
    _stats.bytesSent += payload.size();

    MOTUS_LOG_DEBUG("simpleamqp sent #" << _stats.sent << " to '" << topic << "' ("
                   << payload.size() << " byte(s), "
                   << (options.durable ? "persistent" : "transient") << ")");
    return true;
}

void SimpleAmqpTransport::startConsuming()
{
    // EVERY argument explicit, and two of them are the difference between working and
    // catastrophically broken:
    //   no_ack = FALSE     -- the default is true, i.e. auto-acknowledge, which would ack
    //                         before the handler ran and destroy both the rejection policy and
    //                         the redelivery guarantee.
    //   exclusive = FALSE  -- the default is true, which forbids a second consumer and makes
    //                         competing consumers fail outright.
    // no_local is passed false as well: RabbitMQ accepts the flag but ignores it, so relying on
    // a default whose meaning is "ignored here" is not worth doing.
    _consumerTag = _channel->BasicConsume(_topic,
                                          /*consumer_tag*/ "",
                                          /*no_local*/     false,
                                          /*no_ack*/       false,
                                          /*exclusive*/    false,
                                          /*prefetch*/     1);

    MOTUS_LOG_INFO("simpleamqp subscribed to '" << _topic << "' with prefetch=1, tag='"
                  << _consumerTag << "'");
}

void SimpleAmqpTransport::subscribe(const Topic &topic,
                                    DeliveryHandler handler,
                                    std::chrono::milliseconds)
{
    if (!_channel) throw std::logic_error("subscribe() called before connect()");
    if (!handler)  throw std::invalid_argument("subscribe() requires a callable handler");
    if (_subscribed)
        throw std::logic_error("subscribe() called twice on one transport; use one transport per "
                               "topic instead of overwriting the active subscription to '" +
                               _topic + "'");

    _handler = std::move(handler);
    _topic   = topic;

    startConsuming();
    _subscribed = true;
}

void SimpleAmqpTransport::setReconnectPolicy(const ReconnectPolicy &policy)
{
    _policy         = policy;
    _failedAttempts = 0;
    _nextAttempt    = std::chrono::steady_clock::time_point{};

    MOTUS_LOG_INFO("simpleamqp reconnection " << (policy.enabled ? "enabled" : "disabled")
                  << " (maxAttempts=" << policy.maxAttempts << ")");
}

bool SimpleAmqpTransport::serviceReconnect()
{
    if (!_policy.enabled) return false;

    if (_failedAttempts >= _policy.maxAttempts)
    {
        MOTUS_LOG_ERROR("simpleamqp giving up: " << _failedAttempts << " consecutive reconnection "
                       "attempts to " << _endpoint.host << " failed");
        return false;
    }

    const auto now = std::chrono::steady_clock::now();
    if (now < _nextAttempt) return true;   // backing off; between connections, not dead

    ++_failedAttempts;
    MOTUS_LOG_WARN("simpleamqp connection lost; reconnection attempt " << _failedAttempts
                  << " of " << _policy.maxAttempts);

    try
    {
        _channel.reset();
        openChannel(std::chrono::seconds(5));

        // Restore exactly what was established, not a guess. Redeclaring with defaults would
        // disagree with the surviving destination and earn a 406.
        if (_topicDeclared) ensureTopic(_topic, _topicOptions, std::chrono::seconds(5));
        if (_subscribed)    startConsuming();

        _failedAttempts = 0;
        ++_stats.reconnects;

        MOTUS_LOG_INFO("simpleamqp reconnected (" << _stats.reconnects << " total); any in-flight "
                      "deliveries were returned to the broker");
        return true;
    }
    catch (const std::exception &e)
    {
        const auto delay = std::min(
            _policy.maxDelay,
            std::chrono::milliseconds(_policy.initialDelay.count() << (_failedAttempts - 1)));

        _nextAttempt = std::chrono::steady_clock::now() + delay;
        _connected   = false;
        MOTUS_LOG_WARN("simpleamqp reconnection attempt " << _failedAttempts << " failed: "
                      << e.what() << "; next attempt in " << delay.count() << "ms");
        return true;
    }
}

bool SimpleAmqpTransport::pump(std::chrono::milliseconds slice)
{
    if (!ready())
    {
        if (!_policy.enabled) return false;
        return serviceReconnect();
    }

    // Nothing subscribed: there is no receive to make, so honour the slice and report alive.
    if (!_subscribed || !_handler)
    {
        std::this_thread::sleep_for(slice);
        return true;
    }

    AmqpClient::Envelope::ptr_t envelope;
    bool got = false;

    // The receive is fenced separately from the handler call. Both surface as exceptions, and
    // they mean opposite things: a connection failure here is "the peer is gone", while a throw
    // from the handler is the abandonment clause. Catching them in one block would conflate a
    // dead broker with a crashed worker.
    try
    {
        got = _channel->BasicConsumeMessage(_consumerTag, envelope,
                                            static_cast<int>(slice.count()));
    }
    catch (const AmqpClient::AmqpException &e)
    {
        // Verified against a live broker: a broker restart surfaces here as
        // ConnectionForcedException ("CONNECTION_FORCED ... reason 'shutdown'"). It does NOT
        // return false and does NOT hang.
        MOTUS_LOG_WARN("simpleamqp connection lost during receive: " << e.what());
        _connected = false;
        if (!_policy.enabled) return false;
        return serviceReconnect();
    }
    catch (const std::exception &e)
    {
        MOTUS_LOG_WARN("simpleamqp receive failed: " << e.what());
        _connected = false;
        if (!_policy.enabled) return false;
        return serviceReconnect();
    }

    if (!got) return true;   // bounded timeout, nothing waiting -- still alive

    Delivery delivery;
    delivery.body        = envelope->Message()->Body();
    delivery.redelivered = envelope->Redelivered();
    delivery.tag         = static_cast<std::uint64_t>(envelope->DeliveryTag());

    ++_stats.received;
    _stats.bytesReceived += delivery.body.size();

    Disposition disposition;
    try
    {
        disposition = _handler(delivery);
    }
    catch (...)
    {
        // Contract: an uncooperative failure resolves nothing. No ack, no reject -- the
        // delivery stays unacknowledged, so the broker returns it when this channel drops.
        ++_stats.abandoned;
        throw;
    }

    switch (disposition)
    {
        case Disposition::Ack:
            _channel->BasicAck(envelope);
            ++_stats.acksPerformed;
            break;

        case Disposition::Requeue:
            // requeue = true is the whole difference between "try again" and "destroy it".
            _channel->BasicReject(envelope, /*requeue*/ true);
            ++_stats.requeuesPerformed;
            break;

        case Disposition::Reject:
            _channel->BasicReject(envelope, /*requeue*/ false);
            ++_stats.rejectsPerformed;
            break;
    }

    return true;
}

Stats SimpleAmqpTransport::stats() const
{
    return _stats;
}

} // namespace transport
} // namespace motus
