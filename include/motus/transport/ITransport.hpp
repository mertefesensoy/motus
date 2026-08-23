#pragma once

// The transport seam.
//
// ---------------------------------------------------------------------------
// What this is and why it exists
// ---------------------------------------------------------------------------
// The system this library was extracted from began bound to one messaging library at the
// source level: its producer, consumer and publisher all named AMQP-CPP types in their
// headers, so "swap the client library" meant editing every file that touches messaging.
//
// That mattered for three reasons, of which the third is the sharp one:
//
//   1. AMQP-CPP is the least portable part of the stack. Its own event-loop integrations do
//      not compile on Windows, which is why AmqpConnection is hand-written.
//   2. The next middleware requirement is rarely more of the same middleware -- DDS, Kafka,
//      MQTT -- and it must arrive as a module rather than a rewrite.
//   3. Nothing enforced that two transports behave alike. That system published messages
//      transient onto a durable queue for an entire phase and nothing reported it --
//      publisher and consumer agreed perfectly because both were the same library making the
//      same assumption. A second backend without a shared, enforced contract would simply
//      reproduce that class of failure once per backend.
//
// So the goal is not "support two libraries". It is to make the delivery mechanism a module
// with a contract, and to make the contract executable -- see the conformance suite.
//
// ---------------------------------------------------------------------------
// What stays ABOVE this seam
// ---------------------------------------------------------------------------
// dispatch(), format_version checking, the reject-without-requeue policy, the payload size
// bound and the message counters. A backend supplies a byte pipe, declares what it can do, and
// performs the acknowledgement it is told to perform. It never decides Ack vs Reject.
//
// If poison-message policy lived below this line, every new library would re-implement it and
// one of them would get it wrong -- silently, until a malformed message arrived in production.

#include <chrono>
#include <cstdint>
#include <functional>
#include <stdexcept>
#include <string>
#include <vector>   // routeMatches() below. Was arriving transitively; that is not a guarantee.

namespace motus {

/**
 * What should happen to a received message.
 *
 * Lives here rather than in Consumer.hpp because it is the vocabulary of the seam: the
 * decision is made above it and carried out below it.
 *
 * ---------------------------------------------------------------------------
 * Why there are three outcomes and not two
 * ---------------------------------------------------------------------------
 * Until now the system had only Ack and Reject, and rejection was always *without requeue* --
 * justified by "every failure reachable here is deterministic, so a retry would fail
 * identically and spin the worker forever".
 *
 * That reasoning was correct while handlers only printed what arrived. It stops being correct
 * the moment a handler does real work: a full disk, a locked output directory or an
 * unavailable downstream are all *transient* handler failures, and rejecting one destroys a
 * message permanently for a reason that would have cleared in thirty seconds.
 *
 * Requeue is the missing third answer: "I could not process this now; give it to someone
 * else." It is deliberately NOT the default -- an unbounded requeue on a genuinely permanent
 * failure recreates the infinite spin, which is why the policy in dispatch() requeues at most
 * once and uses Delivery::redelivered to notice the second attempt.
 *
 * ---------------------------------------------------------------------------
 * Deferred
 * ---------------------------------------------------------------------------
 * "I have taken responsibility for this message; do not settle it yet." The handler returns
 * synchronously, exactly as for the other three, but the delivery stays unresolved at the
 * middleware until a LATER, SEPARATE call to ITransport::settle() names it explicitly by its
 * Delivery::tag.
 *
 * This exists for exactly one reason: a consumer that executes work must be able to ack on
 * COMPLETION, not on enqueue, so a crash between "accepted the order" and "the work actually
 * ran" is at-least-once rather than silent loss. Acking on enqueue -- which is what returning
 * Ack from a handler that merely queued the work would do -- defeats that guarantee entirely; a
 * queued-but-not-yet-run task is indistinguishable from a completed one to the broker.
 *
 * Proven against a live broker before this enumerator was added: a delivery tag survives past
 * the onReceived callback's return, across an intervening pump() boundary, and settles correctly
 * from a call that is not inside any middleware callback frame. The conformance suite pins that
 * permanently (tests/conformance/DeliveryConformanceTest.cpp).
 *
 * A backend that cannot support this MUST declare Capabilities::deferredAcknowledgement = false
 * and implement settle() to throw -- Consumer refuses at construction, before any message ever
 * reaches a handler that could return Deferred, exactly like every other capability mismatch in
 * this project.
 */
enum class Disposition
{
    Ack,       ///< processed successfully; remove it from the middleware
    Reject,    ///< permanently unprocessable; discard without requeue
    Requeue,   ///< transiently unprocessable; return it for another attempt
    Deferred,  ///< resolved LATER via settle(), naming this delivery's tag explicitly
};

namespace transport {

/**
 * A logical destination name.
 *
 * TRAP: this is NOT an AMQP topic exchange. It is a middleware-neutral name for "where a
 * message goes". AmqpCppTransport maps it to a queue name (routing key on the default
 * exchange, exactly as before); a DDS backend would map it to a DDS topic; Kafka to a Kafka
 * topic. The AMQP reading of the word is the one that will mislead a reviewer, so it is named
 * here rather than left to be discovered.
 */
using Topic = std::string;

/**
 * Where to reach the middleware.
 *
 * Deliberately not AMQP-specific. `realm` is the vhost for AMQP, would be the domain or
 * partition for DDS, and is unused by backends that have no such concept.
 */
struct Endpoint
{
    std::string   host     = "127.0.0.1";
    std::uint16_t port     = 0;          ///< 0 means "this backend's default port"
    std::string   user     = "guest";
    std::string   password = "guest";
    std::string   realm    = "/";
};

/**
 * What a backend can actually do.
 *
 * This struct exists because of a real defect. The system this library was extracted from
 * published messages transient onto a durable queue for an entire phase and nothing anywhere
 * reported it: the capability was absent and the system behaved exactly as though it were
 * present. Any broker restart would have destroyed every queued message while the empty queue
 * survived to make it look as though nothing had ever been submitted.
 *
 * So a backend now *declares* what it supports, callers state what they require, and a
 * mismatch fails at STARTUP with a diagnostic naming the backend and the missing capability --
 * never silently, and never at 3am after a broker restart.
 *
 * Adding a field here is a contract change: every backend must answer it honestly, and the
 * conformance suite gains a scenario that is skipped-with-a-reason where it is false.
 */
struct Capabilities
{
    const char *name                = "unknown";

    bool durableDelivery            = false;  ///< a sent message survives a middleware restart
    bool publisherConfirms          = false;  ///< send() can confirm the peer STORED it
    bool manualAcknowledgement      = false;  ///< the consumer decides ack vs reject
    bool redeliveryFlag             = false;  ///< a redelivery is distinguishable from a first
    bool deadLetter                 = false;  ///< rejected messages can be diverted, not dropped
    bool routingGroups              = false;  ///< destinations can be grouped and pattern-routed
    bool fifoPerTopic               = false;  ///< ordering holds for a single consumer
    bool competingConsumers         = false;  ///< exactly-once across several consumers
    bool topicDepth                 = false;  ///< can report how many messages are waiting
    bool purge                      = false;  ///< can discard everything waiting at a destination
    bool requeue                    = false;  ///< Disposition::Requeue can actually be honoured
    bool reconnect                  = false;  ///< setReconnectPolicy is honoured, not ignored

    /// Disposition::Deferred can actually be honoured: a delivery tag survives past its
    /// onReceived-equivalent callback and settle() can resolve it later, from outside that
    /// callback. A backend that cannot guarantee this must declare false rather than let a
    /// deferred delivery silently behave like an immediate ack -- exactly the kind of absent
    /// capability this struct exists to make loud instead of discovered at 3am.
    bool deferredAcknowledgement    = false;

    /// A destination can be declared with TopicOptions::maxLength > 0 and genuinely enforces
    /// TopicOptions::overflow once that many messages are waiting, rather than silently
    /// accepting the declaration and remaining unbounded. That shape of failure is exactly
    /// what this struct exists to prevent for every other property here.
    ///
    /// Added for live, lossy state streams -- telemetry being the canonical case: a slow or
    /// absent viewer must never grow an unbounded backlog on the broker, and must never be
    /// able to backpressure the publisher either.
    bool boundedQueue               = false;
};

/**
 * What the caller needs from a destination.
 *
 * Stated by the caller rather than assumed by the backend, and that is the entire point. AMQP
 * rejects a redeclaration whose properties differ with a 406 PRECONDITION_FAILED that closes
 * the channel, so two backends declaring the same topic with different defaults means whichever
 * process starts second dies -- an order-dependent failure that is miserable to reproduce.
 *
 * That risk is not hypothetical. SimpleAmqpClient's DeclareQueue defaults are durable=false,
 * exclusive=true, auto_delete=true: the exact opposite of motus's queue on all three counts.
 *
 * Neutral on purpose. Every middleware expresses these three somehow: AMQP as
 * durable / not-exclusive / not-auto-delete, DDS as durability QoS and shared subscriptions.
 */
/**
 * How a routing group decides which destinations a message reaches.
 *
 * Neutral vocabulary on purpose. AMQP spells these `direct`, `topic` and `fanout`; DDS would
 * express them through partitions and content filters. Naming the AMQP words here would put
 * broker-specific concepts into the one interface built to keep them out -- the same reason
 * this file says `Topic` and never `queue`.
 */
enum class Routing
{
    Exact,       ///< the route must equal the binding exactly       (AMQP: direct)
    Pattern,     ///< the route is matched against a pattern         (AMQP: topic)
    Broadcast,   ///< every destination in the group receives it     (AMQP: fanout)
};

/** Which end of a bounded destination is sacrificed when it reaches maxLength. */
enum class OverflowPolicy
{
    DropHead,         ///< discard/dead-letter the oldest and admit the new publish
    RejectPublish,    ///< preserve the existing prefix and refuse the new publish
    RejectPublishDlx, ///< refuse the new publish and dead-letter it when a DLX exists
};

inline const char *overflowPolicyName(OverflowPolicy policy)
{
    switch (policy)
    {
    case OverflowPolicy::DropHead:         return "drop-head";
    case OverflowPolicy::RejectPublish:    return "reject-publish";
    case OverflowPolicy::RejectPublishDlx: return "reject-publish-dlx";
    }
    return "unknown";
}

/**
 * What `Routing::Pattern` MEANS. Part of the contract, not one backend's convenience.
 *
 * A route and a binding are dot-separated words. `*` matches exactly one word; `#` matches
 * ZERO OR MORE. So `TOPIC.Nav.*` matches `TOPIC.Nav.Position` but neither `TOPIC.Nav` nor
 * `TOPIC.Nav.A.B`, while `TOPIC.#` matches all three and `TOPIC` besides.
 *
 * It lives here because a backend that routed differently would still pass a contract that
 * never said what matching means -- and two backends silently disagreeing about routing is
 * the kind of defect that only shows up in production. A broker-backed transport never calls
 * this (the broker matches), but it is the specification that transport is checked against.
 *
 * Backtracking over `#` is required for correctness, not thoroughness: the cheap version that
 * treats `#` as "matches the rest" gets `A.#.C` wrong.
 */
inline bool routeMatches(const std::string &binding, const std::string &route)
{
    const auto split = [](const std::string &text) {
        std::vector<std::string> parts;
        std::size_t start = 0;
        for (;;)
        {
            const std::size_t dot = text.find('.', start);
            parts.push_back(
                text.substr(start, dot == std::string::npos ? std::string::npos : dot - start));
            if (dot == std::string::npos) break;
            start = dot + 1;
        }
        return parts;
    };

    const std::vector<std::string> pattern = split(binding);
    const std::vector<std::string> words   = split(route);

    // Explicit recursion over indices; depth is bounded by the pattern's word count.
    struct Matcher
    {
        const std::vector<std::string> &p;
        const std::vector<std::string> &k;

        bool at(std::size_t i, std::size_t j) const
        {
            if (i == p.size()) return j == k.size();
            if (p[i] == "#")
            {
                for (std::size_t n = j; n <= k.size(); ++n)
                    if (at(i + 1, n)) return true;
                return false;
            }
            if (j == k.size()) return false;
            if (p[i] != "*" && p[i] != k[j]) return false;
            return at(i + 1, j + 1);
        }
    };

    return Matcher{pattern, words}.at(0, 0);
}

struct TopicOptions
{
    bool durable          = true;   ///< the destination itself survives a middleware restart
    bool shared           = true;   ///< several consumers may attach (AMQP: NOT exclusive)
    bool deleteWhenUnused = false;  ///< remove it once the last consumer detaches

    /**
     * Preserve messages rejected without requeue in this topic's forensic destination.
     *
     * This is a destination property, not a consumer preference. Producer and Consumer both
     * declare the same destination, so they must agree on it just as they agree on durability.
     * AMQP maps it to x-dead-letter-exchange/x-dead-letter-routing-key; a backend without an
     * equivalent must refuse the declaration rather than silently discard rejected messages.
     */
    bool deadLetter = true;

    /**
     * Maximum messages this destination retains. 0 (the default) means unbounded, preserving
     * every call site that predates bounded queues. `overflow` decides whether the oldest or
     * newest message is sacrificed once this limit is reached.
     *
     * A backend must refuse a nonzero value at declaration unless Capabilities::boundedQueue is
     * true (checkTopicOptions below) -- silently accepting it and staying unbounded is the same
     * failure shape deadLetter's capability check exists to prevent.
     *
     * TRAP: any nonzero value diverges from the zero default, so checkTopicOptions's SYMMETRY
     * check (separate from the capability check above) refuses it too unless the caller passes
     * allowAsymmetric=true -- exactly the same requirement deadLetter=false already has. A
     * bounded destination therefore always needs allowAsymmetric; that is not optional cleanup,
     * it is inherent to wanting a non-default value here at all.
     */
    std::uint32_t maxLength = 0;

    /**
     * Overflow policy for a bounded destination. DropHead is the compatibility default and the
     * correct policy for live state streams. RejectPublish preserves a forensic queue's first
     * failures; RejectPublishDlx additionally tries to dead-letter the refused new message.
     * AMQP maps these values directly to x-overflow.
     */
    OverflowPolicy overflow = OverflowPolicy::DropHead;

    /**
     * The routing domain this destination belongs to. AMQP: the exchange. DDS: the partition.
     *
     * EMPTY PRESERVES THE PREVIOUS BEHAVIOUR EXACTLY -- AMQP's default exchange, which routes
     * by destination name and requires no declaration. That default is what makes this field
     * additive: every call site that existed before this field did keeps working unchanged,
     * and a backend that cannot group destinations only fails when one is actually asked for.
     */
    std::string group;

    /**
     * The key this destination is reached by within its group. AMQP: the binding's routing key.
     *
     * Empty means "the topic name", which is what the default exchange does implicitly.
     */
    std::string route;

    /// How `group` matches `route`. Ignored when `group` is empty.
    Routing routing = Routing::Pattern;
};

/// One durable exchange receives every motus dead letter. Each source topic gets its own queue,
/// so inspecting one failure stream never consumes another topic's forensic evidence.
inline constexpr const char *kDeadLetterExchange = "motus.dead-letter";
inline constexpr const char *kDeadLetterPrefix   = "motus.dead-letter.";
inline constexpr std::uint32_t kForensicMaxLength = 2000;

inline Topic deadLetterTopic(const Topic &source)
{
    return std::string(kDeadLetterPrefix) + source;
}

/**
 * How close a forensic destination is to losing evidence.
 *
 * WHY THIS IS A FUNCTION AND NOT AN `if` AT THE CALL SITE. An operator status command is where
 * a full forensic queue has to be reported -- but anything written inside a `main.cpp` is
 * unreachable from a test binary. An alarm that cannot be tested is an alarm nobody has checked
 * fires, so the classification lives here, where a unit test can pin its exact thresholds.
 *
 * `AtCap` is the one that matters and it is not a warning about the future: `forensicTopicOptions`
 * sets `OverflowPolicy::RejectPublish`, so a full queue means the broker is refusing new dead
 * letters RIGHT NOW. That policy is deliberate -- the FIRST failures are the diagnostically
 * valuable ones and a flood of later duplicates must not push them out -- and the cost is that
 * failures after the cap leave no trace at all. This function is what reports that cost.
 */
enum class ForensicPressure
{
    Normal,  ///< room to spare
    NearCap, ///< within 10% of the cap; new evidence will start being refused
    AtCap,   ///< full, and therefore DROPPING new evidence
};

inline ForensicPressure forensicPressure(std::uint32_t depth,
                                         std::uint32_t cap = kForensicMaxLength)
{
    // A zero cap means unbounded, which cannot be at capacity. Guarding it here rather than at the
    // call site keeps the "cap of 0 means no limit" convention in one place -- `maxLength` uses the
    // same convention, and the two must not disagree.
    if (cap == 0) return ForensicPressure::Normal;
    if (depth >= cap) return ForensicPressure::AtCap;

    // 90% without floating point: depth/cap >= 9/10. Integer-only because a rounding difference
    // here would move the boundary by a message and make the threshold untestable by exact value.
    if (static_cast<std::uint64_t>(depth) * 10u >= static_cast<std::uint64_t>(cap) * 9u)
        return ForensicPressure::NearCap;
    return ForensicPressure::Normal;
}

/** One shared declaration contract for every automatically provisioned forensic queue. */
inline TopicOptions forensicTopicOptions(const Topic &source)
{
    TopicOptions forensic;
    forensic.group      = kDeadLetterExchange;
    forensic.route      = source;
    forensic.routing    = Routing::Exact;
    forensic.deadLetter = false;
    forensic.maxLength  = kForensicMaxLength;
    forensic.overflow   = OverflowPolicy::RejectPublish;
    return forensic;
}

/**
 * What the caller needs from one send.
 *
 * `durable` is checked against Capabilities::durableDelivery when the Producer is constructed,
 * not quietly ignored at send time.
 */
/**
 * Check a declaration against what the backend can honour, and against what must match across
 * processes.
 *
 * ONE function, called from every place a topic is declared, because the alternative is the
 * same rule written twice and drifting. Producer and Consumer BOTH declare -- deliberately, so
 * either can start first -- which is exactly the situation that makes a duplicated rule
 * dangerous.
 *
 * Two checks, failing for different reasons:
 *
 * 1. **Capability.** Asking for a routing group from a backend that cannot group destinations
 *    would otherwise be SILENTLY IGNORED: the message goes to the default destination, arrives
 *    somewhere plausible, and nothing reports that the routing asked for never happened. That
 *    is the same shape as the transient-messages-on-a-durable-queue defect this project
 *    shipped for a whole phase -- the capability was absent and the system behaved exactly as
 *    though it were present.
 *
 * 2. **Symmetry.** `durable`, `shared`, `deleteWhenUnused`, `deadLetter`, `maxLength`, and
 *    `overflow` must agree between
 *    every process that declares the destination, or whichever starts second is refused with
 *    AMQP's 406 PRECONDITION_FAILED. Producer and Consumer therefore pin them to the documented
 *    defaults, and a caller that genuinely needs otherwise has to say so.
 *
 * @param allowAsymmetric  opts out of check 2. A named argument at the call site rather than a
 *                         field on the struct, so it cannot be set once and then quietly
 *                         inherited by a later declaration that never intended it.
 *
 * @throws std::runtime_error naming the backend and precisely which property is wrong.
 */
inline void checkTopicOptions(const Capabilities &caps,
                              const TopicOptions &options,
                              bool allowAsymmetric = false)
{
    if (options.deadLetter && !caps.deadLetter)
    {
        throw std::runtime_error(
            std::string("backend '") + caps.name +
            "' cannot preserve rejected messages (Capabilities::deadLetter is false), but "
            "dead-lettering was requested. Refused at declaration rather than silently "
            "discarding the forensic payload.");
    }

    if (!options.group.empty() && !caps.routingGroups)
    {
        throw std::runtime_error(
            std::string("backend '") + caps.name +
            "' cannot group destinations (Capabilities::routingGroups is false), but the "
            "routing group '" + options.group +
            "' was requested. Refused at declaration rather than ignored: a silently dropped "
            "group publishes to the default destination and looks exactly like success.");
    }

    if (options.maxLength > 0 && !caps.boundedQueue)
    {
        throw std::runtime_error(
            std::string("backend '") + caps.name +
            "' cannot bound a destination's depth (Capabilities::boundedQueue is false), but "
            "maxLength=" + std::to_string(options.maxLength) +
            " was requested. Refused at declaration rather than ignored: a silently unbounded "
            "'bounded' destination is exactly the failure this struct exists to make loud.");
    }

    if (allowAsymmetric) return;

    const TopicOptions defaults;
    const char *offender = (options.durable          != defaults.durable)  ? "durable"
                         : (options.shared           != defaults.shared)   ? "shared"
                         : (options.deleteWhenUnused != defaults.deleteWhenUnused)
                                                                           ? "deleteWhenUnused"
                         : (options.deadLetter       != defaults.deadLetter) ? "deadLetter"
                         : (options.maxLength        != defaults.maxLength)  ? "maxLength"
                         : (options.overflow         != defaults.overflow)   ? "overflow"
                                                                           : nullptr;
    if (offender != nullptr)
    {
        throw std::runtime_error(
            std::string("topic property '") + offender +
            "' differs from the default. Producer and Consumer declare the same destination, so "
            "whichever started second would be refused with PRECONDITION_FAILED. Pass "
            "allowAsymmetric if that is intended, or declare through ITransport directly.");
    }
}

struct SendOptions
{
    bool durable = true;    ///< the MESSAGE must survive a middleware restart
    bool confirm = false;   ///< block until the peer confirms it stored the message

    /**
     * Override the routing key for this one message. Empty means "the route this destination
     * was declared with", which is what every caller predating this field gets.
     *
     * Exists because a routing key is a property of a MESSAGE, not of a destination -- that is
     * how AMQP works, and tying the two together made a whole shape of publisher inexpressible:
     * one sender emitting many keys into one group, publishing N related payloads to one
     * exchange under per-item keys without declaring N destinations.
     *
     * THE TRAP, stated because it is silent: a route matching no binding is DISCARDED by the
     * broker, with no error to the publisher. A typo here does not fail; it evaporates. That is
     * the cost of the flexibility and it is why the conformance contract asserts non-delivery
     * as explicitly as it asserts delivery.
     */
    std::string route;
};

/**
 * Whether, and how hard, a transport should try to restore a connection the peer has closed.
 *
 * ---------------------------------------------------------------------------
 * OFF by default, deliberately
 * ---------------------------------------------------------------------------
 * Reconnection is opt-in for two reasons. It changes behaviour that operators plan around --
 * "no reconnection" has been a documented limitation of this system, and a worker that silently
 * comes back is a different operational animal from one that exits and is restarted by a
 * supervisor. And a transport that reconnects by default would make the absence of reconnection
 * untestable, when the absence is exactly what most deployments will still be running.
 *
 * ---------------------------------------------------------------------------
 * What reconnection does NOT do
 * ---------------------------------------------------------------------------
 * It does not recover in-flight work. Any delivery that was outstanding when the connection
 * died has already been returned to the middleware and may have gone to another worker; the
 * old delivery tags are meaningless on the new connection and are never acknowledged. That is
 * the abandonment clause doing its job, and it is why reconnection is safe to add: at-least-once
 * already required every handler to tolerate a repeat.
 *
 * It also does not paper over a broker that is refusing connections for a reason -- bad
 * credentials, a wrong vhost, a full disk. Attempts are bounded, and exhausting them is a
 * permanent failure that pump() reports.
 */
struct ReconnectPolicy
{
    bool     enabled     = false;   ///< opt-in; see above
    unsigned maxAttempts = 5;       ///< consecutive failures before giving up permanently

    /// Exponential backoff: initialDelay, doubled per attempt, capped at maxDelay. Bounded
    /// rather than indefinite, because a worker retrying forever against a broker that will
    /// never answer looks healthy while doing nothing -- the failure mode this project keeps
    /// finding.
    std::chrono::milliseconds initialDelay{250};
    std::chrono::milliseconds maxDelay{5000};
};

/// One received message, as the middleware presented it.
struct Delivery
{
    std::string   body;
    bool          redelivered = false;  ///< false when the backend cannot tell (see Capabilities)

    /// Backend-defined; opaque above the seam. Ordinarily for logging only -- but if the handler
    /// returns Disposition::Deferred, THIS is also the value settle() must be called with later.
    /// A caller that defers a delivery and never keeps this value has no way to resolve it.
    std::uint64_t tag         = 0;
};

/**
 * Decides what to do with one delivery.
 *
 * The DECISION is made above the seam -- ultimately by motus::dispatch(). The ACKNOWLEDGEMENT is
 * performed below it, because every library spells it differently and none of them agree.
 *
 * ---------------------------------------------------------------------------
 * If this handler THROWS -- a mandatory part of the contract
 * ---------------------------------------------------------------------------
 * A throw means the worker failed *uncooperatively*: it did not decide anything, it broke.
 * Every implementation of ITransport MUST then:
 *
 *   1. NOT acknowledge and NOT reject the delivery -- it stays unresolved;
 *   2. leave it eligible for redelivery, so that close() or destruction returns it to the
 *      topic flagged as redelivered;
 *   3. report the failure out of pump(), rather than letting it escape through middleware
 *      callback machinery.
 *
 * This is distinct from Disposition::Requeue, and the distinction is not academic. Requeue is a
 * decision: "I understand this message and cannot process it right now." A throw is the absence
 * of a decision. Conflating them would let a conformance suite claim to verify crash-safety
 * while actually verifying cooperative hand-back -- a green test asserting something other than
 * its own name.
 *
 * This clause is what makes "unacknowledged work comes back when a worker dies" -- the entire
 * reason manual acknowledgement exists -- a portable guarantee every backend must honour,
 * rather than an AMQP implementation detail. A backend that cannot honour it must declare
 * manualAcknowledgement = false.
 *
 * Note point 3 specifically. AMQP-CPP invokes this handler from inside its frame parser, and an
 * exception escaping through there travels the same route that already terminated this process
 * once by unwinding into a destructor. Backends catch, stash, and rethrow from pump().
 */
using DeliveryHandler = std::function<Disposition(const Delivery &)>;

/**
 * What the transport actually DID -- not what it was asked to do.
 *
 * acksPerformed and rejectsPerformed deliberately duplicate Consumer::acked() and rejected(),
 * which count what was *decided* above the seam. The redundancy is the point: a conformance
 * scenario asserts the two agree, which detects a backend that accepts a Reject and then
 * silently fails to send it. That is silent message loss, and no single counter could reveal
 * it -- both sides would report the same comfortable number.
 */
struct Stats
{
    std::uint64_t bytesSent     = 0;
    std::uint64_t bytesReceived = 0;
    std::uint64_t sent          = 0;
    std::uint64_t received      = 0;
    std::uint64_t acksPerformed     = 0;
    std::uint64_t rejectsPerformed  = 0;
    std::uint64_t requeuesPerformed = 0;

    /// Deliveries the handler threw on, left unresolved and returned when the transport closes.
    /// A rising count here means workers are crashing mid-job, which no other counter shows:
    /// acked, rejected and requeued all stay flat while work quietly goes round again.
    std::uint64_t abandoned         = 0;

    /// Successful reconnections. A worker that is quietly reconnecting every few minutes is
    /// serving traffic and looks entirely healthy, so this is the only place that shows it.
    std::uint64_t reconnects        = 0;
};

/**
 * A delivery mechanism.
 *
 * ---------------------------------------------------------------------------
 * Concurrency
 * ---------------------------------------------------------------------------
 * Single-threaded, like everything else in motus. One instance is driven by one thread, and
 * scaling means one transport per thread. The rule is stated at the interface so it is not
 * rediscovered per implementation.
 *
 * ---------------------------------------------------------------------------
 * Error handling
 * ---------------------------------------------------------------------------
 * Every method taking a timeout is bounded -- a middleware that accepts a connection and then
 * goes quiet must produce a timeout with a diagnostic, never an indefinite hang (NFR-1).
 *
 * Failures throw std::runtime_error. send() returns false only for "the peer refused this
 * frame", which is recoverable and which the caller decides how to treat.
 *
 * Implementations must not throw from destructor-driven paths: AMQP-CPP calls back into its
 * handler from ~Channel, and a throw there reaches a noexcept boundary and calls
 * std::terminate. This has already aborted a worker process once.
 */
class ITransport
{
public:
    virtual ~ITransport() = default;

    ITransport()                              = default;
    ITransport(const ITransport &)            = delete;
    ITransport &operator=(const ITransport &) = delete;

    /// Honest declaration of what this backend supports. Must be callable before connect().
    virtual Capabilities capabilities() const = 0;

    virtual void connect(const Endpoint &endpoint, std::chrono::milliseconds timeout) = 0;

    /// Graceful shutdown. Safe to call more than once, and safe after the peer has already
    /// gone -- every shutdown path in the project calls this without a try/catch.
    virtual void close(std::chrono::milliseconds timeout) = 0;

    /// Usable RIGHT NOW: connected and not since closed. Not "was ready once" -- a latched
    /// flag that never clears reports a dead connection as healthy, which has happened here.
    virtual bool ready() const = 0;

    /// Idempotent. Creates the destination if this middleware has the concept.
    /// Options come from the caller so that two backends cannot disagree about them.
    virtual void ensureTopic(const Topic &topic,
                             const TopicOptions &options,
                             std::chrono::milliseconds timeout) = 0;

    /// How many messages are waiting.
    /// Throws std::logic_error unless capabilities().topicDepth -- a fabricated zero would be
    /// worse than a refusal, because it would be believed.
    virtual std::uint32_t topicDepth(const Topic &topic,
                                     std::chrono::milliseconds timeout) = 0;

    /**
     * Discard every message waiting at a destination. Returns how many were removed.
     *
     * DESTRUCTIVE, and deliberately not part of any normal path. It exists because a durable
     * destination accumulates across runs: republishing is idempotent, the queue behind it is
     * not, so a second run leaves a destination holding two copies and a rehearsal poisons the
     * run that matters.
     *
     * Scoped to ONE named destination, so a caller can only ever empty something it named. There
     * is deliberately no "purge everything" -- that would be administering the broker, which is
     * outside this project's scope line.
     *
     * Throws std::logic_error unless capabilities().purge. A silent no-op would be worse than a
     * refusal: the caller would believe the destination was empty and report depths that never
     * dropped.
     */
    virtual std::uint32_t purgeTopic(const Topic &topic,
                                     std::chrono::milliseconds timeout) = 0;

    /// Returns false if the peer refused the frame. A true return means the payload was handed
    /// to the middleware, NOT that it was stored -- see Capabilities::publisherConfirms.
    virtual bool send(const Topic &topic,
                      const std::string &payload,
                      const SendOptions &options) = 0;

    /// Begin delivering from `topic` to `handler`. At most one active subscription per
    /// instance, per the single-threaded model.
    virtual void subscribe(const Topic &topic,
                           DeliveryHandler handler,
                           std::chrono::milliseconds timeout) = 0;

    /**
     * Resolve a delivery that was returned as Disposition::Deferred, using the SAME value
     * Delivery::tag carried for it.
     *
     * MUST be called from the thread that owns this transport instance and drives its pump() --
     * the single-threaded rule (see the class comment) applies here exactly as everywhere else.
     * A pool worker that finished the deferred work must hand the outcome to that thread rather
     * than call this directly; it must never touch the transport itself. SettlementQueue in
     * Consumer.hpp is the boundary built for exactly that hand-off.
     *
     * @param tag          the Delivery::tag from the original, now-deferred delivery.
     * @param disposition  Ack, Reject or Requeue -- never Deferred again, which would mean
     *                     settling a delivery into the very state it is already being settled
     *                     OUT of.
     *
     * Throws std::logic_error unless capabilities().deferredAcknowledgement, or if `tag` does
     * not name a delivery this transport is currently holding deferred (already settled, or
     * never deferred in the first place). Throws std::invalid_argument for
     * disposition == Deferred.
     */
    virtual void settle(std::uint64_t tag, Disposition disposition) = 0;

    /**
     * Ask this transport to restore a connection the peer closes.
     *
     * Default implementation is a no-op, so a backend that cannot reconnect is not forced to
     * pretend. Capabilities::reconnect states which is which -- callers that need reconnection
     * must check it rather than assume this call did anything.
     */
    virtual void setReconnectPolicy(const ReconnectPolicy &policy) { (void)policy; }

    /// Service the transport for up to `slice`, delivering whatever arrives and performing the
    /// acknowledgements the handler asked for. Returns false once the peer is gone.
    ///
    /// With a reconnect policy enabled, returns TRUE while it is still trying -- the transport
    /// is not dead, it is between connections -- and false only once attempts are exhausted.
    /// ready() is the accurate question during that window.
    ///
    /// A blocking backend implements this as a bounded receive; an asynchronous one as its
    /// pump loop. It is also what flushes queued outbound frames on backends where send() is
    /// asynchronous, and is harmless on backends where it is not.
    virtual bool pump(std::chrono::milliseconds slice) = 0;

    virtual Stats stats() const = 0;
};

} // namespace transport
} // namespace motus
