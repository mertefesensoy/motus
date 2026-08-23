/*
 * The declaration guard.
 *
 * This exists because the capability was added and NOT enforced, and the gap was written down
 * as though it had been. A backend declaring routingGroups=false would have had a requested
 * group silently ignored: the message goes to the default destination, arrives somewhere
 * plausible, and nothing reports that the routing never happened.
 *
 * That is the same shape as the defect this whole capability model was built for -- messages
 * published transient onto a durable queue for an entire phase, where the capability was
 * absent and the system behaved exactly as though it were present. A guard that is declared
 * but never fires is worse than no guard, because it is believed.
 */
#include "motus/transport/ITransport.hpp"

#include <gtest/gtest.h>

#include <string>

using namespace motus::transport;

namespace {

Capabilities withGroups(bool supported)
{
    Capabilities caps;
    caps.name          = supported ? "supports-groups" : "no-groups";
    caps.routingGroups = supported;
    caps.deadLetter    = true;
    return caps;
}

Capabilities withDeadLetters(bool supported)
{
    Capabilities caps = withGroups(true);
    caps.name       = supported ? "supports-dead-letters" : "drops-rejections";
    caps.deadLetter = supported;
    return caps;
}

Capabilities withBoundedQueue(bool supported)
{
    Capabilities caps = withGroups(true);
    caps.name         = supported ? "supports-bounded-queue" : "always-unbounded";
    caps.boundedQueue = supported;
    return caps;
}

} // namespace

// ------------------------------------------------------------------------------------------
// Capability
// ------------------------------------------------------------------------------------------

// Production symptom: a backend that can preserve rejects is refused at startup, preventing all
// producers and consumers from attaching after the 1.4.1 migration.
TEST(TopicOptionsCheck, DeadLetteringOnACapableBackendIsAccepted)
{
    EXPECT_NO_THROW(checkTopicOptions(withDeadLetters(true), TopicOptions{}));
}

// Production symptom: a backend silently accepts protected topics and then destroys every
// reject-without-requeue payload instead of preserving forensic evidence.
TEST(TopicOptionsCheck, DeadLetteringOnAnIncapableBackendIsRefusedLoudly)
{
    try
    {
        checkTopicOptions(withDeadLetters(false), TopicOptions{});
        FAIL() << "expected a refusal";
    }
    catch (const std::runtime_error &e)
    {
        const std::string message = e.what();
        EXPECT_NE(message.find("drops-rejections"), std::string::npos) << message;
        EXPECT_NE(message.find("deadLetter"), std::string::npos) << message;
    }
}

TEST(TopicOptionsCheck, AGroupOnACapableBackendIsAccepted)
{
    TopicOptions options;
    options.group = "motus.codegen";
    EXPECT_NO_THROW(checkTopicOptions(withGroups(true), options));
}

TEST(TopicOptionsCheck, AGroupOnAnIncapableBackendIsRefused)
{
    TopicOptions options;
    options.group = "motus.codegen";
    EXPECT_THROW(checkTopicOptions(withGroups(false), options), std::runtime_error);
}

TEST(TopicOptionsCheck, TheRefusalNamesTheBackendAndTheGroup)
{
    // A diagnostic that says only "unsupported" sends someone to the wrong file. Naming both
    // the backend and the group it refused is what makes the message actionable.
    TopicOptions options;
    options.group = "motus.codegen";
    try
    {
        checkTopicOptions(withGroups(false), options);
        FAIL() << "expected a refusal";
    }
    catch (const std::runtime_error &e)
    {
        const std::string message = e.what();
        EXPECT_NE(message.find("no-groups"), std::string::npos) << message;
        EXPECT_NE(message.find("motus.codegen"), std::string::npos) << message;
        EXPECT_NE(message.find("routingGroups"), std::string::npos) << message;
    }
}

TEST(TopicOptionsCheck, NoGroupIsFineOnABackendThatCannotGroup)
{
    // The un-grouped path must stay open, or this guard would break every caller that predates
    // groups -- which is most of them.
    EXPECT_NO_THROW(checkTopicOptions(withGroups(false), TopicOptions{}));
}

TEST(TopicOptionsCheck, AnEmptyGroupIsNotAGroup)
{
    TopicOptions options;
    options.group = "";
    options.route = "some.route";   // a route without a group is meaningless, not an error here
    EXPECT_NO_THROW(checkTopicOptions(withGroups(false), options));
}

// Production symptom: a bounded/lossy telemetry destination is declared against a
// backend that cannot honour x-max-length/x-overflow, and instead of refusing loudly, the queue
// grows without limit -- the exact "capability absent, system behaves as though present" shape
// this whole struct exists to make loud instead of discovered at 3am.
//
// allowAsymmetric=true isolates the CAPABILITY check this test is about from the SYMMETRY
// check below: any nonzero maxLength diverges from the zero default by definition (nothing
// wants a bounded destination at the default), so without it this call would throw for the
// symmetry reason instead -- a real trap this test tripped over on first write. A telemetry
// declaration always needs allowAsymmetric for exactly this reason, same as deadLetter=false
// already does.
TEST(TopicOptionsCheck, MaxLengthOnACapableBackendIsAccepted)
{
    TopicOptions options;
    options.maxLength = 100;
    EXPECT_NO_THROW(checkTopicOptions(withBoundedQueue(true), options, /*allowAsymmetric=*/true));
}

TEST(TopicOptionsCheck, MaxLengthOnAnIncapableBackendIsRefusedLoudly)
{
    TopicOptions options;
    options.maxLength = 100;
    try
    {
        checkTopicOptions(withBoundedQueue(false), options);
        FAIL() << "expected a refusal";
    }
    catch (const std::runtime_error &e)
    {
        const std::string message = e.what();
        EXPECT_NE(message.find("always-unbounded"), std::string::npos) << message;
        EXPECT_NE(message.find("boundedQueue"), std::string::npos) << message;
    }
}

TEST(TopicOptionsCheck, ZeroMaxLengthIsFineOnABackendThatCannotBound)
{
    // The unbounded (default) path must stay open, or this guard would break every caller that
    // predates the field -- which is every existing schema and command topic.
    EXPECT_NO_THROW(checkTopicOptions(withBoundedQueue(false), TopicOptions{}));
}

// ------------------------------------------------------------------------------------------
// Symmetry
// ------------------------------------------------------------------------------------------

TEST(TopicOptionsCheck, TheDefaultsAreAccepted)
{
    EXPECT_NO_THROW(checkTopicOptions(withGroups(true), TopicOptions{}));
}

TEST(TopicOptionsCheck, EachSharedPropertyIsRefusedWhenItDivergesFromTheDefault)
{
    // Producer and Consumer both declare the same destination so either can start first. If
    // they disagree on any shared declaration property, whichever starts SECOND is refused by the broker
    // with PRECONDITION_FAILED -- an order-dependent failure that is miserable to reproduce.
    {
        TopicOptions options;
        options.durable = false;
        EXPECT_THROW(checkTopicOptions(withGroups(true), options), std::runtime_error);
    }
    {
        TopicOptions options;
        options.shared = false;
        EXPECT_THROW(checkTopicOptions(withGroups(true), options), std::runtime_error);
    }
    {
        TopicOptions options;
        options.deleteWhenUnused = true;
        EXPECT_THROW(checkTopicOptions(withGroups(true), options), std::runtime_error);
    }
    {
        TopicOptions options;
        options.deadLetter = false;
        EXPECT_THROW(checkTopicOptions(withGroups(true), options), std::runtime_error);
    }
    {
        // Capability-gated separately from the other three (checkTopicOptions refuses a
        // nonzero maxLength outright on a backend that cannot bound at all), so this uses
        // withBoundedQueue(true) rather than withGroups(true) -- otherwise the throw here would
        // prove the CAPABILITY check fired, not the SYMMETRY check this block is about.
        TopicOptions options;
        options.maxLength = 50;
        EXPECT_THROW(checkTopicOptions(withBoundedQueue(true), options), std::runtime_error);
    }
}

TEST(TopicOptionsCheck, TheRefusalNamesTheOffendingProperty)
{
    TopicOptions options;
    options.shared = false;
    try
    {
        checkTopicOptions(withGroups(true), options);
        FAIL() << "expected a refusal";
    }
    catch (const std::runtime_error &e)
    {
        EXPECT_NE(std::string(e.what()).find("shared"), std::string::npos) << e.what();
    }
}

TEST(TopicOptionsCheck, AllowAsymmetricOptsOutOfTheSymmetryCheckOnly)
{
    TopicOptions options;
    options.durable = false;
    EXPECT_NO_THROW(checkTopicOptions(withGroups(true), options, /*allowAsymmetric=*/true));
}

TEST(TopicOptionsCheck, AllowAsymmetricDoesNotOptOutOfTheCapabilityCheck)
{
    // The two checks fail for entirely different reasons, and the escape hatch is for one of
    // them. Letting it suppress both would turn a deliberate override of a coordination rule
    // into a silent loss of routing.
    TopicOptions options;
    options.group   = "motus.codegen";
    options.durable = false;
    EXPECT_THROW(checkTopicOptions(withGroups(false), options, /*allowAsymmetric=*/true),
                 std::runtime_error);
}
