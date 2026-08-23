/*
 * What `Routing::Pattern` means.
 *
 * Tested directly rather than only through delivery assertions, for two reasons. A routing bug
 * found by "the message did not arrive" is miserable to localise -- it looks identical to a
 * consumer that is not running, a binding that was never created, and a broker that dropped it.
 * And this function is the SPECIFICATION the AMQP backend is checked against: RabbitMQ does its
 * own matching, so if these rules are wrong the two backends disagree and only production finds
 * out.
 *
 * The cases below are the ones the cheap implementation gets wrong. Treating `#` as "matches
 * the rest and return true" passes every single-wildcard case and fails A.#.C.
 */
#include "motus/transport/ITransport.hpp"

#include <gtest/gtest.h>

using motus::transport::routeMatches;

// ------------------------------------------------------------------------------------------
// Literals
// ------------------------------------------------------------------------------------------

TEST(RouteMatch, AnExactBindingMatchesOnlyItself)
{
    EXPECT_TRUE(routeMatches("TOPIC.TrackUpdate", "TOPIC.TrackUpdate"));
    EXPECT_FALSE(routeMatches("TOPIC.TrackUpdate", "TOPIC.TrackDrop"));
}

TEST(RouteMatch, MatchingIsCaseSensitive)
{
    // Topic names are already case-sensitive in the schema grammar; routing must not quietly
    // be more forgiving than the thing it routes.
    EXPECT_FALSE(routeMatches("TOPIC.Nav", "TOPIC.nav"));
}

TEST(RouteMatch, ASingleWordMatchesItself)
{
    EXPECT_TRUE(routeMatches("TOPIC", "TOPIC"));
    EXPECT_FALSE(routeMatches("TOPIC", "OTHER"));
}

// ------------------------------------------------------------------------------------------
// `*` -- exactly one word
// ------------------------------------------------------------------------------------------

TEST(RouteMatch, StarMatchesExactlyOneWord)
{
    EXPECT_TRUE(routeMatches("TOPIC.Nav.*", "TOPIC.Nav.Position"));
}

TEST(RouteMatch, StarDoesNotMatchZeroWords)
{
    EXPECT_FALSE(routeMatches("TOPIC.Nav.*", "TOPIC.Nav"));
}

TEST(RouteMatch, StarDoesNotMatchTwoWords)
{
    // This is the distinction that makes * and # different, and the reason the fixture carries
    // both TOPIC.Nav.Position and TOPIC.Sensor.Raw.Feed.
    EXPECT_FALSE(routeMatches("TOPIC.Nav.*", "TOPIC.Nav.A.B"));
}

TEST(RouteMatch, SeveralStarsEachTakeOneWord)
{
    EXPECT_TRUE(routeMatches("*.*.*", "TOPIC.Sensor.Raw"));
    EXPECT_FALSE(routeMatches("*.*.*", "TOPIC.Sensor"));
}

// ------------------------------------------------------------------------------------------
// `#` -- zero or more words
// ------------------------------------------------------------------------------------------

TEST(RouteMatch, HashMatchesOneOrMoreWords)
{
    EXPECT_TRUE(routeMatches("TOPIC.#", "TOPIC.Nav"));
    EXPECT_TRUE(routeMatches("TOPIC.#", "TOPIC.Sensor.Raw.Feed"));
}

TEST(RouteMatch, HashMatchesZeroWords)
{
    // The case most often got wrong, and the one that makes `TOPIC.#` a genuine "everything
    // under TOPIC, including TOPIC itself" subscription.
    EXPECT_TRUE(routeMatches("TOPIC.#", "TOPIC"));
}

TEST(RouteMatch, ABareHashMatchesAnything)
{
    EXPECT_TRUE(routeMatches("#", "TOPIC.Nav.Position"));
    EXPECT_TRUE(routeMatches("#", "anything"));
}

TEST(RouteMatch, HashInTheMiddleRequiresBacktracking)
{
    // THE case the cheap implementation fails. Treating `#` as "matches the rest" returns true
    // at the wildcard and never checks the suffix, so A.#.C would wrongly accept A.X.Y.
    EXPECT_TRUE(routeMatches("A.#.C", "A.C"));
    EXPECT_TRUE(routeMatches("A.#.C", "A.B.C"));
    EXPECT_TRUE(routeMatches("A.#.C", "A.B.B.C"));
    EXPECT_FALSE(routeMatches("A.#.C", "A.B.B.D"));
    EXPECT_FALSE(routeMatches("A.#.C", "A.C.D"));
}

TEST(RouteMatch, HashAndStarCombine)
{
    EXPECT_TRUE(routeMatches("TOPIC.#.Feed", "TOPIC.Sensor.Raw.Feed"));
    EXPECT_TRUE(routeMatches("TOPIC.*.Raw.#", "TOPIC.Sensor.Raw.Feed"));
    EXPECT_FALSE(routeMatches("TOPIC.*.Raw.#", "TOPIC.Raw.Feed"));
}

// ------------------------------------------------------------------------------------------
// The fixture's real topic names
// ------------------------------------------------------------------------------------------

TEST(RouteMatch, TheDottedFixtureTopicsRouteAsIntended)
{
    // The dotted names were added to the fixture to exercise the C++ mangling rule. Under the
    // agreed convention -- topic-type exchanges keyed on the topic name -- they also make
    // wildcard subscriptions meaningful, which is what this pins.
    EXPECT_TRUE(routeMatches("TOPIC.Nav.*", "TOPIC.Nav.Position"));
    EXPECT_TRUE(routeMatches("TOPIC.Sensor.#", "TOPIC.Sensor.Raw.Feed"));

    // A flat topic name is NOT caught by a subscription expecting a hierarchy.
    EXPECT_FALSE(routeMatches("TOPIC.Nav.*", "TOPIC.TrackUpdate"));
    EXPECT_FALSE(routeMatches("TOPIC.Sensor.#", "TOPIC.TrackUpdate"));

    // But `TOPIC.#` catches everything, flat and dotted alike.
    EXPECT_TRUE(routeMatches("TOPIC.#", "TOPIC.TrackUpdate"));
    EXPECT_TRUE(routeMatches("TOPIC.#", "TOPIC.Sensor.Raw.Feed"));
}

TEST(RouteMatch, AnEmptyBindingMatchesOnlyAnEmptyRoute)
{
    // Degenerate, but reachable: TopicOptions::route defaults to empty and is replaced by the
    // topic name. If that substitution ever regressed, this is where it would show.
    EXPECT_TRUE(routeMatches("", ""));
    EXPECT_FALSE(routeMatches("", "TOPIC"));
    EXPECT_FALSE(routeMatches("TOPIC", ""));
}
