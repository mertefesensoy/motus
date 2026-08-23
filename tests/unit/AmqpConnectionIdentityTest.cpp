#include "motus/AmqpConnection.hpp"
#include "motus/Version.hpp"

#include <amqpcpp.h>
#include <gtest/gtest.h>

#include <string>

namespace {

std::string stringProperty(const AMQP::Table &properties, const std::string &key)
{
    return static_cast<const std::string &>(properties.get(key));
}

TEST(AmqpConnectionIdentity, HandshakeNamesTheOwningMotusBuildAndAnnotatesAnExistingName)
{
    AMQP::Table properties;
    properties.set("connection_name", "C:\\MOTUS\\motus_consumer.exe");
    properties.set("product", "AMQP-CPP");
    properties.set("version", "AMQP-CPP 4.3.27");

    motus::detail::applyMotusClientProperties(properties);

    EXPECT_EQ(stringProperty(properties, "product"), "motus");
    EXPECT_EQ(stringProperty(properties, "version"), motus::kVersion);
    EXPECT_EQ(stringProperty(properties, "motus_commit"), motus::kCommit);
    EXPECT_EQ(stringProperty(properties, "motus_commit_date"), motus::kCommitDate);
    EXPECT_EQ(stringProperty(properties, "motus_provenance"), motus::kProvenance);
    EXPECT_EQ(stringProperty(properties, "information"),
              "https://github.com/mertefesensoy/motus");

    const std::string name = stringProperty(properties, "connection_name");
    EXPECT_NE(name.find("C:\\MOTUS\\motus_consumer.exe"), std::string::npos);
    EXPECT_NE(name.find(motus::versionString()), std::string::npos);
}

TEST(AmqpConnectionIdentity, MissingLibraryConnectionNameStillGetsAnOperationalIdentity)
{
    AMQP::Table properties;

    motus::detail::applyMotusClientProperties(properties);

    EXPECT_EQ(stringProperty(properties, "connection_name"), motus::versionString());
}

} // namespace
