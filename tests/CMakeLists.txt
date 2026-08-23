# motus test suites.
#
# Two binaries, three kinds of ctest entry:
#
#   motus_unit_tests         gtest-discovered, label "unit". Needs nothing external.
#   conformance.inmemory       the portable contract against the in-memory backend.
#                              Label "unit": needs nothing, runs anywhere, and is therefore
#                              deliberately incapable of a quiet skip.
#   conformance.amqpcpp        the same contract against a live RabbitMQ on 127.0.0.1:5672.
#                              Label "integration".
#
# ctest renders a skipped test as `Passed`. For any run that is supposed to PROVE something,
# set the guards so an environment gap fails instead of skipping:
#
#   MOTUS_REQUIRE_BROKER=1 MOTUS_REQUIRE_CAPS=1 ctest --output-on-failure
#
# GoogleTest comes from find_package -- never FetchContent -- so a default configure of this
# repository downloads nothing.

find_package(GTest CONFIG REQUIRED)
include(GoogleTest)

# ---------------------------------------------------------
# Unit suite
# ---------------------------------------------------------
add_executable(motus_unit_tests
    unit/ByteMessageTest.cpp
    unit/DispatchTest.cpp
    unit/LoggerTest.cpp
    unit/RouteMatchTest.cpp
    unit/TopicOptionsCheckTest.cpp
)

if(MOTUS_WITH_INMEMORY)
    target_sources(motus_unit_tests PRIVATE unit/InMemoryTransportTest.cpp)
endif()
if(MOTUS_WITH_AMQPCPP)
    # Tests the AMQP handshake identity string; needs the amqpcpp headers, not a broker.
    target_sources(motus_unit_tests PRIVATE unit/AmqpConnectionIdentityTest.cpp)
endif()

target_link_libraries(motus_unit_tests PRIVATE motus::motus GTest::gtest GTest::gtest_main)
target_include_directories(motus_unit_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")

gtest_discover_tests(motus_unit_tests
    PROPERTIES LABELS "unit"
    DISCOVERY_TIMEOUT 60)

# ---------------------------------------------------------
# Conformance -- the portable transport contract
# ---------------------------------------------------------
#
# Runs against EVERY backend in the build. One ctest entry per backend, so a failure names the
# transport that broke, and entries are generated from the same MOTUS_WITH_* options the
# factory reads -- adding a backend automatically runs the whole contract against it, which is
# what makes this a bar rather than a description of whatever the current backend happens to do.
add_executable(motus_conformance_tests
    conformance/CoreConformanceTest.cpp
    conformance/DeliveryConformanceTest.cpp
    conformance/ScaleConformanceTest.cpp
    conformance/RoutingConformanceTest.cpp
)

target_link_libraries(motus_conformance_tests PRIVATE
    motus::motus GTest::gtest GTest::gtest_main)
target_include_directories(motus_conformance_tests PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/conformance"
    "${CMAKE_CURRENT_SOURCE_DIR}")

function(motus_add_conformance_backend backend label timeout)
    add_test(NAME "conformance.${backend}"
             COMMAND motus_conformance_tests "--gtest_filter=*/${backend}")
    set_tests_properties("conformance.${backend}" PROPERTIES
        LABELS "conformance;${label}" TIMEOUT ${timeout})
endfunction()

if(MOTUS_WITH_INMEMORY)
    motus_add_conformance_backend("inmemory" "unit" 600)
endif()
if(MOTUS_WITH_AMQPCPP)
    motus_add_conformance_backend("amqpcpp" "integration" 900)
endif()
if(MOTUS_WITH_SIMPLEAMQP)
    motus_add_conformance_backend("simpleamqp" "integration" 900)
endif()
