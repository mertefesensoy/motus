cmake_minimum_required(VERSION 3.21)

project(motus
    VERSION 1.0.0
    DESCRIPTION "A transport-agnostic messaging seam for C++17, with a per-backend conformance suite and a Windows-safe AMQP-CPP backend"
    LANGUAGES CXX)

# ---------------------------------------------------------------------------
# Options -- two-stage backend selection, stage one.
#
# CMake decides what a BUILD contains; the operator decides at runtime what it
# USES (Factory.cpp reads the generated transport/Config.hpp and the
# MOTUS_TRANSPORT environment variable / --transport flag). A build with a
# backend compiled out refuses a request for it with a diagnostic naming the
# option to turn on -- never a silent substitution.
# ---------------------------------------------------------------------------
option(MOTUS_WITH_AMQPCPP    "Build the AMQP-CPP transport backend (the default)"        ON)
option(MOTUS_WITH_INMEMORY   "Build the in-memory transport backend (zero dependencies)" ON)
option(MOTUS_WITH_SIMPLEAMQP "Build the SimpleAmqpClient transport backend (opt-in)"     OFF)

if(NOT MOTUS_WITH_AMQPCPP AND NOT MOTUS_WITH_SIMPLEAMQP AND NOT MOTUS_WITH_INMEMORY)
    message(FATAL_ERROR
        "No transport backend selected. Enable at least one of MOTUS_WITH_AMQPCPP, "
        "MOTUS_WITH_SIMPLEAMQP or MOTUS_WITH_INMEMORY.")
endif()

if(PROJECT_IS_TOP_LEVEL)
    set(_motus_tests_default ON)
else()
    set(_motus_tests_default OFF)
endif()
option(MOTUS_BUILD_TESTS "Build the unit and conformance suites" ${_motus_tests_default})

message(STATUS "motus transports: amqpcpp=${MOTUS_WITH_AMQPCPP} inmemory=${MOTUS_WITH_INMEMORY} simpleamqp=${MOTUS_WITH_SIMPLEAMQP}")

configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/TransportConfig.hpp.in"
    "${CMAKE_CURRENT_BINARY_DIR}/generated/motus/transport/Config.hpp")

# ---------------------------------------------------------------------------
# Build identity, embedded in the AMQP handshake (see AmqpConnection.hpp): a
# broker's connections page then names the exact build that owns each
# connection. Resolved from git when the tree is a checkout; "unknown"
# otherwise, which is itself information.
# ---------------------------------------------------------------------------
set(MOTUS_COMMIT      "unknown")
set(MOTUS_COMMIT_DATE "unknown")
set(MOTUS_PROVENANCE  "unidentified")
find_package(Git QUIET)
if(GIT_FOUND)
    # Git is trusted ONLY when this directory is itself the worktree root.
    #
    # `git rev-parse HEAD` searches upward until it finds a repository, so a motus source
    # tree unpacked inside somebody else's checkout answers with THAT project's HEAD. A
    # vcpkg reviewer hit exactly this: building the port produced a Version.hpp naming the
    # vcpkg pull request's own commit as motus provenance.
    #
    # A confident wrong answer is worse here than no answer, because this string reaches the
    # broker as the AMQP connection identity (see AmqpConnection.hpp). Someone debugging a
    # live connection would be sent to an unrelated repository. Falling back to
    # "unidentified" is the honest outcome, and is exactly the case that field exists for.
    execute_process(
        COMMAND "${GIT_EXECUTABLE}" rev-parse --show-toplevel
        WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
        OUTPUT_VARIABLE MOTUS_GIT_TOPLEVEL
        OUTPUT_STRIP_TRAILING_WHITESPACE
        RESULT_VARIABLE MOTUS_GIT_TOPLEVEL_RESULT
        ERROR_QUIET)

    set(MOTUS_GIT_OWNS_THIS_TREE FALSE)
    if(MOTUS_GIT_TOPLEVEL_RESULT EQUAL 0)
        # REALPATH both sides so that symlinks, a mixed drive-letter case or a trailing
        # separator cannot make one directory compare unequal to itself. A submodule still
        # matches, since its own .git file makes it a worktree root in its own right.
        get_filename_component(_motus_source_real   "${CMAKE_CURRENT_SOURCE_DIR}" REALPATH)
        get_filename_component(_motus_toplevel_real "${MOTUS_GIT_TOPLEVEL}"       REALPATH)
        if(_motus_source_real STREQUAL _motus_toplevel_real)
            set(MOTUS_GIT_OWNS_THIS_TREE TRUE)
        endif()
    endif()

    if(MOTUS_GIT_OWNS_THIS_TREE)
        execute_process(
            COMMAND "${GIT_EXECUTABLE}" rev-parse HEAD
            WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
            OUTPUT_VARIABLE MOTUS_GIT_COMMIT
            OUTPUT_STRIP_TRAILING_WHITESPACE
            RESULT_VARIABLE MOTUS_GIT_RESULT
            ERROR_QUIET)
        if(MOTUS_GIT_RESULT EQUAL 0)
            set(MOTUS_COMMIT     "${MOTUS_GIT_COMMIT}")
            set(MOTUS_PROVENANCE "git")
            execute_process(
                COMMAND "${GIT_EXECUTABLE}" show -s --format=%cI HEAD
                WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
                OUTPUT_VARIABLE MOTUS_COMMIT_DATE
                OUTPUT_STRIP_TRAILING_WHITESPACE
                ERROR_QUIET)
        endif()
    else()
        message(STATUS
            "motus provenance: not a motus worktree root, reporting build as unidentified")
    endif()
endif()

configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Version.hpp.in"
    "${CMAKE_CURRENT_BINARY_DIR}/generated/motus/Version.hpp"
    @ONLY)

# ---------------------------------------------------------------------------
# Dependencies.
#
# The in-memory backend needs nothing. Boost.Asio and AMQP-CPP are pulled in
# only by the AMQP-CPP backend; SimpleAmqpClient (and its rabbitmq-c) only by
# the opt-in second AMQP backend. A default configure downloads nothing.
# ---------------------------------------------------------------------------
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

if(MOTUS_WITH_AMQPCPP)
    # AMQP-CPP, located two ways because its packagings disagree about whether to
    # ship a CMake package config. MSYS2/pacman, vcpkg and a from-source install
    # provide amqpcpp-config.cmake, so find_package is tried first. Homebrew does
    # not, and fails with "Could not find a package configuration file" on a
    # machine where the library is demonstrably installed. The fallback locates
    # the pieces directly and synthesises the same imported target, so nothing
    # downstream can tell which route was taken. Note the header is `amqpcpp.h`:
    # the project drops the hyphen its package name carries.
    find_package(amqpcpp QUIET)

    if(NOT TARGET amqpcpp)
        find_path(MOTUS_AMQPCPP_INCLUDE_DIR NAMES amqpcpp.h)
        find_library(MOTUS_AMQPCPP_LIBRARY NAMES amqpcpp)

        if(NOT MOTUS_AMQPCPP_INCLUDE_DIR OR NOT MOTUS_AMQPCPP_LIBRARY)
            message(FATAL_ERROR
                "AMQP-CPP not found. Neither a CMake package config nor amqpcpp.h + libamqpcpp "
                "could be located.\n"
                "  MSYS2/UCRT64 : pacman -S mingw-w64-ucrt-x86_64-amqp-cpp\n"
                "  macOS        : brew install amqp-cpp\n"
                "  vcpkg        : add amqpcpp to your manifest\n"
                "  Ubuntu       : not packaged before 26.04; build from source and set "
                "CMAKE_PREFIX_PATH\n"
                "Or configure with -DMOTUS_WITH_AMQPCPP=OFF for the dependency-free "
                "in-memory backend only.")
        endif()

        add_library(amqpcpp UNKNOWN IMPORTED)
        set_target_properties(amqpcpp PROPERTIES
            IMPORTED_LOCATION             "${MOTUS_AMQPCPP_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${MOTUS_AMQPCPP_INCLUDE_DIR}")

        message(STATUS "AMQP-CPP: no package config; using ${MOTUS_AMQPCPP_LIBRARY}")
    endif()

    # Boost.System is requested OPTIONALLY, which looks wrong and is not. It has
    # been header-only since Boost 1.69; the compiled library survives purely as
    # a compatibility stub, and whether a packaging still ships one is a
    # packaging decision. MSYS2 and vcpkg do; Homebrew's Boost 1.90 does not, and
    # a hard COMPONENTS request fails there on a machine where Asio compiles
    # perfectly well.
    find_package(Boost CONFIG REQUIRED OPTIONAL_COMPONENTS system)

    set(MOTUS_BOOST_LIBS Boost::headers)
    if(TARGET Boost::system)
        list(APPEND MOTUS_BOOST_LIBS Boost::system)
        message(STATUS "Boost: linking the compiled Boost::system stub")
    else()
        message(STATUS "Boost: header-only, no compiled Boost::system in this packaging")
    endif()
endif()

if(MOTUS_WITH_SIMPLEAMQP)
    # PINNED TO A COMMIT, not a tag, and deliberately. The newest RELEASE
    # (v2.5.1) cannot be built on MSYS2/UCRT64 at all: static is a hard
    # FATAL_ERROR ("cannot be built as a static library on Win32") and shared
    # links with every symbol undefined. Commits made after it are load-bearing
    # here -- Windows static libraries, Boost 1.89 support, and letting the
    # build environment override CMAKE_CXX_STANDARD.
    #
    # rabbitmq-c comes from your package manager (MSYS2:
    # mingw-w64-ucrt-x86_64-rabbitmq-c) and is not fetched.
    include(FetchContent)
    set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
    set(ENABLE_TESTING    OFF CACHE BOOL "" FORCE)
    set(BUILD_API_DOCS    OFF CACHE BOOL "" FORCE)

    FetchContent_Declare(SimpleAmqpClient
        GIT_REPOSITORY https://github.com/alanxz/SimpleAmqpClient.git
        GIT_TAG        cf44cc810a07ef68657881c9aedaf5c5a699bbfd)
    FetchContent_MakeAvailable(SimpleAmqpClient)
endif()

# ---------------------------------------------------------------------------
# The library.
# ---------------------------------------------------------------------------
add_library(motus STATIC
    src/Logger.cpp
    src/Consumer.cpp
    src/transport/Factory.cpp
)
add_library(motus::motus ALIAS motus)

if(MOTUS_WITH_AMQPCPP)
    target_sources(motus PRIVATE src/transport/AmqpCppTransport.cpp)
    target_link_libraries(motus PUBLIC amqpcpp ${MOTUS_BOOST_LIBS})
endif()
if(MOTUS_WITH_INMEMORY)
    target_sources(motus PRIVATE src/transport/InMemoryTransport.cpp)
endif()
if(MOTUS_WITH_SIMPLEAMQP)
    target_sources(motus PRIVATE src/transport/SimpleAmqpTransport.cpp)
    target_link_libraries(motus PUBLIC SimpleAmqpClient)

    # SimpleAmqpClient uses legacy include_directories() and never calls
    # target_include_directories(), so a FetchContent consumer inherits NO
    # usable include path from the target: it links successfully and then
    # cannot find the headers.
    target_include_directories(motus PUBLIC
        $<BUILD_INTERFACE:${simpleamqpclient_SOURCE_DIR}/src>)
endif()

target_compile_features(motus PUBLIC cxx_std_17)

target_include_directories(motus PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/generated>
    $<INSTALL_INTERFACE:include>)

target_link_libraries(motus PUBLIC Threads::Threads)

if(WIN32)
    # Boost.Asio on Windows is a thin layer over Winsock and does not pull its
    # own import libraries in; omitting these produces undefined references to
    # __imp_WSAStartup that look like a broken Boost install. PUBLIC because a
    # consumer's final link needs them too, and because _WIN32_WINNT must be
    # defined before ANY translation unit -- ours or a consumer's -- includes a
    # Boost.Asio header through motus/AmqpConnection.hpp.
    target_link_libraries(motus PUBLIC ws2_32 mswsock)
    target_compile_definitions(motus PUBLIC
        _WIN32_WINNT=0x0601
        WIN32_LEAN_AND_MEAN)
endif()

# NOTE for MSYS2 users: build from a UCRT64 login shell (where the toolchain's
# standard include paths are part of the environment). Invoking the MSYS2
# compiler from a bare PowerShell prompt makes CMake strip its standard include
# directories; this project documents that caveat rather than hard-coding a
# GCC-only "-IC:/msys64/ucrt64/include" workaround into every target.

# ---------------------------------------------------------------------------
# Install + export: consumers get find_package(motus) and a
# motus::motus target, nothing else to configure.
# ---------------------------------------------------------------------------
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

install(TARGETS motus
    EXPORT motusTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

install(DIRECTORY include/motus DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/generated/motus/transport/Config.hpp"
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/motus/transport)
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/generated/motus/Version.hpp"
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/motus)

install(EXPORT motusTargets
    NAMESPACE motus::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/motus)

configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/motusConfig.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/motusConfig.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/motus)

write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/motusConfigVersion.cmake"
    COMPATIBILITY SameMajorVersion)

install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/motusConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/motusConfigVersion.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/motus)

# ---------------------------------------------------------------------------
# Tests.
# ---------------------------------------------------------------------------
if(MOTUS_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
