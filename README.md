# motus

[![CI](https://github.com/mertefesensoy/motus/actions/workflows/ci.yml/badge.svg)](https://github.com/mertefesensoy/motus/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/mertefesensoy/motus)](https://github.com/mertefesensoy/motus/releases)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

**AMQP-CPP's bundled event-loop integrations do not compile on Windows.**
`<amqpcpp/libboostasio.h>` is written against `boost::asio::posix::stream_descriptor` with no
platform guards, MSYS2 ships the header anyway, and the build dies deep inside Boost templates
looking like a broken Boost install. motus ships the missing piece — a hand-written,
**Windows-safe AMQP-CPP connection handler over Boost.Asio** — and the seam that makes it more
than a snippet:

> A transport-agnostic messaging seam for C++17. One shared implementation of wire versioning,
> poison-message policy, payload bounds and acknowledgement discipline, sitting above a
> swappable byte pipe — with a **conformance suite every backend must pass**.

```text
ByteMessage                       versioned envelope + 4 MiB bound
     |
Producer / Consumer / dispatch()  versioning, poison policy, counters, capability checks
     |                            ONE implementation, shared by every backend
     |  holds a reference to
ITransport                        <- THE SEAM
     |-- AmqpCppTransport         wraps AmqpConnection. Windows-safe. The default.
     |-- InMemoryTransport        no middleware, zero dependencies. Full fidelity on purpose.
     `-- SimpleAmqpTransport      SimpleAmqpClient over rabbitmq-c. Opt-in, OFF by default.
```

## The conformance contract

Most messaging abstractions assert nothing about their backends. Here, **39 scenarios** run
against **every backend the build contains** — delivery fidelity (including arbitrary binary
bytes and payloads at the 4 MiB bound), ordering, competing consumers, poison-message
rejection and recovery, crash abandonment and redelivery, deferred acknowledgement, bounded
destinations with dead-lettered evictions, and routing-group semantics including the negative
cases. One ctest entry per backend:

- `conformance.inmemory` — needs **nothing**. The portable contract, checkable on any machine.
- `conformance.amqpcpp` — the same 39 scenarios against a live RabbitMQ on `127.0.0.1:5672`.

Adding a backend to CMake automatically subjects it to the whole contract. That is the
difference between "two libraries that both happen to work" and an abstraction with a
contract.

## Three clauses people get wrong

1. **A throwing handler resolves nothing.** No ack, no reject — the delivery stays outstanding
   and is returned for redelivery when the transport closes. This is *distinct from*
   `Disposition::Requeue`: Requeue is a decision; a throw is the absence of one. Conflating
   them would make a suite claim crash-safety while verifying cooperative hand-back.
2. **`ready()` means usable *right now***, not "was ready once." A latched flag reports a dead
   connection as healthy — a real defect this design exists to prevent.
3. **Never throw from a destructor-driven path.** AMQP-CPP calls back into its handler from
   `~Channel`; destructors are `noexcept`, so a throw there is `std::terminate` with no stack
   trace. Backends catch, stash, and rethrow from `pump()`.

## Building

Dependencies for the default build: **Boost.Asio and AMQP-CPP** (plus GoogleTest for the
tests). The in-memory backend alone needs nothing at all. A default configure downloads
nothing — see [THIRD-PARTY.md](THIRD-PARTY.md).

```console
# MSYS2 / UCRT64 (run from a UCRT64 login shell -- see the caveat below)
pacman -S mingw-w64-ucrt-x86_64-boost mingw-w64-ucrt-x86_64-amqp-cpp mingw-w64-ucrt-x86_64-gtest

cmake -S . -B build -G Ninja
cmake --build build --parallel
ctest --test-dir build -L unit --output-on-failure
```

Consuming it is one line of CMake after `cmake --install`:

```cmake
find_package(motus REQUIRED)
target_link_libraries(your_app PRIVATE motus::motus)
```

Or pin a commit with FetchContent — how [aftershock](https://github.com/mertefesensoy/aftershock),
the system this library was extracted from, consumes it. A vcpkg port and a Conan recipe are
staged under [`packaging/registries/`](packaging/registries/) (usable today as a vcpkg
overlay port) pending registry submission.

### Options

| Option | Default | Effect |
|---|---|---|
| `MOTUS_WITH_AMQPCPP` | `ON` | The AMQP-CPP backend (pulls Boost + AMQP-CPP) |
| `MOTUS_WITH_INMEMORY` | `ON` | The dependency-free reference backend |
| `MOTUS_WITH_SIMPLEAMQP` | `OFF` | Second AMQP backend; FetchContents SimpleAmqpClient at a pinned commit |
| `MOTUS_BUILD_TESTS` | `ON` when top-level | Unit + conformance suites |

Backend selection is two-stage: CMake decides what a build *contains*; the operator decides
what it *uses* (`MOTUS_TRANSPORT` / `--transport` in your application, via
`transport::parseBackend`). A build refuses a request for a backend it does not contain, with
a message naming the CMake option to turn on.

## Testing honestly

ctest renders a skipped test as `Passed`. For any run that is supposed to *prove* something,
set the guards so an environment gap fails instead of skipping:

```console
MOTUS_REQUIRE_BROKER=1 MOTUS_REQUIRE_CAPS=1 ctest --test-dir build --output-on-failure
```

`MOTUS_REQUIRE_BROKER` turns "could not connect" into a failure; `MOTUS_REQUIRE_CAPS`
turns "backend declares this capability false" into one. A suspiciously fast conformance run
means everything skipped and the run proved nothing.

Broker-facing conformance runs declare durable scratch queues named `conf-*`/`route-*` on your
broker and clean up what they consume, but a crashed run can leave some behind; they are
harmless and can be deleted by name pattern.

## Platform caveats

- **MSYS2: build from a UCRT64 login shell**, not from a bare PowerShell prompt. When the
  MSYS2 compiler is invoked outside its environment, CMake silently strips the toolchain's
  standard include directories and the failure looks like a missing standard library.
- **Verified**: MSYS2 UCRT64 / GCC on Windows, against RabbitMQ 4.3.4. The Windows claim is
  specifically that toolchain — **MSVC and Apple Clang are not yet verified**; reports and
  fixes welcome.
- `localhost` may not resolve on locked-down networks; `AmqpConnection` tries the host as an
  IP literal first, so `127.0.0.1` always works without touching the resolver.

## The name

*Motus* is Latin for movement — an impulse, a setting-in-motion — and moving messages is the
one thing this library does. *Motus terrae*, "the movement of the earth", is an earthquake:
the library was extracted from a post-earthquake response system, and the name keeps that
kinship without carrying any of it as a dependency.

## License

Apache-2.0 — see [LICENSE](LICENSE). External dependencies and when each is needed are
catalogued in [THIRD-PARTY.md](THIRD-PARTY.md).
