First stable release.

motus is a transport-agnostic messaging seam for C++17: one shared implementation of wire
versioning, poison-message policy, payload bounds and acknowledgement discipline, sitting
above a swappable byte pipe — held together by a **conformance suite every backend must
pass**.

## Why it exists

AMQP-CPP's bundled event-loop integrations do not compile on Windows:
`<amqpcpp/libboostasio.h>` is written against POSIX stream descriptors with no platform
guards, and the failure surfaces as an inscrutable Boost template error. motus ships the
missing piece — a hand-written, **Windows-safe AMQP-CPP connection handler over
Boost.Asio** — plus the seam that makes it reusable rather than a snippet.

## What v1.0.0 contains

- **`ITransport`** — the seam — with three backends: `AmqpCppTransport` (RabbitMQ, the
  default), `InMemoryTransport` (zero dependencies, full fidelity), and an opt-in
  `SimpleAmqpTransport`.
- **The conformance contract**: 39 scenarios × every backend the build contains —
  delivery fidelity to the 4 MiB bound, ordering, competing consumers, poison-message
  rejection and recovery, crash abandonment and redelivery, deferred acknowledgement,
  bounded destinations with dead-lettered evictions, routing groups including the
  negative cases. Adding a backend to CMake automatically subjects it to the whole
  contract.
- `Producer` / `Consumer` / `dispatch()` over a versioned `ByteMessage` envelope, a
  structured logger, and capability introspection.
- Honest test discipline: `MOTUS_REQUIRE_BROKER` / `MOTUS_REQUIRE_CAPS` turn environment
  gaps into failures instead of silent skips.

## Verification

CI matrix: Ubuntu (GCC), Windows MSVC, Windows MSYS2 UCRT64, macOS arm64 — unit suite and
the in-memory conformance leg everywhere, plus the full 39-scenario contract against a
live RabbitMQ on the Linux integration leg. Developed against AMQP-CPP 4.3.27 and
RabbitMQ 4.3.4.

## Consuming

```cmake
find_package(motus REQUIRED)
target_link_libraries(your_app PRIVATE motus::motus)
```

A vcpkg port and a Conan recipe are staged in `packaging/registries/` (usable immediately
as a vcpkg overlay port) pending registry submission.

motus was extracted from [aftershock](https://github.com/mertefesensoy/aftershock), a
complete post-earthquake response C2 exercise system, which consumes it at a pinned commit
and exercises it end-to-end against a real broker.
