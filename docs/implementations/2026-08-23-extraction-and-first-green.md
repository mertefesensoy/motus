# Extraction from the predecessor project, and the first green run

**Date:** 2026-08-23
**Author:** Claude Code, at the author's direction
**Status:** implemented

---

## 1. Problem / Motivation

motus began life as the transport subsystem of a larger private project. Inside that system
it had the two properties worth extracting: a hand-written, Windows-safe AMQP-CPP connection
handler (the upstream event-loop integrations do not compile on Windows), and a genuinely
enforced transport contract — a 39-scenario conformance suite every backend must pass. Neither
was usable by anyone else while buried in an application.

The extraction had one hard cleanliness bar: the library must stand entirely alone. No include
path into the predecessor, no application wire types, and none of the predecessor's heavyweight
dependency tail — in particular OpenXLSX, which reached the transport tests through exactly one
helper and dragged a FetchContent clone, a `CMAKE_POLICY_VERSION_MINIMUM` workaround for miniz,
and a MAX_PATH failure mode along with it.

## 2. What Changed

Everything is new to this repository; the table groups it by provenance.

| Group | Files | Provenance |
|---|---|---|
| Scaffold | `LICENSE` (Apache-2.0), `.gitignore`, `.gitattributes`, `THIRD-PARTY.md`, `CMakeLists.txt`, `cmake/TransportConfig.hpp.in`, `cmake/Version.hpp.in`, `cmake/motusConfig.cmake.in`, `tests/CMakeLists.txt`, `README.md` | New. Installable package exporting `motus::motus`; two-stage backend selection preserved via the generated `transport/Config.hpp`. |
| The seam, verbatim + rename | `transport/ITransport.hpp`, `transport/Factory.hpp` + `.cpp`, three backend `.hpp`/`.cpp` pairs, `AmqpConnection.hpp`, `Logger.hpp`/`.cpp`, `MessagePublisher.hpp` | Ported from the predecessor with namespaces (`motus::`), macros (`MOTUS_LOG_*`), options (`MOTUS_WITH_*`) and environment variables (`MOTUS_TRANSPORT`, `MOTUS_REQUIRE_*`) renamed, and predecessor-specific prose in comments generalized by hand. |
| Replaced wire types | `ByteMessage.hpp` (new), `Producer.hpp`, `Consumer.hpp`, `src/Consumer.cpp` | The predecessor's two application wire types collapse into one generic `ByteMessage`; `dispatch()` becomes a single format-version check; the poison-message policy ports intact. |
| Conformance suite | `tests/conformance/*` (fixture + 4 files, 39 scenarios), `tests/support/*` | Adapted onto `ByteMessage`; the workbook-derived payload generator replaced with a deterministic synthetic one. |
| Unit suite | `tests/unit/*` (7 files, 84 gtest cases) | Four port with renames; `DispatchTest` and `InMemoryTransportTest` adapted; `ByteMessageTest` new. |

## 3. Implementation Approach

Copy out, rename mechanically, then hand-pass the prose. The mechanical rename ran three
ordered substitutions (the predecessor's uppercase macro prefixes first, then its project
name, then the lowercase namespace/path form) so that case variants could not survive
half-translated. A
grep inventory then drove a hand pass over every comment that referenced the predecessor's
documents, decision records, binaries or environment — the *reasoning* in those comments is
the library's documentation and was kept; the references that would dangle were generalized,
never deleted-with-the-rationale.

`ByteMessage`'s envelope is a binary-safe ASCII prefix — `motus:<version>:` — rather than
JSON. The two architecturally load-bearing properties survive exactly: an explicit
format_version that is rejected by name in both directions rather than guessed at, and a hard
4 MiB bound enforced at serialize(), deserialize() and dispatch(). Everything after the second
colon is payload, verbatim, so arbitrary bytes (NUL, newlines, invalid UTF-8) need no base64.

The conformance fixture's round-trip discriminator moved from a wire-type field to the
payload's first line (`markerOf`/`markedMessage`), deliberately preserving the suite's ability
to distinguish "arrived but corrupted" from "never came".

## 4. Mathematical / Statistical Details

Omitted — no numeric algorithms changed; `routeMatches()` (the `*`/`#` pattern semantics with
backtracking over `#`) ported byte-for-byte and its unit tests with it.

## 5. Design Decisions

- **Envelope without JSON, and nlohmann-json dropped.** The predecessor's envelope was JSON,
  which is fine for text payloads and wrong for a *byte* message type: nlohmann refuses or
  mangles non-UTF-8 strings, and base64 would tax every payload 33%. A ten-byte ASCII prefix
  keeps the version check and costs nothing. This is also what makes the advertised dependency
  list true: Boost.Asio + AMQP-CPP, and GTest for tests. The alternative — keeping JSON and
  documenting a text-only payload restriction — was rejected as a trap for exactly the protobuf
  /CBOR users a generic library invites.
- **One format version, one check.** The predecessor dispatched across two live wire versions;
  a fresh library starts at version 1 with the rejection machinery already proven by tests in
  both directions (version 0 and version 99 both reject, naming both versions).
- **The MSYS2 include-path hack was not ported.** The predecessor force-fed
  `-IC:/msys64/ucrt64/include` to GCC because invoking the MSYS2 compiler from bare PowerShell
  loses the toolchain's default include paths. That is an environment defect, not a project
  property; the README documents "build from a UCRT64 login shell" as the supported path, and
  the clean build here confirms the hack is unnecessary there. Hard-coding an absolute local
  path into an installable package was the alternative, and it was rejected as inheriting a
  wart other people would ship.
- **GoogleTest via `find_package` only.** A default configure fetches nothing; the pinned
  SimpleAmqpClient FetchContent exists solely behind the OFF-by-default option, for the
  documented reason that its newest release cannot be built on MSYS2 at all.
- **Scenario count preserved at exactly 39.** The one scenario whose subject was
  predecessor-specific (a wire-type logical path) was replaced by an arbitrary-binary-bytes
  scenario — the envelope decision's own risk — rather than deleted, so the contract got
  stronger, not thinner, and the count stayed comparable.
- **Curated milestone commits.** Six commits tell the story (scaffold → seam → ByteMessage →
  conformance → unit → README). Only the tip is build-gated; intermediate commits group files
  by meaning, not by buildability.

## 6. Verification

All local verification is **MSYS2 UCRT64 / GCC with Ninja on one Windows machine**, build
directory `/c/motus-build`, source tree at its committed tip.

- **Clean-tree configure + build:** `rm -rf` of the build directory, then configure (1.8 s,
  FetchContent populates nothing, no `_deps/` directory exists) and build of all 19 targets
  with zero warnings surfaced. A case-insensitive `git grep` for the predecessor's name and
  another for `openxlsx` both return nothing — there is no reference, textual or
  include-path, to the predecessor. (The sweep strings themselves are deliberately not
  spelled here, so this document can never be the sweep's one hit.)
- **Full suite, guards set:**
  `MOTUS_REQUIRE_BROKER=1 MOTUS_REQUIRE_CAPS=1 ctest --output-on-failure` →
  **86/86 passed in 71.7 s**: 84 discovered unit tests, `conformance.inmemory` (39 scenarios,
  32 s), and `conformance.amqpcpp` (39 scenarios, 37 s) against a live portable RabbitMQ 4.3.4
  on `127.0.0.1:5672`. With both guards set, a skipped scenario or an unreachable broker is a
  *failure*, so the pass count cannot hide a quiet skip.
- **Scenario census:** `--gtest_list_tests` over the `*/inmemory` filter lists exactly 39
  scenarios (10 Core + 15 Delivery + 7 Routing + 7 Scale); the same suite instantiates per
  backend from `availableBackends()`.
- **Sanitization sweeps:** predecessor name, scenario vocabulary, employer/environment terms,
  decision-record references, local usernames and paths — all zero across the tree, verified
  with corrected (unescaped-alternation) patterns after an earlier pattern bug was caught
  hiding real hits.

**What was NOT verified:**

- **MSVC, Apple Clang, and Linux GCC.** One toolchain of four. The README's Windows claim is
  scoped to MSYS2 UCRT64 / GCC for exactly this reason. No CI exists yet in this repository.
- **The SimpleAmqp backend.** `MOTUS_WITH_SIMPLEAMQP=OFF` by default and not enabled in any
  run here; its conformance leg has not executed against this tree.
- **`find_package(motus)` from an installed prefix.** The export machinery is written and
  configures, but no consumer project has been built against `cmake --install` output yet.
- **Broker-restart behaviour.** The predecessor's restart suite was application-level
  integration and was deliberately not ported; reconnect policy is covered only by unit-level
  and conformance-level scenarios that do not kill a broker.

## 7. Related Docs

- [`../../README.md`](../../README.md) — the contract clauses and platform caveats this
  document's verification backs.
- [`../../THIRD-PARTY.md`](../../THIRD-PARTY.md) — the dependency story §5's envelope decision
  makes true.
