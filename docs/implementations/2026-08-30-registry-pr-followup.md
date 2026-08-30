# Following the three open packaging PRs, and unblocking the Conan one

**Date:** 2026-08-30
**Author:** MERT EFE ŞENSOY
**Status:** implemented

---

## 1. Problem / Motivation

Three pull requests filed on 2026-08-20/24 had gone quiet, and it was not clear whether they
were waiting on maintainers or waiting on us:

| PR | Filed | State on 2026-08-30 |
|---|---|---|
| [conan-io/conan-center-index#30838](https://github.com/conan-io/conan-center-index/pull/30838) | 08-24 | open, `BLOCKED`, CI had never run |
| [microsoft/vcpkg#53562](https://github.com/microsoft/vcpkg/pull/53562) | 08-24 | open, `BLOCKED`, all 16 CI legs green |
| [getsentry/sentry-native#2007](https://github.com/getsentry/sentry-native/pull/2007) | 08-20 | open, checks green, reviewer silent 7 days |

Triage separated the two cases. vcpkg and sentry-native were genuinely in a human queue with
nothing technically wrong. The Conan PR was not: it was **missing `test_package/`**, which
every conan-center-index recipe is required to ship, so it would have failed the instant a
maintainer approved its CI run.

That failure had been invisible because of a second fact worth recording. The "Job scheduler"
check on #30838 reports as a *failure* in `gh pr checks`, which reads as "our recipe is
broken". It is not a failure. Its conclusion is `action_required` and its summary is "Conan
Center maintainers need to approve this CI run and have been notified", a first-time
contributor gate. The recipe had therefore never been compiled by anyone, and the missing
`test_package` had never been reported.

## 2. What Changed

| File | Change |
|---|---|
| `packaging/registries/conan/conanfile.py` | Hardened: `transitive_headers`, Windows `system_libs` + `defines`, pinned boost, `cmake` tool_requires, `validate()`, `fPIC`, `required_conan_version`. |
| `packaging/registries/conan/test_package/conanfile.py` | New. Matches the current `docs/package_templates/cmake_package` shape in CCI verbatim. |
| `packaging/registries/conan/test_package/CMakeLists.txt` | New. `find_package(motus CONFIG)` plus a link against `motus::motus`. |
| `packaging/registries/conan/test_package/test_package.cpp` | New. Links against the compiled library and the generated header. |
| `docs/implementations/2026-08-30-registry-pr-followup.md` | This document. |

The same four recipe files were pushed to `mertefesensoy/conan-center-index@motus-new-recipe`
under `recipes/motus/all/`, as one commit, which updated #30838 in place. No local clone of
conan-center-index was made; the commit was assembled through the GitHub Git Data API
(blobs, then a tree on top of the existing one, then a commit, then a ref update), which is
why it landed atomically rather than as four file-at-a-time commits.

Nothing in `src/`, `include/` or `CMakeLists.txt` was touched. The published v1.0.0 tarball
and its `sha256` in `conandata.yml` are therefore unchanged, which matters: editing the
library would have invalidated the checksum the recipe pins.

## 3. Implementation Approach

### 3.1 The test_package

A ConanCenter `test_package` exists to prove the *package* is consumable, not to re-run the
project's own suite. It must not need a broker, a network, or any fixture.

The chosen program calls `motus::Logger::parseLevel`, `motus::hexDump` and
`motus::Logger::levelName`, then prints `motus::versionString()`. That specific selection is
load-bearing:

- The first three are **declared in `motus/Logger.hpp` but defined in `src/Logger.cpp`**, so
  the test only links if `libmotus` itself was packaged. A header-only test would pass against
  a broken package.
- `versionString()` lives in `motus/Version.hpp`, which CMake *generates* at build time and
  installs separately from the hand-written headers. Including it proves the generated header
  reached the package, which a test using only `include/` headers would not catch.
- None of the four emits a log record, so the `Logger`'s sink thread never starts. The test
  stays deterministic on every platform ConanCenter builds, with no shutdown ordering to get
  wrong.

### 3.2 The four recipe fixes

Three are correctness, one is policy.

**`transitive_headers=True` on `amqp-cpp` and `boost`.** `include/motus/AmqpConnection.hpp` is
an *installed* public header, and it includes `<amqpcpp.h>` (line 20) and five
`<boost/asio/*.hpp>` headers (lines 14-18); `AmqpCppTransport.hpp` includes `<amqpcpp.h>` too.
Conan's rule is that a package whose *public* headers include a dependency's headers must
mark that requirement `transitive_headers=True`, otherwise the dependency's include paths are
not propagated and a consumer that includes `AmqpConnection.hpp` cannot compile. This was a
genuine defect, not a style point. See section 6 for what could and could not be demonstrated
locally.

**`mswsock` and the compile definitions on Windows.** Upstream `CMakeLists.txt` puts
`ws2_32 mswsock` and `_WIN32_WINNT=0x0601` / `WIN32_LEAN_AND_MEAN` on the `motus` target as
`PUBLIC`, and they reach consumers through the exported `motusTargets.cmake`. But `package()`
deletes `lib/cmake` so that CMakeDeps can generate the config file instead, which means the
exported interface is discarded and the recipe has to restate it. The recipe previously
declared only `ws2_32` and no defines, so a Windows consumer would have hit undefined
`__imp_WSA*` references from Asio and a `_WIN32_WINNT` mismatch. The upstream comment explains
why the defines must precede any consumer translation unit that reaches Boost.Asio through
`AmqpConnection.hpp`; that requirement survives the repackaging and so must the defines.

**`boost` pinned to `1.88.0`.** The recipe used `boost/[>=1.81 <2]`.
`docs/adding_packages/dependencies.md` in CCI states version ranges are "generally not
allowed" outside an explicit allowlist, and enumerates it: OpenSSL, CMake, doxygen, libcurl,
zlib, libpng, expat, libxml2, libuv, qt5, qt6, c-ares, zstd, ninja, meson, pkgconf, xz_utils.
Boost is not on that list, so the range was a guaranteed review rejection.

**`cmake/[>=3.21]` tool_requires.** Upstream sets `cmake_minimum_required(VERSION 3.21)`. The
same CCI document states recipes may assume only 3.15 on the build machine and must
tool-require anything newer. Without it the recipe would fail to configure on a ConanCenter
builder with a stock CMake. The open-ended lower bound matches current CCI practice
(`au`, `loon` and `md4qt` all use that form).

Alongside these: `validate()` with `check_min_cppstd(self, 17)`, a `ConanInvalidConfiguration`
for `with_amqpcpp=False` *and* `with_inmemory=False` (upstream turns that combination into a
configure-time `FATAL_ERROR`, so failing earlier with a message naming the Conan options is
strictly better), an `fPIC` option managed by `implements = ["auto_shared_fpic"]`, and
`required_conan_version = ">=2.0.9"`.

There is deliberately **no `shared` option**. Upstream declares `add_library(motus STATIC ...)`
unconditionally, so there is no shared build to expose; `package_type = "static-library"` is
the honest declaration and a comment in the recipe says so, pre-empting the review question.

### 3.3 sentry-native

No code change was needed. The branch was 20 commits behind `master` (though still
`MERGEABLE`, so nothing was blocked by it); it was brought current through the PR
update-branch endpoint, and a short follow-up comment was posted answering the reviewer's
upstreaming question and asking for a reviewer pointer if build-system changes are not his
area.

## 4. Mathematical / Statistical Details

Not applicable. This change is packaging metadata and a linkage test; no formula, statistical
test or numeric algorithm is involved.

## 5. Design Decisions

**Fix the recipe now rather than wait for CI to report it.** The alternative was to leave
#30838 alone until a maintainer approved the run and let their CI name the problem. Rejected:
the approval is the scarce resource here. Spending one of them on a failure we could already
predict would cost another full wait for the second attempt.

**Push through the Git Data API instead of cloning conan-center-index.** A `--depth=1` clone of
CCI is large and entirely wasted on a four-file change. Assembling blobs, a tree and a commit
directly produces one atomic commit with a proper message, which is also what a reviewer
reading the PR history wants to see.

**Verify locally against the real install rather than trusting the recipe by inspection.**
Conan is not installed on this machine, so a true `conan create` was unavailable. Rather than
skip verification, the same thing CCI's CI ultimately does was reproduced without Conan: build
and install motus to a prefix, then configure the `test_package` CMakeLists against that
prefix with `CMAKE_PREFIX_PATH` and run the binary. This exercises the real
`motusConfig.cmake`, the real installed headers and the real static library. What it does
*not* cover is the Conan graph itself (`transitive_headers`, the boost pin), which remains
checked by reasoning against the CCI documentation rather than executed.

**`-DMOTUS_WITH_AMQPCPP=OFF` for the local verification.** The in-memory backend has no
external dependencies, so the check needed no amqp-cpp or Boost install. This does not weaken
the result: `install(DIRECTORY include/motus)` copies every public header regardless of
backend selection, and the test program deliberately touches only backend-independent symbols.

**A read-only routine.** The daily watch is explicitly forbidden from pushing, commenting or
editing. A scheduled agent that could act on maintainer comments would be acting on text
written by third parties without review; the routine quotes and flags instead, and every
outward action stays a human decision.

## 6. Verification

Local end-to-end check, run under an MSYS2 UCRT64 login shell (gcc 16.1.0, cmake 4.4.0,
ninja 1.13.2):

```bash
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DMOTUS_WITH_AMQPCPP=OFF -DMOTUS_WITH_INMEMORY=ON -DMOTUS_BUILD_TESTS=OFF -DCMAKE_INSTALL_PREFIX=/tmp/prefix
```

```bash
cmake --build build && cmake --install build
```

```bash
cmake -S packaging/registries/conan/test_package -B tpb -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=/tmp/prefix && cmake --build tpb && ./tpb/test_package.exe
```

Observed: the install placed all 11 public headers plus the two generated ones
(`motus/Version.hpp`, `motus/transport/Config.hpp`) and the four CMake package files;
`test_package` configured, compiled, linked against `motus::motus`, and printed

```
motus 1.0.0 (commit c1f43c6, 2026-08-24T09:41:24+03:00, git)
parsed level: WARN
```

with exit code 0.

Recipe syntax was checked with `python -c "import ast; ast.parse(...)"` on both conanfiles.

### 6.1 Under real Conan

Conan 2.31.2 was later installed (`pip install conan`) and the recipe was run for real, against
a gcc 16 profile driving the MSYS2 UCRT64 toolchain. Note that the recipe and the Conan home
must live under a short path such as `C:\mtc`; from the usual scratch directory the build dies
on `CMAKE_OBJECT_PATH_MAX` before it compiles anything.

Established:

1. `conan create` with `-o motus/*:with_amqpcpp=False` passes end to end. The v1.0.0 tarball is
   downloaded and its `sha256` verified, the library builds, the package is created, and
   `test_package` links against the CMakeDeps-generated `motus::motus` and runs.
2. `conan create` with the default options and `-o boost/*:header_only=True` also passes, and
   `src/transport/AmqpCppTransport.cpp` compiles against Conan's amqp-cpp and Boost. This is
   the path that actually exercises the AMQP backend.
3. `conan graph info` with the default options resolves against `amqp-cpp/4.3.27`,
   `boost/1.88.0` and `cmake/4.4.2`, so the boost pin and the open-ended cmake range are good.
4. A consumer that includes `motus/AmqpConnection.hpp` compiles against the package.
5. With `transitive_headers=True` the consumer's CMakeDeps run declares `Boost::headers` and
   `Boost::boost`; with the flag removed it declares neither. The flag demonstrably changes
   what reaches a consumer.

**What is still not verified, and why.**

- **That omitting `transitive_headers` actually breaks a consumer.** A control build with the
  flag removed still compiled, but that result is worthless here: MSYS2 ships both
  `/ucrt64/include/boost/` and `/ucrt64/include/amqpcpp.h`, and those sit in this gcc's default
  include search path, so the headers resolve from the system whatever Conan propagates. The
  negative case cannot be demonstrated on this machine. Point 5 above is the real evidence that
  the flag does something; the requirement itself rests on Conan's documented rule.
- **The default option set with a compiled Boost.** That build fails inside boost's own recipe
  (`boost/1.88.0`, `build()` line 1179, b2 reporting "failed updating 0 target"). The failure is
  in the dependency, not in this recipe, and ConanCenter does not build Windows with MinGW
  anyway, so it says nothing either way about motus.
- **The Windows `system_libs` and `defines`.** MinGW links `ws2_32`/`mswsock` from the sysroot
  regardless, so this toolchain cannot distinguish a correct declaration from a missing one.
  MSVC on ConanCenter CI will.

Conan also emits `WARN: risk: Transitive dependencies with 'headers=False' effect in
'package_id' is not necessary and suboptimal. Use required_conan_version='>=2.28'`. That was
left alone deliberately: CCI's own `docs/package_templates/cmake_package` still pins
`>=2.0.9`, and no recipe in the index uses `>=2.28`.

Once a Conan maintainer approves the CI run on #30838, the authoritative check is CCI's own:

```bash
conan create recipes/motus/all --version 1.0.0 --build=missing
```

## 7. Related Docs

- `packaging/registries/SUBMITTING.md`, how the two registry submissions are produced
- `packaging/registries/PR-BODY-conan.md`, `packaging/registries/PR-BODY-vcpkg.md`
- `docs/implementations/2026-08-23-extraction-and-first-green.md`
- CCI dependency policy: `docs/adding_packages/dependencies.md` in conan-io/conan-center-index
- CCI recipe template: `docs/package_templates/cmake_package/` in the same repo
- Daily watch routine: https://claude.ai/code/routines/trig_01HS4cQf4f9XMBipGQFzeNQK
