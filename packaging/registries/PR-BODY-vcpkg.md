Adds a new port: **motus** 1.0.0 — a transport-agnostic messaging seam for C++17 with a
per-backend conformance suite and a Windows-safe AMQP-CPP connection handler over Boost.Asio.

- Upstream: https://github.com/mertefesensoy/motus (I am the upstream author)
- License: Apache-2.0 (`vcpkg_install_copyright` from the repo's LICENSE)
- Builds the library only (`MOTUS_BUILD_TESTS=OFF`); depends on `amqpcpp`, `boost-asio`,
  `boost-system`.

Upstream CI covers Ubuntu (GCC), Windows MSVC, Windows MSYS2/UCRT64 and macOS arm64,
including a full conformance run against a live RabbitMQ:
https://github.com/mertefesensoy/motus/actions

<!-- vcpkg PR checklist -->
- [x] Changes comply with the [maintainer guide](https://learn.microsoft.com/vcpkg/contributing/maintainer-guide).
- [x] SHA512s are updated for each updated download.
- [x] The "supports" clause reflects platforms that may be fixed by this new version. <!-- n/a: new port -->
- [x] Any fixed [CI baseline](https://github.com/microsoft/vcpkg/blob/master/scripts/ci.baseline.txt) entries are removed from that file. <!-- n/a -->
- [x] Any patches that are no longer applied are deleted from the port's directory. <!-- n/a: no patches -->
- [x] The version database is fixed by rerunning `./vcpkg x-add-version --all` and committing the result.
- [x] Only one version is added to each modified port's versions file.
