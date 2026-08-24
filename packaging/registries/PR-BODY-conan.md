### motus/1.0.0: new recipe

Specify library name and version: **motus/1.0.0**

New recipe for [motus](https://github.com/mertefesensoy/motus) — a transport-agnostic
messaging seam for C++17 with a per-backend conformance suite and a Windows-safe AMQP-CPP
connection handler over Boost.Asio. Apache-2.0. I am the upstream author.

- `with_amqpcpp` (default True) pulls `amqp-cpp/4.3.27` + Boost (Asio); with it off the
  in-memory backend builds with zero dependencies.
- Static library, C++17, CMake ≥ 3.21.
- Upstream CI: Ubuntu GCC, Windows MSVC, Windows MSYS2/UCRT64, macOS arm64, plus the full
  39-scenario conformance contract against a live RabbitMQ.

---

- [x] I've read the [contributing guidelines](https://github.com/conan-io/conan-center-index/blob/master/CONTRIBUTING.md).
- [x] I've followed the [PEP8](https://www.python.org/dev/peps/pep-0008/) style guides for Python code.
- [x] I've opened another PR in the Conan docs repo to the docs if necessary. <!-- n/a -->
