# Third-party components

motus's own source is Apache-2.0 (see [LICENSE](LICENSE)). Nothing third-party
is vendored into this repository; everything below comes from your system's
package manager or toolchain, except where noted.

| Component | Licence | When it is needed |
|---|---|---|
| [Boost](https://www.boost.org/) (Asio, Beast not used) | BSL-1.0 | `MOTUS_WITH_AMQPCPP` (the default) — the AMQP backend's event loop |
| [AMQP-CPP](https://github.com/CopernicaMarketingSoftware/AMQP-CPP) | Apache-2.0 | `MOTUS_WITH_AMQPCPP` (the default) |
| [GoogleTest](https://github.com/google/googletest) | BSD-3-Clause | `MOTUS_BUILD_TESTS` only; found via `find_package`, never fetched |
| [SimpleAmqpClient](https://github.com/alanxz/SimpleAmqpClient) | MIT | `MOTUS_WITH_SIMPLEAMQP` (**off** by default) — fetched via FetchContent at a pinned commit, because the newest release cannot be built on MSYS2/UCRT64 |
| [rabbitmq-c](https://github.com/alanxz/rabbitmq-c) | MIT | `MOTUS_WITH_SIMPLEAMQP` only — SimpleAmqpClient's own dependency, from your package manager |

A default configure (`MOTUS_WITH_AMQPCPP=ON`, `MOTUS_WITH_INMEMORY=ON`,
`MOTUS_WITH_SIMPLEAMQP=OFF`) downloads **nothing** at configure time.

The in-memory backend alone (`-DMOTUS_WITH_AMQPCPP=OFF`) has **zero**
dependencies beyond a C++17 standard library and, for the tests, GoogleTest.
