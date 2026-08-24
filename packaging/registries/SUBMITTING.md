# Submitting motus to the package registries

**Status: both v1.0.0 submissions are FILED (2026-08-24)** —
[microsoft/vcpkg#53562](https://github.com/microsoft/vcpkg/pull/53562) and
[conan-io/conan-center-index#30838](https://github.com/conan-io/conan-center-index/pull/30838).
The steps below remain as the playbook for future version bumps.

## vcpkg (microsoft/vcpkg)

1. The port already carries the real v1.0.0 SHA512. For any future version, recompute:

   ```console
   curl -sL https://github.com/mertefesensoy/motus/archive/refs/tags/vX.Y.Z.tar.gz -o motus.tar.gz
   vcpkg hash motus.tar.gz
   ```

2. **Test the port locally** before filing (this is the exact check vcpkg CI runs first):

   ```console
   git clone https://github.com/microsoft/vcpkg && cd vcpkg && ./bootstrap-vcpkg.sh
   cp -r <motus>/packaging/registries/vcpkg/motus ports/motus
   ./vcpkg x-add-version motus
   ./vcpkg install motus --triplet x64-windows   # or x64-linux
   ```

3. **File the PR**: branch, commit `ports/motus/*` plus the `versions/` change from
   `x-add-version`, push to your fork, open the PR. Title: `[motus] new port`.
   Their CI builds the port on every triplet; expect a review cycle about supported
   platforms (it is reasonable to add `"supports": "!uwp & !android & !ios"` to the port's
   vcpkg.json if their CI flags those triplets).

## Conan (conan-io/conan-center-index)

1. Fork conan-io/conan-center-index, then copy the files staged here:

   - `conan/conanfile.py`  -> `recipes/motus/all/conanfile.py`
   - `conan/conandata.yml` -> `recipes/motus/all/conandata.yml`  (real v1.0.0 sha256)
   - `conan/config.yml`    -> `recipes/motus/config.yml`

2. **Test locally**: `conan create recipes/motus/all --version 1.0.0`.

3. File the PR. Conan Center's bot walks you through the rest; first-time recipe
   submissions get a human review pass.

## Notes

- Registries build from the **tagged tarball**, not from git — any future fix that should
  reach users needs a new tag and a version bump in both registries.
- The in-memory backend keeps the library usable with zero dependencies; the port builds
  with the AMQP backend ON because that is the library's reason to exist.
