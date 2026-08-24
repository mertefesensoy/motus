# Submitting motus to the package registries

Both submissions are pull requests against third-party repositories, filed from your own
GitHub account. Everything here is staged; the steps below are the only parts that must be
done by hand.

## vcpkg (microsoft/vcpkg)

1. **Compute the release SHA512** (once v1.0.0 is tagged):

   ```console
   curl -sL https://github.com/mertefesensoy/motus/archive/refs/tags/v1.0.0.tar.gz -o motus-1.0.0.tar.gz
   vcpkg hash motus-1.0.0.tar.gz
   ```

   Paste the result over `FILL-ME-AFTER-TAGGING` in `vcpkg/motus/portfile.cmake`.

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

1. Fork conan-io/conan-center-index; create `recipes/motus/all/` containing
   `conanfile.py` (from `conan/conanfile.py` here) and a `conandata.yml`:

   ```yaml
   sources:
     "1.0.0":
       url: "https://github.com/mertefesensoy/motus/archive/refs/tags/v1.0.0.tar.gz"
       sha256: "<sha256 of the same tarball>"
   ```

   and `recipes/motus/config.yml`:

   ```yaml
   versions:
     "1.0.0":
       folder: all
   ```

2. **Test locally**: `conan create recipes/motus/all --version 1.0.0`.

3. File the PR. Conan Center's bot walks you through the rest; first-time recipe
   submissions get a human review pass.

## Notes

- Registries build from the **tagged tarball**, not from git — any future fix that should
  reach users needs a new tag and a version bump in both registries.
- The in-memory backend keeps the library usable with zero dependencies; the port builds
  with the AMQP backend ON because that is the library's reason to exist.
