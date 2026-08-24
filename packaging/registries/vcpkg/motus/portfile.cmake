# vcpkg port for motus.
#
# The SHA512 below is the checksum of the v1.0.0 source archive; recompute for any new REF
# with `vcpkg hash <tarball>` (the SUBMITTING.md beside this port has the exact command).

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO mertefesensoy/motus
    REF "v${VERSION}"
    SHA512 FILL-ME-AFTER-TAGGING
    HEAD_REF main
)

vcpkg_cmake_configure(
    SOURCE_PATH "${SOURCE_PATH}"
    OPTIONS
        -DMOTUS_BUILD_TESTS=OFF
        -DMOTUS_WITH_AMQPCPP=ON
        -DMOTUS_WITH_INMEMORY=ON
        -DMOTUS_WITH_SIMPLEAMQP=OFF
)

vcpkg_cmake_install()
vcpkg_cmake_config_fixup(CONFIG_PATH "lib/cmake/motus")
vcpkg_copy_pdbs()

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")
vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/LICENSE")
