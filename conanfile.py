from os.path import join
from conan import ConanFile
from conan.errors import ConanInvalidConfiguration
from conan.tools.files import copy
from conan.tools.build import check_min_cppstd
from conans import CMake

required_conan_version = ">=1.50.0"

class HomeReplicationConan(ConanFile):
    name = "home_replication"
    version = "0.0.3"
    homepage = "https://github.com/eBay/HomeReplication"
    description = "Fast Storage Replication using NuRaft"
    topics = ("ebay")
    url = "https://github.com/eBay/HomeReplication"
    license = "Apache-2.0"

    settings = "arch", "os", "compiler", "build_type"

    options = {
                "shared": ['True', 'False'],
                "fPIC": ['True', 'False'],
                "sanitize": ['True', 'False'],
              }
    default_options = {
                'shared': False,
                'fPIC': True,
                'sanitize': False,
            }

    generators = "cmake", "cmake_find_package"
    exports_sources = ("CMakeLists.txt", "cmake/*", "src/*", "LICENSE")

    def build_requirements(self):
        self.build_requires("gtest/1.12.1")

    def requirements(self):
        self.requires("nuraft_mesg/[~=0,    include_prerelease=True]@oss/master")
        self.requires("homestore/[~=4,      include_prerelease=True]@oss/develop")
        self.requires("sisl/[~=9,           include_prerelease=True]@oss/master")

        self.requires("nuraft/2.1.0", override=True)
        self.requires("openssl/1.1.1s", override=True)
        self.requires("zlib/1.2.12", override=True)

    def validate(self):
        if self.info.settings.os in ["Macos", "Windows"]:
            raise ConanInvalidConfiguration("{} Builds are unsupported".format(self.info.settings.os))
        if self.info.settings.compiler.cppstd:
            check_min_cppstd(self, 11)

    def configure(self):
        if self.options.shared:
            del self.options.fPIC

    def build(self):
        definitions = {
            'CMAKE_EXPORT_COMPILE_COMMANDS': 'ON',
            'MEMORY_SANITIZER_ON': 'OFF',
        }
        if self.settings.build_type == "Debug" and self.options.sanitize:
            definitions['MEMORY_SANITIZER_ON'] = 'ON'

        cmake = CMake(self)
        cmake.configure(defs=definitions)
        cmake.build()

    def package(self):
        lib_dir = join(self.package_folder, "lib")
        copy(self, "LICENSE", self.source_folder, join(self.package_folder, "licenses"), keep_path=False)
        copy(self, "*.lib", self.build_folder, lib_dir, keep_path=False)
        copy(self, "*.a", self.build_folder, lib_dir, keep_path=False)
        copy(self, "*.dylib*", self.build_folder, lib_dir, keep_path=False)
        copy(self, "*.dll*", self.build_folder, join(self.package_folder, "bin"), keep_path=False)
        copy(self, "*.so*", self.build_folder, lib_dir, keep_path=False)
        copy(self, "*", join(self.source_folder, "src", "flip", "client", "python"), join(self.package_folder, "bindings", "flip", "python"), keep_path=False)

        copy(self, "*.h*", join(self.source_folder, "src", "include"), join(self.package_folder, "include"), keep_path=True)

    def package_info(self):
        self.cpp_info.libs = ["home_replication"]

        if self.settings.os == "Linux":
            self.cpp_info.system_libs.append("pthread")

        if  self.options.sanitize:
            self.cpp_info.sharedlinkflags.append("-fsanitize=address")
            self.cpp_info.exelinkflags.append("-fsanitize=address")
            self.cpp_info.sharedlinkflags.append("-fsanitize=undefined")
            self.cpp_info.exelinkflags.append("-fsanitize=undefined")
