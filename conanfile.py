from conan import ConanFile
from conan.errors import ConanInvalidConfiguration
from conan.tools.build import check_min_cppstd
from conan.tools.cmake import CMakeToolchain, CMakeDeps, CMake, cmake_layout
from conan.tools.env import Environment
from conan.tools.files import copy
from os.path import join

required_conan_version = ">=1.60.0"

class HomeBlocksConan(ConanFile):
    name = "homeblocks"
    version = "6.0.0"

    homepage = "https://github.com/eBay/HomeBlocks"
    description = "Block Store built on HomeStore"
    topics = ("ebay")
    url = "https://github.com/eBay/HomeBlocks"
    license = "Apache-2.0"

    settings = "arch", "os", "compiler", "build_type"

    options = {
                "shared": ['True', 'False'],
                "fPIC": ['True', 'False'],
                "coverage": ['True', 'False'],
                "sanitize": ['address', 'thread', 'False'],
                "fixed_index": [True, False],
              }
    default_options = {
                'shared': False,
                'fPIC': True,
                'coverage': False,
                'sanitize': False,
                'fixed_index': True,
            }

    exports_sources = ("CMakeLists.txt", "cmake/*", "src/*", "LICENSE")

    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")
        if self.settings.build_type == "Debug":
            if self.options.coverage and self.options.sanitize != 'False':
                raise ConanInvalidConfiguration("Sanitizer does not work with Code Coverage!")
        else:
            self.options['sisl/*'].malloc_impl = 'tcmalloc'

    def build_requirements(self):
        self.test_requires("gtest/[^1.17]")
        self.test_requires("ublkpp/[^0.32]@oss/dev")

    def requirements(self):
        self.requires("homestore/[^8.0]@oss/dev", transitive_headers=True)
        self.requires("iomgr/[^13.0]@oss/dev", transitive_headers=True)
        self.requires("sisl/[^14.6]@oss/dev", transitive_headers=True)

    def validate(self):
        if self.info.settings.compiler.cppstd:
            check_min_cppstd(self, 23)

    def layout(self):
        self.folders.source = "."
        if self.options.get_safe("sanitize") and self.options.sanitize != "False":
            self.folders.build = join("build", f"Sanitized-{self.options.sanitize}")
        elif self.options.get_safe("coverage"):
            self.folders.build = join("build", "Coverage")
        else:
            self.folders.build = join("build", str(self.settings.build_type))
        self.folders.generators = join(self.folders.build, "generators")

        self.cpp.source.includedirs = ["src/include"]

        self.cpp.build.libdirs = ["src/lib/volume"]

        self.cpp.package.libs = ["homeblocks"]
        self.cpp.package.includedirs = ["include"] # includedirs is already set to 'include' by
        self.cpp.package.libdirs = ["lib"]

    def generate(self):
        # This generates "conan_toolchain.cmake" in self.generators_folder
        tc = CMakeToolchain(self)
        tc.variables["CONAN_CMAKE_SILENT_OUTPUT"] = "ON"
        tc.variables['CMAKE_EXPORT_COMPILE_COMMANDS'] = 'ON'
        tc.variables["CTEST_OUTPUT_ON_FAILURE"] = "ON"
        tc.variables["MEMORY_SANITIZER_ON"] = "OFF"
        tc.variables["CODE_COVERAGE"] = "OFF"
        tc.variables["PROJECT_VERSION"] = self.version
        tc.variables["USE_FIXED_INDEX"] = "ON" if self.options.fixed_index else "OFF"
        if self.settings.build_type == "Debug":
            if self.options.get_safe("coverage"):
                tc.variables['BUILD_COVERAGE'] = 'ON'
            elif self.options.get_safe("sanitize") and self.options.sanitize != "False":
                if self.options.sanitize == "thread":
                    tc.variables['THREAD_SANITIZER_ON'] = 'ON'
                else:  # address
                    tc.variables['ADDRESS_SANITIZER_ON'] = 'ON'
        if self.settings.build_type != "Debug":
            tc.variables['TCMALLOC_ON'] = 'ON'
        tc.generate()

        deps = CMakeDeps(self)
        deps.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
        if not self.conf.get("tools.build:skip_test", default=False):
            jobs = self.conf.get("tools.build:jobs", default=3)
            env = Environment()
            env.define("CTEST_PARALLEL_LEVEL", str(jobs))
            if self.options.get_safe("sanitize") == "thread":
                suppression_file = join(self.source_folder, "src", "test", "tsan_suppressions.txt")
                env.define("TSAN_OPTIONS", f"suppressions={suppression_file}:second_deadlock_stack=1")
            with env.vars(self).apply():
                cmake.test()

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
        if self.options.get_safe("sanitize") and self.options.sanitize != "False":
            if self.options.sanitize == "thread":
                self.cpp_info.sharedlinkflags.append("-fsanitize=thread")
                self.cpp_info.exelinkflags.append("-fsanitize=thread")
            else:
                self.cpp_info.sharedlinkflags.append("-fsanitize=address")
                self.cpp_info.exelinkflags.append("-fsanitize=address")
                self.cpp_info.sharedlinkflags.append("-fsanitize=undefined")
                self.cpp_info.exelinkflags.append("-fsanitize=undefined")

        self.cpp_info.set_property("cmake_file_name", "HomeBlocks")
        self.cpp_info.set_property("cmake_target_name", "HomeBlocks::HomeBlocks")
        self.cpp_info.names["cmake_find_package"] = "HomeBlocks"
        self.cpp_info.names["cmake_find_package_multi"] = "HomeBlocks"
