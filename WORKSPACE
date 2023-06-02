load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
  name = "com_google_googletest",
  urls = ["https://github.com/google/googletest/archive/refs/tags/v1.13.0.zip"],
  strip_prefix = "googletest-1.13.0",
)

register_execution_platforms(
    ":x64_windows-clang-cl"
)

register_toolchains(
    "@local_config_cc//:cc-toolchain-x64_windows-clang-cl",
)