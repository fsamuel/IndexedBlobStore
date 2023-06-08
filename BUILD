platform(
    name = "x64_windows-clang-cl",
    constraint_values = [
        "@platforms//cpu:x86_64",
        "@platforms//os:windows",
        "@bazel_tools//tools/cpp:clang-cl",
    ],
)

cc_library(
    name = "b_plus_tree_lib",
    srcs = [
        "blob_store.cpp",
        "chunk_manager.cpp",
        "fixed_string.cpp",
        "nodes.cpp",
        "shared_memory_buffer.cpp",
        "shm_allocator.cpp",
        "string_slice.cpp",
        "utils.cpp"
    ],
    hdrs = [
        "b_plus_tree.h",
        "b_plus_tree_base.h",
        "blob_store.h",
        "chunk_manager.h",
        "chunked_vector.h",
        "fixed_string.h",
        "nodes.h",
        "shared_memory_buffer.h",
        "shm_allocator.h",
        "storage_traits.h",
        "string_slice.h",
        "transaction.h",
        "tree_iterator.h",
        "utils.h"     
    ],

)

cc_test(
    name = "b_plus_tree_tests",
    srcs = [
        "blob_store_test.cpp",
        "b_plus_tree_test.cpp",
        "chunk_manager_test.cpp",
        "chunked_vector_test.cpp",
        "fixed_string_test.cpp",
        "nodes_test.cpp",
        "shared_memory_buffer_test.cpp",
        "shm_allocator_test.cpp",
        "string_slice_test.cpp"
    ],
    deps = [
        ":b_plus_tree_lib",
        "@com_google_googletest//:gtest_main",
    ],
)