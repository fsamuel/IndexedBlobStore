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
        "src/allocation_logger.cpp",
        "src/blob_store.cpp",
        "src/chunk_manager.cpp",
        "src/fixed_string.cpp",
        "src/nodes.cpp",
        "src/shared_memory_buffer.cpp",
        "src/shm_allocator.cpp",
        "src/string_slice.cpp",
        "src/utils.cpp"
    ],
    hdrs = [
        "include/allocation_logger.h",
        "include/b_plus_tree.h",
        "include/b_plus_tree_base.h",
        "include/blob_metadata.h",
        "include/blob_store.h",
        "include/blob_store_base.h",
        "include/blob_store_object.h",
        "include/buffer.h",
        "include/buffer_factory.h",
        "include/chunk_manager.h",
        "include/chunked_vector.h",
        "include/fixed_string.h",
        "include/nodes.h",
        "include/shared_memory_buffer.h",
        "include/shared_memory_buffer_factory.h",
        "include/shm_allocator.h",
        "include/shm_node.h",
        "include/storage_traits.h",
        "include/string_slice.h",
        "include/test_memory_buffer.h",
        "include/test_memory_buffer_factory.h",
        "include/transaction.h",
        "include/tree_iterator.h",
        "include/utils.h"     
    ],
    includes = [
        "include/",
    ]
)

cc_test(
    name = "b_plus_tree_tests",
    srcs = [
        "test/b_plus_tree_test.cpp",
        "test/blob_store_test.cpp",
        "test/chunk_manager_test.cpp",
        "test/chunked_vector_test.cpp",
        "test/fixed_string_test.cpp",
        "test/nodes_test.cpp",
        "test/shared_memory_buffer_test.cpp",
        "test/shm_allocator_test.cpp",
        "test/string_slice_test.cpp"
    ],
    deps = [
        ":b_plus_tree_lib",
        "@com_google_googletest//:gtest_main",
    ],
)