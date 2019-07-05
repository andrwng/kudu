# Prerequisites for Windows:
#     This cmake build is for Windows 64-bit only.
#
# Prerequisites:
#     You must have at least Visual Studio 2015 Update 3. Start the Developer Command Prompt window that is a part of Visual Studio installation.
#     Run the build commands from within the Developer Command Prompt window to have paths to the compiler and runtime libraries set.
#     You must have git.exe in your %PATH% environment variable.
#
# To build Rocksdb for Windows is as easy as 1-2-3-4-5:
#
# 1. Update paths to third-party libraries in thirdparty.inc file
# 2. Create a new directory for build artifacts
#        mkdir build
#        cd build
# 3. Run cmake to generate project files for Windows, add more options to enable required third-party libraries.
#    See thirdparty.inc for more information.
#        sample command: cmake -G "Visual Studio 15 Win64" -DWITH_GFLAGS=1 -DWITH_SNAPPY=1 -DWITH_JEMALLOC=1 -DWITH_JNI=1 ..
# 4. Then build the project in debug mode (you may want to add /m[:<N>] flag to run msbuild in <N> parallel threads
#                                          or simply /m to use all avail cores)
#        msbuild rocksdb.sln
#
#        rocksdb.sln build features exclusions of test only code in Release. If you build ALL_BUILD then everything
#        will be attempted but test only code does not build in Release mode.
#
# 5. And release mode (/m[:<N>] is also supported)
#        msbuild rocksdb.sln /p:Configuration=Release
#
# Linux:
#
# 1. Install a recent toolchain such as devtoolset-3 if you're on a older distro. C++11 required.
# 2. mkdir build; cd build
# 3. cmake ..
# 4. make -j

set(ROCKSDB_SRCS
    cache/clock_cache.cc
    cache/lru_cache.cc
    cache/sharded_cache.cc
    db/builder.cc
    db/c.cc
    db/column_family.cc
    db/compacted_db_impl.cc
    db/compaction.cc
    db/compaction_iterator.cc
    db/compaction_job.cc
    db/compaction_picker.cc
    db/compaction_picker_fifo.cc
    db/compaction_picker_universal.cc
    db/convenience.cc
    db/db_filesnapshot.cc
    db/db_impl.cc
    db/db_impl_write.cc
    db/db_impl_compaction_flush.cc
    db/db_impl_files.cc
    db/db_impl_open.cc
    db/db_impl_debug.cc
    db/db_impl_experimental.cc
    db/db_impl_readonly.cc
    db/db_impl_secondary.cc
    db/db_info_dumper.cc
    db/db_iter.cc
    db/dbformat.cc
    db/error_handler.cc
    db/event_helpers.cc
    db/experimental.cc
    db/external_sst_file_ingestion_job.cc
    db/file_indexer.cc
    db/flush_job.cc
    db/flush_scheduler.cc
    db/forward_iterator.cc
    db/internal_stats.cc
    db/in_memory_stats_history.cc
    db/logs_with_prep_tracker.cc
    db/log_reader.cc
    db/log_writer.cc
    db/malloc_stats.cc
    db/memtable.cc
    db/memtable_list.cc
    db/merge_helper.cc
    db/merge_operator.cc
    db/range_del_aggregator.cc
    db/range_tombstone_fragmenter.cc
    db/repair.cc
    db/snapshot_impl.cc
    db/table_cache.cc
    db/table_properties_collector.cc
    db/transaction_log_impl.cc
    db/version_builder.cc
    db/version_edit.cc
    db/version_set.cc
    db/wal_manager.cc
    db/write_batch.cc
    db/write_batch_base.cc
    db/write_controller.cc
    db/write_thread.cc
    env/env.cc
    env/env_chroot.cc
    env/env_encryption.cc
    env/env_hdfs.cc
    env/mock_env.cc
    memtable/alloc_tracker.cc
    memtable/hash_linklist_rep.cc
    memtable/hash_skiplist_rep.cc
    memtable/skiplistrep.cc
    memtable/vectorrep.cc
    memtable/write_buffer_manager.cc
    monitoring/histogram.cc
    monitoring/histogram_windowing.cc
    monitoring/instrumented_mutex.cc
    monitoring/iostats_context.cc
    monitoring/perf_context.cc
    monitoring/perf_level.cc
    monitoring/statistics.cc
    monitoring/thread_status_impl.cc
    monitoring/thread_status_updater.cc
    monitoring/thread_status_util.cc
    monitoring/thread_status_util_debug.cc
    options/cf_options.cc
    options/db_options.cc
    options/options.cc
    options/options_helper.cc
    options/options_parser.cc
    options/options_sanity_check.cc
    port/stack_trace.cc
    table/adaptive_table_factory.cc
    table/block.cc
    table/block_based_filter_block.cc
    table/block_based_table_builder.cc
    table/block_based_table_factory.cc
    table/block_based_table_reader.cc
    table/block_builder.cc
    table/block_fetcher.cc
    table/block_prefix_index.cc
    table/bloom_block.cc
    table/cuckoo_table_builder.cc
    table/cuckoo_table_factory.cc
    table/cuckoo_table_reader.cc
    table/data_block_hash_index.cc
    table/data_block_footer.cc
    table/flush_block_policy.cc
    table/format.cc
    table/full_filter_block.cc
    table/get_context.cc
    table/index_builder.cc
    table/iterator.cc
    table/merging_iterator.cc
    table/meta_blocks.cc
    table/partitioned_filter_block.cc
    table/persistent_cache_helper.cc
    table/plain_table_builder.cc
    table/plain_table_factory.cc
    table/plain_table_index.cc
    table/plain_table_key_coding.cc
    table/plain_table_reader.cc
    table/sst_file_reader.cc
    table/sst_file_writer.cc
    table/table_properties.cc
    table/two_level_iterator.cc
    tools/db_bench_tool.cc
    tools/dump/db_dump_tool.cc
    tools/ldb_cmd.cc
    tools/ldb_tool.cc
    tools/sst_dump_tool.cc
    tools/trace_analyzer_tool.cc
    util/arena.cc
    util/auto_roll_logger.cc
    util/bloom.cc
    util/coding.cc
    util/compaction_job_stats_impl.cc
    util/comparator.cc
    util/compression_context_cache.cc
    util/concurrent_arena.cc
    util/concurrent_task_limiter_impl.cc
    util/crc32c.cc
    util/delete_scheduler.cc
    util/dynamic_bloom.cc
    util/event_logger.cc
    util/file_reader_writer.cc
    util/file_util.cc
    util/filename.cc
    util/filter_policy.cc
    util/hash.cc
    util/jemalloc_nodump_allocator.cc
    util/log_buffer.cc
    util/murmurhash.cc
    util/random.cc
    util/rate_limiter.cc
    util/slice.cc
    util/sst_file_manager_impl.cc
    util/status.cc
    util/string_util.cc
    util/sync_point.cc
    util/sync_point_impl.cc
    util/testutil.cc
    util/thread_local.cc
    util/threadpool_imp.cc
    util/trace_replay.cc
    util/transaction_test_util.cc
    util/xxhash.cc
    utilities/backupable/backupable_db.cc
    utilities/blob_db/blob_compaction_filter.cc
    utilities/blob_db/blob_db.cc
    utilities/blob_db/blob_db_impl.cc
    utilities/blob_db/blob_db_impl_filesnapshot.cc
    utilities/blob_db/blob_dump_tool.cc
    utilities/blob_db/blob_file.cc
    utilities/blob_db/blob_log_reader.cc
    utilities/blob_db/blob_log_writer.cc
    utilities/blob_db/blob_log_format.cc
    utilities/cassandra/cassandra_compaction_filter.cc
    utilities/cassandra/format.cc
    utilities/cassandra/merge_operator.cc
    utilities/checkpoint/checkpoint_impl.cc
    utilities/compaction_filters/remove_emptyvalue_compactionfilter.cc
    utilities/debug.cc
    utilities/env_mirror.cc
    utilities/env_timed.cc
    utilities/leveldb_options/leveldb_options.cc
    utilities/memory/memory_util.cc
    utilities/merge_operators/bytesxor.cc
    utilities/merge_operators/max.cc
    utilities/merge_operators/put.cc
    utilities/merge_operators/string_append/stringappend.cc
    utilities/merge_operators/string_append/stringappend2.cc
    utilities/merge_operators/uint64add.cc
    utilities/option_change_migration/option_change_migration.cc
    utilities/options/options_util.cc
    utilities/persistent_cache/block_cache_tier.cc
    utilities/persistent_cache/block_cache_tier_file.cc
    utilities/persistent_cache/block_cache_tier_metadata.cc
    utilities/persistent_cache/persistent_cache_tier.cc
    utilities/persistent_cache/volatile_tier_impl.cc
    utilities/simulator_cache/sim_cache.cc
    utilities/table_properties_collectors/compact_on_deletion_collector.cc
    utilities/trace/file_trace_reader_writer.cc
    utilities/transactions/optimistic_transaction_db_impl.cc
    utilities/transactions/optimistic_transaction.cc
    utilities/transactions/pessimistic_transaction.cc
    utilities/transactions/pessimistic_transaction_db.cc
    utilities/transactions/snapshot_checker.cc
    utilities/transactions/transaction_base.cc
    utilities/transactions/transaction_db_mutex_impl.cc
    utilities/transactions/transaction_lock_mgr.cc
    utilities/transactions/transaction_util.cc
    utilities/transactions/write_prepared_txn.cc
    utilities/transactions/write_prepared_txn_db.cc
    utilities/transactions/write_unprepared_txn.cc
    utilities/transactions/write_unprepared_txn_db.cc
    utilities/ttl/db_ttl_impl.cc
    utilities/write_batch_with_index/write_batch_with_index.cc
    utilities/write_batch_with_index/write_batch_with_index_internal.cc
    ${ROCKSDB_PROTO_SRCS}
)

set(CMAKE_CXX_FLAGS
  "${CMAKE_CXX_FLAGS} \
   -Wextra -Wsign-compare -Wshadow -Woverloaded-virtual \
   -Wno-missing-field-initializers -Wno-unused-parameter -Wno-unused-variable")

if (NOT APPLE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DOS_LINUX -DROCKSDB_FALLOCATE_PRESENT")
endif()

if (${KUDU_USE_TSAN})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DROCKSDB_TSAN_RUN")
endif()

if (${KUDU_USE_ASAN})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DROCKSDB_MALLOC_USABLE_SIZE")
endif()

add_library(rocksdb ${ROCKSDB_SRCS})

# option(WITH_TESTS "build with tests" ON)
# if(WITH_TESTS)
#   set(TESTS
#         cache/cache_test.cc
#         cache/lru_cache_test.cc
#         db/column_family_test.cc
#         db/compact_files_test.cc
#         db/compaction_iterator_test.cc
#         db/compaction_job_stats_test.cc
#         db/compaction_job_test.cc
#         db/compaction_picker_test.cc
#         db/comparator_db_test.cc
#         db/corruption_test.cc
#         db/cuckoo_table_db_test.cc
#         db/db_basic_test.cc
#         db/db_blob_index_test.cc
#         db/db_block_cache_test.cc
#         db/db_bloom_filter_test.cc
#         db/db_compaction_filter_test.cc
#         db/db_compaction_test.cc
#         db/db_dynamic_level_test.cc
#         db/db_flush_test.cc
#         db/db_inplace_update_test.cc
#         db/db_io_failure_test.cc
#         db/db_iter_test.cc
#         db/db_iter_stress_test.cc
#         db/db_iterator_test.cc
#         db/db_log_iter_test.cc
#         db/db_memtable_test.cc
#         db/db_merge_operator_test.cc
#         db/db_options_test.cc
#         db/db_properties_test.cc
#         db/db_range_del_test.cc
#         db/db_secondary_test.cc
#         db/db_sst_test.cc
#         db/db_statistics_test.cc
#         db/db_table_properties_test.cc
#         db/db_tailing_iter_test.cc
#         db/db_test.cc
#         db/db_test2.cc
#         db/db_universal_compaction_test.cc
#         db/db_wal_test.cc
#         db/db_write_test.cc
#         db/dbformat_test.cc
#         db/deletefile_test.cc
#         db/error_handler_test.cc
#         db/obsolete_files_test.cc
#         db/external_sst_file_basic_test.cc
#         db/external_sst_file_test.cc
#         db/fault_injection_test.cc
#         db/file_indexer_test.cc
#         db/filename_test.cc
#         db/flush_job_test.cc
#         db/listener_test.cc
#         db/log_test.cc
#         db/manual_compaction_test.cc
#         db/memtable_list_test.cc
#         db/merge_helper_test.cc
#         db/merge_test.cc
#         db/options_file_test.cc
#         db/perf_context_test.cc
#         db/plain_table_db_test.cc
#         db/prefix_test.cc
#         db/range_del_aggregator_test.cc
#         db/range_tombstone_fragmenter_test.cc
#         db/repair_test.cc
#         db/table_properties_collector_test.cc
#         db/version_builder_test.cc
#         db/version_edit_test.cc
#         db/version_set_test.cc
#         db/wal_manager_test.cc
#         db/write_batch_test.cc
#         db/write_callback_test.cc
#         db/write_controller_test.cc
#         env/env_basic_test.cc
#         env/env_test.cc
#         env/mock_env_test.cc
#         memtable/inlineskiplist_test.cc
#         memtable/skiplist_test.cc
#         memtable/write_buffer_manager_test.cc
#         monitoring/histogram_test.cc
#         monitoring/iostats_context_test.cc
#         monitoring/statistics_test.cc
#         options/options_settable_test.cc
#         options/options_test.cc
#         table/block_based_filter_block_test.cc
#         table/block_test.cc
#         table/cleanable_test.cc
#         table/cuckoo_table_builder_test.cc
#         table/cuckoo_table_reader_test.cc
#         table/data_block_hash_index_test.cc
#         table/full_filter_block_test.cc
#         table/merger_test.cc
#         table/sst_file_reader_test.cc
#         table/table_test.cc
#         tools/ldb_cmd_test.cc
#         tools/reduce_levels_test.cc
#         tools/sst_dump_test.cc
#         tools/trace_analyzer_test.cc
#         util/arena_test.cc
#         util/auto_roll_logger_test.cc
#         util/autovector_test.cc
#         util/bloom_test.cc
#         util/coding_test.cc
#         util/crc32c_test.cc
#         util/delete_scheduler_test.cc
#         util/dynamic_bloom_test.cc
#         util/event_logger_test.cc
#         util/file_reader_writer_test.cc
#         util/filelock_test.cc
#         util/hash_test.cc
#         util/heap_test.cc
#         util/rate_limiter_test.cc
#         util/repeatable_thread_test.cc
#         util/slice_transform_test.cc
#         util/timer_queue_test.cc
#         util/thread_list_test.cc
#         util/thread_local_test.cc
#         utilities/backupable/backupable_db_test.cc
#         utilities/blob_db/blob_db_test.cc
#         utilities/cassandra/cassandra_functional_test.cc
#         utilities/cassandra/cassandra_format_test.cc
#         utilities/cassandra/cassandra_row_merge_test.cc
#         utilities/cassandra/cassandra_serialize_test.cc
#         utilities/checkpoint/checkpoint_test.cc
#         utilities/memory/memory_test.cc
#         utilities/merge_operators/string_append/stringappend_test.cc
#         utilities/object_registry_test.cc
#         utilities/option_change_migration/option_change_migration_test.cc
#         utilities/options/options_util_test.cc
#         utilities/persistent_cache/hash_table_test.cc
#         utilities/persistent_cache/persistent_cache_test.cc
#         utilities/simulator_cache/sim_cache_test.cc
#         utilities/table_properties_collectors/compact_on_deletion_collector_test.cc
#         utilities/transactions/optimistic_transaction_test.cc
#         utilities/transactions/transaction_test.cc
#         utilities/transactions/write_prepared_transaction_test.cc
#         utilities/transactions/write_unprepared_transaction_test.cc
#         utilities/ttl/ttl_test.cc
#         utilities/write_batch_with_index/write_batch_with_index_test.cc
#   )
#   if(WITH_LIBRADOS)
#     list(APPEND TESTS utilities/env_librados_test.cc)
#   endif()
# 
#   set(BENCHMARKS
#     cache/cache_bench.cc
#     memtable/memtablerep_bench.cc
#     db/range_del_aggregator_bench.cc
#     tools/db_bench.cc
#     table/table_reader_bench.cc
#     utilities/persistent_cache/hash_table_bench.cc)
#   add_library(testharness OBJECT util/testharness.cc)
#   foreach(sourcefile ${BENCHMARKS})
#     get_filename_component(exename ${sourcefile} NAME_WE)
#     add_executable(${exename}${ARTIFACT_SUFFIX} ${sourcefile}
#       $<TARGET_OBJECTS:testharness>)
#     target_link_libraries(${exename}${ARTIFACT_SUFFIX} gtest ${LIBS})
#   endforeach(sourcefile ${BENCHMARKS})
# 
#   # For test util library that is build only in DEBUG mode
#   # and linked to tests. Add test only code that is not #ifdefed for Release here.
#   set(TESTUTIL_SOURCE
#       db/db_test_util.cc
#       monitoring/thread_status_updater_debug.cc
#       table/mock_table.cc
#       util/fault_injection_test_env.cc
#       utilities/cassandra/test_utils.cc
#   )
#   # test utilities are only build in debug
#   enable_testing()
#   add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND})
#   set(TESTUTILLIB testutillib${ARTIFACT_SUFFIX})
#   add_library(${TESTUTILLIB} STATIC ${TESTUTIL_SOURCE})
#   if(MSVC)
#     set_target_properties(${TESTUTILLIB} PROPERTIES COMPILE_FLAGS "/Fd${CMAKE_CFG_INTDIR}/testutillib${ARTIFACT_SUFFIX}.pdb")
#   endif()
#   set_target_properties(${TESTUTILLIB}
#         PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
#         EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
#         EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
#         )
# 
#   # Tests are excluded from Release builds
#   set(TEST_EXES ${TESTS})
# 
#   foreach(sourcefile ${TEST_EXES})
#       get_filename_component(exename ${sourcefile} NAME_WE)
#       add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${sourcefile}
#         $<TARGET_OBJECTS:testharness>)
#       set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
#         PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
#         EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
#         EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
#         OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
#         )
#       target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} testutillib${ARTIFACT_SUFFIX} gtest ${LIBS})
#       if(NOT "${exename}" MATCHES "db_sanity_test")
#         add_test(NAME ${exename} COMMAND ${exename}${ARTIFACT_SUFFIX})
#         add_dependencies(check ${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX})
#       endif()
#   endforeach(sourcefile ${TEST_EXES})
# 
#   # C executables must link to a shared object
#   set(C_TESTS db/c_test.c)
#   set(C_TEST_EXES ${C_TESTS})
# 
#   foreach(sourcefile ${C_TEST_EXES})
#       string(REPLACE ".c" "" exename ${sourcefile})
#       string(REGEX REPLACE "^((.+)/)+" "" exename ${exename})
#       add_executable(${exename}${ARTIFACT_SUFFIX} ${sourcefile})
#       set_target_properties(${exename}${ARTIFACT_SUFFIX}
#         PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
#         EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
#         EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
#         )
#       target_link_libraries(${exename}${ARTIFACT_SUFFIX} ${ROCKSDB_IMPORT_LIB} testutillib${ARTIFACT_SUFFIX})
#       add_test(NAME ${exename} COMMAND ${exename}${ARTIFACT_SUFFIX})
#       add_dependencies(check ${exename}${ARTIFACT_SUFFIX})
#   endforeach(sourcefile ${C_TEST_EXES})
# endif()
# 
# option(WITH_TOOLS "build with tools" ON)
# if(WITH_TOOLS)
#   add_subdirectory(tools)
# endif()
