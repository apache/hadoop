cmake_minimum_required(VERSION 2.8)
PROJECT(hdfsdb)

ENABLE_TESTING()

set(VERSION_MAJOR 1)
set(VERSION_MINOR 15)
set(VERRION_PATCH 0)
set(VERSION_STRING ${VERSION_MAJOR}.${VERSION_MINOR})

include_directories(${CMAKE_CURRENT_SOURCE_DIR} include)

set(HDFSDBSRCS db/builder.cc db/db_impl.cc db/db_iter.cc db/dbformat.cc
  db/filename.cc db/log_reader.cc db/log_writer.cc db/memtable.cc
  db/repair.cc db/table_cache.cc db/version_edit.cc db/version_set.cc
  db/write_batch.cc
  helpers/memenv/memenv.cc
  table/block_builder.cc table/block.cc table/filter_block.cc table/format.cc
  table/iterator.cc table/merger.cc table/table_builder.cc table/table.cc
  table/two_level_iterator.cc
  util/arena.cc util/bloom.cc util/cache.cc util/coding.cc util/comparator.cc
  util/crc32c.cc util/env.cc util/filter_policy.cc util/hash.cc util/histogram.cc
  util/logging.cc util/options.cc util/status.cc
)

if(APPLE)
add_definitions(-DOS_MACOSX -DLEVELDB_PLATFORM_POSIX)
set(PORT_SRCS port/port_posix.cc util/env_posix.cc)
endif()

add_library(hdfsdb STATIC ${HDFSDBSRCS} ${PORT_SRCS})

set(TESTHARNESS_SRCS util/testutil.cc util/testharness.cc)
add_library(hdfsdb-test-harness ${TESTHARNESS_SRCS})

macro(hdfsdb_tests)
  get_filename_component(_tname ${ARGN} NAME_WE)
  add_executable(${_tname} ${ARGN})
  target_link_libraries(${_tname} hdfsdb hdfsdb-test-harness)
  add_test(NAME ${_tname} COMMAND $<TARGET_FILE:${_tname}>)
endmacro()

hdfsdb_tests(util/arena_test.cc)
hdfsdb_tests(db/autocompact_test.cc)
hdfsdb_tests(util/bloom_test.cc)
hdfsdb_tests(util/cache_test.cc)
hdfsdb_tests(util/coding_test.cc)
hdfsdb_tests(db/corruption_test.cc)
hdfsdb_tests(util/crc32c_test.cc)
hdfsdb_tests(db/db_test.cc)
hdfsdb_tests(db/dbformat_test.cc)
hdfsdb_tests(util/env_test.cc)
hdfsdb_tests(db/filename_test.cc)
hdfsdb_tests(table/filter_block_test.cc)
hdfsdb_tests(issues/issue178_test.cc)
hdfsdb_tests(issues/issue200_test.cc)
hdfsdb_tests(db/log_test.cc)
hdfsdb_tests(db/skiplist_test.cc)
hdfsdb_tests(table/table_test.cc)
hdfsdb_tests(db/version_edit_test.cc)
hdfsdb_tests(db/version_set_test.cc)
hdfsdb_tests(db/write_batch_test.cc)
hdfsdb_tests(db/db_bench.cc)
hdfsdb_tests(helpers/memenv/memenv_test.cc)
