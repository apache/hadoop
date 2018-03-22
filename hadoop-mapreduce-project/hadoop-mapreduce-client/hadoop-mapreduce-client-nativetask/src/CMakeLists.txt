#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

cmake_minimum_required(VERSION 3.1 FATAL_ERROR)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/../../../../hadoop-common-project/hadoop-common/)
include(HadoopCommon)

# Set gtest path
set(GTEST_SRC_DIR ${CMAKE_SOURCE_DIR}/../../../../hadoop-common-project/hadoop-common/src/main/native/gtest)

# Add extra compiler and linker flags.
# -Wno-sign-compare
hadoop_add_compiler_flags("-DNDEBUG -DSIMPLE_MEMCPY -fno-strict-aliasing -fsigned-char")

# Source location.
set(SRC main/native)

# The caller must specify where the generated headers have been placed.
if(NOT GENERATED_JAVAH)
    message(FATAL_ERROR "You must set the CMake variable GENERATED_JAVAH")
endif()

# Configure JNI.
include(HadoopJNI)

# Probe for headers and functions.
include(CheckFunctionExists)
include(CheckIncludeFiles)

check_include_files(fcntl.h HAVE_FCNTL_H)
check_include_files(malloc.h HAVE_MALLOC_H)
check_include_files(mach/mach.h HAVE_MACH_MACH_H)
check_include_files(memory.h HAVE_MEMORY_H)
check_include_files(stddef.h HAVE_STDDEF_H)
check_include_files(stdint.h HAVE_STDINT_H)
check_include_files(stdlib.h HAVE_STDLIB_H)
Check_include_files(string.h HAVE_STRING_H)
check_include_files(unistd.h HAVE_UNITSTD_H)
check_function_exists(clock_gettime HAVE_CLOCK_GETTIME)
check_function_exists(localtime_r HAVE_LOCALTIME_R)
check_function_exists(memset HAVE_MEMSET)
check_function_exists(strchr HAVE_STRCHR)
check_function_exists(strtoul HAVE_STRTOUL)

# Require snappy.
set(STORED_CMAKE_FIND_LIBRARY_SUFFIXES CMAKE_FIND_LIBRARY_SUFFIXES)
hadoop_set_find_shared_library_version("1")
find_library(SNAPPY_LIBRARY
    NAMES snappy
    PATHS ${CUSTOM_SNAPPY_PREFIX} ${CUSTOM_SNAPPY_PREFIX}/lib
          ${CUSTOM_SNAPPY_PREFIX}/lib64 ${CUSTOM_SNAPPY_LIB})
set(CMAKE_FIND_LIBRARY_SUFFIXES STORED_CMAKE_FIND_LIBRARY_SUFFIXES)
find_path(SNAPPY_INCLUDE_DIR
    NAMES snappy.h
    PATHS ${CUSTOM_SNAPPY_PREFIX} ${CUSTOM_SNAPPY_PREFIX}/include
          ${CUSTOM_SNAPPY_INCLUDE})
if(SNAPPY_LIBRARY AND SNAPPY_INCLUDE_DIR)
    GET_FILENAME_COMPONENT(HADOOP_SNAPPY_LIBRARY ${SNAPPY_LIBRARY} NAME)
    set(SNAPPY_SOURCE_FILES
        "${SRC}/src/codec/SnappyCodec.cc")
    set(REQUIRE_SNAPPY ${REQUIRE_SNAPPY}) # Stop warning about unused variable.
    message(STATUS "Found Snappy: ${SNAPPY_LIBRARY}")
else()
    set(SNAPPY_LIBRARY "")
    set(SNAPPY_INCLUDE_DIR "")
    set(SNAPPY_SOURCE_FILES "")
    if(REQUIRE_SNAPPY)
        message(FATAL_ERROR "Required snappy library could not be found.  SNAPPY_LIBRARY=${SNAPPY_LIBRARY}, SNAPPY_INCLUDE_DIR=${SNAPPY_INCLUDE_DIR}, CUSTOM_SNAPPY_INCLUDE_DIR=${CUSTOM_SNAPPY_INCLUDE_DIR}, CUSTOM_SNAPPY_PREFIX=${CUSTOM_SNAPPY_PREFIX}, CUSTOM_SNAPPY_INCLUDE=${CUSTOM_SNAPPY_INCLUDE}")
    endif()
endif()

configure_file(${CMAKE_SOURCE_DIR}/config.h.cmake ${CMAKE_BINARY_DIR}/config.h)

include_directories(
    ${GENERATED_JAVAH}
    ${SRC}/src
    ${SRC}/src/util
    ${SRC}/src/lib
    ${SRC}/test
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_BINARY_DIR}
    ${JNI_INCLUDE_DIRS}
    ${SNAPPY_INCLUDE_DIR}
    ${GTEST_SRC_DIR}/include
)
# add gtest as system library to suppress gcc warnings
include_directories(SYSTEM ${GTEST_SRC_DIR}/include)

set(CMAKE_MACOSX_RPATH TRUE)
set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
    # macosx does not have -lrt
    set(NT_DEPEND_LIBRARY dl pthread z ${SNAPPY_LIBRARY} ${JAVA_JVM_LIBRARY})
    set(SYSTEM_MAC TRUE)
else()
    set(NT_DEPEND_LIBRARY dl rt pthread z ${SNAPPY_LIBRARY} ${JAVA_JVM_LIBRARY})
    set(SYSTEM_MAC FALSE)
endif()

configure_file(main/native/test.sh test/test.sh)

hadoop_add_dual_library(nativetask
    ${CMAKE_BINARY_DIR}/lz4.c
    ${SRC}/src/codec/BlockCodec.cc
    ${SRC}/src/codec/GzipCodec.cc
    ${SRC}/src/codec/Lz4Codec.cc
    ${SNAPPY_SOURCE_FILES}
    ${SRC}/src/handler/BatchHandler.cc
    ${SRC}/src/handler/MCollectorOutputHandler.cc
    ${SRC}/src/handler/AbstractMapHandler.cc
    ${SRC}/src/handler/CombineHandler.cc
    ${SRC}/src/lib/Buffers.cc
    ${SRC}/src/lib/BufferStream.cc
    ${SRC}/src/lib/Compressions.cc
    ${SRC}/src/lib/PartitionBucket.cc
    ${SRC}/src/lib/PartitionBucketIterator.cc
    ${SRC}/src/lib/FileSystem.cc
    ${SRC}/src/lib/IFile.cc
    ${SRC}/src/lib/jniutils.cc
    ${SRC}/src/lib/Log.cc
    ${SRC}/src/lib/MapOutputCollector.cc
    ${SRC}/src/lib/MapOutputSpec.cc
    ${SRC}/src/lib/MemoryBlock.cc
    ${SRC}/src/lib/Merge.cc
    ${SRC}/src/lib/NativeLibrary.cc
    ${SRC}/src/lib/Iterator.cc
    ${SRC}/src/lib/NativeObjectFactory.cc
    ${SRC}/src/lib/NativeRuntimeJniImpl.cc
    ${SRC}/src/lib/NativeTask.cc
    ${SRC}/src/lib/SpillInfo.cc
    ${SRC}/src/lib/Path.cc
    ${SRC}/src/lib/Streams.cc
    ${SRC}/src/lib/TaskCounters.cc
    ${SRC}/src/util/Checksum.cc
    ${SRC}/src/util/Random.cc
    ${SRC}/src/util/StringUtil.cc
    ${SRC}/src/util/SyncUtils.cc
    ${SRC}/src/util/Timer.cc
    ${SRC}/src/util/WritableUtils.cc
)

target_link_libraries(nativetask ${NT_DEPEND_LIBRARY})

add_library(gtest ${GTEST_SRC_DIR}/gtest-all.cc)
set_target_properties(gtest PROPERTIES COMPILE_FLAGS "-w")
add_executable(nttest
    ${SRC}/test/lib/TestByteArray.cc
    ${SRC}/test/lib/TestByteBuffer.cc
    ${SRC}/test/lib/TestComparatorForDualPivotQuickSort.cc
    ${SRC}/test/lib/TestComparatorForStdSort.cc
    ${SRC}/test/lib/TestFixSizeContainer.cc
    ${SRC}/test/lib/TestMemoryPool.cc
    ${SRC}/test/lib/TestIterator.cc
    ${SRC}/test/lib/TestKVBuffer.cc
    ${SRC}/test/lib/TestMemBlockIterator.cc
    ${SRC}/test/lib/TestMemoryBlock.cc
    ${SRC}/test/lib/TestPartitionBucket.cc
    ${SRC}/test/lib/TestReadBuffer.cc
    ${SRC}/test/lib/TestReadWriteBuffer.cc
    ${SRC}/test/util/TestChecksum.cc
    ${SRC}/test/util/TestStringUtil.cc
    ${SRC}/test/util/TestWritableUtils.cc
    ${SRC}/test/TestCommand.cc
    ${SRC}/test/TestConfig.cc
    ${SRC}/test/TestCounter.cc
    ${SRC}/test/TestCompressions.cc
    ${SRC}/test/TestFileSystem.cc
    ${SRC}/test/TestIFile.cc
    ${SRC}/test/TestPrimitives.cc
    ${SRC}/test/TestSort.cc
    ${SRC}/test/TestMain.cc
    ${SRC}/test/test_commons.cc)

target_link_libraries(nttest
     nativetask_static
     gtest
     ${NT_DEPEND_LIBRARY}
)

# By embedding '$ORIGIN' into the RPATH of libnativetask.so, dlopen will look in
# the directory containing libnativetask.so. However, $ORIGIN is not supported by
# all operating systems.
if(CMAKE_SYSTEM_NAME MATCHES "Linux|SunOS")
    set_target_properties(nativetask PROPERTIES INSTALL_RPATH "\$ORIGIN/")
endif()

set(LIBNATIVETASK_VERSION "1.0.0")
set_target_properties(nativetask PROPERTIES SOVERSION ${LIBNATIVETASK_VERSION})
hadoop_dual_output_directory(nativetask target/usr/local/lib)
hadoop_output_directory(nttest test)
