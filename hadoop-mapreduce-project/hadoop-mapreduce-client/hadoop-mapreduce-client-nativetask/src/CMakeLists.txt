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

cmake_minimum_required(VERSION 2.6 FATAL_ERROR)

# Default to release builds
set(CMAKE_BUILD_TYPE, Release)

include(JNIFlags.cmake NO_POLICY_SCOPE)

# Compile a library with both shared and static variants
function(add_dual_library LIBNAME)
    add_library(${LIBNAME} SHARED ${ARGN})
    add_library(${LIBNAME}_static STATIC ${ARGN})
    set_target_properties(${LIBNAME}_static PROPERTIES OUTPUT_NAME ${LIBNAME})
endfunction(add_dual_library)

# Link both a static and a dynamic target against some libraries
function(target_link_dual_libraries LIBNAME)
    target_link_libraries(${LIBNAME} ${ARGN})
    target_link_libraries(${LIBNAME}_static ${ARGN})
endfunction(target_link_dual_libraries)

function(output_directory TGT DIR)
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
endfunction(output_directory TGT DIR)

function(dual_output_directory TGT DIR)
    output_directory(${TGT} "${DIR}")
    output_directory(${TGT}_static "${DIR}")
endfunction(dual_output_directory TGT DIR)

#
# This macro alters the behavior of find_package and find_library.
# It does this by setting the CMAKE_FIND_LIBRARY_SUFFIXES global variable.
# You should save that variable before calling this function and restore it
# after you have accomplished your goal.
#
# The behavior is altered in two ways:
# 1. We always find shared libraries, never static;
# 2. We find shared libraries with the given version number.
#
# On Windows this function is a no-op.  Windows does not encode
# version number information information into library path names.
#
macro(set_find_shared_library_version LVERS)
    IF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
        # Mac OS uses .dylib
        SET(CMAKE_FIND_LIBRARY_SUFFIXES ".${LVERS}.dylib")
    ELSEIF(${CMAKE_SYSTEM_NAME} MATCHES "FreeBSD")
        # FreeBSD has always .so installed.
        SET(CMAKE_FIND_LIBRARY_SUFFIXES ".so")
    ELSEIF(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
        # Windows doesn't support finding shared libraries by version.
    ELSE()
        # Most UNIX variants use .so
        SET(CMAKE_FIND_LIBRARY_SUFFIXES ".so.${LVERS}")
    ENDIF()
endmacro(set_find_shared_library_version LVERS)

if (NOT GENERATED_JAVAH)
    #Must identify where the generated headers have been placed
    MESSAGE(FATAL_ERROR "You must set the cmake variable GENERATED_JAVAH")
endif (NOT GENERATED_JAVAH)
find_package(JNI REQUIRED)

SET(STORED_CMAKE_FIND_LIBRARY_SUFFIXES CMAKE_FIND_LIBRARY_SUFFIXES)
set_find_shared_library_version("1")
SET(CMAKE_FIND_LIBRARY_SUFFIXES STORED_CMAKE_FIND_LIBRARY_SUFFIXES)

# primitive configs
set(PRFLAGS "-DSIMPLE_MEMCPY")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${PRFLAGS} -fno-strict-aliasing -Wall -Wno-sign-compare")
set(CMAKE_LD_FLAGS "${CMAKE_LD_FLAGS} -no-undefined -version-info 0:1:0
    -L${_JAVA_HOME}/jre/lib/amd64/server -ljvm")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_C_FLAGS} -g -O2 -DNDEBUG -fPIC")
set(D main/native/)

SET(STORED_CMAKE_FIND_LIBRARY_SUFFIXES CMAKE_FIND_LIBRARY_SUFFIXES)
set_find_shared_library_version("1")
SET(CMAKE_FIND_LIBRARY_SUFFIXES STORED_CMAKE_FIND_LIBRARY_SUFFIXES)

INCLUDE(CheckFunctionExists)
INCLUDE(CheckCSourceCompiles)
#INCLUDE(CheckLibraryExists)
INCLUDE(CheckIncludeFiles)
#CHECK_FUNCTION_EXISTS(sync_file_range HAVE_SYNC_FILE_RANGE)
#CHECK_FUNCTION_EXISTS(posix_fadvise HAVE_POSIX_FADVISE)
#CHECK_LIBRARY_EXISTS(dl dlopen "" NEED_LINK_DL)
CHECK_INCLUDE_FILES(fcntl.h HAVE_FCNTL_H)
CHECK_INCLUDE_FILES(malloc.h HAVE_MALLOC_H)
CHECK_INCLUDE_FILES(mach/mach.h HAVE_MACH_MACH_H)
CHECK_INCLUDE_FILES(memory.h HAVE_MEMORY_H)
CHECK_INCLUDE_FILES(stddef.h HAVE_STDDEF_H)
CHECK_INCLUDE_FILES(stdint.h HAVE_STDINT_H)
CHECK_INCLUDE_FILES(stdlib.h HAVE_STDLIB_H)
CHECK_INCLUDE_FILES(string.h HAVE_STRING_H)
CHECK_INCLUDE_FILES(unistd.h HAVE_UNITSTD_H)
CHECK_FUNCTION_EXISTS(clock_gettime HAVE_CLOCK_GETTIME)
CHECK_FUNCTION_EXISTS(localtime_r HAVE_LOCALTIME_R)
CHECK_FUNCTION_EXISTS(memset HAVE_MEMSET)
CHECK_FUNCTION_EXISTS(strchr HAVE_STRCHR)
CHECK_FUNCTION_EXISTS(strtoul HAVE_STRTOUL)

SET(STORED_CMAKE_FIND_LIBRARY_SUFFIXES CMAKE_FIND_LIBRARY_SUFFIXES)
set_find_shared_library_version("1")
find_library(SNAPPY_LIBRARY
    NAMES snappy
    PATHS ${CUSTOM_SNAPPY_PREFIX} ${CUSTOM_SNAPPY_PREFIX}/lib
          ${CUSTOM_SNAPPY_PREFIX}/lib64 ${CUSTOM_SNAPPY_LIB})
SET(CMAKE_FIND_LIBRARY_SUFFIXES STORED_CMAKE_FIND_LIBRARY_SUFFIXES)
find_path(SNAPPY_INCLUDE_DIR
    NAMES snappy.h
    PATHS ${CUSTOM_SNAPPY_PREFIX} ${CUSTOM_SNAPPY_PREFIX}/include
          ${CUSTOM_SNAPPY_INCLUDE})
if (SNAPPY_LIBRARY AND SNAPPY_INCLUDE_DIR)
    GET_FILENAME_COMPONENT(HADOOP_SNAPPY_LIBRARY ${SNAPPY_LIBRARY} NAME)
    set(SNAPPY_SOURCE_FILES
        "${D}/src/codec/SnappyCodec.cc")
else (SNAPPY_LIBRARY AND SNAPPY_INCLUDE_DIR)
    set(SNAPPY_LIBRARY "")
    set(SNAPPY_INCLUDE_DIR "")
    set(SNAPPY_SOURCE_FILES "")
    IF(REQUIRE_SNAPPY)
        MESSAGE(FATAL_ERROR "Required snappy library could not be found.  SNAPPY_LIBRARY=${SNAPPY_LIBRARY}, SNAPPY_INCLUDE_DIR=${SNAPPY_INCLUDE_DIR}, CUSTOM_SNAPPY_INCLUDE_DIR=${CUSTOM_SNAPPY_INCLUDE_DIR}, CUSTOM_SNAPPY_PREFIX=${CUSTOM_SNAPPY_PREFIX}, CUSTOM_SNAPPY_INCLUDE=${CUSTOM_SNAPPY_INCLUDE}")
    ENDIF(REQUIRE_SNAPPY)
endif (SNAPPY_LIBRARY AND SNAPPY_INCLUDE_DIR)

CONFIGURE_FILE(${CMAKE_SOURCE_DIR}/config.h.cmake ${CMAKE_BINARY_DIR}/config.h)

include_directories(
    ${GENERATED_JAVAH}
    ${D}/src
    ${D}/src/util
    ${D}/src/lib
    ${D}/test
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_BINARY_DIR}
    ${JNI_INCLUDE_DIRS}
    ${SNAPPY_INCLUDE_DIR}
)
# add gtest as system library to suppress gcc warnings
include_directories(SYSTEM ${D}/gtest/include)


SET(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)

if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    # macosx does not have -lrt
    set(NT_DEPEND_LIBRARY dl pthread z ${SNAPPY_LIBRARY} ${JAVA_JVM_LIBRARY})
    set(SYSTEM_MAC TRUE)
else (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    set(NT_DEPEND_LIBRARY dl rt pthread z ${SNAPPY_LIBRARY} ${JAVA_JVM_LIBRARY})
    set(SYSTEM_MAC FALSE)
endif (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")

configure_file(main/native/test.sh test/test.sh)

add_dual_library(nativetask
    ${CMAKE_BINARY_DIR}/lz4.c
    ${D}/src/codec/BlockCodec.cc
    ${D}/src/codec/GzipCodec.cc
    ${D}/src/codec/Lz4Codec.cc
    ${SNAPPY_SOURCE_FILES}
    ${D}/src/handler/BatchHandler.cc
    ${D}/src/handler/MCollectorOutputHandler.cc
    ${D}/src/handler/AbstractMapHandler.cc
    ${D}/src/handler/CombineHandler.cc
    ${D}/src/lib/Buffers.cc
    ${D}/src/lib/BufferStream.cc
    ${D}/src/lib/Compressions.cc
    ${D}/src/lib/PartitionBucket.cc
    ${D}/src/lib/PartitionBucketIterator.cc
    ${D}/src/lib/FileSystem.cc
    ${D}/src/lib/IFile.cc
    ${D}/src/lib/jniutils.cc
    ${D}/src/lib/Log.cc
    ${D}/src/lib/MapOutputCollector.cc
    ${D}/src/lib/MapOutputSpec.cc
    ${D}/src/lib/MemoryBlock.cc
    ${D}/src/lib/Merge.cc
    ${D}/src/lib/NativeLibrary.cc
    ${D}/src/lib/Iterator.cc
    ${D}/src/lib/NativeObjectFactory.cc
    ${D}/src/lib/NativeRuntimeJniImpl.cc
    ${D}/src/lib/NativeTask.cc
    ${D}/src/lib/SpillInfo.cc
    ${D}/src/lib/Path.cc
    ${D}/src/lib/Streams.cc
    ${D}/src/lib/TaskCounters.cc
    ${D}/src/util/Checksum.cc
    ${D}/src/util/Random.cc
    ${D}/src/util/StringUtil.cc
    ${D}/src/util/SyncUtils.cc
    ${D}/src/util/Timer.cc
    ${D}/src/util/WritableUtils.cc
)

target_link_libraries(nativetask ${NT_DEPEND_LIBRARY})

add_library(gtest ${D}/gtest/gtest-all.cc)
set_target_properties(gtest PROPERTIES COMPILE_FLAGS "-w")
add_executable(nttest
    ${D}/test/lib/TestByteArray.cc
    ${D}/test/lib/TestByteBuffer.cc
    ${D}/test/lib/TestComparatorForDualPivotQuickSort.cc
    ${D}/test/lib/TestComparatorForStdSort.cc
    ${D}/test/lib/TestFixSizeContainer.cc
    ${D}/test/lib/TestMemoryPool.cc
    ${D}/test/lib/TestIterator.cc
    ${D}/test/lib/TestKVBuffer.cc
    ${D}/test/lib/TestMemBlockIterator.cc
    ${D}/test/lib/TestMemoryBlock.cc
    ${D}/test/lib/TestPartitionBucket.cc
    ${D}/test/lib/TestReadBuffer.cc
    ${D}/test/lib/TestReadWriteBuffer.cc
    ${D}/test/util/TestChecksum.cc
    ${D}/test/util/TestStringUtil.cc
    ${D}/test/util/TestWritableUtils.cc
    ${D}/test/TestCommand.cc
    ${D}/test/TestConfig.cc
    ${D}/test/TestCounter.cc
    ${D}/test/TestCompressions.cc
    ${D}/test/TestFileSystem.cc
    ${D}/test/TestIFile.cc
    ${D}/test/TestPrimitives.cc
    ${D}/test/TestSort.cc
    ${D}/test/TestMain.cc
    ${D}/test/test_commons.cc)


IF (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    # macos clang with libc++ does not have tr1/tuple, just tuple
    SET_TARGET_PROPERTIES(nttest PROPERTIES COMPILE_FLAGS "-DGTEST_USE_OWN_TR1_TUPLE=1")
ENDIF()

target_link_libraries(nttest
     nativetask_static
     gtest
     ${NT_DEPEND_LIBRARY}
)

IF (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    #
    # By embedding '$ORIGIN' into the RPATH of libnativetask.so,
    # dlopen will look in the directory containing libnativetask.so.
    # However, $ORIGIN is not supported by all operating systems.
    #
    SET_TARGET_PROPERTIES(nativetask
        PROPERTIES INSTALL_RPATH "\$ORIGIN/")
ENDIF()

SET(LIBNATIVETASK_VERSION "1.0.0")
SET_TARGET_PROPERTIES(nativetask PROPERTIES SOVERSION ${LIBNATIVETASK_VERSION})
dual_output_directory(nativetask target/usr/local/lib)
output_directory(nttest test)
