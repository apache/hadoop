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

OPTION(ENABLE_COVERAGE "enable code coverage" OFF)
OPTION(ENABLE_DEBUG "enable debug build" OFF)
OPTION(ENABLE_SSE "enable SSE4.2 buildin function" ON)
OPTION(ENABLE_FRAME_POINTER "enable frame pointer on 64bit system with flag -fno-omit-frame-pointer, on 32bit system, it is always enabled" ON)
OPTION(ENABLE_LIBCPP "using libc++ instead of libstdc++, only valid for clang compiler" OFF)
OPTION(ENABLE_BOOST "using boost instead of native compiler c++0x support" OFF)

INCLUDE (CheckFunctionExists)
CHECK_FUNCTION_EXISTS(dladdr HAVE_DLADDR)
CHECK_FUNCTION_EXISTS(nanosleep HAVE_NANOSLEEP)

IF(ENABLE_DEBUG STREQUAL ON)
    # TODO: set platform debug flag (/Z7) for Windows.
    SET(CMAKE_BUILD_TYPE Debug CACHE 
        STRING "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel." FORCE)
    SET(CMAKE_CXX_FLAGS_DEBUG "-g -O0" CACHE STRING "compiler flags for debug" FORCE)
    SET(CMAKE_C_FLAGS_DEBUG "-g -O0" CACHE STRING "compiler flags for debug" FORCE)
ELSE(ENABLE_DEBUG STREQUAL ON)
    SET(CMAKE_BUILD_TYPE RelWithDebInfo CACHE 
        STRING "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel." FORCE)
    SET(CMAKE_CXX_FLAGS_DEBUG "-g -O2" CACHE STRING "compiler flags for RelWithDebInfo" FORCE)
    SET(CMAKE_C_FLAGS_DEBUG "-g -O2" CACHE STRING "compiler flags for debug" FORCE)
ENDIF(ENABLE_DEBUG STREQUAL ON)

IF(NOT MSVC)
    # Ignore no-strict-aliasing in windows.
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-strict-aliasing")
    SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fno-strict-aliasing")
ENDIF(NOT MSVC)

IF(ENABLE_COVERAGE STREQUAL ON)
    INCLUDE(CodeCoverage)
ENDIF(ENABLE_COVERAGE STREQUAL ON)

IF(ENABLE_FRAME_POINTER STREQUAL ON)
    IF(MSVC)
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Oy-")
    ELSE(MSVC)
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
    ENDIF(MSVC)
ENDIF(ENABLE_FRAME_POINTER STREQUAL ON) 

IF(ENABLE_SSE STREQUAL ON)
    IF(MSVC)
        # In Visual Studio 2013, this option will use SS4.2 instructions
        # if available. Not sure about the behaviour in Visual Studio 2010.
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /arch:SSE2")
    ELSE(MSVC)
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2")
    ENDIF(MSVC)
ENDIF(ENABLE_SSE STREQUAL ON) 

IF(NOT TEST_HDFS_PREFIX)
SET(TEST_HDFS_PREFIX "./" CACHE STRING "default directory prefix used for test." FORCE)
ENDIF(NOT TEST_HDFS_PREFIX)

ADD_DEFINITIONS(-DTEST_HDFS_PREFIX="${TEST_HDFS_PREFIX}")
ADD_DEFINITIONS(-D__STDC_FORMAT_MACROS)
ADD_DEFINITIONS(-D_GNU_SOURCE)

IF(OS_MACOSX AND CMAKE_COMPILER_IS_GNUCXX)
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wl,-bind_at_load")
ENDIF(OS_MACOSX AND CMAKE_COMPILER_IS_GNUCXX)


IF(OS_LINUX)
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wl,--export-dynamic")
ENDIF(OS_LINUX)

SET(BOOST_ROOT ${CMAKE_PREFIX_PATH})
IF(ENABLE_BOOST STREQUAL ON)
    MESSAGE(STATUS "using boost instead of native compiler c++0x support.")
    IF(MSVC)
        # Find boost libraries with flavor: mt-sgd (multi-thread, static, and debug)
        SET(Boost_USE_STATIC_LIBS ON)
        SET(Boost_USE_MULTITHREADED ON)
        SET(Boost_USE_STATIC_RUNTIME ON)
        FIND_PACKAGE(Boost 1.53 COMPONENTS thread chrono system atomic iostreams REQUIRED)			
        INCLUDE_DIRECTORIES("${Boost_INCLUDE_DIRS}")
        LINK_DIRECTORIES("${Boost_LIBRARY_DIRS}")
    ELSE(MSVC)
        FIND_PACKAGE(Boost 1.53 REQUIRED)
    ENDIF(MSVC)
    SET(NEED_BOOST true CACHE INTERNAL "boost is required")
ELSE(ENABLE_BOOST STREQUAL ON)
    SET(NEED_BOOST false CACHE INTERNAL "boost is required")
ENDIF(ENABLE_BOOST STREQUAL ON)

IF(CMAKE_COMPILER_IS_GNUCXX)
    IF(ENABLE_LIBCPP STREQUAL ON)
        MESSAGE(FATAL_ERROR "Unsupport using GCC compiler with libc++")
    ENDIF(ENABLE_LIBCPP STREQUAL ON)
    
    IF((GCC_COMPILER_VERSION_MAJOR EQUAL 4) AND (GCC_COMPILER_VERSION_MINOR EQUAL 4) AND OS_MACOSX)
    	SET(NEED_GCCEH true CACHE INTERNAL "Explicitly link with gcc_eh")
    	MESSAGE(STATUS "link with -lgcc_eh for TLS")
    ENDIF((GCC_COMPILER_VERSION_MAJOR EQUAL 4) AND (GCC_COMPILER_VERSION_MINOR EQUAL 4) AND OS_MACOSX)
  
    IF((GCC_COMPILER_VERSION_MAJOR LESS 4) OR ((GCC_COMPILER_VERSION_MAJOR EQUAL 4) AND (GCC_COMPILER_VERSION_MINOR LESS 4)))
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")
        IF(NOT ENABLE_BOOST STREQUAL ON)
            MESSAGE(STATUS "gcc version is older than 4.6.0, boost is required.")
            FIND_PACKAGE(Boost 1.53 REQUIRED)
            SET(NEED_BOOST true CACHE INTERNAL "boost is required")
        ENDIF(NOT ENABLE_BOOST STREQUAL ON)
    ELSEIF((GCC_COMPILER_VERSION_MAJOR EQUAL 4) AND (GCC_COMPILER_VERSION_MINOR LESS 6))
        IF(NOT ENABLE_BOOST STREQUAL ON)
            MESSAGE(STATUS "gcc version is older than 4.6.0, boost is required.")
            FIND_PACKAGE(Boost 1.53 REQUIRED)
            SET(NEED_BOOST true CACHE INTERNAL "boost is required")
        ENDIF(NOT ENABLE_BOOST STREQUAL ON)
        MESSAGE(STATUS "adding c++0x support for gcc compiler")
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")
    ELSE((GCC_COMPILER_VERSION_MAJOR LESS 4) OR ((GCC_COMPILER_VERSION_MAJOR EQUAL 4) AND (GCC_COMPILER_VERSION_MINOR LESS 4)))
        MESSAGE(STATUS "adding c++0x support for gcc compiler")
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")
    ENDIF((GCC_COMPILER_VERSION_MAJOR LESS 4) OR ((GCC_COMPILER_VERSION_MAJOR EQUAL 4) AND (GCC_COMPILER_VERSION_MINOR LESS 4)))
    
    IF(NEED_BOOST)
        IF((Boost_MAJOR_VERSION LESS 1) OR ((Boost_MAJOR_VERSION EQUAL 1) AND (Boost_MINOR_VERSION LESS 53)))
            MESSAGE(FATAL_ERROR "boost 1.53+ is required")
        ENDIF()
    ELSE(NEED_BOOST)
        IF(HAVE_NANOSLEEP)
           SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_GLIBCXX_USE_NANOSLEEP") 
        ELSE(HAVE_NANOSLEEP)
            MESSAGE(FATAL_ERROR "nanosleep() is required")
        ENDIF(HAVE_NANOSLEEP)
    ENDIF(NEED_BOOST)
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")
ELSEIF(CMAKE_COMPILER_IS_CLANG)
    MESSAGE(STATUS "adding c++0x support for clang compiler")
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")
    SET(CMAKE_XCODE_ATTRIBUTE_CLANG_CXX_LANGUAGE_STANDARD "c++0x")
    IF(ENABLE_LIBCPP STREQUAL ON)
        MESSAGE(STATUS "using libc++ instead of libstdc++")
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
        SET(CMAKE_XCODE_ATTRIBUTE_CLANG_CXX_LIBRARY "libc++")
    ENDIF(ENABLE_LIBCPP STREQUAL ON)
ELSEIF(MSVC)
    SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /MTd")
ENDIF(CMAKE_COMPILER_IS_GNUCXX)

TRY_COMPILE(STRERROR_R_RETURN_INT
	${CMAKE_BINARY_DIR}
	${CMAKE_SOURCE_DIR}/CMake/CMakeTestCompileStrerror.cpp
	OUTPUT_VARIABLE OUTPUT)

MESSAGE(STATUS "Checking whether strerror_r returns an int")

IF(STRERROR_R_RETURN_INT)
	MESSAGE(STATUS "Checking whether strerror_r returns an int -- yes")
ELSE(STRERROR_R_RETURN_INT)
	MESSAGE(STATUS "Checking whether strerror_r returns an int -- no")
ENDIF(STRERROR_R_RETURN_INT)
