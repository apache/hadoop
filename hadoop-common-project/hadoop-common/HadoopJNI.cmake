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

#
# Common JNI detection for CMake, shared by all Native components.
#

# Check the JVM_ARCH_DATA_MODEL variable as been set to 32 or 64 by maven.
if(NOT DEFINED JVM_ARCH_DATA_MODEL)
    message(FATAL_ERROR "JVM_ARCH_DATA_MODEL is not defined")
elseif(NOT (JVM_ARCH_DATA_MODEL EQUAL 32 OR JVM_ARCH_DATA_MODEL EQUAL 64))
    message(FATAL_ERROR "JVM_ARCH_DATA_MODEL is not 32 or 64")
endif()

#
# Linux-specific JNI configuration.
#
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    # Locate JNI_INCLUDE_DIRS and JNI_LIBRARIES.
    # Since we were invoked from Maven, we know that the JAVA_HOME environment
    # variable is valid.  So we ignore system paths here and just use JAVA_HOME.
    file(TO_CMAKE_PATH "$ENV{JAVA_HOME}" _java_home)
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "^i.86$")
        set(_java_libarch "i386")
    elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" OR CMAKE_SYSTEM_PROCESSOR STREQUAL "amd64")
        set(_java_libarch "amd64")
    elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^arm")
        set(_java_libarch "arm")
    elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(powerpc|ppc)64le")
        if(EXISTS "${_java_home}/jre/lib/ppc64le")
            set(_java_libarch "ppc64le")
        else()
            set(_java_libarch "ppc64")
        endif()
    else()
        set(_java_libarch ${CMAKE_SYSTEM_PROCESSOR})
    endif()
    set(_JDK_DIRS "${_java_home}/jre/lib/${_java_libarch}/*"
                  "${_java_home}/jre/lib/${_java_libarch}"
                  "${_java_home}/jre/lib/*"
                  "${_java_home}/jre/lib"
                  "${_java_home}/lib/*"
                  "${_java_home}/lib"
                  "${_java_home}/include/*"
                  "${_java_home}/include"
                  "${_java_home}"
    )
    find_path(JAVA_INCLUDE_PATH
        NAMES jni.h
        PATHS ${_JDK_DIRS}
        NO_DEFAULT_PATH)
    #In IBM java, it's jniport.h instead of jni_md.h
    find_path(JAVA_INCLUDE_PATH2
        NAMES jni_md.h jniport.h
        PATHS ${_JDK_DIRS}
        NO_DEFAULT_PATH)
    set(JNI_INCLUDE_DIRS ${JAVA_INCLUDE_PATH} ${JAVA_INCLUDE_PATH2})
    find_library(JAVA_JVM_LIBRARY
        NAMES jvm JavaVM
        PATHS ${_JDK_DIRS}
        NO_DEFAULT_PATH)
    set(JNI_LIBRARIES ${JAVA_JVM_LIBRARY})
    unset(_java_libarch)
    unset(_java_home)

    message("JAVA_HOME=${JAVA_HOME}, JAVA_JVM_LIBRARY=${JAVA_JVM_LIBRARY}")
    message("JAVA_INCLUDE_PATH=${JAVA_INCLUDE_PATH}, JAVA_INCLUDE_PATH2=${JAVA_INCLUDE_PATH2}")
    if(JAVA_JVM_LIBRARY AND JAVA_INCLUDE_PATH AND JAVA_INCLUDE_PATH2)
        message("Located all JNI components successfully.")
    else()
        message(FATAL_ERROR "Failed to find a viable JVM installation under JAVA_HOME.")
    endif()

    # Use the standard FindJNI module to locate the JNI components.
    find_package(JNI REQUIRED)

#
# Otherwise, use the standard FindJNI module to locate the JNI components.
#
else()
    find_package(Java REQUIRED)
    include(UseJava)
    find_package(JNI REQUIRED)
endif()

# Determine float ABI of JVM on ARM Linux
if (CMAKE_SYSTEM_PROCESSOR MATCHES "^arm" AND CMAKE_SYSTEM_NAME STREQUAL "Linux")
    find_program(READELF readelf)
    if (READELF MATCHES "NOTFOUND")
        message(WARNING "readelf not found; JVM float ABI detection disabled")
    else (READELF MATCHES "NOTFOUND")
        message(STATUS "Checking float ABI of ${JAVA_JVM_LIBRARY}")
        execute_process(
            COMMAND ${READELF} -A ${JAVA_JVM_LIBRARY}
            OUTPUT_VARIABLE JVM_ELF_ARCH
            ERROR_QUIET)
        if (JVM_ELF_ARCH MATCHES "Tag_ABI_VFP_args: VFP registers")
           message(STATUS "Hard-float JVM detected")
        else ()
            message(STATUS "Soft-float JVM detected")

            # Test compilation with -mfloat-abi=softfp using an arbitrary libc function
            # (typically fails with "fatal error: bits/predefs.h: No such file or directory"
            # if soft-float dev libraries are not installed)
            include(CMakePushCheckState)
            cmake_push_check_state()
            set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} -mfloat-abi=softfp")
            include(CheckSymbolExists)
            check_symbol_exists(exit stdlib.h SOFTFP_AVAILABLE)
            if (NOT SOFTFP_AVAILABLE)
                message(FATAL_ERROR "Soft-float dev libraries required (e.g. 'apt-get install libc6-dev-armel' on Debian/Ubuntu)")
            endif (NOT SOFTFP_AVAILABLE)
            cmake_pop_check_state()

            set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -mfloat-abi=softfp")
        endif ()
    endif (READELF MATCHES "NOTFOUND")
endif (CMAKE_SYSTEM_PROCESSOR MATCHES "^arm" AND CMAKE_SYSTEM_NAME STREQUAL "Linux")

