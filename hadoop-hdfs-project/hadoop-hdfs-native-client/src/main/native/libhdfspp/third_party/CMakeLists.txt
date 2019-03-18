# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This file detects which of the dependencies we need are already installed
# on the system and downloads and builds the rest. The versions listed here
# are the version that we download. We'll use whichever version they have
# installed. The one exception is that we insist that they have the right
# form of libraries (static vs shared).

set(ASIO_VERSION "1.10.2")
set(CYRUS_SASL_VERSION "2.1.27")
set(GTEST_VERSION "1.8.1")
set(MIT_KERBEROS_VERSION "1.16.3")
set(OPENSSL_VERSION "1.0.2q")
# The newer versions of protobuf have cmake build files, which is much
# easier to deal with. If this is a HADOOP_BUILD, we'll use the protobuf
# 2.5 from the environment.
set(PROTOBUF_VERSION "3.5.1")
set(RAPIDXML_VERSION "1.13")
set(URIPARSER_VERSION "0.9.1")

add_custom_target(third_party_stage1)
add_custom_target(third_party_stage2 DEPENDS third_party_stage1)
add_custom_target(third_party DEPENDS third_party_stage2)

set(THIRDPARTY_LOG_OPTIONS LOG_CONFIGURE 1
                           LOG_BUILD 1
                           LOG_INSTALL 1
                           LOG_DOWNLOAD 1)
set(THIRDPARTY_CMAKE_OPTIONS
    -DCMAKE_INSTALL_PREFIX=${STAGING_DIR}
    -DCMAKE_PREFIX_PATH=${quoted_prefix_path})
set(THIRDPARTY_LIB_CMAKE_OPTIONS
    ${THIRDPARTY_CMAKE_OPTIONS}
    -DBUILD_SHARED_LIBS=${BUILD_SHARED_LIBS}
    -DCMAKE_INSTALL_LIBDIR=lib
    -DCMAKE_MACOSX_RPATH=${CMAKE_MACOSX_RPATH}
    -DCMAKE_INSTALL_RPATH=${CMAKE_INSTALL_RPATH}
    )

set(THIRDPARTY_CONFIGURE_COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}")
if (CMAKE_GENERATOR_TOOLSET)
  list(APPEND THIRDPARTY_CONFIGURE_COMMAND -T "${CMAKE_GENERATOR_TOOLSET}")
endif ()

# ----------------------------------------------------------------------
# ASIO

find_package(ASIO)
if (NOT ASIO_FOUND)
  ExternalProject_Add (asio
    URL "https://downloads.sourceforge.net/project/asio/asio/${ASIO_VERSION}%20%28Stable%29/asio-${ASIO_VERSION}.tar.gz"
    CMAKE_ARGS ${THIRDPARTY_CMAKE_OPTIONS}
    INSTALL_DIR ${STAGING_DIR}
    LIST_SEPARATOR |
    PATCH_COMMAND patch -u -N -p0 < ${CMAKE_CURRENT_SOURCE_DIR}/asio.patch
    )
  add_dependencies(third_party_stage1 asio)
endif (NOT ASIO_FOUND)

# ----------------------------------------------------------------------
# GoogleTest (gtest now includes gmock)

find_package(GTest ${GTEST_VERSION})

if (NOT GTEST_FOUND)
  set(GTEST_CMAKE_ARGS ${THIRDPARTY_LIB_CMAKE_OPTIONS})
  if(APPLE)
    set(GTEST_CMAKE_CXX_FLAGS
        " -DGTEST_USE_OWN_TR1_TUPLE=1 -Wno-unused-value -Wno-ignored-attributes")
  else()
    set(GTEST_CMAKE_CXX_FLAGS "")
  endif()

  list(APPEND GTEST_CMAKE_ARGS
              -Dgtest_force_shared_crt=ON
              -DCMAKE_CXX_FLAGS=${GTEST_CMAKE_CXX_FLAGS})
  ExternalProject_Add (gtest
    URL "https://github.com/google/googletest/archive/release-${GTEST_VERSION}.tar.gz"
    CMAKE_ARGS "${GTEST_CMAKE_ARGS}"
    ${THIRDPARTY_LOG_OPTIONS}
    INSTALL_DIR ${STAGING_DIR}
    LIST_SEPARATOR |
    PATCH_COMMAND patch -u -N -p0 < ${CMAKE_CURRENT_SOURCE_DIR}/gtest.patch
  )
  add_dependencies(third_party_stage1 gtest)
endif ()

# ----------------------------------------------------------------------
# OpenSSL

find_package(OpenSSL)
if (NOT OPENSSL_FOUND)
  set (OPENSSL_OPTIONS --prefix=${STAGING_DIR})
  if (BUILD_SHARED_LIBS)
    set(OPENSSL_OPTIONS ${OPENSSL_OPTIONS} --shared)
  endif ()
  string (REPLACE "." "_" OPENSSL_UNDER_VERSION ${OPENSSL_VERSION})
  if (APPLE)
    set(OPENSSL_CONFIG_COMMAND ./Configure ${OPENSSL_OPTIONS} darwin64-x86_64-cc )
  else ()
    set(OPENSSL_CONFIG_COMMAND ./config ${OPENSSL_OPTIONS})
  endif ()
  ExternalProject_Add (openssl
    URL "https://github.com/openssl/openssl/archive/OpenSSL_${OPENSSL_UNDER_VERSION}.tar.gz"
    CONFIGURE_COMMAND ${OPENSSL_CONFIG_COMMAND}
    INSTALL_DIR ${STAGING_DIR}
    BUILD_IN_SOURCE TRUE
    LIST_SEPARATOR |
    PATCH_COMMAND patch -u -N -p0 < ${CMAKE_CURRENT_SOURCE_DIR}/openssl.patch
    )
  add_dependencies(third_party_stage1 openssl)
endif (NOT OPENSSL_FOUND)

# ----------------------------------------------------------------------
# MIT Kerberos

find_package(MIT_Kerberos)
if (NOT MIT_KERBEROS_FOUND)
  # Get the major/minor versions of MIT Kerberos.
  string (REGEX MATCHALL "[.]?[0-9]+" MIT_VERSION_LIST
          ${MIT_KERBEROS_VERSION})
  list (GET MIT_VERSION_LIST 0 MIT_MAJOR_VERSION)
  list (GET MIT_VERSION_LIST 1 MIT_MINOR_VERSION)

  # Set the options that we need
  set (MIT_KERBEROS_OPTIONS --prefix=${STAGING_DIR})
  if (BUILD_SHARED_LIBS)
    list (APPEND MIT_KERBEROS_OPTIONS --enable-shared=yes)
  else ()
    list (APPEND MIT_KERBEROS_OPTIONS --enable-shared=no --enable-static=yes)
  endif ()
  set (MIT_KERBEROS_CONFIG_COMMAND src/configure ${MIT_KERBEROS_OPTIONS})
  ExternalProject_Add (mit_kerberos
    URL "http://web.mit.edu/kerberos/dist/krb5/${MIT_MAJOR_VERSION}${MIT_MINOR_VERSION}/krb5-${MIT_KERBEROS_VERSION}.tar.gz"
    CONFIGURE_COMMAND ${MIT_KERBEROS_CONFIG_COMMAND}
    INSTALL_DIR ${STAGING_DIR}
    BUILD_IN_SOURCE TRUE
    LIST_SEPARATOR |
    )
  add_dependencies(third_party_stage1 mit_kerberos)
endif (NOT MIT_KERBEROS_FOUND)

# ----------------------------------------------------------------------
# Rapid XML

find_package(RapidXML)
if (NOT RAPIDXML_FOUND)
  ExternalProject_Add (rapidxml
    URL "https://downloads.sourceforge.net/project/rapidxml/rapidxml/rapidxml%20${RAPIDXML_VERSION}/rapidxml-${RAPIDXML_VERSION}.zip"
    CMAKE_ARGS ${THIRDPARTY_CMAKE_OPTIONS}
    INSTALL_DIR ${STAGING_DIR}
    LIST_SEPARATOR |
    PATCH_COMMAND patch -u -N -p0 < ${CMAKE_CURRENT_SOURCE_DIR}/rapidxml.patch
    )
  add_dependencies(third_party_stage1 rapidxml)
endif (NOT RAPIDXML_FOUND)

# ----------------------------------------------------------------------
# URI Parser

find_package(URIparser)
if (NOT URIPARSER_FOUND)
  if (CMAKE_MACOSX_RPATH)
    set(FIX_URI_PARSER_LIB LDFLAGS=-Wl,-install_name,@rpath/liburiparser.dylib)
  else ()
    set(FIX_URI_PARSER_LIB)
  endif ()
  ExternalProject_Add (uriparser
    URL "https://github.com/uriparser/uriparser/archive/uriparser-${URIPARSER_VERSION}.tar.gz"
    CONFIGURE_COMMAND ./autogen.sh
      COMMAND ./configure --prefix=${STAGING_DIR} --disable-test --disable-doc
                          ${FIX_URI_PARSER_LIB}
    INSTALL_DIR ${STAGING_DIR}
    LIST_SEPARATOR |
    BUILD_IN_SOURCE TRUE
    )
  add_dependencies(third_party_stage1 uriparser)
endif (NOT URIPARSER_FOUND)

# ----------------------------------------------------------------------
# Protobuf

find_package(Protobuf)
if (NOT PROTOBUF_FOUND)
  set(PROTOBUF_CMAKE_OPTIONS ${THIRDPARTY_LIB_CMAKE_OPTIONS})
  list (APPEND PROTOBUF_CMAKE_OPTIONS
               -Dprotobuf_BUILD_TESTS=OFF)
  if (CMAKE_VERSION VERSION_GREATER "3.7")
    set(PROTOBUF_CONFIGURE
        SOURCE_SUBDIR "cmake" CMAKE_ARGS ${PROTOBUF_CMAKE_OPTIONS})
  else()
    set(PROTOBUF_CONFIGURE
        CONFIGURE_COMMAND "${THIRDPARTY_CONFIGURE_COMMAND}"
        ${PROTOBUF_CMAKE_OPTIONS}
        ${CMAKE_CURRENT_BINARY_DIR}/../projects/Source/protobuf_project/cmake)
  endif()
  ExternalProject_Add (protobuf
    URL "https://github.com/protocolbuffers/protobuf/archive/v${PROTOBUF_VERSION}.tar.gz"
    ${PROTOBUF_CONFIGURE}
    LIST_SEPARATOR |
    INSTALL_DIR ${STAGING_DIR}
    )
  add_dependencies(third_party_stage1 protobuf)
endif ()

# ----------------------------------------------------------------------
# SASL - Cyrus SASL or GSASL

if (HDFSPP_SASL_IMPL STREQUAL "CYRUS")
  find_package(CyrusSASL)
  if (NOT CYRUSSASL_FOUND)
    set(SASL_OPTIONS --enable-sample=no
                     --without-saslauthd
                     --with-gss_impl=mit
                     --with-dblib=none
                     --prefix=${STAGING_DIR})
    if (BUILD_SHARED_LIBS)
      list (APPEND SASL_OPTIONS --enable-shared=yes)
    else ()
      list (APPEND SASL_OPTIONS --enable-static=yes --enable-shared=no)
    endif (BUILD_SHARED_LIBS)
    
    if (OPENSSL_FOUND)
      list(APPEND SASL_OPTIONS --with-openssl=${OPENSSL_INCLUDE_DIR}/..)
    else ()
      list(APPEND SASL_OPTIONS --with-openssl=${STAGING_DIR})
    endif (OPENSSL_FOUND)

    if (MIT_KERBEROS_FOUND)
      list(APPEND SASL_OPTIONS
             CPPFLAGS=-I${MIT_KERBEROS_INCLUDE_DIR}
             LDFLAGS=-L${MIT_KERBEROS_INCLUDE_DIR}/..)
    else ()
      list(APPEND SASL_OPTIONS
             CPPFLAGS=-I${STAGING_DIR}/include
             LDFLAGS=-L${STAGING_DIR}/lib)
    endif (MIT_KERBEROS_FOUND)
    
    if (APPLE)
      list (APPEND SASL_OPTIONS --disable-macos-framework)
      if (CMAKE_MACOSX_RPATH)
        list (APPEND SASL_OPTIONS 
              LDFLAGS=-Wl,-install_name,@rpath/libsasl2.3.dylib)
      endif (CMAKE_MACOSX_RPATH)
    endif (APPLE)
    
    ExternalProject_Add (cyrus_sasl
      URL "https://github.com/cyrusimap/cyrus-sasl/archive/cyrus-sasl-${CYRUS_SASL_VERSION}.tar.gz"
      CONFIGURE_COMMAND ./autogen.sh ${SASL_OPTIONS}
      LIST_SEPARATOR |
      INSTALL_DIR ${STAGING_DIR}
      DEPENDS third_party_stage1
      PATCH_COMMAND patch -u -N -p0 < ${CMAKE_CURRENT_SOURCE_DIR}/cyrus_sasl.patch
      BUILD_IN_SOURCE TRUE
      )
    add_dependencies(third_party_stage2 cyrus_sasl)
  endif ()
elseif (HDFSPP_SASL_IMPL STREQUAL "GSASL")
  # GNU SASL is licenced under GPL, so we don't build it.
  find_package(GSasl REQUIRED)
endif ()
