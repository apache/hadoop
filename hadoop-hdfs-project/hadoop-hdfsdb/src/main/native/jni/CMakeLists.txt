find_package(JNI REQUIRED)

cmake_policy(SET CMP0042 NEW)

include_directories(${CMAKE_BINARY_DIR}/javah ${CMAKE_SOURCE_DIR}/hdfsdb/include)
include_directories(${JAVA_INCLUDE_PATH} ${JAVA_INCLUDE_PATH2})

add_library(hdfsdb-jni SHARED bindings.cc)
target_link_libraries(hdfsdb-jni hdfsdb)

set_target_properties(hdfsdb-jni PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/target/usr/local/lib)
set_target_properties(hdfsdb-jni PROPERTIES ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/target/usr/local/lib)
set_target_properties(hdfsdb-jni PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/target/usr/local/lib)
