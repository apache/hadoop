MESSAGE(STATUS "Processing hadoop-core protobuf definitions.")

add_executable(shorten rpc/shorten.c)

include_directories(${PROTOC_HEADER_PATH})
add_executable(protoc-gen-hrpc rpc/protoc-gen-hrpc.cc)
target_link_libraries(protoc-gen-hrpc ${PROTOC_LIB} ${PROTOBUF_LIB})

function(DECLARE_PROTOS OUTPUT_SRC_LIST GENERATOR_DIR INCLUDE_DIRS)
    if (NOT ARGN)
        message(SEND_ERROR "Error: DECLARE_PROTOS requires protobuf files as arguments.")
    endif()
    set(CFILES)
    set(HFILES)
    get_filename_component(GENERATOR_DIR "${GENERATOR_DIR}" REALPATH)
    set(INCLUDE_FLAGS)
    foreach(IDIR ${INCLUDE_DIRS})
        set(INCLUDE_FLAGS ${INCLUDE_FLAGS} -I ${IDIR})
    endforeach()
    foreach(PB_FILE ${ARGN})
        get_filename_component(DIRNAME_F ${PB_FILE} PATH)
        get_filename_component(ABSNAME_F ${PB_FILE} ABSOLUTE)
        get_filename_component(BASENAME_F ${PB_FILE} NAME_WE)
        set(PB_C_FILE "${GENERATOR_DIR}/${BASENAME_F}.pb-c.c")
        set(PB_H_FILE "${GENERATOR_DIR}/${BASENAME_F}.pb-c.h")
        set(CALL_C_FILE "${GENERATOR_DIR}/${BASENAME_F}.call.c")
        set(CALL_H_FILE "${GENERATOR_DIR}/${BASENAME_F}.call.h")
        execute_process(COMMAND ${CMAKE_COMMAND} -E make_directory ${GENERATOR_DIR})
        add_custom_command(
            OUTPUT ${PB_C_FILE} ${PB_H_FILE} ${CALL_C_FILE} ${CALL_H_FILE}
            COMMAND  ${PROTOBUFC_EXE}
                ARGS --c_out  ${GENERATOR_DIR} ${INCLUDE_FLAGS} --proto_path ${DIRNAME_F} ${ABSNAME_F}
                COMMENT "Running protoc-c on ${PB_FILE}"
                DEPENDS ${ABSNAME_F}
                VERBATIM 
            COMMAND  "${CMAKE_CURRENT_BINARY_DIR}/shorten"
                ARGS ${PB_H_FILE}
                COMMENT "Processing ${PB_H_FILE}"
                DEPENDS ${ABSNAME_F} shorten
                VERBATIM 
            COMMAND "${PROTOC_EXE}"
                ARGS --plugin=protoc-gen-hrpc --hrpc_out ${GENERATOR_DIR} ${INCLUDE_FLAGS} ${ABSNAME_F}
                COMMENT "Running HRPC protocol buffer compiler on ${ABSNAME_F}"
                DEPENDS ${ABSNAME_F} protoc-gen-hrpc
                VERBATIM
            )
        list(APPEND CFILES ${PB_C_FILE} ${CALL_C_FILE})
        list(APPEND HFILES ${PB_H_FILE} ${PB_H_FILE}.s ${CALL_H_FILE})
        set_source_files_properties(${PB_C_FILE} ${PB_H_FILE} "${PB_H_FILE}.s" PROPERTIES GENERATED TRUE)
    endforeach()
    #MESSAGE(STATUS "OUTPUT_SRC_LIST = ${OUTPUT_SRC_LIST}, CFILES = ${CFILES}, HFILES = ${HFILES}")
    set(${OUTPUT_SRC_LIST} ${CFILES} ${HFILES} PARENT_SCOPE)
endfunction()

get_filename_component(R "${CMAKE_CURRENT_LIST_DIR}/.." REALPATH)

# Common protobuf files.  In general, the other subprojects such as HDFS and
# YARN may rely on definitions in these protobuf files.
DECLARE_PROTOS(
    COMMON_PROTOBUF_SRCS
    ${CMAKE_CURRENT_BINARY_DIR}/protobuf
    "${R}/hadoop-common-project/hadoop-common/src/main/proto/"
    ${R}/hadoop-common-project/hadoop-common/src/main/proto/GetUserMappingsProtocol.proto
    ${R}/hadoop-common-project/hadoop-common/src/main/proto/HAServiceProtocol.proto
    ${R}/hadoop-common-project/hadoop-common/src/main/proto/IpcConnectionContext.proto
    ${R}/hadoop-common-project/hadoop-common/src/main/proto/ProtobufRpcEngine.proto
    ${R}/hadoop-common-project/hadoop-common/src/main/proto/ProtocolInfo.proto
    ${R}/hadoop-common-project/hadoop-common/src/main/proto/RefreshAuthorizationPolicyProtocol.proto
    ${R}/hadoop-common-project/hadoop-common/src/main/proto/RefreshCallQueueProtocol.proto
    ${R}/hadoop-common-project/hadoop-common/src/main/proto/RefreshUserMappingsProtocol.proto
    ${R}/hadoop-common-project/hadoop-common/src/main/proto/RpcHeader.proto
    ${R}/hadoop-common-project/hadoop-common/src/main/proto/Security.proto
    ${R}/hadoop-common-project/hadoop-common/src/main/proto/ZKFCProtocol.proto
)

# HDFS protobuf files.
DECLARE_PROTOS(
    HDFS_PROTOBUF_SRCS
    ${CMAKE_CURRENT_BINARY_DIR}/protobuf
    "${R}/hadoop-common-project/hadoop-common/src/main/proto/;${R}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/"
    #${R}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/DatanodeProtocol.proto
    ${R}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/HAZKInfo.proto
    ${R}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/ClientDatanodeProtocol.proto
    ${R}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/acl.proto
    ${R}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/fsimage.proto
    ${R}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/hdfs.proto
    ${R}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/datatransfer.proto
    ${R}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/InterDatanodeProtocol.proto
    #${R}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/QJournalProtocol.proto
    ${R}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/JournalProtocol.proto
    ${R}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/NamenodeProtocol.proto
    ${R}/hadoop-hdfs-project/hadoop-hdfs/src/main/proto/ClientNamenodeProtocol.proto
)
