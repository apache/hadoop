include_directories(${OPENSSL_INCLUDE_DIRS})
add_library(rpc rpc_connection.cc rpc_engine.cc)
add_dependencies(rpc proto)
