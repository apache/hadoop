set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-missing-field-initializers -Wno-unused-const-variable")
add_library(gmock_main gmock-gtest-all.cc gmock_main.cc)
