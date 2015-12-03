add_library(fs filesystem.cc filehandle.cc bad_datanode_tracker.cc)
add_dependencies(fs proto)
