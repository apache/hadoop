add_library(fs filesystem.cc inputstream.cc bad_datanode_tracker.cc)
add_dependencies(fs proto)
