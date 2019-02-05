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

# Input:
#   h_file: the name of a header file to find
#   lib_names: a list of library names to find
#   allow_any: Allow either static or shared libraries

# Environment:
#   CMAKE_FIND_PACKAGE_NAME: the name of the package to build
#   <name>_HOME: variable is used to check for headers and library
#   BUILD_SHARED_LIBRARIES: whether to find shared instead of static libraries

# Outputs:
#   <name>_INCLUDE_DIR: directory containing headers
#   <name>_LIBRARIES: libraries to link with
#   <name>_FOUND: whether uriparser has been found

function (findPackageExtension h_file lib_names allow_any)
  set (_name ${CMAKE_FIND_PACKAGE_NAME})
  string (TOUPPER ${_name} _upper_name)

  # protect against running it a second time
  if (NOT DEFINED ${_upper_name}_FOUND)

    # find the name of the home variable and get it from the environment
    set (_home_name "${_upper_name}_HOME")
    if (DEFINED ENV{${_home_name}})
      set(_home "$ENV{${_home_name}}")
    elseif (DEFINED ${_home_name})
      set(_home ${${_home_name}})
    endif ()

    # If <name>_HOME is set, use that alone as the path, otherwise use
    # PACKAGE_SEARCH_PATH ahead of the default_path.
    if(DEFINED _home AND NOT ${_home} STREQUAL "")
      set(_no_default TRUE)
    else()
      set(_no_default FALSE)
    endif()

    set (_include_dir "${h_file}-NOTFOUND")
    if (_no_default)
      find_path (_include_dir ${h_file}
                 PATHS ${_home} NO_DEFAULT_PATH
                 PATH_SUFFIXES "include")
    else ()
      find_path (_include_dir ${h_file}
                 PATH_SUFFIXES "include")
    endif (_no_default)

    set(_libraries)
    foreach (lib ${lib_names})
      expandLibName(${lib} ${allow_any} _full)
      set (_match "${_full}-NOTFOUND")
      if (_no_default)
        find_library (_match NAMES ${_full}
                      PATHS ${_home}
                      NO_DEFAULT_PATH
                      PATH_SUFFIXES "lib" "lib64")
      else ()
        find_library (_match NAMES ${_full}
                      HINTS ${_include_dir}/..
                      PATH_SUFFIXES "lib" "lib64")
      endif (_no_default)
      if (_match)
        list (APPEND _libraries ${_match})
      endif ()
      unset(_full)
    endforeach ()

    list (LENGTH _libraries _libraries_len)
    list (LENGTH lib_names _name_len)

    if (_include_dir AND _libraries_len EQUAL _name_len)
      message (STATUS "Found the ${_name} header: ${_include_dir}")
      if (NOT _libraries_len EQUAL 0)
        message (STATUS "Found the ${_name} libraries: ${_libraries}")
      endif ()
      set(${_upper_name}_FOUND TRUE PARENT_SCOPE)
      set(${_upper_name}_INCLUDE_DIR ${_include_dir} PARENT_SCOPE)
      set(${_upper_name}_LIBRARIES "${_libraries}" PARENT_SCOPE)

      # Create the libraries
      set(i 0)
      foreach (lib ${lib_names})
        list (GET _libraries ${i} _path)
        if (NOT TARGET ${lib})
          if (BUILD_SHARED_LIBRARIES)
            add_library(${lib} SHARED IMPORTED)
          else ()
            add_library(${lib} STATIC IMPORTED)
          endif (BUILD_SHARED_LIBRARIES)
          set_target_properties(${lib} PROPERTIES
                                INTERFACE_INCLUDE_DIRECTORIES ${_include_dir})
          set_property(TARGET ${lib} APPEND PROPERTY
                       IMPORTED_LOCATION ${_path})
        endif ()
        math(EXPR i ${i}+1)
      endforeach ()
    elseif (${_name}_FIND_REQUIRED)
      message (STATUS "ERROR: Did not find required library ${_name}")
      message (STATUS "Include dir: ${_include_dir}")
      message (STATUS "Libraries: ${lib_names} -> ${_libraries}")
      message (STATUS "Search path: ${CMAKE_PREFIX_PATH}")
      message (FATAL_ERROR "STOP: Did not find ${_name} for ${CMAKE_CURRENT_SOURCE_DIR}")
    else ()
      message (STATUS "Did not find ${_name}")
    endif ()
  else ()
    message (STATUS "Skipped checking ${_name}")
  endif ()
endfunction (findPackageExtension)

function (expandLibName name allow_any result)
  if (allow_any)
    set(${result} ${name} PARENT_SCOPE)
  elseif (BUILD_SHARED_LIBS)
    set(${result}
        "${CMAKE_SHARED_LIBRARY_PREFIX}${name}${CMAKE_SHARED_LIBRARY_SUFFIX}"
        PARENT_SCOPE)
  else ()
    set(${result}
        "${CMAKE_STATIC_LIBRARY_PREFIX}${name}${CMAKE_STATIC_LIBRARY_SUFFIX}"
        PARENT_SCOPE)
  endif (allow_any)
endfunction (expandLibName)
