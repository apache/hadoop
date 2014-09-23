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

FUNCTION(AUTO_SOURCES RETURN_VALUE PATTERN SOURCE_SUBDIRS)

	IF ("${SOURCE_SUBDIRS}" STREQUAL "RECURSE")
		SET(PATH ".")
		IF (${ARGC} EQUAL 4)
			LIST(GET ARGV 3 PATH)
		ENDIF ()
	ENDIF()

	IF ("${SOURCE_SUBDIRS}" STREQUAL "RECURSE")
		UNSET(${RETURN_VALUE})
		FILE(GLOB SUBDIR_FILES "${PATH}/${PATTERN}")
		LIST(APPEND ${RETURN_VALUE} ${SUBDIR_FILES})

		FILE(GLOB SUBDIRS RELATIVE ${PATH} ${PATH}/*)

		FOREACH(DIR ${SUBDIRS})
			IF (IS_DIRECTORY ${PATH}/${DIR})
				IF (NOT "${DIR}" STREQUAL "CMAKEFILES")
					FILE(GLOB_RECURSE SUBDIR_FILES "${PATH}/${DIR}/${PATTERN}")
					LIST(APPEND ${RETURN_VALUE} ${SUBDIR_FILES})
				ENDIF()
			ENDIF()
		ENDFOREACH()
	ELSE ()
		FILE(GLOB ${RETURN_VALUE} "${PATTERN}")

		FOREACH (PATH ${SOURCE_SUBDIRS})
			FILE(GLOB SUBDIR_FILES "${PATH}/${PATTERN}")
			LIST(APPEND ${RETURN_VALUE} ${SUBDIR_FILES})
		ENDFOREACH(PATH ${SOURCE_SUBDIRS})
	ENDIF ()

	IF (${FILTER_OUT})
		LIST(REMOVE_ITEM ${RETURN_VALUE} ${FILTER_OUT})
	ENDIF()

	SET(${RETURN_VALUE} ${${RETURN_VALUE}} PARENT_SCOPE)
ENDFUNCTION(AUTO_SOURCES)

FUNCTION(CONTAINS_STRING FILE SEARCH RETURN_VALUE)
	FILE(STRINGS ${FILE} FILE_CONTENTS REGEX ".*${SEARCH}.*")
	IF (FILE_CONTENTS)
		SET(${RETURN_VALUE} TRUE PARENT_SCOPE)
	ENDIF()
ENDFUNCTION(CONTAINS_STRING)
