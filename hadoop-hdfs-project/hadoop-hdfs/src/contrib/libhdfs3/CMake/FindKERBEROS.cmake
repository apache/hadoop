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

# - Find kerberos
# Find the native KERBEROS includes and library
#
#  KERBEROS_INCLUDE_DIRS - where to find krb5.h, etc.
#  KERBEROS_LIBRARIES    - List of libraries when using krb5.
#  KERBEROS_FOUND        - True if krb5 found.

IF (KERBEROS_INCLUDE_DIRS)
  # Already in cache, be silent
  SET(KERBEROS_FIND_QUIETLY TRUE)
ENDIF (KERBEROS_INCLUDE_DIRS)

FIND_PATH(KERBEROS_INCLUDE_DIR krb5.h)

SET(KERBEROS_NAMES krb5 k5crypto com_err)
FIND_LIBRARY(KERBEROS_LIBRARIES NAMES ${KERBEROS_NAMES})

# handle the QUIETLY and REQUIRED arguments and set KERBEROS_FOUND to TRUE if 
# all listed variables are TRUE
INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(KERBEROS DEFAULT_MSG KERBEROS_LIBRARIES KERBEROS_INCLUDE_DIR)

MARK_AS_ADVANCED(KERBEROS_LIBRARIES KERBEROS_INCLUDE_DIRS)
