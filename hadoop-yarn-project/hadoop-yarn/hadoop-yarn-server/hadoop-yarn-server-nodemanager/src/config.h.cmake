/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
#ifndef CONFIG_H
#define CONFIG_H

/* custom configs */

#cmakedefine HADOOP_CONF_DIR "@HADOOP_CONF_DIR@"

#cmakedefine HADOOP_CONF_DIR_IS_ABS "@HADOOP_CONF_DIR_IS_ABS@"

/* specific functions */

#cmakedefine HAVE_CANONICALIZE_FILE_NAME @HAVE_CANONICALIZE_FILE_NAME@
#cmakedefine HAVE_FCHMODAT @HAVE_FCHMODAT@
#cmakedefine HAVE_FCLOSEALL @HAVE_FCLOSEALL@
#cmakedefine HAVE_FDOPENDIR @HAVE_FDOPENDIR@
#cmakedefine HAVE_FSTATAT @HAVE_FSTATAT@
#cmakedefine HAVE_OPENAT @HAVE_OPENAT@
#cmakedefine HAVE_SYSCTL @HAVE_SYSCTL@
#cmakedefine HAVE_UNLINKAT @HAVE_UNLINKAT@


/* specific headers */

#cmakedefine HAVE_SYS_SYSCTL_H @HAVE_SYS_SYSCTL_H@

#endif
