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

#cmakedefine HADOOP_ZLIB_LIBRARY "@HADOOP_ZLIB_LIBRARY@"
#cmakedefine HADOOP_BZIP2_LIBRARY "@HADOOP_BZIP2_LIBRARY@"
#cmakedefine HADOOP_SNAPPY_LIBRARY "@HADOOP_SNAPPY_LIBRARY@"
#cmakedefine HADOOP_ZSTD_LIBRARY "@HADOOP_ZSTD_LIBRARY@"
#cmakedefine HADOOP_OPENSSL_LIBRARY "@HADOOP_OPENSSL_LIBRARY@"
#cmakedefine HADOOP_ISAL_LIBRARY "@HADOOP_ISAL_LIBRARY@"
#cmakedefine HAVE_SYNC_FILE_RANGE
#cmakedefine HAVE_POSIX_FADVISE

#endif
