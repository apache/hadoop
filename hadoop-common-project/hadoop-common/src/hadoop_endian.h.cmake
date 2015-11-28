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

/* Hadoop versions of http://austingroupbugs.net/view.php?id=162#c665 */

#ifndef HADOOP_ENDIAN_H
#define HADOOP_ENDIAN_H

#include <@HADOOP_ENDIAN_H@>

#define HADOOP_LITTLE_ENDIAN 1234
#define HADOOP_BIG_ENDIAN    4321
#cmakedefine HADOOP_BYTE_ORDER @HADOOP_BYTE_ORDER@

#define hadoop_htobe16(X) @HADOOP_HTOBE16@(X)
#define hadoop_htole16(X) @HADOOP_HTOLE16@(X)
#define hadoop_be16toh(X) @HADOOP_BE16TOH@(X)
#define hadoop_le16toh(X) @HADOOP_LE16TOH@(X)
#define hadoop_htobe32(X) @HADOOP_HTOBE32@(X)
#define hadoop_htole32(X) @HADOOP_HTOLE32@(X)
#define hadoop_be32toh(X) @HADOOP_BE32TOH@(X)
#define hadoop_le32toh(X) @HADOOP_LE32TOH@(X)
#define hadoop_htobe64(X) @HADOOP_HTOBE64@(X)
#define hadoop_htole64(X) @HADOOP_HTOLE64@(X)
#define hadoop_be64toh(X) @HADOOP_BE64TOH@(X)
#define hadoop_le64toh(X) @HADOOP_LE64TOH@(X)

#endif
