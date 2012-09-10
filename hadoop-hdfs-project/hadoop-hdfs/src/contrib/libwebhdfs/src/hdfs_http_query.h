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


#ifndef _HDFS_HTTP_QUERY_H_
#define _HDFS_HTTP_QUERY_H_

#include <stdint.h>
#include <stdio.h>

char *prepareMKDIR(const char *host, int nnPort, const char *dirsubpath, const char *user);
char *prepareMKDIRwithMode(const char *host, int nnPort, const char *dirsubpath, int mode, const char *user);
char *prepareRENAME(const char *host, int nnPort, const char *srcpath, const char *destpath, const char *user);
char *prepareCHMOD(const char *host, int nnPort, const char *dirsubpath, int mode, const char *user);
char *prepareGFS(const char *host, int nnPort, const char *dirsubpath, const char *user);
char *prepareLS(const char *host, int nnPort, const char *dirsubpath, const char *user);
char *prepareDELETE(const char *host, int nnPort, const char *dirsubpath, int recursive, const char *user);
char *prepareCHOWN(const char *host, int nnPort, const char *dirsubpath, const char *owner, const char *group, const char *user);
char *prepareOPEN(const char *host, int nnPort, const char *dirsubpath, const char *user, size_t offset, size_t length);
char *prepareUTIMES(const char *host, int nnPort, const char *dirsubpath, long unsigned mTime, long unsigned aTime, const char *user);
char *prepareNnWRITE(const char *host, int nnPort, const char *dirsubpath, const char *user, int16_t replication, size_t blockSize);
char *prepareNnAPPEND(const char *host, int nnPort, const char *dirsubpath, const char *user);
char *prepareSETREPLICATION(const char *host, int nnPort, const char *path, int16_t replication, const char *user);


#endif  //_HDFS_HTTP_QUERY_H_
