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

#ifndef LIBHDFS_PLATFORM_H
#define LIBHDFS_PLATFORM_H

#include <pthread.h>

/* Use gcc type-checked format arguments. */
#define TYPE_CHECKED_PRINTF_FORMAT(formatArg, varArgs) \
  __attribute__((format(printf, formatArg, varArgs)))

/*
 * Mutex and thread data types defined by pthreads.
 */
typedef pthread_mutex_t mutex;
typedef pthread_t threadId;

#endif
