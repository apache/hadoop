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

#ifndef COMMON_HDFS_NEW_DELETE_H_
#define COMMON_HDFS_NEW_DELETE_H_

#include <cstring>

struct mem_struct {
  size_t mem_size;
};

#ifndef NDEBUG
#define MEMCHECKED_CLASS(clazz) \
static void* operator new(size_t size) { \
  void* p = ::malloc(size); \
  return p; \
} \
static void* operator new[](size_t size) { \
  mem_struct* p = (mem_struct*)::malloc(sizeof(mem_struct) + size); \
  p->mem_size = size; \
  return (void*)++p; \
} \
static void operator delete(void* p) { \
  ::memset(p, 0, sizeof(clazz)); \
  ::free(p); \
} \
static void operator delete[](void* p) { \
  mem_struct* header = (mem_struct*)p; \
  size_t size = (--header)->mem_size; \
  ::memset(p, 0, size); \
  ::free(header); \
}
#else
#define MEMCHECKED_CLASS(clazz)
#endif
#endif
