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

#ifndef __YARN_POSIX_CONTAINER_EXECUTOR_GET_EXECUTABLE_H__
#define __YARN_POSIX_CONTAINER_EXECUTOR_GET_EXECUTABLE_H__

/**
 * Get the path to executable that is currently running
 * @param argv0 the name of the executable
 * @return the path to the currently running executable
 */
char* get_executable(char *argv0);

#endif
