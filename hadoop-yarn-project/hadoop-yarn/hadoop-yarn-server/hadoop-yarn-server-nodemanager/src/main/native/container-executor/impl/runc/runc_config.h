/*
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
#ifndef RUNC_RUNC_CONFIG_H
#define RUNC_RUNC_CONFIG_H

// Section for all runC config keys
#define CONTAINER_EXECUTOR_CFG_RUNC_SECTION "runc"

// Configuration for top-level directory of runtime database
// Ideally this should be configured to a tmpfs or other RAM-based filesystem.
#define RUNC_RUN_ROOT_KEY    "runc.run-root"
#define DEFAULT_RUNC_ROOT    "/run/yarn-container-executor"

// Configuration for the path to the runC executable on the host
#define RUNC_BINARY_KEY      "runc.binary"
#define DEFAULT_RUNC_BINARY  "/usr/bin/runc"

#endif /* RUNC_RUNC_CONFIG_H */
