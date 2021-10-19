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
#ifndef RUNC_RUNC_REAP_H
#define RUNC_RUNC_REAP_H

#include "runc_base_ctx.h"

/**
 * Attempt to trim the number of layer mounts to the specified target number to
 * preserve. Layers are unmounted in a least-recently-used fashion. Layers that
 * are still in use by containers are preserved, so the number of layers mounts
 * after trimming may exceed the target number.
 *
 * Returns 0 on success or a non-zero error code on failure.
 */
int reap_runc_layer_mounts(int num_preserve);

/**
 * Equivalent to reap_runc_layer_mounts but avoids the need to re-create the
 * runC base context.
 */
int reap_runc_layer_mounts_with_ctx(runc_base_ctx* ctx, int num_preserve);

#endif /* RUNC_RUNC_REAP_H */
