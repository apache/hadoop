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

/**
 * RISC-V CRC32 hardware acceleration (placeholder)
 *
 * Phase 1: provide a RISC-V-specific compilation unit that currently makes
 * no runtime changes and falls back to the generic software path in
 * bulk_crc32.c. Future work will add Zbc-based acceleration and runtime
 * dispatch.
 */

#include <assert.h>
#include <stddef.h> // for size_t

#include "bulk_crc32.h"
#include "gcc_optimizations.h"

/* Constructor hook reserved for future HW capability detection and
 * function-pointer dispatch. Intentionally a no-op for the initial phase. */
void __attribute__((constructor)) init_riscv_crc_support(void)
{
  /* No-op: keep using the default software implementations. */
}
