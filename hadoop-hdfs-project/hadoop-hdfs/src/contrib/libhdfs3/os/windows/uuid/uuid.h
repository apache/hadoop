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

#ifndef _UUID_HEADER_FOR_WIN_
#define _UUID_HEADER_FOR_WIN_

/*
 * This file a Windows equivalence of libuuid.
 */

#include <Rpc.h>
#include <RpcDce.h>
#pragma comment(lib, "rpcrt4.lib")

#undef uuid_t
typedef unsigned char uuid_t[16];

// It is OK to reinterpret cast, as UUID is a struct with 16 bytes.
// TODO: write our own uuid generator to get rid of libuuid dependency.
#define uuid_generate(id) UuidCreate(reinterpret_cast<UUID *>(id))

#endif
