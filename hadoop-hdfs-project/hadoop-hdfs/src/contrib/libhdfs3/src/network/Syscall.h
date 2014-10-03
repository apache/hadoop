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

#ifndef _HDFS_LIBHDFS3_NETWORK_SYSCALL_H_
#define _HDFS_LIBHDFS3_NETWORK_SYSCALL_H_

#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace real_syscalls {

using ::recv;
using ::send;
using ::getaddrinfo;
using ::freeaddrinfo;
using ::socket;
using ::connect;
using ::getpeername;
using ::fcntl;
using ::setsockopt;
using ::poll;
using ::shutdown;
using ::close;

}

#ifdef MOCK

#include "MockSystem.h"
namespace syscalls = mock_systems;

#else

namespace syscalls = real_syscalls;

#endif

#endif /* _HDFS_LIBHDFS3_NETWORK_SYSCALL_H_ */
