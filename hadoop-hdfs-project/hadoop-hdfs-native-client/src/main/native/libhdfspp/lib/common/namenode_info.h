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

#ifndef COMMON_HDFS_NAMENODE_INFO_H_
#define COMMON_HDFS_NAMENODE_INFO_H_

#include <asio.hpp>

#include <hdfspp/options.h>

#include <string>
#include <vector>

namespace hdfs {

// Forward decl
class IoService;

// Internal representation of namenode info that keeps track
// of its endpoints.
struct ResolvedNamenodeInfo : public NamenodeInfo {
  ResolvedNamenodeInfo& operator=(const NamenodeInfo &info);
  std::string str() const;

  std::vector<::asio::ip::tcp::endpoint> endpoints;
};

// Clear endpoints if set and resolve all of them in parallel.
// Only successful lookups will be placed in the result set.
std::vector<ResolvedNamenodeInfo> BulkResolve(std::shared_ptr<IoService> ioservice, const std::vector<NamenodeInfo> &nodes);

// Clear endpoints, if any, and resolve them again
// Return true if endpoints were resolved
bool ResolveInPlace(std::shared_ptr<IoService> ioservice, ResolvedNamenodeInfo &info);

}

#endif
