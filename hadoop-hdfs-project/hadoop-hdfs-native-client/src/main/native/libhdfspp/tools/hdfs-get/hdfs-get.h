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

#ifndef LIBHDFSPP_TOOLS_HDFS_GET
#define LIBHDFSPP_TOOLS_HDFS_GET

#include "hdfs-copy-to-local/hdfs-copy-to-local.h"

namespace hdfs::tools {
class Get : public CopyToLocal {
public:
  /**
   * {@inheritdoc}
   */
  Get(int argc, char **argv);

  // Abiding to the Rule of 5
  Get(const Get &) = default;
  Get(Get &&) = default;
  Get &operator=(const Get &) = delete;
  Get &operator=(Get &&) = delete;
  ~Get() override = default;

protected:
  [[nodiscard]] std::string GetToolName() const override;
};
} // namespace hdfs::tools

#endif