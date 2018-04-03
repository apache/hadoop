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

#include <hdfspp/content_summary.h>
#include <sstream>
#include <iomanip>

namespace hdfs {

ContentSummary::ContentSummary()
: length(0),
  filecount(0),
  directorycount(0),
  quota(0),
  spaceconsumed(0),
  spacequota(0) {
}

std::string ContentSummary::str(bool include_quota) const {
  std::stringstream ss;
  if(include_quota){
    ss  << this->quota << " "
        << spacequota << " "
        << spaceconsumed << " ";
  }
  ss  << directorycount << " "
      << filecount << " "
      << length << " "
      << path;
  return ss.str();
}

std::string ContentSummary::str_du() const {
  std::stringstream ss;
  ss  << std::left << std::setw(10) << length
      << path;
  return ss.str();
}

}
