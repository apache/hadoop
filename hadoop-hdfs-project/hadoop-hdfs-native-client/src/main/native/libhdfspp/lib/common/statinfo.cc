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

#include <hdfspp/statinfo.h>
#include <sys/stat.h>
#include <sstream>
#include <iomanip>

namespace hdfs {

StatInfo::StatInfo()
  : file_type(0),
    length(0),
    permissions(0),
    modification_time(0),
    access_time(0),
    block_replication(0),
    blocksize(0),
    fileid(0),
    children_num(0) {
}

std::string StatInfo::str() const {
  char perms[11];
  perms[0] = file_type == StatInfo::IS_DIR ? 'd' : '-';
  perms[1] = permissions & S_IRUSR? 'r' : '-';
  perms[2] = permissions & S_IWUSR? 'w': '-';
  perms[3] = permissions & S_IXUSR? 'x': '-';
  perms[4] = permissions & S_IRGRP? 'r' : '-';
  perms[5] = permissions & S_IWGRP? 'w': '-';
  perms[6] = permissions & S_IXGRP? 'x': '-';
  perms[7] = permissions & S_IROTH? 'r' : '-';
  perms[8] = permissions & S_IWOTH? 'w': '-';
  perms[9] = permissions & S_IXOTH? 'x': '-';
  perms[10] = 0;

  //Convert to seconds from milliseconds
  const int time_field_length = 17;
  time_t rawtime = modification_time/1000;
  struct tm * timeinfo;
  char buffer[time_field_length];
  timeinfo = localtime(&rawtime);

  strftime(buffer,time_field_length,"%Y-%m-%d %H:%M",timeinfo);
  buffer[time_field_length-1] = 0;  //null terminator
  std::string time(buffer);

  std::stringstream ss;
  ss  << std::left << std::setw(12) << perms
      << std::left << std::setw(3) << (!block_replication ? "-" : std::to_string(block_replication))
      << std::left << std::setw(15) << owner
      << std::left << std::setw(15) << group
      << std::right << std::setw(5) << length
      << std::right << std::setw(time_field_length + 2) << time//modification_time
      << "  " << full_path;
  return ss.str();
}

}
