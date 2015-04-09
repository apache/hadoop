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

#ifndef _HDFS_LIBHDFS3_COMMON_UNORDERED_MAP_H_
#define _HDFS_LIBHDFS3_COMMON_UNORDERED_MAP_H_

// Use boost for Windows, to avoid xutility type cast complain.
#if (defined NEED_BOOST && defined _WIN32)

#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

namespace hdfs {
namespace internal {

using boost::unordered_map;
using boost::unordered_set;

}
}

#elif (defined _LIBCPP_VERSION || defined _WIN32)

#include <unordered_map>
#include <unordered_set>

namespace hdfs {
namespace internal {

using std::unordered_set;
using std::unordered_map;
}
}

#else

#include <tr1/unordered_map>
#include <tr1/unordered_set>

namespace hdfs {
namespace internal {

using std::tr1::unordered_set;
using std::tr1::unordered_map;
}
}

#endif
#endif
