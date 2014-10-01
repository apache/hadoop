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

#ifndef _HDFS_LIBHDFS3_COMMON_DATETIME_H_
#define _HDFS_LIBHDFS3_COMMON_DATETIME_H_

#include "platform.h"

#include <ctime>
#include <cassert>

#ifdef NEED_BOOST

#include <boost/chrono.hpp>

namespace hdfs {
namespace internal {

using namespace boost::chrono;

}
}

#else

#include <chrono>

namespace hdfs {
namespace internal {

using namespace std::chrono;

}
}
#endif

namespace hdfs {
namespace internal {

template<typename TimeStamp>
static int64_t ToMilliSeconds(TimeStamp const & s, TimeStamp const & e) {
    assert(e >= s);
    return duration_cast<milliseconds>(e - s).count();
}

}
}

#endif /* _HDFS_LIBHDFS3_COMMON_DATETIME_H_ */
