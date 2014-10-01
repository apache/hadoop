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

#ifndef _HDFS_LIBHDFS3_COMMON_FUNCTION_H_
#define _HDFS_LIBHDFS3_COMMON_FUNCTION_H_

#include "platform.h"

#ifdef NEED_BOOST
#include <boost/function.hpp>
#include <boost/bind.hpp>

namespace hdfs {

using boost::function;
using boost::bind;
using boost::reference_wrapper;

}

#else

#include <functional>

namespace hdfs {

using std::function;
using std::bind;
using std::reference_wrapper;
using namespace std::placeholders;

}

#endif

#endif /* _HDFS_LIBHDFS3_COMMON_FUNCTION_H_ */
