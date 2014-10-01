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

#include "Hash.h"

#ifdef NEED_BOOST

#include <boost/functional/hash.hpp>

namespace hdfs {
namespace internal {

/**
 * A hash function object used to hash a boolean value.
 */
boost::hash<bool> BoolHasher;

/**
 * A hash function object used to hash an int value.
 */
boost::hash<int> Int32Hasher;

/**
 * A hash function object used to hash an 64 bit int value.
 */
boost::hash<int64_t> Int64Hasher;

/**
 * A hash function object used to hash a size_t value.
 */
boost::hash<size_t> SizeHasher;

/**
 * A hash function object used to hash a std::string object.
 */
boost::hash<std::string> StringHasher;
}
}

#else

#include <functional>

namespace hdfs {
namespace internal {

/**
 * A hash function object used to hash a boolean value.
 */
std::hash<bool> BoolHasher;

/**
 * A hash function object used to hash an int value.
 */
std::hash<int> Int32Hasher;

/**
 * A hash function object used to hash an 64 bit int value.
 */
std::hash<int64_t> Int64Hasher;

/**
 * A hash function object used to hash a size_t value.
 */
std::hash<size_t> SizeHasher;

/**
 * A hash function object used to hash a std::string object.
 */
std::hash<std::string> StringHasher;

}
}

#endif
