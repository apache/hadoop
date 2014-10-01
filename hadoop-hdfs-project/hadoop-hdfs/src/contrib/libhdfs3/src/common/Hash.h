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

#ifndef _HDFS_LIBHDFS3_COMMON_HASH_H_
#define _HDFS_LIBHDFS3_COMMON_HASH_H_

#include "platform.h"

#include <string>
#include <vector>

#ifdef NEED_BOOST

#include <boost/functional/hash.hpp>

namespace hdfs {
namespace internal {

/**
 * A hash function object used to hash a boolean value.
 */
extern boost::hash<bool> BoolHasher;

/**
 * A hash function object used to hash an int value.
 */
extern boost::hash<int> Int32Hasher;

/**
 * A hash function object used to hash an 64 bit int value.
 */
extern boost::hash<int64_t> Int64Hasher;

/**
 * A hash function object used to hash a size_t value.
 */
extern boost::hash<size_t> SizeHasher;

/**
 * A hash function object used to hash a std::string object.
 */
extern boost::hash<std::string> StringHasher;

}
}

#define HDFS_HASH_DEFINE(TYPE) \
    namespace boost{ \
    template<> \
    struct hash<TYPE> { \
        std::size_t operator()(const TYPE & key) const { \
            return key.hash_value(); \
        } \
    }; \
    }

#else

#include <functional>

namespace hdfs {
namespace internal {

/**
 * A hash function object used to hash a boolean value.
 */
extern std::hash<bool> BoolHasher;

/**
 * A hash function object used to hash an int value.
 */
extern std::hash<int> Int32Hasher;

/**
 * A hash function object used to hash an 64 bit int value.
 */
extern std::hash<int64_t> Int64Hasher;

/**
 * A hash function object used to hash a size_t value.
 */
extern std::hash<size_t> SizeHasher;

/**
 * A hash function object used to hash a std::string object.
 */
extern std::hash<std::string> StringHasher;

}
}

#define HDFS_HASH_DEFINE(TYPE) \
    namespace std{ \
    template<> \
    struct hash<TYPE> { \
        std::size_t operator()(const TYPE & key) const { \
            return key.hash_value(); \
        } \
    }; \
    }

#endif

namespace hdfs {
namespace internal {

/**
 * A hash function used to hash a vector of size_t values.
 * @param vec The vector's reference which items are to be hashed.
 * @param size The size of vec.
 * @return The hash value.
 * @throw nothrow
 */
static inline size_t CombineHasher(const size_t *vec, size_t size) {
    size_t value = 0;

    for (size_t i = 0; i < size; ++i) {
        value ^= SizeHasher(vec[i]) << 1;
    }

    return value;
}

}
}

#endif /* _HDFS_LIBHDFS3_COMMON_HASH_H_ */
