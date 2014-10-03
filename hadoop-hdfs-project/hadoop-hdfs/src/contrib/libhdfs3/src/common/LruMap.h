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

#ifndef _HDFS_LIBHDFS3_COMMON_LRUMAP_H_
#define _HDFS_LIBHDFS3_COMMON_LRUMAP_H_

#include "Thread.h"
#include "UnorderedMap.h"

#include <list>

namespace hdfs {
namespace internal {

template<typename K, typename V>
class LruMap {
public:
    typedef K KeyType;
    typedef V ValueType;
    typedef std::pair<K, V> ItmeType;
    typedef std::list<ItmeType> ListType;
    typedef unordered_map<K, typename ListType::iterator> MapType;

public:
    LruMap() :
        count(0), size(1000) {
    }

    LruMap(size_t size) :
        count(0), size(size) {
    }

    ~LruMap() {
        lock_guard<mutex> lock(mut);
        map.clear();
        list.clear();
    }

    void resize(size_t s) {
        lock_guard<mutex> lock(mut);
        size = s;

        for (size_t i = count; i > s; --i) {
            map.erase(list.back().first);
            list.pop_back();
        }
    }

    void insert(const KeyType & key, const ValueType & value) {
        lock_guard<mutex> lock(mut);
        typename MapType::iterator it = map.find(key);

        if (it != map.end()) {
            --count;
            list.erase(it->second);
        }

        list.push_front(std::make_pair(key, value));
        map[key] = list.begin();
        ++count;

        if (count > size) {
            map.erase(list.back().first);
            list.pop_back();
        }
    }

    void erase(const KeyType & key) {
        lock_guard<mutex> lock(mut);
        typename MapType::iterator it = map.find(key);

        if (it != map.end()) {
            list.erase(it->second);
            map.erase(it);
            --count;
        }
    }

    bool find(const KeyType & key, ValueType & value) {
        lock_guard<mutex> lock(mut);
        typename MapType::iterator it = map.find(key);

        if (it != map.end()) {
            list.push_front(*(it->second));
            list.erase(it->second);
            value = list.front().second;
            map[key] = list.begin();
            return true;
        }

        return false;
    }

private:
    size_t count;
    size_t size;
    ListType list;
    MapType map;
    mutex mut;
};

}
}
#endif /* _HDFS_LIBHDFS3_COMMON_LRU_H_ */
