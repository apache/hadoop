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

#ifndef _HDFS_LIBHDFS3_COMMON_XMLCONFIGIMPL_H_
#define _HDFS_LIBHDFS3_COMMON_XMLCONFIGIMPL_H_

#include <stdint.h>
#include <string>
#include <sstream>
#include <map>

namespace hdfs {
namespace internal {

/**
 * A configure file parser.
 */
class ConfigImpl {
public:
    ConfigImpl() {
    }

    /**
     * Construct a empty Config instance.
     */
    ConfigImpl(const std::map<std::string, std::string> &kv);

    /**
     * Operator equal
     */
    bool operator==(const ConfigImpl &other) const {
        if (this == &other) {
            return true;
        }

        return this->kv == other.kv;
    }

    /**
     * Get a string with given configure key.
     * @param key The key of the configure item.
     * @return The value of configure item.
     * @throw HdfsConfigNotFound
     */
    const char *getString(const std::string &key) const;

    /**
     * Get a string with given configure key.
     * Return the default value def if key is not found.
     * @param key The key of the configure item.
     * @param def The defalut value.
     * @return The value of configure item.
     */
    const char *getString(const std::string &key, const std::string &def) const;

    /**
     * Get a 64 bit integer with given configure key.
     * @param key The key of the configure item.
     * @return The value of configure item.
     * @throw HdfsConfigNotFound
     */
    int64_t getInt64(const std::string &key) const;

    /**
     * Get a 64 bit integer with given configure key.
     * Return the default value def if key is not found.
     * @param key The key of the configure item.
     * @param def The defalut value.
     * @return The value of configure item.
     */
    int64_t getInt64(const std::string &key, int64_t def) const;

    /**
     * Get a 32 bit integer with given configure key.
     * @param key The key of the configure item.
     * @return The value of configure item.
     * @throw HdfsConfigNotFound
     */
    int32_t getInt32(const std::string &key) const;

    /**
     * Get a 32 bit integer with given configure key.
     * Return the default value def if key is not found.
     * @param key The key of the configure item.
     * @param def The defalut value.
     * @return The value of configure item.
     */
    int32_t getInt32(const std::string &key, int32_t def) const;

    /**
     * Get a double with given configure key.
     * @param key The key of the configure item.
     * @return The value of configure item.
     * @throw HdfsConfigNotFound
     */
    double getDouble(const std::string &key) const;

    /**
     * Get a double with given configure key.
     * Return the default value def if key is not found.
     * @param key The key of the configure item.
     * @param def The defalut value.
     * @return The value of configure item.
     */
    double getDouble(const std::string &key, double def) const;

    /**
     * Get a boolean with given configure key.
     * @param key The key of the configure item.
     * @return The value of configure item.
     * @throw HdfsConfigNotFound
     */
    bool getBool(const std::string &key) const;

    /**
     * Get a boolean with given configure key.
     * Return the default value def if key is not found.
     * @param key The key of the configure item.
     * @param def The default value.
     * @return The value of configure item.
     */
    bool getBool(const std::string &key, bool def) const;

    /**
     * Set a configure item
     * @param key The key will set.
     * @param value The value will be set to.
     */
    template <typename T>
    void set(const std::string &key, T const &value) {
        std::stringstream ss;
        ss << value;
        kv[key] = ss.str();
    }

    /**
     * Get the hash value of this object
     *
     * @return The hash value
     */
    size_t hash_value() const;

private:
    std::string path;
    std::map<std::string, std::string> kv;
};
}
}

#endif /* _HDFS_LIBHDFS3_COMMON_XMLCONFIGIMPL_H_ */
