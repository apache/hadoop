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

#ifndef _HDFS_LIBHDFS3_COMMON_CONFIG_H_
#define _HDFS_LIBHDFS3_COMMON_CONFIG_H_

#include "Status.h"

#include <stdint.h>
#include <string>
#include <map>

namespace hdfs {

class FileSystem;
class NamenodeInfo;

namespace internal {
class ConfigImpl;
class FileSystemImpl;
}

/**
 * A configure file parser.
 */
class Config {
public:
    /**
     * Create an instance from a XML file
     * @param path the path of the configure file.
     */
    static Config CreateFromXmlFile(const std::string &path);

    /**
     * Construct a empty Config instance.
     */
    Config();

    /**
     * Copy constructor
     */
    Config(const Config &other);

    /**
     * Assignment operator.
     */
    Config &operator=(const Config &other);

    /**
     * Operator equal
     */
    bool operator==(const Config &other) const;

    /**
     * Destroy this instance
     */
    ~Config();

    /**
     * Get a string with given configure key.
     * @param key The key of the configure item.
     * @param output the pointer of the output parameter.
     * @return the result status of this operation
     */
    Status getString(const std::string &key, std::string *output) const;

    /**
     * Get a string with given configure key.
     * Return the default value def if key is not found.
     * @param key The key of the configure item.
     * @param def The defalut value.
     * @param output the pointer of the output parameter.
     * @return the result status of this operation
     */
    Status getString(const std::string &key, const std::string &def,
                     std::string *output) const;

    /**
     * Get a 64 bit integer with given configure key.
     * @param key The key of the configure item.
     * @param output the pointer of the output parameter.
     * @return the result status of this operation
     */
    Status getInt64(const std::string &key, std::string *output) const;

    /**
     * Get a 64 bit integer with given configure key.
     * Return the default value def if key is not found.
     * @param key The key of the configure item.
     * @param def The defalut value.
     * @param output the pointer of the output parameter.
     * @return the result status of this operation
     */
    Status getInt64(const std::string &key, int64_t def,
                    std::string *output) const;

    /**
     * Get a 32 bit integer with given configure key.
     * @param key The key of the configure item.
     * @param output the pointer of the output parameter.
     * @return the result status of this operation
     */
    Status getInt32(const std::string &key, std::string *output) const;

    /**
     * Get a 32 bit integer with given configure key.
     * Return the default value def if key is not found.
     * @param key The key of the configure item.
     * @param def The defalut value.
     * @param output the pointer of the output parameter.
     * @return the result status of this operation
     */
    Status getInt32(const std::string &key, int32_t def,
                    std::string *output) const;

    /**
     * Get a double with given configure key.
     * @param key The key of the configure item.
     * @param output the pointer of the output parameter.
     * @return the result status of this operation
     */
    Status getDouble(const std::string &key, std::string *output) const;

    /**
     * Get a double with given configure key.
     * Return the default value def if key is not found.
     * @param key The key of the configure item.
     * @param def The defalut value.
     * @param output the pointer of the output parameter.
     * @return the result status of this operation
     */
    Status getDouble(const std::string &key, double def,
                     std::string *output) const;

    /**
     * Get a boolean with given configure key.
     * @param key The key of the configure item.
     * @param output the pointer of the output parameter.
     * @return the result status of this operation
     */
    Status getBool(const std::string &key, std::string *output) const;

    /**
     * Get a boolean with given configure key.
     * Return the default value def if key is not found.
     * @param key The key of the configure item.
     * @param def The default value.
     * @param output the pointer of the output parameter.
     * @return the result status of this operation
     */
    Status getBool(const std::string &key, bool def, std::string *output) const;

    /**
     * Set a configure item
     * @param key The key will set.
     * @param value The value will be set to.
     */
    void set(const std::string &key, const std::string &value);

    /**
     * Set a configure item
     * @param key The key will set.
     * @param value The value will be set to.
     */
    void set(const std::string &key, int32_t value);

    /**
     * Set a configure item
     * @param key The key will set.
     * @param value The value will be set to.
     */
    void set(const std::string &key, int64_t value);

    /**
     * Set a configure item
     * @param key The key will set.
     * @param value The value will be set to.
     */
    void set(const std::string &key, double value);

    /**
     * Set a configure item
     * @param key The key will set.
     * @param value The value will be set to.
     */
    void set(const std::string &key, bool value);

    /**
     * Get the hash value of this object
     * @return The hash value
     */
    size_t hash_value() const;

private:
    Config(hdfs::internal::ConfigImpl *impl);
    hdfs::internal::ConfigImpl *impl;
    friend class hdfs::FileSystem;
    friend class hdfs::internal::FileSystemImpl;
    friend class hdfs::NamenodeInfo;
};
}

#endif /* _HDFS_LIBHDFS3_COMMON_CONFIG_H_ */
