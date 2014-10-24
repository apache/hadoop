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

#ifndef _HDFS_LIBHDFS3_COMMON_XMLCONFIGPARSER_H_
#define _HDFS_LIBHDFS3_COMMON_XMLCONFIGPARSER_H_

#include <stdint.h>
#include <string>
#include <sstream>
#include <map>

namespace hdfs {
namespace internal {

/**
 * A configure file parser.
 */
class XmlConfigParser {
public:
    /**
     * Construct a empty Config instance.
     */
    XmlConfigParser() {
    }

    /**
     * Construct a Config with given configure file.
     * @param path The path of configure file.
     * @throw HdfsBadConfigFoumat
     */
    XmlConfigParser(const char *path);

    /**
     * Parse the configure file.
     * @throw HdfsBadConfigFoumat
     */
    void update(const char *path);

    /**
     * Get Key Values
     * @return Return the Key Value pairs.
     */
    std::map<std::string, std::string> getKeyValue() {
        return kv;
    }

private:
    std::string path;
    std::map<std::string, std::string> kv;
};
}
}

#endif /* _HDFS_LIBHDFS3_COMMON_XMLCONFIGPARSER_H_ */
