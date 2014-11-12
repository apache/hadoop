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

#include "StatusInternal.h"

#include <string>

namespace hdfs {
class Config;
}

namespace hdfs {
namespace internal {

/**
 * A configure file parser.
 */
class XmlConfigParser {
public:
    XmlConfigParser(hdfs::Config *conf);

    /**
     * Parse a colon-separated list of paths for HDFS configuration files.
     * The format is the same as that of CLASSPATH.
     *
     * Even when an error is returned, we may still load some entries into the
     * Config file.
     */
    Status ParseXmls(const std::string &pathList);

    /**
     * Parse a single XML file.
     *
     * Even when an error is returned, we may still load some entries into the
     * Config file.
     */
    Status ParseXml(const std::string &path);

private:
    XmlConfigParser(const XmlConfigParser &);
    XmlConfigParser &operator=(const XmlConfigParser &);

    hdfs::Config *conf_;
};

}
}

#endif /* _HDFS_LIBHDFS3_COMMON_XMLCONFIGPARSER_H_ */
