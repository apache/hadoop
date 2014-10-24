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

#include "Exception.h"
#include "ExceptionInternal.h"
#include "Hash.h"
#include "ConfigImpl.h"

#include <cassert>
#include <errno.h>
#include <fstream>
#include <limits>
#include <string.h>
#include <unistd.h>
#include <vector>

using std::map;
using std::string;
using std::vector;

namespace hdfs {
namespace internal {

typedef map<string, string>::const_iterator Iterator;
typedef map<string, string> Map;

static int32_t StrToInt32(const char *str) {
    long retval;
    char *end = NULL;
    errno = 0;
    retval = strtol(str, &end, 0);

    if (EINVAL == errno || 0 != *end) {
        THROW(HdfsBadNumFoumat, "Invalid int32_t type: %s", str);
    }

    if (ERANGE == errno || retval > std::numeric_limits<int32_t>::max() ||
        retval < std::numeric_limits<int32_t>::min()) {
        THROW(HdfsBadNumFoumat, "Underflow/Overflow int32_t type: %s", str);
    }

    return retval;
}

static int64_t StrToInt64(const char *str) {
    long long retval;
    char *end = NULL;
    errno = 0;
    retval = strtoll(str, &end, 0);

    if (EINVAL == errno || 0 != *end) {
        THROW(HdfsBadNumFoumat, "Invalid int64_t type: %s", str);
    }

    if (ERANGE == errno || retval > std::numeric_limits<int64_t>::max() ||
        retval < std::numeric_limits<int64_t>::min()) {
        THROW(HdfsBadNumFoumat, "Underflow/Overflow int64_t type: %s", str);
    }

    return retval;
}

static bool StrToBool(const char *str) {
    bool retval = false;

    if (!strcasecmp(str, "true") || !strcmp(str, "1")) {
        retval = true;
    } else if (!strcasecmp(str, "false") || !strcmp(str, "0")) {
        retval = false;
    } else {
        THROW(HdfsBadBoolFoumat, "Invalid bool type: %s", str);
    }

    return retval;
}

static double StrToDouble(const char *str) {
    double retval;
    char *end = NULL;
    errno = 0;
    retval = strtod(str, &end);

    if (EINVAL == errno || 0 != *end) {
        THROW(HdfsBadNumFoumat, "Invalid double type: %s", str);
    }

    if (ERANGE == errno || retval > std::numeric_limits<double>::max() ||
        retval < std::numeric_limits<double>::min()) {
        THROW(HdfsBadNumFoumat, "Underflow/Overflow int64_t type: %s", str);
    }

    return retval;
}

ConfigImpl::ConfigImpl(const Map &kv) : kv(kv) {
}

const char *ConfigImpl::getString(const std::string &key) const {
    Iterator it = kv.find(key.c_str());

    if (kv.end() == it) {
        THROW(HdfsConfigNotFound, "ConfigImpl key: %s not found", key.c_str());
    }

    return it->second.c_str();
}

const char *ConfigImpl::getString(const std::string &key,
                                  const std::string &def) const {
    Iterator it = kv.find(key.c_str());

    if (kv.end() == it) {
        return def.c_str();
    } else {
        return it->second.c_str();
    }
}

int64_t ConfigImpl::getInt64(const std::string &key) const {
    int64_t retval;
    Iterator it = kv.find(key.c_str());

    if (kv.end() == it) {
        THROW(HdfsConfigNotFound, "ConfigImpl key: %s not found", key.c_str());
    }

    try {
        retval = StrToInt64(it->second.c_str());
    } catch (const HdfsBadNumFoumat &e) {
        NESTED_THROW(HdfsConfigNotFound, "ConfigImpl key: %s not found",
                     key.c_str());
    }

    return retval;
}

int64_t ConfigImpl::getInt64(const std::string &key, int64_t def) const {
    int64_t retval;
    Iterator it = kv.find(key.c_str());

    if (kv.end() == it) {
        return def;
    }

    try {
        retval = StrToInt64(it->second.c_str());
    } catch (const HdfsBadNumFoumat &e) {
        NESTED_THROW(HdfsConfigNotFound, "ConfigImpl key: %s not found",
                     key.c_str());
    }

    return retval;
}

int32_t ConfigImpl::getInt32(const std::string &key) const {
    int32_t retval;
    Iterator it = kv.find(key.c_str());

    if (kv.end() == it) {
        THROW(HdfsConfigNotFound, "ConfigImpl key: %s not found", key.c_str());
    }

    try {
        retval = StrToInt32(it->second.c_str());
    } catch (const HdfsBadNumFoumat &e) {
        NESTED_THROW(HdfsConfigNotFound, "ConfigImpl key: %s not found",
                     key.c_str());
    }

    return retval;
}

int32_t ConfigImpl::getInt32(const std::string &key, int32_t def) const {
    int32_t retval;
    Iterator it = kv.find(key.c_str());

    if (kv.end() == it) {
        return def;
    }

    try {
        retval = StrToInt32(it->second.c_str());
    } catch (const HdfsBadNumFoumat &e) {
        NESTED_THROW(HdfsConfigNotFound, "ConfigImpl key: %s not found",
                     key.c_str());
    }

    return retval;
}

double ConfigImpl::getDouble(const std::string &key) const {
    double retval;
    Iterator it = kv.find(key.c_str());

    if (kv.end() == it) {
        THROW(HdfsConfigNotFound, "ConfigImpl key: %s not found", key.c_str());
    }

    try {
        retval = StrToDouble(it->second.c_str());
    } catch (const HdfsBadNumFoumat &e) {
        NESTED_THROW(HdfsConfigNotFound, "ConfigImpl key: %s not found",
                     key.c_str());
    }

    return retval;
}

double ConfigImpl::getDouble(const std::string &key, double def) const {
    double retval;
    Iterator it = kv.find(key.c_str());

    if (kv.end() == it) {
        return def;
    }

    try {
        retval = StrToDouble(it->second.c_str());
    } catch (const HdfsBadNumFoumat &e) {
        NESTED_THROW(HdfsConfigNotFound, "ConfigImpl key: %s not found",
                     key.c_str());
    }

    return retval;
}

bool ConfigImpl::getBool(const std::string &key) const {
    bool retval;
    Iterator it = kv.find(key.c_str());

    if (kv.end() == it) {
        THROW(HdfsConfigNotFound, "ConfigImpl key: %s not found", key.c_str());
    }

    try {
        retval = StrToBool(it->second.c_str());
    } catch (const HdfsBadBoolFoumat &e) {
        NESTED_THROW(HdfsConfigNotFound, "ConfigImpl key: %s not found",
                     key.c_str());
    }

    return retval;
}

bool ConfigImpl::getBool(const std::string &key, bool def) const {
    bool retval;
    Iterator it = kv.find(key.c_str());

    if (kv.end() == it) {
        return def;
    }

    try {
        retval = StrToBool(it->second.c_str());
    } catch (const HdfsBadNumFoumat &e) {
        NESTED_THROW(HdfsConfigNotFound, "ConfigImpl key: %s not found",
                     key.c_str());
    }

    return retval;
}

size_t ConfigImpl::hash_value() const {
    vector<size_t> values;
    map<string, string>::const_iterator s, e;
    e = kv.end();

    for (s = kv.begin(); s != e; ++s) {
        values.push_back(StringHasher(s->first));
        values.push_back(StringHasher(s->second));
    }

    return CombineHasher(&values[0], values.size());
}
}
}
