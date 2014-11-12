/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"){} you may not use this file except in compliance
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

#include "Atoi.h"
#include "Config.h"
#include "StatusInternal.h"

#include <inttypes.h>
#include <map>
#include <stdint.h>
#include <stdlib.h>
#include <string>

using namespace hdfs::internal;
using std::string;

typedef std::map<std::string, std::string> map_t;


namespace hdfs {

Config::Config() {
}

Status Config::getString(const std::string &key, std::string *output) const {
    map_t::const_iterator i = map_.find(key);
    if (i == map_.end()) {
        return Status(ENOENT, "Configuration key " + key + " was not found.");
    }
    *output = i->second;
    return Status::OK();
}

Status Config::getString(const std::string &key, const std::string &def,
                         std::string *output) const {
    map_t::const_iterator i = map_.find(key);
    if (i == map_.end()) {
        *output = def;
    }
    *output = i->second;
    return Status::OK();
}

Status Config::getInt64(const std::string &key, int64_t *output) const {
    string out;
    Status status = getString(key, &out);
    if (status.isError()) {
        return status;
    }
    status = StrToInt64(out.c_str(), output);
    if (status.isError()) {
        return status;
    }
    return Status::OK();
}

Status Config::getInt64(const std::string &key, int64_t def,
                        int64_t *output) const {
    string out;
    Status status = getString(key, &out);
    if (status.isError()) {
        *output = def;
        return Status::OK();
    }
    status = StrToInt64(out.c_str(), output);
    if (status.isError()) {
        return status;
    }
    return Status::OK();
}

Status Config::getInt32(const std::string &key, int32_t *output) const {
    std::string out;
    Status status = getString(key, &out);
    if (status.isError()) {
        return status;
    }
    status = StrToInt32(out.c_str(), output);
    if (status.isError()) {
        return status;
    }
    return Status::OK();
}

Status Config::getInt32(const std::string &key, int32_t def,
                        int32_t *output) const {
    string out;
    Status status = getString(key, &out);
    if (status.isError()) {
        *output = def;
        return Status::OK();
    }
    status = StrToInt32(out.c_str(), output);
    if (status.isError()) {
        return status;
    }
    return Status::OK();
}

Status Config::getDouble(const std::string &key, double *output) const {
    string out;
    Status status = getString(key, &out);
    if (status.isError()) {
        return status;
    }
    status = StrToDouble(out.c_str(), output);
    if (status.isError()) {
        return status;
    }
    return Status::OK();
}

Status Config::getDouble(const std::string &key, double def,
                         double *output) const {
    string out;
    Status status = getString(key, &out);
    if (status.isError()) {
        *output = def;
        return Status::OK();
    }
    status = StrToDouble(out.c_str(), output);
    if (status.isError()) {
        return status;
    }
    return Status::OK();
}

Status Config::getBool(const std::string &key, bool *output) const {
    string out;
    Status status = getString(key, &out);
    if (status.isError()) {
        return status;
    }
    status = StrToBool(out.c_str(), output);
    if (status.isError()) {
        return status;
    }
    return Status::OK();
}

Status Config::getBool(const std::string &key, bool def,
                       bool *output) const {
    string out;
    Status status = getString(key, &out);
    if (status.isError()) {
        *output = def;
        return Status::OK();
    }
    status = StrToBool(out.c_str(), output);
    if (status.isError()) {
        return status;
    }
    return Status::OK();
}

void Config::set(const std::string &key, const std::string &value) {
    map_[key] = value;
}

void Config::set(const std::string &key, int32_t value) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%" PRId32, value);
    set(key.c_str(), string(buf));
}

void Config::set(const std::string &key, int64_t value) {
    char buf[64];
    snprintf(buf, sizeof(buf), "%" PRId64, value);
    set(key.c_str(), string(buf));
}

void Config::set(const std::string &key, double value) {
    char buf[64];
    snprintf(buf, sizeof(buf), "%f", value);
    set(key.c_str(), string(buf));
}

void Config::set(const std::string &key, bool value) {
    set(key.c_str(), value ? string("true") : string("false"));
}

}
