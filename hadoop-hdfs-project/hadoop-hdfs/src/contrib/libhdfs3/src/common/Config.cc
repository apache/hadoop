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

#include "Config.h"
#include "ConfigImpl.h"
#include "XmlConfigParser.h"
#include "StatusInternal.h"

using namespace hdfs::internal;

namespace hdfs {

Config Config::CreateFromXmlFile(const std::string &path) {
    return Config(new ConfigImpl(XmlConfigParser(path.c_str()).getKeyValue()));
}

Config::Config() : impl(new ConfigImpl) {
}

Config::Config(const Config &other) {
    impl = new ConfigImpl(*other.impl);
}

Config::Config(ConfigImpl *impl) : impl(impl) {
}

Config &Config::operator=(const Config &other) {
    if (this == &other) {
        return *this;
    }

    ConfigImpl *temp = impl;
    impl = new ConfigImpl(*other.impl);
    delete temp;
    return *this;
}

bool Config::operator==(const Config &other) const {
    if (this == &other) {
        return true;
    }

    return *impl == *other.impl;
}

Config::~Config() {
    delete impl;
}

Status Config::getString(const std::string &key, std::string *output) const {
    CHECK_PARAMETER(NULL != output, EINVAL, "invalid parameter \"output\"");

    try {
        *output = impl->getString(key.c_str());
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status Config::getString(const std::string &key, const std::string &def,
                         std::string *output) const {
    CHECK_PARAMETER(NULL != output, EINVAL, "invalid parameter \"output\"");

    try {
        *output = impl->getString(key.c_str(), def.c_str());
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status Config::getInt64(const std::string &key, std::string *output) const {
    CHECK_PARAMETER(NULL != output, EINVAL, "invalid parameter \"output\"");

    try {
        *output = impl->getInt64(key.c_str());
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status Config::getInt64(const std::string &key, int64_t def,
                        std::string *output) const {
    CHECK_PARAMETER(NULL != output, EINVAL, "invalid parameter \"output\"");

    try {
        *output = impl->getInt64(key.c_str(), def);
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status Config::getInt32(const std::string &key, std::string *output) const {
    CHECK_PARAMETER(NULL != output, EINVAL, "invalid parameter \"output\"");

    try {
        *output = impl->getInt32(key.c_str());
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status Config::getInt32(const std::string &key, int32_t def,
                        std::string *output) const {
    CHECK_PARAMETER(NULL != output, EINVAL, "invalid parameter \"output\"");

    try {
        *output = impl->getInt32(key.c_str(), def);
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status Config::getDouble(const std::string &key, std::string *output) const {
    CHECK_PARAMETER(NULL != output, EINVAL, "invalid parameter \"output\"");

    try {
        *output = impl->getDouble(key.c_str());
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status Config::getDouble(const std::string &key, double def,
                         std::string *output) const {
    CHECK_PARAMETER(NULL != output, EINVAL, "invalid parameter \"output\"");

    try {
        *output = impl->getDouble(key.c_str(), def);
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status Config::getBool(const std::string &key, std::string *output) const {
    CHECK_PARAMETER(NULL != output, EINVAL, "invalid parameter \"output\"");

    try {
        *output = impl->getBool(key.c_str());
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

Status Config::getBool(const std::string &key, bool def,
                       std::string *output) const {
    CHECK_PARAMETER(NULL != output, EINVAL, "invalid parameter \"output\"");

    try {
        *output = impl->getBool(key.c_str(), def);
    } catch (...) {
        return CreateStatusFromException(current_exception());
    }

    return Status::OK();
}

void Config::set(const std::string &key, const std::string &value) {
    impl->set(key.c_str(), value);
}

void Config::set(const std::string &key, int32_t value) {
    impl->set(key.c_str(), value);
}

void Config::set(const std::string &key, int64_t value) {
    impl->set(key.c_str(), value);
}

void Config::set(const std::string &key, double value) {
    impl->set(key.c_str(), value);
}

void Config::set(const std::string &key, bool value) {
    impl->set(key.c_str(), value);
}

size_t Config::hash_value() const {
    return impl->hash_value();
}
}
