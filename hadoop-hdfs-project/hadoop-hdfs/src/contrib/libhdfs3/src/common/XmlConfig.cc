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
#include "XmlConfig.h"

#include <cassert>
#include <errno.h>
#include <fstream>
#include <libxml/parser.h>
#include <libxml/tree.h>
#include <limits>
#include <string.h>
#include <unistd.h>
#include <vector>

using namespace hdfs::internal;

using std::map;
using std::string;
using std::vector;

namespace hdfs {

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

    if (ERANGE == errno || retval > std::numeric_limits<int32_t>::max()
            || retval < std::numeric_limits<int32_t>::min()) {
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

    if (ERANGE == errno || retval > std::numeric_limits<int64_t>::max()
            || retval < std::numeric_limits<int64_t>::min()) {
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

    if (ERANGE == errno || retval > std::numeric_limits<double>::max()
            || retval < std::numeric_limits<double>::min()) {
        THROW(HdfsBadNumFoumat, "Underflow/Overflow int64_t type: %s", str);
    }

    return retval;
}

static void readConfigItem(xmlNodePtr root, Map & kv, const char *path) {
    std::string key, value;
    xmlNodePtr curNode;
    bool hasname = false, hasvalue = false;

    for (curNode = root; NULL != curNode; curNode = curNode->next) {
        if (curNode->type != XML_ELEMENT_NODE) {
            continue;
        }

        if (!hasname && !strcmp((const char *) curNode->name, "name")) {
            if (NULL != curNode->children
                    && XML_TEXT_NODE == curNode->children->type) {
                key = (const char *) curNode->children->content;
                hasname = true;
            }
        } else if (!hasvalue
                   && !strcmp((const char *) curNode->name, "value")) {
            if (NULL != curNode->children
                    && XML_TEXT_NODE == curNode->children->type) {
                value = (const char *) curNode->children->content;
                hasvalue = true;
            }
        } else {
            continue;
        }
    }

    if (hasname && hasvalue) {
        kv[key] = value;
        return;
    } else if (hasname) {
        kv[key] = "";
        return;
    }

    THROW(HdfsBadConfigFoumat, "Config cannot parse configure file: \"%s\"",
          path);
}

static void readConfigItems(xmlDocPtr doc, Map & kv, const char *path) {
    xmlNodePtr root, curNode;
    root = xmlDocGetRootElement(doc);

    if (NULL == root || strcmp((const char *) root->name, "configuration")) {
        THROW(HdfsBadConfigFoumat, "Config cannot parse configure file: \"%s\"",
              path);
    }

    /*
     * for each property
     */
    for (curNode = root->children; NULL != curNode; curNode = curNode->next) {
        if (curNode->type != XML_ELEMENT_NODE) {
            continue;
        }

        if (strcmp((const char *) curNode->name, "property")) {
            THROW(HdfsBadConfigFoumat,
                  "Config cannot parse configure file: \"%s\"", path);
        }

        readConfigItem(curNode->children, kv, path);
    }
}

Config::Config(const char *p) :
    path(p) {
    update(p);
}

void Config::update(const char *p) {
    char msg[64];
    xmlDocPtr doc; /* the resulting document tree */
    LIBXML_TEST_VERSION
    kv.clear();
    path = p;

    if (access(path.c_str(), R_OK)) {
        strerror_r(errno, msg, sizeof(msg));
        THROW(HdfsBadConfigFoumat, "Cannot read configure file: \"%s\", %s",
              path.c_str(), msg);
    }

    /* parse the file */
    doc = xmlReadFile(path.c_str(), NULL, 0);

    try {
        /* check if parsing succeeded */
        if (doc == NULL) {
            THROW(HdfsBadConfigFoumat,
                  "Config cannot parse configure file: \"%s\"", path.c_str());
        } else {
            readConfigItems(doc, kv, path.c_str());
            /* free up the resulting document */
            xmlFreeDoc(doc);
        }
    } catch (...) {
        xmlFreeDoc(doc);
        throw;
    }
}

const char *Config::getString(const char *key) const {
    Iterator it = kv.find(key);

    if (kv.end() == it) {
        THROW(HdfsConfigNotFound, "Config key: %s not found", key);
    }

    return it->second.c_str();
}

const char *Config::getString(const char *key, const char *def) const {
    Iterator it = kv.find(key);

    if (kv.end() == it) {
        return def;
    } else {
        return it->second.c_str();
    }
}

const char *Config::getString(const std::string & key) const {
    return getString(key.c_str());
}

const char *Config::getString(const std::string & key,
                               const std::string & def) const {
    return getString(key.c_str(), def.c_str());
}

int64_t Config::getInt64(const char *key) const {
    int64_t retval;
    Iterator it = kv.find(key);

    if (kv.end() == it) {
        THROW(HdfsConfigNotFound, "Config key: %s not found", key);
    }

    try {
        retval = StrToInt64(it->second.c_str());
    } catch (const HdfsBadNumFoumat & e) {
        NESTED_THROW(HdfsConfigNotFound, "Config key: %s not found", key);
    }

    return retval;
}

int64_t Config::getInt64(const char *key, int64_t def) const {
    int64_t retval;
    Iterator it = kv.find(key);

    if (kv.end() == it) {
        return def;
    }

    try {
        retval = StrToInt64(it->second.c_str());
    } catch (const HdfsBadNumFoumat & e) {
        NESTED_THROW(HdfsConfigNotFound, "Config key: %s not found", key);
    }

    return retval;
}

int32_t Config::getInt32(const char *key) const {
    int32_t retval;
    Iterator it = kv.find(key);

    if (kv.end() == it) {
        THROW(HdfsConfigNotFound, "Config key: %s not found", key);
    }

    try {
        retval = StrToInt32(it->second.c_str());
    } catch (const HdfsBadNumFoumat & e) {
        NESTED_THROW(HdfsConfigNotFound, "Config key: %s not found", key);
    }

    return retval;
}

int32_t Config::getInt32(const char *key, int32_t def) const {
    int32_t retval;
    Iterator it = kv.find(key);

    if (kv.end() == it) {
        return def;
    }

    try {
        retval = StrToInt32(it->second.c_str());
    } catch (const HdfsBadNumFoumat & e) {
        NESTED_THROW(HdfsConfigNotFound, "Config key: %s not found", key);
    }

    return retval;
}

double Config::getDouble(const char *key) const {
    double retval;
    Iterator it = kv.find(key);

    if (kv.end() == it) {
        THROW(HdfsConfigNotFound, "Config key: %s not found", key);
    }

    try {
        retval = StrToDouble(it->second.c_str());
    } catch (const HdfsBadNumFoumat & e) {
        NESTED_THROW(HdfsConfigNotFound, "Config key: %s not found", key);
    }

    return retval;
}

double Config::getDouble(const char *key, double def) const {
    double retval;
    Iterator it = kv.find(key);

    if (kv.end() == it) {
        return def;
    }

    try {
        retval = StrToDouble(it->second.c_str());
    } catch (const HdfsBadNumFoumat & e) {
        NESTED_THROW(HdfsConfigNotFound, "Config key: %s not found", key);
    }

    return retval;
}

bool Config::getBool(const char *key) const {
    bool retval;
    Iterator it = kv.find(key);

    if (kv.end() == it) {
        THROW(HdfsConfigNotFound, "Config key: %s not found", key);
    }

    try {
        retval = StrToBool(it->second.c_str());
    } catch (const HdfsBadBoolFoumat & e) {
        NESTED_THROW(HdfsConfigNotFound, "Config key: %s not found", key);
    }

    return retval;
}

bool Config::getBool(const char *key, bool def) const {
    bool retval;
    Iterator it = kv.find(key);

    if (kv.end() == it) {
        return def;
    }

    try {
        retval = StrToBool(it->second.c_str());
    } catch (const HdfsBadNumFoumat & e) {
        NESTED_THROW(HdfsConfigNotFound, "Config key: %s not found", key);
    }

    return retval;
}

size_t Config::hash_value() const {
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

