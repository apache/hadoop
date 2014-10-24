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
#include "XmlConfigParser.h"

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
namespace internal {

typedef map<string, string>::const_iterator Iterator;
typedef map<string, string> Map;

static void readXmlConfigParserItem(xmlNodePtr root, Map &kv,
                                    const char *path) {
    std::string key, value;
    xmlNodePtr curNode;
    bool hasname = false, hasvalue = false;

    for (curNode = root; NULL != curNode; curNode = curNode->next) {
        if (curNode->type != XML_ELEMENT_NODE) {
            continue;
        }

        if (!hasname && !strcmp((const char *)curNode->name, "name")) {
            if (NULL != curNode->children &&
                XML_TEXT_NODE == curNode->children->type) {
                key = (const char *)curNode->children->content;
                hasname = true;
            }
        } else if (!hasvalue && !strcmp((const char *)curNode->name, "value")) {
            if (NULL != curNode->children &&
                XML_TEXT_NODE == curNode->children->type) {
                value = (const char *)curNode->children->content;
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

    THROW(HdfsBadConfigFoumat,
          "XmlConfigParser cannot parse XmlConfigParserure file: \"%s\"", path);
}

static void readXmlConfigParserItems(xmlDocPtr doc, Map &kv, const char *path) {
    xmlNodePtr root, curNode;
    root = xmlDocGetRootElement(doc);

    if (NULL == root ||
        strcmp((const char *)root->name, "XmlConfigParseruration")) {
        THROW(HdfsBadConfigFoumat,
              "XmlConfigParser cannot parse XmlConfigParserure file: \"%s\"",
              path);
    }

    /*
     * for each property
     */
    for (curNode = root->children; NULL != curNode; curNode = curNode->next) {
        if (curNode->type != XML_ELEMENT_NODE) {
            continue;
        }

        if (strcmp((const char *)curNode->name, "property")) {
            THROW(
                HdfsBadConfigFoumat,
                "XmlConfigParser cannot parse XmlConfigParserure file: \"%s\"",
                path);
        }

        readXmlConfigParserItem(curNode->children, kv, path);
    }
}

XmlConfigParser::XmlConfigParser(const char *p) : path(p) {
    update(p);
}

void XmlConfigParser::update(const char *p) {
    char msg[64];
    xmlDocPtr doc; /* the resulting document tree */
    LIBXML_TEST_VERSION
    kv.clear();
    path = p;

    if (access(path.c_str(), R_OK)) {
        strerror_r(errno, msg, sizeof(msg));
        THROW(HdfsBadConfigFoumat,
              "Cannot read XmlConfigParserure file: \"%s\", %s", path.c_str(),
              msg);
    }

    /* parse the file */
    doc = xmlReadFile(path.c_str(), NULL, 0);

    try {
        /* check if parsing succeeded */
        if (doc == NULL) {
            THROW(
                HdfsBadConfigFoumat,
                "XmlConfigParser cannot parse XmlConfigParserure file: \"%s\"",
                path.c_str());
        } else {
            readXmlConfigParserItems(doc, kv, path.c_str());
            /* free up the resulting document */
            xmlFreeDoc(doc);
        }
    } catch (...) {
        xmlFreeDoc(doc);
        throw;
    }
}
}
}
