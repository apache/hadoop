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

#include "Atoi.h"
#include "Config.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "Logger.h"
#include "SharedPtr.h"
#include "StatusInternal.h"
#include "StringUtil.h"
#include "XmlConfigParser.h"

#include <cassert>
#include <errno.h>
#include <expat.h>
#include <fstream>
#include <limits>
#include <map>
#include <string.h>
#include <unistd.h>
#include <vector>

using namespace hdfs::internal;
using std::string;
using std::vector;

namespace hdfs {
namespace internal {

enum XmlParseState {
    HCXML_PARSE_INIT = 0,
    HCXML_PARSE_IN_CONFIG,
    HCXML_PARSE_IN_PROPERTY,
    HCXML_PARSE_IN_NAME,
    HCXML_PARSE_IN_VALUE,
    HCXML_PARSE_IN_FINAL,
};

// TODO: deprecation table

struct Value {
    Value() : final_(false) {
    }

    Value(const std::string &text, bool f)
        : text_(text), final_(f) { }
    std::string text_;
    bool final_;
};

typedef std::map<std::string, Value> accum_map_t;

class XmlData {
public:
    XmlData(const std::string &path, XML_Parser parser) 
            : path_(path),
              parser_(parser),
              state_(HCXML_PARSE_INIT),
              ignoredParents_(0),
              final_(false) {
        XML_SetUserData(parser_, this);
        XML_SetElementHandler(parser_, XmlData::startElement, XmlData::endElement);
        XML_SetCharacterDataHandler(parser_, handleData);
    }

    ~XmlData() {
        XML_ParserFree(parser_);
    }

    void populateConf(hdfs::Config *conf) {
        for (accum_map_t::iterator i = accumMap_.begin();
                 i != accumMap_.end(); ++i) {
            conf->set(i->first, i->second.text_);
        }
    }

private:
    XmlData(const XmlData&);
    XmlData &operator=(const XmlData &);

    static void startElement(void *data, const char *element,
                const char **attribute) {
        XmlData *ctx = reinterpret_cast<XmlData*>(data);
        if (ctx->ignoredParents_ > 0) {
            ctx->ignoredParents_++;
            return;
        }
        switch (ctx->state_) {
        case HCXML_PARSE_INIT:
            if (!strcmp(element, "configuration")) {
                ctx->state_ = HCXML_PARSE_IN_CONFIG;
                return;
            }
            break;
        case HCXML_PARSE_IN_CONFIG:
            if (!strcmp(element, "property")) {
                ctx->state_ = HCXML_PARSE_IN_PROPERTY;
                return;
            }
            break;
        case HCXML_PARSE_IN_PROPERTY:
            if (!strcmp(element, "name")) {
                ctx->state_ = HCXML_PARSE_IN_NAME;
                return;
            } else if (!strcmp(element, "value")) {
                ctx->state_ = HCXML_PARSE_IN_VALUE;
                return;
            } else if (!strcmp(element, "final")) {
                ctx->state_ = HCXML_PARSE_IN_FINAL;
                return;
            }
            break;
        default:
            break;
        }
        LOG(LOG_ERROR, "XmlData(%s): ignoring "
                "element '%s'\n", ctx->path_.c_str(), element);
        ctx->ignoredParents_++;
    }

    static void endElement(void *data, const char *el) {
        XmlData *ctx = reinterpret_cast<XmlData*>(data);

        if (ctx->ignoredParents_ > 0) {
            ctx->ignoredParents_--;
            return;
        }
        switch (ctx->state_) {
        case HCXML_PARSE_IN_CONFIG:
            ctx->state_ = HCXML_PARSE_INIT;
            break;
        case HCXML_PARSE_IN_PROPERTY:
            ctx->state_ = HCXML_PARSE_IN_CONFIG;
            if (ctx->name_.empty()) {
                LOG(LOG_ERROR, "hconf_builder_load_xml(%s): property "
                    "tag is missing <name> on line %lld\n",
                    ctx->path_.c_str(),
                    (long long)XML_GetCurrentLineNumber(ctx->parser_));
            } else if (ctx->value_.empty()) {
                LOG(LOG_ERROR, "hconf_builder_load_xml(%s): property "
                    "tag is missing <value> on line %lld\n",
                    ctx->path_.c_str(),
                    (long long)XML_GetCurrentLineNumber(ctx->parser_));
            } else {
                accum_map_t::iterator i = ctx->accumMap_.find(ctx->name_);
                if ((i != ctx->accumMap_.end()) && (i->second.final_)) {
                    LOG(LOG_ERROR, "XmlData(%s): ignoring attempt to "
                        "re-set final key '%s'\n", ctx->path_.c_str(),
                        i->second.text_.c_str());
                    break;
                }
                ctx->accumMap_[ctx->name_] = Value(ctx->name_, ctx->final_);
            }
            ctx->name_.clear();
            ctx->value_.clear();
            ctx->final_ = false;
            break;
        case HCXML_PARSE_IN_NAME:
            ctx->state_ = HCXML_PARSE_IN_PROPERTY;
            break;
        case HCXML_PARSE_IN_VALUE:
            ctx->state_ = HCXML_PARSE_IN_PROPERTY;
            break;
        case HCXML_PARSE_IN_FINAL:
            ctx->state_ = HCXML_PARSE_IN_PROPERTY;
            break;
        default:
            break;
        }
    }

    static void handleData(void *data, const char *content, int length) {
        XmlData *ctx = reinterpret_cast<XmlData*>(data);

        switch (ctx->state_) {
        case HCXML_PARSE_IN_NAME: {
            if (ctx->name_.empty()) {
                ctx->name_ = string(content, length);
            } else {
                    LOG(LOG_ERROR, "XmlData(%s): duplicate "
                        "<name> tag on line %lld\n", ctx->path_.c_str(),
                        (long long)XML_GetCurrentLineNumber(ctx->parser_));
                }
                break;
        }
        case HCXML_PARSE_IN_VALUE: {
            if (ctx->value_.empty()) {
                ctx->value_ = string(content, length);
            } else {
                LOG(LOG_ERROR, "XmlData(%s): duplicate "
                    "<value> tag on line %lld\n", ctx->path_.c_str(),
                    (long long)XML_GetCurrentLineNumber(ctx->parser_));
            }
            break;
        }
        case HCXML_PARSE_IN_FINAL: {
            string boolStr(content, length);
            bool val;
            Status status = StrToBool(boolStr.c_str(), &val);
            if (status.isError()) {
                LOG(LOG_ERROR, "XmlData(%s): error parsing "
                    "<final> tag on line %lld\n", ctx->path_.c_str(),
                    (long long)XML_GetCurrentLineNumber(ctx->parser_));
            } else {
                ctx->final_ = val;
            }
            break;
        }
        default:
            break;
        }
    }

    /** Path of the current XML file we're parsing. */
    std::string path_;

    /** The XML parser we're using. */
    XML_Parser parser_;

    /** XML parse state. */
    XmlParseState state_;

    /** The number of parent elements we are ignoring. */
    int ignoredParents_;

    /** Nonzero if the current property is final. */
    bool final_;

    /** Malloced key, if we saw one. */
    string name_;

    /** Malloced value, if we saw one. */
    string value_;

    /** The keys and values we've seen. */
    accum_map_t accumMap_;
};

static const char *const HDFS_XML_NAMES[] = {
    "core-default.xml",
    "core-site.xml",
    "hdfs-default.xml",
    "hdfs-site.xml",
    NULL
};

XmlConfigParser::XmlConfigParser(hdfs::Config *conf)
    : conf_(conf) {
}

Status XmlConfigParser::ParseXmls(const std::string &pathList) {
    string errors, prefix;
    vector<string> paths = StringSplit(pathList, ":");
    for (vector<string>::const_iterator p = paths.begin();
             p != paths.end(); ++p) {
        for (size_t x = 0; HDFS_XML_NAMES[x]; ++x) {
            Status status = ParseXml(*p + "/" + HDFS_XML_NAMES[x]);
            if (status.getCode() != ENOENT) {
                errors = errors + prefix + status.getErrorMsg();
                prefix = ", ";
            }
        }
    }
    if (errors.empty()) {
        return Status::OK();
    }
    return Status(EINVAL, errors);
}

Status XmlConfigParser::ParseXml(const std::string &path) {
    ::FILE *fp = ::fopen(path.c_str(), "r");
    if (!fp) {
        int err = errno;
        if (err == ENOENT) {
            return Status(ENOENT, "No such file as " + path);
        }
        return Status(err);
    }
    shared_ptr<FILE> fpPtr(fp, ::fclose);
    XML_Parser parser = XML_ParserCreate("UTF-8");
    if (!parser) {
        return Status(ENOMEM, "Failed to create libexpat XML parser for " +
                      path);
    }
    XmlData xmlData(path, parser);

    vector<uint8_t> buf(16384, 0x0);
    int res = 0;
    do {
        res = fread(&buf[0], 1, buf.size(), fp);
        if (res <= 0) {
            if (feof(fp)) {
                res = 0;
            } else {
                int e = errno;
                return Status(e, "Failed to read from configuration XML "
                              "file " + path);
            }
        }
        int pstatus = XML_Parse(parser,
                reinterpret_cast<const char *>(&buf[0]), res,
                res ? XML_FALSE : XML_TRUE);
        if (pstatus != XML_STATUS_OK) {
            enum XML_Error error = XML_GetErrorCode(parser);
            return Status(EINVAL, "hconf_builder_load_xml(" + path + "): " +
                            "parse error: " + XML_ErrorString(error));
        }
    } while (res);
    xmlData.populateConf(conf_);
    return Status::OK();
}

}
}

// vim: ts=4:sw=4:tw=79:et
