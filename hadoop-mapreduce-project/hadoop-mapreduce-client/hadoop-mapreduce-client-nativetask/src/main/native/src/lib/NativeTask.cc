/*
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
#ifndef __CYGWIN__
#include <execinfo.h>
#endif
#include "lib/commons.h"
#include "util/StringUtil.h"
#include "NativeTask.h"
#include "lib/NativeObjectFactory.h"

namespace NativeTask {

//////////////////////////////////////////////////////////////////
// NativeObjectType methods
//////////////////////////////////////////////////////////////////

const string NativeObjectTypeToString(NativeObjectType type) {
  switch (type) {
  case BatchHandlerType:
    return string("BatchHandlerType");
  default:
    return string("UnknownObjectType");
  }
}

NativeObjectType NativeObjectTypeFromString(const string type) {
  if (type == "BatchHandlerType") {
    return BatchHandlerType;
  }
  return UnknownObjectType;
}

HadoopException::HadoopException(const string & what) {
  // remove long path prefix
  size_t n = 0;
  if (what[0] == '/') {
    size_t p = what.find(':');
    if (p != what.npos) {
      while (true) {
        size_t np = what.find('/', n + 1);
        if (np == what.npos || np >= p) {
          break;
        }
        n = np;
      }
    }
  }
  _reason.append(what.c_str() + n, what.length() - n);
  void *array[64];
  size_t size;

#ifndef __CYGWIN__
  size = backtrace(array, 64);
  char ** traces = backtrace_symbols(array, size);
  for (size_t i = 0; i < size; i++) {
    _reason.append("\n\t");
    _reason.append(traces[i]);
  }
#endif
}

///////////////////////////////////////////////////////////

void Config::load(const string & path) {
  FILE * fin = fopen(path.c_str(), "r");
  if (NULL == fin) {
    THROW_EXCEPTION(IOException, "file not found or can not open for read");
  }
  char buff[256];
  while (fgets(buff, 256, fin) != NULL) {
    if (buff[0] == '#') {
      continue;
    }
    std::string key = buff;
    if (key[key.length() - 1] == '\n') {
      size_t br = key.find('=');
      if (br != key.npos) {
        set(key.substr(0, br), StringUtil::Trim(key.substr(br + 1)));
      }
    }
  }
  fclose(fin);
}

void Config::set(const string & key, const string & value) {
  _configs[key] = value;
}

void Config::setInt(const string & name, int64_t value) {
  _configs[name] = StringUtil::ToString(value);
}

void Config::setBool(const string & name, bool value) {
  _configs[name] = StringUtil::ToString(value);
}

void Config::parse(int32_t argc, const char ** argv) {
  for (int32_t i = 0; i < argc; i++) {
    const char * equ = strchr(argv[i], '=');
    if (NULL == equ) {
      LOG("[NativeTask] config argument not recognized: %s", argv[i]);
      continue;
    }
    if (argv[i][0] == '-') {
      LOG("[NativeTask] config argument with '-' prefix ignored: %s", argv[i]);
      continue;
    }
    string key(argv[i], equ - argv[i]);
    string value(equ + 1, strlen(equ + 1));
    map<string, string>::iterator itr = _configs.find(key);
    if (itr == _configs.end()) {
      _configs[key] = value;
    } else {
      itr->second.append(",");
      itr->second.append(value);
    }
  }
}

const char * Config::get(const string & name) {
  map<string, string>::iterator itr = _configs.find(name);
  if (itr == _configs.end()) {
    return NULL;
  } else {
    return itr->second.c_str();
  }
}

string Config::get(const string & name, const string & defaultValue) {
  map<string, string>::iterator itr = _configs.find(name);
  if (itr == _configs.end()) {
    return defaultValue;
  } else {
    return itr->second;
  }
}

int64_t Config::getInt(const string & name, int64_t defaultValue) {
  map<string, string>::iterator itr = _configs.find(name);
  if (itr == _configs.end()) {
    return defaultValue;
  } else {
    return StringUtil::toInt(itr->second);
  }
}

bool Config::getBool(const string & name, bool defaultValue) {
  map<string, string>::iterator itr = _configs.find(name);
  if (itr == _configs.end()) {
    return defaultValue;
  } else {
    return StringUtil::toBool(itr->second);
  }
}

float Config::getFloat(const string & name, float defaultValue) {
  map<string, string>::iterator itr = _configs.find(name);
  if (itr == _configs.end()) {
    return defaultValue;
  } else {
    return StringUtil::toFloat(itr->second);
  }
}

void Config::getStrings(const string & name, vector<string> & dest) {
  map<string, string>::iterator itr = _configs.find(name);
  if (itr != _configs.end()) {
    StringUtil::Split(itr->second, ",", dest, true);
  }
}

void Config::getInts(const string & name, vector<int64_t> & dest) {
  vector<string> sdest;
  getStrings(name, sdest);
  for (size_t i = 0; i < sdest.size(); i++) {
    dest.push_back(StringUtil::toInt(sdest[i]));
  }
}

void Config::getFloats(const string & name, vector<float> & dest) {
  vector<string> sdest;
  getStrings(name, sdest);
  for (size_t i = 0; i < sdest.size(); i++) {
    dest.push_back(StringUtil::toFloat(sdest[i]));
  }
}

///////////////////////////////////////////////////////////

Counter * ProcessorBase::getCounter(const string & group, const string & name) {
  return NULL;
}

///////////////////////////////////////////////////////////

} // namespace NativeTask
