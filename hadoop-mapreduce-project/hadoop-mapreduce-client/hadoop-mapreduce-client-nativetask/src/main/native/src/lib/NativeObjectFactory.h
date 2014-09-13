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

#ifndef NATIVEOBJECTFACTORY_H_
#define NATIVEOBJECTFACTORY_H_

#include <string>
#include <vector>
#include <set>
#include <map>

#include "NativeTask.h"

namespace NativeTask {

using std::string;
using std::vector;
using std::map;
using std::set;
using std::pair;

class NativeLibrary;

class CounterPtrCompare {
public:
  bool operator()(const Counter * lhs, const Counter * rhs) const {
    if (lhs->group() < rhs->group()) {
      return true;
    } else if (lhs->group() == rhs->group()) {
      return lhs->name() < rhs->name();
    } else {
      return false;
    }
  }
};

/**
 * Native object factory
 */
class NativeObjectFactory {
private:
  static vector<NativeLibrary *> Libraries;
  static map<NativeObjectType, string> DefaultClasses;
  static Config * GlobalConfig;
  static float LastProgress;
  static Progress * TaskProgress;
  static string LastStatus;
  static set<Counter *, CounterPtrCompare> CounterSet;
  static vector<Counter *> Counters;
  static vector<uint64_t> CounterLastUpdateValues;
  static bool Inited;
public:
  static bool Init();
  static void Release();
  static void CheckInit();
  static Config & GetConfig();
  static Config * GetConfigPtr();
  static void SetTaskProgressSource(Progress * progress);
  static float GetTaskProgress();
  static void SetTaskStatus(const string & status);
  static void GetTaskStatusUpdate(string & statusData);
  static Counter * GetCounter(const string & group, const string & name);
  static void RegisterClass(const string & clz, ObjectCreatorFunc func);
  static NativeObject * CreateObject(const string & clz);
  static void * GetFunction(const string & clz);
  static ObjectCreatorFunc GetObjectCreator(const string & clz);
  static void ReleaseObject(NativeObject * obj);
  static bool RegisterLibrary(const string & path, const string & name);
  static void SetDefaultClass(NativeObjectType type, const string & clz);
  static NativeObject * CreateDefaultObject(NativeObjectType type);
  static int BytesComparator(const char * src, uint32_t srcLength, const char * dest,
      uint32_t destLength);
  static int ByteComparator(const char * src, uint32_t srcLength, const char * dest,
      uint32_t destLength);
  static int IntComparator(const char * src, uint32_t srcLength, const char * dest,
      uint32_t destLength);
  static int LongComparator(const char * src, uint32_t srcLength, const char * dest,
      uint32_t destLength);
  static int VIntComparator(const char * src, uint32_t srcLength, const char * dest,
      uint32_t destLength);
  static int VLongComparator(const char * src, uint32_t srcLength, const char * dest,
      uint32_t destLength);
  static int FloatComparator(const char * src, uint32_t srcLength, const char * dest,
      uint32_t destLength);
  static int DoubleComparator(const char * src, uint32_t srcLength, const char * dest,
      uint32_t destLength);
};

} // namespace NativeTask

#endif /* NATIVEOBJECTFACTORY_H_ */
