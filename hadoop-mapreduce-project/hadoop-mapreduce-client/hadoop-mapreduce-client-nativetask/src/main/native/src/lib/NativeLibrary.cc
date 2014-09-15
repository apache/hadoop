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

#include <dlfcn.h>

#include "lib/commons.h"
#include "lib/NativeObjectFactory.h"
#include "lib/NativeLibrary.h"

namespace NativeTask {

//////////////////////////////////////////////////////////////////
// NativeLibrary methods
//////////////////////////////////////////////////////////////////

NativeLibrary::NativeLibrary(const string & path, const string & name)
    : _path(path), _name(name), _getObjectCreatorFunc(NULL), _functionGetter(NULL) {
}

bool NativeLibrary::init() {
  void *library = dlopen(_path.c_str(), RTLD_LAZY | RTLD_GLOBAL);
  if (NULL == library) {
    LOG("[NativeLibrary] Load object library %s failed.", _path.c_str());
    return false;
  }
  // clean error status
  dlerror();

  string create_object_func_name = _name + "GetObjectCreator";
  _getObjectCreatorFunc = (GetObjectCreatorFunc)dlsym(library, create_object_func_name.c_str());
  if (NULL == _getObjectCreatorFunc) {
    LOG("[NativeLibrary] ObjectCreator function [%s] not found", create_object_func_name.c_str());
  }

  string functionGetter = _name + "GetFunctionGetter";
  _functionGetter = (FunctionGetter)dlsym(library, functionGetter.c_str());
  if (NULL == _functionGetter) {
    LOG("[NativeLibrary] function getter [%s] not found", functionGetter.c_str());
  }

  string init_library_func_name = _name + "Init";
  InitLibraryFunc init_library_func = (InitLibraryFunc)dlsym(library,
      init_library_func_name.c_str());
  if (NULL == init_library_func) {
    LOG("[NativeLibrary] Library init function [%s] not found", init_library_func_name.c_str());
  } else {
    init_library_func();
  }
  return true;
}

NativeObject * NativeLibrary::createObject(const string & clz) {
  if (NULL == _getObjectCreatorFunc) {
    return NULL;
  }
  return (NativeObject*)((_getObjectCreatorFunc(clz))());
}

void * NativeLibrary::getFunction(const string & functionName) {
  if (NULL == _functionGetter) {
    return NULL;
  }
  return (*_functionGetter)(functionName);
}

ObjectCreatorFunc NativeLibrary::getObjectCreator(const string & clz) {
  if (NULL == _getObjectCreatorFunc) {
    return NULL;
  }
  return _getObjectCreatorFunc(clz);
}

} // namespace NativeTask

