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

#ifndef NATIVELIBRARY_H_
#define NATIVELIBRARY_H_

#include <string>

namespace NativeTask {

using std::string;
class NativeObject;
class NativeObjectFactory;

/**
 * User level object library abstraction
 */
class NativeLibrary {
  friend class NativeObjectFactory;
private:
  string _path;
  string _name;
  GetObjectCreatorFunc _getObjectCreatorFunc;
  FunctionGetter _functionGetter;
public:
  NativeLibrary(const string & path, const string & name);

  bool init();

  NativeObject * createObject(const string & clz);

  void * getFunction(const string & functionName);

  ObjectCreatorFunc getObjectCreator(const string & clz);

  ~NativeLibrary() {
  }
};

} // namespace NativeTask

#endif /* NATIVELIBRARY_H_ */
