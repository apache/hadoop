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

#ifndef SPILL_OUTPUT_SERVICE_H_
#define SPILL_OUTPUT_SERVICE_H_

#include <stdint.h>
#include <string>

namespace NativeTask {

class CombineHandler;

using std::string;

class SpillOutputService {
public:
  virtual ~SpillOutputService() {}

  virtual string * getSpillPath() = 0;
  virtual string * getOutputPath() = 0;
  virtual string * getOutputIndexPath() = 0;

  virtual CombineHandler * getJavaCombineHandler() = 0;
};

} // namespace NativeTask

#endif /* SPILL_OUTPUT_SERVICE_H_ */
