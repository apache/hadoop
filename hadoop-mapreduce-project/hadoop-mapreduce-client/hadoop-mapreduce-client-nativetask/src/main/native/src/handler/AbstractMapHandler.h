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

#ifndef ABSTRACT_MAP_HANDLER_H
#define ABSTRACT_MAP_HANDLER_H

#include "NativeTask.h"
#include "BatchHandler.h"
#include "lib/SpillOutputService.h"
#include "lib/Combiner.h"
#include "CombineHandler.h"

namespace NativeTask {

class AbstractMapHandler : public BatchHandler,  public SpillOutputService {
public:
  static const Command GET_OUTPUT_PATH;
  static const Command GET_OUTPUT_INDEX_PATH;
  static const Command GET_SPILL_PATH;
  static const Command GET_COMBINE_HANDLER;

public:
  AbstractMapHandler() {}

  virtual ~AbstractMapHandler() {}

  virtual void configure(Config * config) {
    _config = config;
  }

  virtual string * getOutputPath() {
    ResultBuffer * outputPathResult = call(GET_OUTPUT_PATH, NULL);
    if (NULL == outputPathResult) {
      return NULL;
    }
    string * outputPath = outputPathResult->readString();

    delete outputPathResult;
    return outputPath;
  }

  virtual string * getOutputIndexPath() {

    ResultBuffer * outputIndexPath = call(GET_OUTPUT_INDEX_PATH, NULL);
    if (NULL == outputIndexPath) {
      return NULL;
    }
    string * indexpath = outputIndexPath->readString();
    delete outputIndexPath;
    return indexpath;
  }


  virtual string * getSpillPath() {
    ResultBuffer * spillPathBuffer = call(GET_SPILL_PATH, NULL);
    if (NULL == spillPathBuffer) {
      return NULL;
    }
    string * spillpath = spillPathBuffer->readString();
    delete spillPathBuffer;
    return spillpath;
  }

  virtual CombineHandler * getJavaCombineHandler() {

    LOG("[MapOutputCollector::configure] java combiner is configured");

    ResultBuffer * getCombineHandlerResult = call(GET_COMBINE_HANDLER, NULL);
    if (NULL != getCombineHandlerResult) {

      getCombineHandlerResult->setReadPoint(0);

      CombineHandler * javaCombiner = (CombineHandler *)((BatchHandler * )(getCombineHandlerResult->readPointer()));
      delete getCombineHandlerResult;
      return javaCombiner;
    }



    return NULL;
  }

};

} // namespace NativeTask

#endif /* MMAPPERHANDLER_H_ */
