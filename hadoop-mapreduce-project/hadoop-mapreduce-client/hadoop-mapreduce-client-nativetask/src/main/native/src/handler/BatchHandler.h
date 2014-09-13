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

#ifndef BATCHHANDLER_H_
#define BATCHHANDLER_H_

#include "NativeTask.h"
#include "lib/Buffers.h"

namespace NativeTask {

/**
 * Native side counterpart of java side NativeBatchProcessor
 */
class BatchHandler : public Configurable {
protected:
  ByteBuffer _in;
  ByteBuffer _out;
  void * _processor;
  Config * _config;
public:
  BatchHandler();
  virtual ~BatchHandler();

  virtual NativeObjectType type() {
    return BatchHandlerType;
  }

  /**
   * Called by native jni functions to set global jni reference
   */
  void setProcessor(void * processor) {
    _processor = processor;
  }

  void releaseProcessor();

  /**
   * Called by java side to setup native side BatchHandler
   * initialize buffers by default
   */
  void onSetup(Config * config, char * inputBuffer, uint32_t inputBufferCapacity,
      char * outputBuffer, uint32_t outputBufferCapacity);

  /**
   * Called by java side to notice that input data available to handle
   * @param length input buffer's available data length
   */
  void onInputData(uint32_t length);

  virtual void onLoadData() {
  }

  /**
   * Called by java side to notice that input has finished
   */
  void onFinish() {
    finish();
  }

  /**
   * Called by java side to send command to this handler
   * BatchHandler ignore all command by default
   * @param cmd command data
   * @return command return value
   */
  virtual ResultBuffer * onCall(const Command& command, ReadWriteBuffer * param) {
    return NULL;
  }

protected:
  virtual ResultBuffer * call(const Command& cmd, ParameterBuffer * param);

  /**
   * Used by subclass, call java side flushOutput(int length)
   * @param length output buffer's available data length
   */
  virtual void flushOutput();

  /**
   * Used by subclass, call java side finishOutput()
   */
  void finishOutput();

  /**
   * Write output buffer and use flushOutput manually,
   * or use this helper method
   */
  inline void output(const char * buff, uint32_t length) {
    while (length > 0) {
      uint32_t remain = _out.remain();
      if (length > remain) {
        flushOutput();
      }
      uint32_t cp = length < remain ? length : remain;
      simple_memcpy(_out.current(), buff, cp);
      buff += cp;
      length -= cp;
      _out.advance(cp);
    }
  }

  inline void outputInt(uint32_t v) {
    if (4 > _out.remain()) {
      flushOutput();
    }
    *(uint32_t*)(_out.current()) = v;
    _out.advance(4);
  }

  /////////////////////////////////////////////////////////////
  // Subclass should implement these if needed
  /////////////////////////////////////////////////////////////

  /**
   * Called by onSetup, do nothing by default
   * Subclass should override this if needed
   */
  virtual void configure(Config * config) {
  }

  /**
   * Called by onFinish, flush & close output by default
   * Subclass should override this if needed
   */
  virtual void finish() {
    flushOutput();
    finishOutput();
  }
  ;

  /**
   * Called by onInputData, internal input data processor,
   * Subclass should override this if needed
   */
  virtual void handleInput(ByteBuffer & byteBuffer) {
  }
};

} // namespace NativeTask

#endif /* BATCHHANDLER_H_ */
