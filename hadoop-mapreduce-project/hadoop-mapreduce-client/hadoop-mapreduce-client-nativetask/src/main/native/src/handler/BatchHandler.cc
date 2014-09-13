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

#ifndef QUICK_BUILD
#include "org_apache_hadoop_mapred_nativetask_NativeBatchProcessor.h"
#endif
#include "lib/commons.h"
#include "jni_md.h"
#include "lib/jniutils.h"
#include "BatchHandler.h"
#include "lib/NativeObjectFactory.h"

///////////////////////////////////////////////////////////////
// NativeBatchProcessor jni util methods
///////////////////////////////////////////////////////////////

static jfieldID InputBufferFieldID = NULL;
static jfieldID OutputBufferFieldID = NULL;
static jmethodID FlushOutputMethodID = NULL;
static jmethodID FinishOutputMethodID = NULL;
static jmethodID SendCommandToJavaMethodID = NULL;

///////////////////////////////////////////////////////////////
// BatchHandler methods
///////////////////////////////////////////////////////////////

namespace NativeTask {

ReadWriteBuffer * JNU_ByteArraytoReadWriteBuffer(JNIEnv * jenv, jbyteArray src) {
  if (NULL == src) {
    return NULL;
  }
  jsize len = jenv->GetArrayLength(src);

  ReadWriteBuffer * ret = new ReadWriteBuffer(len);
  jenv->GetByteArrayRegion(src, 0, len, (jbyte*)ret->getBuff());
  ret->setWritePoint(len);
  return ret;
}

jbyteArray JNU_ReadWriteBufferToByteArray(JNIEnv * jenv, ReadWriteBuffer * result) {
  if (NULL == result || result->getWritePoint() == 0) {
    return NULL;
  }

  jbyteArray ret = jenv->NewByteArray(result->getWritePoint());
  jenv->SetByteArrayRegion(ret, 0, result->getWritePoint(), (jbyte*)result->getBuff());
  return ret;
}

BatchHandler::BatchHandler()
    : _processor(NULL), _config(NULL) {
}

BatchHandler::~BatchHandler() {
  releaseProcessor();
  if (NULL != _config) {
    delete _config;
    _config = NULL;
  }
}

void BatchHandler::releaseProcessor() {
  if (_processor != NULL) {
    JNIEnv * env = JNU_GetJNIEnv();
    env->DeleteGlobalRef((jobject)_processor);
    _processor = NULL;
  }
}

void BatchHandler::onInputData(uint32_t length) {
  _in.rewind(0, length);
  handleInput(_in);
}

void BatchHandler::flushOutput() {

  if (NULL == _out.base()) {
    return;
  }

  uint32_t length = _out.position();
  _out.position(0);

  if (length == 0) {
    return;
  }

  JNIEnv * env = JNU_GetJNIEnv();
  env->CallVoidMethod((jobject)_processor, FlushOutputMethodID, (jint)length);
  if (env->ExceptionCheck()) {
    THROW_EXCEPTION(JavaException, "FlushOutput throw exception");
  }
}

void BatchHandler::finishOutput() {
  if (NULL == _out.base()) {
    return;
  }
  JNIEnv * env = JNU_GetJNIEnv();
  env->CallVoidMethod((jobject)_processor, FinishOutputMethodID);
  if (env->ExceptionCheck()) {
    THROW_EXCEPTION(JavaException, "FinishOutput throw exception");
  }
}

void BatchHandler::onSetup(Config * config, char * inputBuffer, uint32_t inputBufferCapacity,
    char * outputBuffer, uint32_t outputBufferCapacity) {
  this->_config = config;
  _in.reset(inputBuffer, inputBufferCapacity);
  if (NULL != outputBuffer) {
    if (outputBufferCapacity <= 1024) {
      THROW_EXCEPTION(IOException, "Output buffer size too small for BatchHandler");
    }
    _out.reset(outputBuffer, outputBufferCapacity);
    _out.rewind(0, outputBufferCapacity);

    LOG("[BatchHandler::onSetup] input Capacity %d, output capacity %d",
        inputBufferCapacity, _out.limit());
  }
  configure(_config);
}

ResultBuffer * BatchHandler::call(const Command& cmd, ParameterBuffer * param) {
  JNIEnv * env = JNU_GetJNIEnv();
  jbyteArray jcmdData = JNU_ReadWriteBufferToByteArray(env, param);
  jbyteArray ret = (jbyteArray)env->CallObjectMethod((jobject)_processor, SendCommandToJavaMethodID,
      cmd.id(), jcmdData);


  if (env->ExceptionCheck()) {
    THROW_EXCEPTION(JavaException, "SendCommandToJava throw exception");
  }
  return JNU_ByteArraytoReadWriteBuffer(env, ret);
}

} // namespace NativeTask

///////////////////////////////////////////////////////////////
// NativeBatchProcessor jni methods
///////////////////////////////////////////////////////////////
using namespace NativeTask;

/*
 * Class:     org_apache_hadoop_mapred_nativetask_NativeBatchProcessor
 * Method:    setupHandler
 * Signature: (J)V
 */
void JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeBatchProcessor_setupHandler(
    JNIEnv * jenv, jobject processor, jlong handler, jobjectArray configs) {
  try {

    NativeTask::Config * config = new NativeTask::Config();
    jsize len = jenv->GetArrayLength(configs);
    for (jsize i = 0; i + 1 < len; i += 2) {
      jbyteArray key_obj = (jbyteArray)jenv->GetObjectArrayElement(configs, i);
      jbyteArray val_obj = (jbyteArray)jenv->GetObjectArrayElement(configs, i + 1);
      config->set(JNU_ByteArrayToString(jenv, key_obj), JNU_ByteArrayToString(jenv, val_obj));
    }

    NativeTask::BatchHandler * batchHandler = (NativeTask::BatchHandler *)((void*)handler);
    if (NULL == batchHandler) {
      JNU_ThrowByName(jenv, "java/lang/IllegalArgumentException", "BatchHandler is null");
      return;
    }
    jobject jinputBuffer = jenv->GetObjectField(processor, InputBufferFieldID);
    char * inputBufferAddr = NULL;
    uint32_t inputBufferCapacity = 0;
    if (NULL != jinputBuffer) {
      inputBufferAddr = (char*)(jenv->GetDirectBufferAddress(jinputBuffer));
      inputBufferCapacity = jenv->GetDirectBufferCapacity(jinputBuffer);
    }
    jobject joutputBuffer = jenv->GetObjectField(processor, OutputBufferFieldID);
    char * outputBufferAddr = NULL;
    uint32_t outputBufferCapacity = 0;
    if (NULL != joutputBuffer) {
      outputBufferAddr = (char*)(jenv->GetDirectBufferAddress(joutputBuffer));
      outputBufferCapacity = jenv->GetDirectBufferCapacity(joutputBuffer);
    }
    batchHandler->setProcessor(jenv->NewGlobalRef(processor));
    batchHandler->onSetup(config, inputBufferAddr, inputBufferCapacity, outputBufferAddr,
        outputBufferCapacity);
  } catch (NativeTask::UnsupportException & e) {
    JNU_ThrowByName(jenv, "java/lang/UnsupportedOperationException", e.what());
  } catch (NativeTask::OutOfMemoryException & e) {
    JNU_ThrowByName(jenv, "java/lang/OutOfMemoryError", e.what());
  } catch (NativeTask::IOException & e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  } catch (NativeTask::JavaException & e) {
    LOG("JavaException: %s", e.what());
    // Do nothing, let java side handle
  } catch (std::exception & e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  } catch (...) {
    JNU_ThrowByName(jenv, "java/io/IOException", "Unknown exception");
  }
}

/*
 * Class:     org_apache_hadoop_mapred_nativetask_NativeBatchProcessor
 * Method:    nativeProcessInput
 * Signature: (JI)V
 */
void JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeBatchProcessor_nativeProcessInput(
    JNIEnv * jenv, jobject processor, jlong handler, jint length) {

  try {
    NativeTask::BatchHandler * batchHandler = (NativeTask::BatchHandler *)((void*)handler);
    if (NULL == batchHandler) {
      JNU_ThrowByName(jenv, "java/lang/IllegalArgumentException",
          "handler not instance of BatchHandler");
      return;
    }
    batchHandler->onInputData(length);
  } catch (NativeTask::UnsupportException & e) {
    JNU_ThrowByName(jenv, "java/lang/UnsupportedOperationException", e.what());
  } catch (NativeTask::OutOfMemoryException & e) {
    JNU_ThrowByName(jenv, "java/lang/OutOfMemoryError", e.what());
  } catch (NativeTask::IOException & e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  } catch (NativeTask::JavaException & e) {
    LOG("JavaException: %s", e.what());
    // Do nothing, let java side handle
  } catch (std::exception & e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  } catch (...) {
    JNU_ThrowByName(jenv, "java/io/IOException", "Unknown exception");
  }
}

/*
 * Class:     org_apache_hadoop_mapred_nativetask_NativeBatchProcessor
 * Method:    nativeFinish
 * Signature: (J)V
 */
void JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeBatchProcessor_nativeFinish(
    JNIEnv * jenv, jobject processor, jlong handler) {
  try {
    NativeTask::BatchHandler * batchHandler = (NativeTask::BatchHandler *)((void*)handler);
    if (NULL == batchHandler) {
      JNU_ThrowByName(jenv, "java/lang/IllegalArgumentException",
          "handler not instance of BatchHandler");
      return;
    }
    batchHandler->onFinish();
  } catch (NativeTask::UnsupportException & e) {
    JNU_ThrowByName(jenv, "java/lang/UnsupportedOperationException", e.what());
  } catch (NativeTask::OutOfMemoryException & e) {
    JNU_ThrowByName(jenv, "java/lang/OutOfMemoryError", e.what());
  } catch (NativeTask::IOException & e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  } catch (NativeTask::JavaException & e) {
    LOG("JavaException: %s", e.what());
    // Do nothing, let java side handle
  } catch (std::exception & e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  } catch (...) {
    JNU_ThrowByName(jenv, "java/io/IOException", "Unknown exception");
  }
}

void JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeBatchProcessor_nativeLoadData(
    JNIEnv * jenv, jobject processor, jlong handler) {
  try {
    NativeTask::BatchHandler * batchHandler = (NativeTask::BatchHandler *)((void*)handler);
    if (NULL == batchHandler) {
      JNU_ThrowByName(jenv, "java/lang/IllegalArgumentException",
          "handler not instance of BatchHandler");
      return;
    }
    batchHandler->onLoadData();
  } catch (NativeTask::UnsupportException & e) {
    JNU_ThrowByName(jenv, "java/lang/UnsupportedOperationException", e.what());
  } catch (NativeTask::OutOfMemoryException & e) {
    JNU_ThrowByName(jenv, "java/lang/OutOfMemoryError", e.what());
  } catch (NativeTask::IOException & e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  } catch (NativeTask::JavaException & e) {
    LOG("JavaException: %s", e.what());
    // Do nothing, let java side handle
  } catch (std::exception & e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  } catch (...) {
    JNU_ThrowByName(jenv, "java/io/IOException", "Unknown exception");
  }
}

/*
 * Class:     org_apache_hadoop_mapred_nativetask_NativeBatchProcessor
 * Method:    nativeCommand
 * Signature: (J[B)[B
 */
jbyteArray JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeBatchProcessor_nativeCommand(
    JNIEnv * jenv, jobject processor, jlong handler, jint command, jbyteArray cmdData) {
  try {
    NativeTask::BatchHandler * batchHandler = (NativeTask::BatchHandler *)((void*)handler);
    if (NULL == batchHandler) {
      JNU_ThrowByName(jenv, "java/lang/IllegalArgumentException",
          "handler not instance of BatchHandler");
      return NULL;
    }
    Command cmd(command);
    ParameterBuffer * param = JNU_ByteArraytoReadWriteBuffer(jenv, cmdData);
    ResultBuffer * result = batchHandler->onCall(cmd, param);
    jbyteArray ret = JNU_ReadWriteBufferToByteArray(jenv, result);

    delete result;
    delete param;
    return ret;
  } catch (NativeTask::UnsupportException & e) {
    JNU_ThrowByName(jenv, "java/lang/UnsupportedOperationException", e.what());
  } catch (NativeTask::OutOfMemoryException & e) {
    JNU_ThrowByName(jenv, "java/lang/OutOfMemoryError", e.what());
  } catch (NativeTask::IOException & e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  } catch (const NativeTask::JavaException & e) {
    LOG("JavaException: %s", e.what());
    // Do nothing, let java side handle
  } catch (std::exception & e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  } catch (...) {
    JNU_ThrowByName(jenv, "java/io/IOException", "Unknown exception");
  }
  return NULL;
}

/*
 * Class:     org_apace_hadoop_mapred_nativetask_NativeBatchProcessor
 * Method:    InitIDs
 * Signature: ()V
 */
void JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeBatchProcessor_InitIDs(JNIEnv * jenv,
    jclass processorClass) {
  InputBufferFieldID = jenv->GetFieldID(processorClass, "rawOutputBuffer", "Ljava/nio/ByteBuffer;");
  OutputBufferFieldID = jenv->GetFieldID(processorClass, "rawInputBuffer", "Ljava/nio/ByteBuffer;");
  FlushOutputMethodID = jenv->GetMethodID(processorClass, "flushOutput", "(I)V");
  FinishOutputMethodID = jenv->GetMethodID(processorClass, "finishOutput", "()V");
  SendCommandToJavaMethodID = jenv->GetMethodID(processorClass, "sendCommandToJava", "(I[B)[B");
}

