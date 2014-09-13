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
#include "org_apache_hadoop_mapred_nativetask_NativeRuntime.h"
#endif
#include "config.h"
#include "lib/commons.h"
#include "lib/jniutils.h"
#include "lib/NativeObjectFactory.h"

using namespace NativeTask;

///////////////////////////////////////////////////////////////
// NativeRuntime JNI methods
///////////////////////////////////////////////////////////////

/*
 * Class:     org_apache_hadoop_mapred_nativetask_NativeRuntime
 * Method:    supportCompressionCodec
 * Signature: ([B)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeRuntime_supportsCompressionCodec
  (JNIEnv *jenv, jclass clazz, jbyteArray codec) {
  const std::string codecString = JNU_ByteArrayToString(jenv, codec);
  if ("org.apache.hadoop.io.compress.GzipCodec" == codecString) {
    return JNI_TRUE;
  } else if ("org.apache.hadoop.io.compress.Lz4Codec" == codecString) {
    return JNI_TRUE;
  } else if ("org.apache.hadoop.io.compress.SnappyCodec" == codecString) {
#if defined HADOOP_SNAPPY_LIBRARY
    return JNI_TRUE;
#else
    return JNI_FALSE;
#endif
  } else {
    return JNI_FALSE;
  }
}

/*
 * Class:     org_apache_hadoop_mapred_nativetask_NativeRuntime
 * Method:    JNIRelease
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeRuntime_JNIRelease(
    JNIEnv * jenv, jclass nativeRuntimeClass) {
  try {
    NativeTask::NativeObjectFactory::Release();
  } catch (NativeTask::UnsupportException & e) {
    JNU_ThrowByName(jenv, "java/lang/UnsupportedOperationException", e.what());
  } catch (NativeTask::OutOfMemoryException & e) {
    JNU_ThrowByName(jenv, "java/lang/OutOfMemoryError", e.what());
  } catch (NativeTask::IOException & e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  } catch (NativeTask::JavaException & e) {
    LOG("[NativeRuntimeJniImpl] JavaException: %s", e.what());
    // Do nothing, let java side handle
  } catch (std::exception & e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  } catch (...) {
    JNU_ThrowByName(jenv, "java/io/IOException", "[NativeRuntimeJniImpl] Unkown std::exception");
  }
}

/*
 * Class:     org_apache_hadoop_mapred_nativetask_NativeRuntime
 * Method:    JNIConfigure
 * Signature: ([[B)V
 */
JNIEXPORT void JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeRuntime_JNIConfigure(
    JNIEnv * jenv, jclass nativeRuntimeClass, jobjectArray configs) {
  try {
    NativeTask::Config & config = NativeTask::NativeObjectFactory::GetConfig();
    jsize len = jenv->GetArrayLength(configs);
    for (jsize i = 0; i + 1 < len; i += 2) {
      jbyteArray key_obj = (jbyteArray)jenv->GetObjectArrayElement(configs, i);
      jbyteArray val_obj = (jbyteArray)jenv->GetObjectArrayElement(configs, i + 1);
      config.set(JNU_ByteArrayToString(jenv, key_obj), JNU_ByteArrayToString(jenv, val_obj));
    }
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
    JNU_ThrowByName(jenv, "java/io/IOException", "Unkown std::exception");
  }
}

/*
 * Class:     org_apache_hadoop_mapred_nativetask_NativeRuntime
 * Method:    JNICreateNativeObject
 * Signature: ([B[B)J
 */
jlong JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeRuntime_JNICreateNativeObject(
    JNIEnv * jenv, jclass nativeRuntimeClass, jbyteArray clazz) {
  try {
    std::string typeString = JNU_ByteArrayToString(jenv, clazz);
    return (jlong)(NativeTask::NativeObjectFactory::CreateObject(typeString));
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
  return 0;
}

/*
 * Class:     org_apache_hadoop_mapred_nativetask_NativeRuntime
 * Method:    JNICreateDefaultNativeObject
 * Signature: ([B)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeRuntime_JNICreateDefaultNativeObject(
    JNIEnv * jenv, jclass nativeRuntimeClass, jbyteArray type) {
  try {
    std::string typeString = JNU_ByteArrayToString(jenv, type);
    NativeTask::NativeObjectType type = NativeTask::NativeObjectTypeFromString(typeString.c_str());
    return (jlong)(NativeTask::NativeObjectFactory::CreateDefaultObject(type));
  } catch (NativeTask::UnsupportException & e) {
    JNU_ThrowByName(jenv, "java/lang/UnsupportedOperationException", e.what());
  } catch (NativeTask::OutOfMemoryException & e) {
    JNU_ThrowByName(jenv, "java/lang/OutOfMemoryError", e.what());
  } catch (NativeTask::IOException & e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  } catch (NativeTask::JavaException & e) {
    LOG("[NativeRuntimeJniImpl] JavaException: %s", e.what());
    // Do nothing, let java side handle
  } catch (std::exception & e) {
    JNU_ThrowByName(jenv, "java/io/IOException", e.what());
  } catch (...) {
    JNU_ThrowByName(jenv, "java/io/IOException", "[NativeRuntimeJniImpl] Unknown exception");
  }
  return 0;
}

/*
 * Class:     org_apache_hadoop_mapred_nativetask_NativeRuntime
 * Method:    JNIReleaseNativeObject
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeRuntime_JNIReleaseNativeObject(
    JNIEnv * jenv, jclass nativeRuntimeClass, jlong objectAddr) {
  try {
    NativeTask::NativeObject * nobj = ((NativeTask::NativeObject *)objectAddr);
    if (NULL == nobj) {
      JNU_ThrowByName(jenv, "java/lang/IllegalArgumentException",
          "Object addr not instance of NativeObject");
      return;
    }
    NativeTask::NativeObjectFactory::ReleaseObject(nobj);
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
 * Class:     org_apache_hadoop_mapred_nativetask_NativeRuntime
 * Method:    JNIRegisterModule
 * Signature: ([B[B)I
 */
JNIEXPORT jint JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeRuntime_JNIRegisterModule(
    JNIEnv * jenv, jclass nativeRuntimeClass, jbyteArray modulePath, jbyteArray moduleName) {
  try {
    std::string pathString = JNU_ByteArrayToString(jenv, modulePath);
    std::string nameString = JNU_ByteArrayToString(jenv, moduleName);
    if (NativeTask::NativeObjectFactory::RegisterLibrary(pathString, nameString)) {
      return 0;
    }
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
  return 1;
}

/*
 * Class:     org_apache_hadoop_mapred_nativetask_NativeRuntime
 * Method:    JNIUpdateStatus
 * Signature: ()[B
 */
JNIEXPORT jbyteArray JNICALL Java_org_apache_hadoop_mapred_nativetask_NativeRuntime_JNIUpdateStatus(
    JNIEnv * jenv, jclass nativeRuntimeClass) {
  try {
    std::string statusData;
    NativeTask::NativeObjectFactory::GetTaskStatusUpdate(statusData);
    jbyteArray ret = jenv->NewByteArray(statusData.length());
    jenv->SetByteArrayRegion(ret, 0, statusData.length(), (jbyte*)statusData.c_str());
    return ret;
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
  return NULL;
}
