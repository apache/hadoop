#include <jni.h>
#include <stdio.h>
#include <iostream>
#include <string>
using namespace std;

#include "exception_jni.h"
#include "org_tensorflow_bridge_TFServer.h"
#include "tensorflow/core/distributed_runtime/server_lib.h"

using tensorflow::ServerDef;

/*
 * Class:     org_tensorflow_bridge_TFServer
 * Method:    createServer
 * Signature: ([B)J
 */
JNIEXPORT jlong JNICALL Java_org_tensorflow_bridge_TFServer_createServer
  (JNIEnv * env, jobject jobj, jbyteArray array) {

  jbyte* elements = env->GetByteArrayElements(array, NULL);
  jsize textLength = env->GetArrayLength(array);
  char* b = new char[textLength + 1];
  memcpy(b, elements, textLength);
  b[textLength] = '\0';

  env->ReleaseByteArrayElements(array, elements, JNI_ABORT);

  std::unique_ptr< tensorflow::ServerInterface > *arg2 = (std::unique_ptr< tensorflow::ServerInterface > *) 0 ;
  std::unique_ptr< tensorflow::ServerInterface > temp2 ;
  arg2 = &temp2;

  ServerDef *arg1 = 0 ;
  tensorflow::ServerDef temp1 ;
  if(!temp1.ParseFromString(string(b, textLength))) {
    throwException(env, kTFServerException,
                       "The ServerDef could not be parsed as a valid protocol buffer");
    return -1;
  }
//  cout << temp1.DebugString() << "\n";
  arg1 = &temp1;

  tensorflow::Status status = tensorflow::NewServer((ServerDef const &)*arg1, arg2);
  if (!status.ok()) {
    throwException(env, kTFServerException, status.error_message().c_str());
    return -1;
  }

  tensorflow::ServerInterface * server = arg2->release();
  return (jlong)std::addressof(*server);
}

/*
 * Class:     org_tensorflow_bridge_TFServer
 * Method:    startServer
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_tensorflow_bridge_TFServer_startServer
  (JNIEnv * env, jobject jobj, jlong serverAddr) {
  long pointer = (long)serverAddr;
  tensorflow::ServerInterface* server = (tensorflow::ServerInterface*)pointer;
  server->Start();
}

/*
 * Class:     org_tensorflow_bridge_TFServer
 * Method:    join
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_tensorflow_bridge_TFServer_join
  (JNIEnv * env, jobject jobj, jlong serverAddr) {
  long pointer = (long)serverAddr;
  tensorflow::ServerInterface* server = (tensorflow::ServerInterface*)pointer;
  server->Join();
}

/*
 * Class:     org_tensorflow_bridge_TFServer
 * Method:    stop
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_tensorflow_bridge_TFServer_stop
  (JNIEnv * env, jobject jobj, jlong serverAddr) {
  long pointer = (long)serverAddr;
  tensorflow::ServerInterface* server = (tensorflow::ServerInterface*)pointer;
  server->Stop();
}

/*
 * Class:     org_tensorflow_bridge_TFServer
 * Method:    target
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_tensorflow_bridge_TFServer_target
  (JNIEnv * env, jobject jobj, jlong serverAddr) {
  long pointer = (long)serverAddr;
  tensorflow::ServerInterface* server = (tensorflow::ServerInterface*)pointer;
  string target = server->target();
  return env->NewStringUTF(target.c_str());
}