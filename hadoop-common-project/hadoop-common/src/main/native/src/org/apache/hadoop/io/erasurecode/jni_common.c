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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "org_apache_hadoop.h"
#include "isal_load.h"
#include "erasure_code.h"
#include "jni_common.h"

void loadLib(JNIEnv *env) {
  char errMsg[1024];
  load_erasurecode_lib(errMsg, sizeof(errMsg));
  if (strlen(errMsg) > 0) {
    THROW(env, "java/lang/UnsatisfiedLinkError", errMsg);
  }
}

void setCoder(JNIEnv* env, jobject thiz, IsalCoder* pCoder) {
  jclass clazz = (*env)->GetObjectClass(env, thiz);
  jfieldID __coderState = (*env)->GetFieldID(env, clazz, "__native_coder", "J");
  (*env)->SetLongField(env, thiz, __coderState, (jlong) pCoder);
}

IsalCoder* getCoder(JNIEnv* env, jobject thiz) {
  jclass clazz = (*env)->GetObjectClass(env, thiz);

  jfieldID __verbose = (*env)->GetFieldID(env, clazz, "__native_verbose", "J");
  int verbose = (int)(*env)->GetIntField(env, thiz, __verbose);

  jfieldID __coderState = (*env)->GetFieldID(env, clazz, "__native_coder", "J");
  IsalCoder* pCoder = (IsalCoder*)(*env)->GetLongField(env,
                                                       thiz, __coderState);
  pCoder->verbose = verbose;

  return pCoder;
}

void getInputs(JNIEnv *env, jobjectArray inputs, jintArray inputOffsets,
                              unsigned char** destInputs, int num) {
  int numInputs = (*env)->GetArrayLength(env, inputs);
  int* tmpInputOffsets;
  int i;
  jobject byteBuffer;

  if (numInputs != num) {
    THROW(env, "java/lang/InternalError", "Invalid inputs");
  }

  tmpInputOffsets = (int*)(*env)->GetIntArrayElements(env,
                                                      inputOffsets, NULL);
  for (i = 0; i < numInputs; i++) {
    byteBuffer = (*env)->GetObjectArrayElement(env, inputs, i);
    if (byteBuffer != NULL) {
      destInputs[i] = (unsigned char *)((*env)->GetDirectBufferAddress(env,
                                                                byteBuffer));
      destInputs[i] += tmpInputOffsets[i];
    } else {
      destInputs[i] = NULL;
    }
  }
}

void getOutputs(JNIEnv *env, jobjectArray outputs, jintArray outputOffsets,
                              unsigned char** destOutputs, int num) {
  int numOutputs = (*env)->GetArrayLength(env, outputs);
  int i, *tmpOutputOffsets;
  jobject byteBuffer;

  if (numOutputs != num) {
    THROW(env, "java/lang/InternalError", "Invalid outputs");
  }

  tmpOutputOffsets = (int*)(*env)->GetIntArrayElements(env,
                                                          outputOffsets, NULL);
  for (i = 0; i < numOutputs; i++) {
    byteBuffer = (*env)->GetObjectArrayElement(env, outputs, i);
    destOutputs[i] = (unsigned char *)((*env)->GetDirectBufferAddress(env,
                                                                  byteBuffer));
    destOutputs[i] += tmpOutputOffsets[i];
  }
}