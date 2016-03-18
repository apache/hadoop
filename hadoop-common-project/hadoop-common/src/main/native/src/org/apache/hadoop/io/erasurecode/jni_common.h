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

#ifndef _JNI_CODER_COMMON_H_
#define _JNI_CODER_COMMON_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <jni.h>

#include "erasure_coder.h"

void loadLib(JNIEnv *env);

void setCoder(JNIEnv* env, jobject thiz, IsalCoder* coder);

IsalCoder* getCoder(JNIEnv* env, jobject thiz);

void getInputs(JNIEnv *env, jobjectArray inputs, jintArray inputOffsets,
                              unsigned char** destInputs, int num);

void getOutputs(JNIEnv *env, jobjectArray outputs, jintArray outputOffsets,
                              unsigned char** destOutputs, int num);

#endif //_JNI_CODER_COMMON_H_