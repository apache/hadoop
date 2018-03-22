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
#include "erasure_code.h"
#include "gf_util.h"
#include "jni_common.h"
#include "org_apache_hadoop_io_erasurecode_rawcoder_NativeRSRawEncoder.h"

typedef struct _RSEncoder {
  IsalEncoder encoder;
  unsigned char* inputs[MMAX];
  unsigned char* outputs[MMAX];
} RSEncoder;

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_erasurecode_rawcoder_NativeRSRawEncoder_initImpl(
JNIEnv *env, jobject thiz, jint numDataUnits, jint numParityUnits) {
  RSEncoder* rsEncoder = (RSEncoder*)malloc(sizeof(RSEncoder));
  memset(rsEncoder, 0, sizeof(*rsEncoder));
  initEncoder(&rsEncoder->encoder, (int)numDataUnits, (int)numParityUnits);

  setCoder(env, thiz, &rsEncoder->encoder.coder);
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_erasurecode_rawcoder_NativeRSRawEncoder_encodeImpl(
JNIEnv *env, jobject thiz, jobjectArray inputs, jintArray inputOffsets,
jint dataLen, jobjectArray outputs, jintArray outputOffsets) {
  RSEncoder* rsEncoder = (RSEncoder*)getCoder(env, thiz);
  if (!rsEncoder) {
    THROW(env, "java/io/IOException", "NativeRSRawEncoder closed");
    return;
  }

  int numDataUnits = rsEncoder->encoder.coder.numDataUnits;
  int numParityUnits = rsEncoder->encoder.coder.numParityUnits;
  int chunkSize = (int)dataLen;

  getInputs(env, inputs, inputOffsets, rsEncoder->inputs, numDataUnits);
  getOutputs(env, outputs, outputOffsets, rsEncoder->outputs, numParityUnits);

  encode(&rsEncoder->encoder, rsEncoder->inputs, rsEncoder->outputs, chunkSize);
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_erasurecode_rawcoder_NativeRSRawEncoder_destroyImpl(
JNIEnv *env, jobject thiz) {
  RSEncoder* rsEncoder = (RSEncoder*)getCoder(env, thiz);
  if (rsEncoder) {
    free(rsEncoder);
    setCoder(env, thiz, NULL);
  }
}
