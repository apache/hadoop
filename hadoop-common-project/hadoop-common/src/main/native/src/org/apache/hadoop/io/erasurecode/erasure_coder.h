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

/**
 * This is a sample program illustrating how to use the Intel ISA-L library.
 * Note it's adapted from erasure_code_test.c test program, but trying to use
 * variable names and styles we're more familiar with already similar to Java
 * coders.
 */

#ifndef _ERASURE_CODER_H_
#define _ERASURE_CODER_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MMAX 14
#define KMAX 10

typedef struct _IsalCoder {
  int verbose;
  int numParityUnits;
  int numDataUnits;
  int numAllUnits;
} IsalCoder;

typedef struct _IsalEncoder {
  IsalCoder coder;

  unsigned char gftbls[MMAX * KMAX * 32];

  unsigned char encodeMatrix[MMAX * KMAX];
} IsalEncoder;

typedef struct _IsalDecoder {
  IsalCoder coder;

  unsigned char encodeMatrix[MMAX * KMAX];

  // Below are per decode call
  unsigned char gftbls[MMAX * KMAX * 32];
  unsigned int decodeIndex[MMAX];
  unsigned char tmpMatrix[MMAX * KMAX];
  unsigned char invertMatrix[MMAX * KMAX];
  unsigned char decodeMatrix[MMAX * KMAX];
  unsigned char erasureFlags[MMAX];
  int erasedIndexes[MMAX];
  int numErased;
  int numErasedDataUnits;
  unsigned char* realInputs[MMAX];
} IsalDecoder;

void initCoder(IsalCoder* pCoder, int numDataUnits, int numParityUnits);

void allowVerbose(IsalCoder* pCoder, int flag);

void initEncoder(IsalEncoder* encoder, int numDataUnits, int numParityUnits);

void initDecoder(IsalDecoder* decoder, int numDataUnits, int numParityUnits);

void clearDecoder(IsalDecoder* decoder);

int encode(IsalEncoder* encoder, unsigned char** dataUnits,
    unsigned char** parityUnits, int chunkSize);

int decode(IsalDecoder* decoder, unsigned char** allUnits,
    int* erasedIndexes, int numErased,
    unsigned char** recoveredUnits, int chunkSize);

int generateDecodeMatrix(IsalDecoder* pCoder);

#endif //_ERASURE_CODER_H_
