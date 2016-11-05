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

#include "erasure_code.h"
#include "gf_util.h"
#include "erasure_coder.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void dump(unsigned char* buf, int len) {
  int i;
  for (i = 0; i < len;) {
    printf(" %2x", 0xff & buf[i++]);
    if (i % 32 == 0)
      printf("\n");
  }
}

void dumpMatrix(unsigned char** buf, int n1, int n2) {
  int i, j;
  for (i = 0; i < n1; i++) {
    for (j = 0; j < n2; j++) {
      printf(" %2x", buf[i][j]);
    }
    printf("\n");
  }
  printf("\n");
}

void dumpCodingMatrix(unsigned char* buf, int n1, int n2) {
  int i, j;
  for (i = 0; i < n1; i++) {
    for (j = 0; j < n2; j++) {
      printf(" %d", 0xff & buf[j + (i * n2)]);
    }
    printf("\n");
  }
  printf("\n");
}

void dumpEncoder(IsalEncoder* pCoder) {
  int numDataUnits = pCoder->coder.numDataUnits;
  int numParityUnits = pCoder->coder.numParityUnits;
  int numAllUnits = pCoder->coder.numAllUnits;

  printf("Encoding (numAllUnits = %d, numParityUnits = %d, numDataUnits = %d)\n",
                                    numAllUnits, numParityUnits, numDataUnits);

  printf("\n\nEncodeMatrix:\n");
  dumpCodingMatrix((unsigned char*) pCoder->encodeMatrix,
                                           numDataUnits, numAllUnits);
}

void dumpDecoder(IsalDecoder* pCoder) {
  int i, j;
  int numDataUnits = pCoder->coder.numDataUnits;
  int numAllUnits = pCoder->coder.numAllUnits;

  printf("Recovering (numAllUnits = %d, numDataUnits = %d, numErased = %d)\n",
                       numAllUnits, numDataUnits, pCoder->numErased);

  printf(" - ErasedIndexes = ");
  for (j = 0; j < pCoder->numErased; j++) {
    printf(" %d", pCoder->erasedIndexes[j]);
  }
  printf("       - DecodeIndex = ");
  for (i = 0; i < numDataUnits; i++) {
    printf(" %d", pCoder->decodeIndex[i]);
  }

  printf("\n\nEncodeMatrix:\n");
  dumpCodingMatrix((unsigned char*) pCoder->encodeMatrix,
                                    numDataUnits, numAllUnits);

  printf("InvertMatrix:\n");
  dumpCodingMatrix((unsigned char*) pCoder->invertMatrix,
                                   numDataUnits, numAllUnits);

  printf("DecodeMatrix:\n");
  dumpCodingMatrix((unsigned char*) pCoder->decodeMatrix,
                                    numDataUnits, numAllUnits);
}

