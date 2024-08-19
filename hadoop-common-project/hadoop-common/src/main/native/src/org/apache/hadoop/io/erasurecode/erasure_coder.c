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
#include "dump.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void initCoder(IsalCoder* pCoder, int numDataUnits, int numParityUnits) {
  pCoder->verbose = 0;
  pCoder->numParityUnits = numParityUnits;
  pCoder->numDataUnits = numDataUnits;
  pCoder->numAllUnits = numDataUnits + numParityUnits;
}

// 0 not to verbose, 1 to verbose
void allowVerbose(IsalCoder* pCoder, int flag) {
  pCoder->verbose = flag;
}

static void initEncodeMatrix(int numDataUnits, int numParityUnits,
                                                unsigned char* encodeMatrix) {
  // Generate encode matrix, always invertible
  h_gf_gen_cauchy_matrix(encodeMatrix,
                          numDataUnits + numParityUnits, numDataUnits);
}

void initEncoder(IsalEncoder* pCoder, int numDataUnits,
                            int numParityUnits) {
  initCoder(&pCoder->coder, numDataUnits, numParityUnits);

  initEncodeMatrix(numDataUnits, numParityUnits, pCoder->encodeMatrix);

  // Generate gftbls from encode matrix
  h_ec_init_tables(numDataUnits, numParityUnits,
               &pCoder->encodeMatrix[numDataUnits * numDataUnits],
               pCoder->gftbls);

  if (pCoder->coder.verbose > 0) {
    dumpEncoder(pCoder);
  }
}

void initDecoder(IsalDecoder* pCoder, int numDataUnits,
                                  int numParityUnits) {
  initCoder(&pCoder->coder, numDataUnits, numParityUnits);

  initEncodeMatrix(numDataUnits, numParityUnits, pCoder->encodeMatrix);
}

int encode(IsalEncoder* pCoder, unsigned char** dataUnits,
    unsigned char** parityUnits, int chunkSize) {
  int numDataUnits = pCoder->coder.numDataUnits;
  int numParityUnits = pCoder->coder.numParityUnits;
  int i;

  for (i = 0; i < numParityUnits; i++) {
    memset(parityUnits[i], 0, chunkSize);
  }

  h_ec_encode_data(chunkSize, numDataUnits, numParityUnits,
                         pCoder->gftbls, dataUnits, parityUnits);

  return 0;
}

// Return 1 when diff, 0 otherwise
static int compare(int* arr1, int len1, int* arr2, int len2) {
  int i;

  if (len1 == len2) {
    for (i = 0; i < len1; i++) {
      if (arr1[i] != arr2[i]) {
        return 1;
      }
    }
    return 0;
  }

  return 1;
}

static int processErasures(IsalDecoder* pCoder, unsigned char** inputs,
                                    int* erasedIndexes, int numErased) {
  int i, r, ret, index;
  int numDataUnits = pCoder->coder.numDataUnits;
  int isChanged = 0;

  for (i = 0, r = 0; i < numDataUnits; i++, r++) {
    while (inputs[r] == NULL) {
      r++;
    }

    if (pCoder->decodeIndex[i] != r) {
      pCoder->decodeIndex[i] = r;
      isChanged = 1;
    }
  }

  for (i = 0; i < numDataUnits; i++) {
    pCoder->realInputs[i] = inputs[pCoder->decodeIndex[i]];
  }

  if (isChanged == 0 &&
          compare(pCoder->erasedIndexes, pCoder->numErased,
                           erasedIndexes, numErased) == 0) {
    return 0; // Optimization, nothing to do
  }

  clearDecoder(pCoder);

  for (i = 0; i < numErased; i++) {
    index = erasedIndexes[i];
    pCoder->erasedIndexes[i] = index;
    pCoder->erasureFlags[index] = 1;
    if (index < numDataUnits) {
      pCoder->numErasedDataUnits++;
    }
  }

  pCoder->numErased = numErased;

  ret = generateDecodeMatrix(pCoder);
  if (ret != 0) {
    printf("Failed to generate decode matrix\n");
    return -1;
  }

  h_ec_init_tables(numDataUnits, pCoder->numErased,
                      pCoder->decodeMatrix, pCoder->gftbls);

  if (pCoder->coder.verbose > 0) {
    dumpDecoder(pCoder);
  }

  return 0;
}

int decode(IsalDecoder* pCoder, unsigned char** inputs,
                  int* erasedIndexes, int numErased,
                   unsigned char** outputs, int chunkSize) {
  int numDataUnits = pCoder->coder.numDataUnits;
  int i;

  processErasures(pCoder, inputs, erasedIndexes, numErased);

  for (i = 0; i < numErased; i++) {
    memset(outputs[i], 0, chunkSize);
  }

  h_ec_encode_data(chunkSize, numDataUnits, pCoder->numErased,
      pCoder->gftbls, pCoder->realInputs, outputs);

  return 0;
}

// Clear variables used per decode call
void clearDecoder(IsalDecoder* decoder) {
  decoder->numErasedDataUnits = 0;
  decoder->numErased = 0;
  memset(decoder->gftbls, 0, sizeof(decoder->gftbls));
  memset(decoder->decodeMatrix, 0, sizeof(decoder->decodeMatrix));
  memset(decoder->tmpMatrix, 0, sizeof(decoder->tmpMatrix));
  memset(decoder->invertMatrix, 0, sizeof(decoder->invertMatrix));
  memset(decoder->erasureFlags, 0, sizeof(decoder->erasureFlags));
  memset(decoder->erasedIndexes, 0, sizeof(decoder->erasedIndexes));
}

// Generate decode matrix from encode matrix
int generateDecodeMatrix(IsalDecoder* pCoder) {
  int i, j, r, p;
  unsigned char s;
  int numDataUnits;

  numDataUnits = pCoder->coder.numDataUnits;

  // Construct matrix b by removing error rows
  for (i = 0; i < numDataUnits; i++) {
    r = pCoder->decodeIndex[i];
    for (j = 0; j < numDataUnits; j++) {
      pCoder->tmpMatrix[numDataUnits * i + j] =
                pCoder->encodeMatrix[numDataUnits * r + j];
    }
  }

  h_gf_invert_matrix(pCoder->tmpMatrix,
                                pCoder->invertMatrix, numDataUnits);

  for (i = 0; i < pCoder->numErasedDataUnits; i++) {
    for (j = 0; j < numDataUnits; j++) {
      pCoder->decodeMatrix[numDataUnits * i + j] =
                      pCoder->invertMatrix[numDataUnits *
                      pCoder->erasedIndexes[i] + j];
    }
  }

  for (p = pCoder->numErasedDataUnits; p < pCoder->numErased; p++) {
    for (i = 0; i < numDataUnits; i++) {
      s = 0;
      for (j = 0; j < numDataUnits; j++) {
        s ^= h_gf_mul(pCoder->invertMatrix[j * numDataUnits + i],
          pCoder->encodeMatrix[numDataUnits *
                                        pCoder->erasedIndexes[p] + j]);
      }

      pCoder->decodeMatrix[numDataUnits * p + i] = s;
    }
  }

  return 0;
}
