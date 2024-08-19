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

#include "isal_load.h"
#include "erasure_code.h"
#include "gf_util.h"
#include "erasure_coder.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char *argv[]) {
  int i, j;
  char err[256];
  size_t err_len = sizeof(err);
  int chunkSize = 1024;
  int numDataUnits = 6;
  int numParityUnits = 3;
  unsigned char** dataUnits;
  unsigned char** parityUnits;
  IsalEncoder* pEncoder;
  int erasedIndexes[2];
  unsigned char* allUnits[MMAX];
  IsalDecoder* pDecoder;
  unsigned char* decodingOutput[2];
  unsigned char** backupUnits;

  if (0 == build_support_erasurecode()) {
    printf("The native library isn't available, skipping this test\n");
    return 0; // Normal, not an error
  }

  load_erasurecode_lib(err, err_len);
  if (strlen(err) > 0) {
    printf("Loading erasurecode library failed: %s\n", err);
    return -1;
  }

  printf("Performing erasure code test\n");

  dataUnits = calloc(numDataUnits, sizeof(unsigned char*));
  parityUnits = calloc(numParityUnits, sizeof(unsigned char*));
  backupUnits = calloc(numParityUnits, sizeof(unsigned char*));

  // Allocate and generate data units
  srand(135);
  for (i = 0; i < numDataUnits; i++) {
    dataUnits[i] = calloc(chunkSize, sizeof(unsigned char));
    for (j = 0; j < chunkSize; j++) {
      dataUnits[i][j] = rand();
    }
  }

  // Allocate and initialize parity units
  for (i = 0; i < numParityUnits; i++) {
    parityUnits[i] = calloc(chunkSize, sizeof(unsigned char));
    for (j = 0; j < chunkSize; j++) {
      parityUnits[i][j] = 0;
    }
  }

  pEncoder = (IsalEncoder*)malloc(sizeof(IsalEncoder));
  memset(pEncoder, 0, sizeof(*pEncoder));
  initEncoder(pEncoder, numDataUnits, numParityUnits);
  encode(pEncoder, dataUnits, parityUnits, chunkSize);

  pDecoder = (IsalDecoder*)malloc(sizeof(IsalDecoder));
  memset(pDecoder, 0, sizeof(*pDecoder));
  initDecoder(pDecoder, numDataUnits, numParityUnits);

  memcpy(allUnits, dataUnits, numDataUnits * (sizeof (unsigned char*)));
  memcpy(allUnits + numDataUnits, parityUnits,
                            numParityUnits * (sizeof (unsigned char*)));

  erasedIndexes[0] = 1;
  erasedIndexes[1] = 7;

  backupUnits[0] = allUnits[1];
  backupUnits[1] = allUnits[7];

  allUnits[0] = NULL; // Not to read
  allUnits[1] = NULL;
  allUnits[7] = NULL;

  decodingOutput[0] = malloc(chunkSize);
  decodingOutput[1] = malloc(chunkSize);

  decode(pDecoder, allUnits, erasedIndexes, 2, decodingOutput, chunkSize);

  for (i = 0; i < pDecoder->numErased; i++) {
    if (0 != memcmp(decodingOutput[i], backupUnits[i], chunkSize)) {
      fprintf(stderr, "Decoding failed\n\n");
      dumpDecoder(pDecoder);
      return -1;
    }
  }

  dumpDecoder(pDecoder);
  fprintf(stdout, "Successfully done, passed!\n\n");

  return 0;
}
