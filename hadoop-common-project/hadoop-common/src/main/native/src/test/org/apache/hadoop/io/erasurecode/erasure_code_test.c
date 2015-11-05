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
 * This is a lightweight version of the same file in Intel ISA-L library to test
 * and verify the basic functions of ISA-L integration. Note it's not serving as
 * a complete ISA-L library test nor as any sample to write an erasure coder
 * using the library. A sample is to be written and provided separately.
 */

#include "org_apache_hadoop.h"
#include "erasure_code.h"
#include "gf_util.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TEST_LEN 8192
#define TEST_SOURCES  127
#define MMAX TEST_SOURCES
#define KMAX TEST_SOURCES
#define TEST_SEED 11

static void dump(unsigned char *buf, int len)
{
  int i;
  for (i = 0; i < len;) {
    printf(" %2x", 0xff & buf[i++]);
    if (i % 32 == 0)
      printf("\n");
  }
  printf("\n");
}

static void dump_matrix(unsigned char **s, int k, int m)
{
  int i, j;
  for (i = 0; i < k; i++) {
    for (j = 0; j < m; j++) {
      printf(" %2x", s[i][j]);
    }
    printf("\n");
  }
  printf("\n");
}

static void dump_u8xu8(unsigned char *s, int k, int m)
{
  int i, j;
  for (i = 0; i < k; i++) {
    for (j = 0; j < m; j++) {
      printf(" %2x", 0xff & s[j + (i * m)]);
    }
    printf("\n");
  }
  printf("\n");
}

// Generate Random errors
static void gen_err_list(unsigned char *src_err_list,
       unsigned char *src_in_err, int *pnerrs, int *pnsrcerrs, int k, int m)
{
  int i, err;
  int nerrs = 0, nsrcerrs = 0;

  for (i = 0, nerrs = 0, nsrcerrs = 0; i < m && nerrs < m - k; i++) {
    err = 1 & rand();
    src_in_err[i] = err;
    if (err) {
      src_err_list[nerrs++] = i;
      if (i < k) {
        nsrcerrs++;
      }
    }
  }
  if (nerrs == 0) { // should have at least one error
    while ((err = (rand() % KMAX)) >= m) ;
    src_err_list[nerrs++] = err;
    src_in_err[err] = 1;
    if (err < k)
      nsrcerrs = 1;
  }
  *pnerrs = nerrs;
  *pnsrcerrs = nsrcerrs;
  return;
}

#define NO_INVERT_MATRIX -2
// Generate decode matrix from encode matrix
static int gf_gen_decode_matrix(unsigned char *encode_matrix,
        unsigned char *decode_matrix,
        unsigned char *invert_matrix,
        unsigned int *decode_index,
        unsigned char *src_err_list,
        unsigned char *src_in_err,
        int nerrs, int nsrcerrs, int k, int m)
{
  int i, j, p;
  int r;
  unsigned char *backup, *b, s;
  int incr = 0;

  b = malloc(MMAX * KMAX);
  backup = malloc(MMAX * KMAX);

  if (b == NULL || backup == NULL) {
    printf("Test failure! Error with malloc\n");
    free(b);
    free(backup);
    return -1;
  }
  // Construct matrix b by removing error rows
  for (i = 0, r = 0; i < k; i++, r++) {
    while (src_in_err[r])
      r++;
    for (j = 0; j < k; j++) {
      b[k * i + j] = encode_matrix[k * r + j];
      backup[k * i + j] = encode_matrix[k * r + j];
    }
    decode_index[i] = r;
  }
  incr = 0;
  while (h_gf_invert_matrix(b, invert_matrix, k) < 0) {
    if (nerrs == (m - k)) {
      free(b);
      free(backup);
      printf("BAD MATRIX\n");
      return NO_INVERT_MATRIX;
    }
    incr++;
    memcpy(b, backup, MMAX * KMAX);
    for (i = nsrcerrs; i < nerrs - nsrcerrs; i++) {
      if (src_err_list[i] == (decode_index[k - 1] + incr)) {
        // skip the erased parity line
        incr++;
        continue;
      }
    }
    if (decode_index[k - 1] + incr >= m) {
      free(b);
      free(backup);
      printf("BAD MATRIX\n");
      return NO_INVERT_MATRIX;
    }
    decode_index[k - 1] += incr;
    for (j = 0; j < k; j++)
      b[k * (k - 1) + j] = encode_matrix[k * decode_index[k - 1] + j];

  };

  for (i = 0; i < nsrcerrs; i++) {
    for (j = 0; j < k; j++) {
      decode_matrix[k * i + j] = invert_matrix[k * src_err_list[i] + j];
    }
  }
  /* src_err_list from encode_matrix * invert of b for parity decoding */
  for (p = nsrcerrs; p < nerrs; p++) {
    for (i = 0; i < k; i++) {
      s = 0;
      for (j = 0; j < k; j++)
        s ^= h_gf_mul(invert_matrix[j * k + i],
              encode_matrix[k * src_err_list[p] + j]);

      decode_matrix[k * p + i] = s;
    }
  }
  free(b);
  free(backup);
  return 0;
}

int main(int argc, char *argv[])
{
  char err[256];
  size_t err_len = sizeof(err);
  int re, i, j, p, m, k;
  int nerrs, nsrcerrs;
  unsigned int decode_index[MMAX];
  unsigned char *temp_buffs[TEST_SOURCES], *buffs[TEST_SOURCES];
  unsigned char *encode_matrix, *decode_matrix, *invert_matrix, *g_tbls;
  unsigned char src_in_err[TEST_SOURCES], src_err_list[TEST_SOURCES];
  unsigned char *recov[TEST_SOURCES];

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
  srand(TEST_SEED);

  // Allocate the arrays
  for (i = 0; i < TEST_SOURCES; i++) {
    buffs[i] = malloc(TEST_LEN);
  }

  for (i = 0; i < TEST_SOURCES; i++) {
    temp_buffs[i] = malloc(TEST_LEN);
  }

  // Test erasure code by encode and recovery

  encode_matrix = malloc(MMAX * KMAX);
  decode_matrix = malloc(MMAX * KMAX);
  invert_matrix = malloc(MMAX * KMAX);
  g_tbls = malloc(KMAX * TEST_SOURCES * 32);
  if (encode_matrix == NULL || decode_matrix == NULL
      || invert_matrix == NULL || g_tbls == NULL) {
    snprintf(err, err_len, "%s", "allocating test matrix buffers error");
    return -1;
  }

  m = 9;
  k = 5;
  if (m > MMAX || k > KMAX)
    return -1;

  // Make random data
  for (i = 0; i < k; i++)
    for (j = 0; j < TEST_LEN; j++)
      buffs[i][j] = rand();

  // The matrix generated by gf_gen_cauchy1_matrix
  // is always invertable.
  h_gf_gen_cauchy_matrix(encode_matrix, m, k);

  // Generate g_tbls from encode matrix encode_matrix
  h_ec_init_tables(k, m - k, &encode_matrix[k * k], g_tbls);

  // Perform matrix dot_prod for EC encoding
  // using g_tbls from encode matrix encode_matrix
  h_ec_encode_data(TEST_LEN, k, m - k, g_tbls, buffs, &buffs[k]);

  // Choose random buffers to be in erasure
  memset(src_in_err, 0, TEST_SOURCES);
  gen_err_list(src_err_list, src_in_err, &nerrs, &nsrcerrs, k, m);

  // Generate decode matrix
  re = gf_gen_decode_matrix(encode_matrix, decode_matrix,
          invert_matrix, decode_index, src_err_list, src_in_err,
          nerrs, nsrcerrs, k, m);
  if (re != 0) {
    snprintf(err, err_len, "%s", "gf_gen_decode_matrix failed");
    return -1;
  }
  // Pack recovery array as list of valid sources
  // Its order must be the same as the order
  // to generate matrix b in gf_gen_decode_matrix
  for (i = 0; i < k; i++) {
    recov[i] = buffs[decode_index[i]];
  }

  // Recover data
  h_ec_init_tables(k, nerrs, decode_matrix, g_tbls);
  h_ec_encode_data(TEST_LEN, k, nerrs, g_tbls, recov, &temp_buffs[k]);
  for (i = 0; i < nerrs; i++) {
    if (0 != memcmp(temp_buffs[k + i], buffs[src_err_list[i]], TEST_LEN)) {
      snprintf(err, err_len, "%s", "Error recovery failed");
      printf("Fail error recovery (%d, %d, %d)\n", m, k, nerrs);

      printf(" - erase list = ");
      for (j = 0; j < nerrs; j++) {
        printf(" %d", src_err_list[j]);
      }

      printf(" - Index = ");
      for (p = 0; p < k; p++) {
        printf(" %d", decode_index[p]);
      }

      printf("\nencode_matrix:\n");
      dump_u8xu8((unsigned char *) encode_matrix, m, k);
      printf("inv b:\n");
      dump_u8xu8((unsigned char *) invert_matrix, k, k);
      printf("\ndecode_matrix:\n");
      dump_u8xu8((unsigned char *) decode_matrix, m, k);
      printf("recov %d:", src_err_list[i]);
      dump(temp_buffs[k + i], 25);
      printf("orig   :");
      dump(buffs[src_err_list[i]], 25);

      return -1;
    }
  }

  printf("done EC tests: Pass\n");
  return 0;
}
