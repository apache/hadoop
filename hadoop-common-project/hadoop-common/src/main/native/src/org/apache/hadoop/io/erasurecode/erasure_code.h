/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#ifndef _ERASURE_CODE_H_
#define _ERASURE_CODE_H_

#include <stddef.h>

/**
 *  Interface to functions supporting erasure code encode and decode.
 *
 *  This file defines the interface to optimized functions used in erasure
 *  codes.  Encode and decode of erasures in GF(2^8) are made by calculating the
 *  dot product of the symbols (bytes in GF(2^8)) across a set of buffers and a
 *  set of coefficients.  Values for the coefficients are determined by the type
 *  of erasure code.  Using a general dot product means that any sequence of
 *  coefficients may be used including erasure codes based on random
 *  coefficients.
 *  Multiple versions of dot product are supplied to calculate 1-6 output
 *  vectors in one pass.
 *  Base GF multiply and divide functions can be sped up by defining
 *  GF_LARGE_TABLES at the expense of memory size.
 *
 */

/**
 * Initialize tables for fast Erasure Code encode and decode.
 *
 * Generates the expanded tables needed for fast encode or decode for erasure
 * codes on blocks of data.  32bytes is generated for each input coefficient.
 *
 * @param k      The number of vector sources or rows in the generator matrix
 *               for coding.
 * @param rows   The number of output vectors to concurrently encode/decode.
 * @param a      Pointer to sets of arrays of input coefficients used to encode
 *               or decode data.
 * @param gftbls Pointer to start of space for concatenated output tables
 *               generated from input coefficients.  Must be of size 32*k*rows.
 * @returns none
 */
void h_ec_init_tables(int k, int rows, unsigned char* a, unsigned char* gftbls);

/**
 * Generate or decode erasure codes on blocks of data, runs appropriate version.
 *
 * Given a list of source data blocks, generate one or multiple blocks of
 * encoded data as specified by a matrix of GF(2^8) coefficients. When given a
 * suitable set of coefficients, this function will perform the fast generation
 * or decoding of Reed-Solomon type erasure codes.
 *
 * This function determines what instruction sets are enabled and
 * selects the appropriate version at runtime.
 *
 * @param len    Length of each block of data (vector) of source or dest data.
 * @param k      The number of vector sources or rows in the generator matrix
 *        for coding.
 * @param rows   The number of output vectors to concurrently encode/decode.
 * @param gftbls Pointer to array of input tables generated from coding
 *        coefficients in ec_init_tables(). Must be of size 32*k*rows
 * @param data   Array of pointers to source input buffers.
 * @param coding Array of pointers to coded output buffers.
 * @returns none
 */
void h_ec_encode_data(int len, int k, int rows, unsigned char *gftbls,
                                 unsigned char **data, unsigned char **coding);

/**
 * @brief Generate update for encode or decode of erasure codes from single
 *        source, runs appropriate version.
 *
 * Given one source data block, update one or multiple blocks of encoded data as
 * specified by a matrix of GF(2^8) coefficients. When given a suitable set of
 * coefficients, this function will perform the fast generation or decoding of
 * Reed-Solomon type erasure codes from one input source at a time.
 *
 * This function determines what instruction sets are enabled and selects the
 * appropriate version at runtime.
 *
 * @param len    Length of each block of data (vector) of source or dest data.
 * @param k      The number of vector sources or rows in the generator matrix
 *               for coding.
 * @param rows   The number of output vectors to concurrently encode/decode.
 * @param vec_i  The vector index corresponding to the single input source.
 * @param gftbls Pointer to array of input tables generated from coding
 *               coefficients in ec_init_tables(). Must be of size 32*k*rows
 * @param data   Pointer to single input source used to update output parity.
 * @param coding Array of pointers to coded output buffers.
 * @returns none
 */
void h_ec_encode_data_update(int len, int k, int rows, int vec_i,
           unsigned char *gftbls, unsigned char *data, unsigned char **coding);

#endif //_ERASURE_CODE_H_
