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

#ifndef _GF_UTIL_H
#define _GF_UTIL_H

/**
 *  gf_util.h
 *  Interface to functions for vector (block) multiplication in GF(2^8).
 *
 *  This file defines the interface to routines used in fast RAID rebuild and
 *  erasure codes.
 */


/**
 * Single element GF(2^8) multiply.
 *
 * @param a  Multiplicand a
 * @param b  Multiplicand b
 * @returns  Product of a and b in GF(2^8)
 */
unsigned char h_gf_mul(unsigned char a, unsigned char b);

/**
 * Single element GF(2^8) inverse.
 *
 * @param a  Input element
 * @returns  Field element b such that a x b = {1}
 */
unsigned char h_gf_inv(unsigned char a);

/**
 * Generate a matrix of coefficients to be used for encoding.
 *
 * Vandermonde matrix example of encoding coefficients where high portion of
 * matrix is identity matrix I and lower portion is constructed as 2^{i*(j-k+1)}
 * i:{0,k-1} j:{k,m-1}. Commonly used method for choosing coefficients in
 * erasure encoding but does not guarantee invertable for every sub matrix.  For
 * large k it is possible to find cases where the decode matrix chosen from
 * sources and parity not in erasure are not invertable. Users may want to
 * adjust for k > 5.
 *
 * @param a  [mxk] array to hold coefficients
 * @param m  number of rows in matrix corresponding to srcs + parity.
 * @param k  number of columns in matrix corresponding to srcs.
 * @returns  none
 */
void h_gf_gen_rs_matrix(unsigned char *a, int m, int k);

/**
 * Generate a Cauchy matrix of coefficients to be used for encoding.
 *
 * Cauchy matrix example of encoding coefficients where high portion of matrix
 * is identity matrix I and lower portion is constructed as 1/(i + j) | i != j,
 * i:{0,k-1} j:{k,m-1}.  Any sub-matrix of a Cauchy matrix should be invertable.
 *
 * @param a  [mxk] array to hold coefficients
 * @param m  number of rows in matrix corresponding to srcs + parity.
 * @param k  number of columns in matrix corresponding to srcs.
 * @returns  none
 */
void h_gf_gen_cauchy_matrix(unsigned char *a, int m, int k);

/**
 * Invert a matrix in GF(2^8)
 *
 * @param in  input matrix
 * @param out output matrix such that [in] x [out] = [I] - identity matrix
 * @param n   size of matrix [nxn]
 * @returns 0 successful, other fail on singular input matrix
 */
int h_gf_invert_matrix(unsigned char *in, unsigned char *out, const int n);

/**
 * GF(2^8) vector multiply by constant, runs appropriate version.
 *
 * Does a GF(2^8) vector multiply b = Ca where a and b are arrays and C
 * is a single field element in GF(2^8). Can be used for RAID6 rebuild
 * and partial write functions. Function requires pre-calculation of a
 * 32-element constant array based on constant C. gftbl(C) = {C{00},
 * C{01}, C{02}, ... , C{0f} }, {C{00}, C{10}, C{20}, ... , C{f0} }.
 * Len and src must be aligned to 32B.
 *
 * This function determines what instruction sets are enabled
 * and selects the appropriate version at runtime.
 *
 * @param len   Length of vector in bytes. Must be aligned to 32B.
 * @param gftbl Pointer to 32-byte array of pre-calculated constants based on C.
 * @param src   Pointer to src data array. Must be aligned to 32B.
 * @param dest  Pointer to destination data array. Must be aligned to 32B.
 * @returns 0 pass, other fail
 */
int h_gf_vect_mul(int len, unsigned char *gftbl, void *src, void *dest);


#endif //_GF_UTIL_H
