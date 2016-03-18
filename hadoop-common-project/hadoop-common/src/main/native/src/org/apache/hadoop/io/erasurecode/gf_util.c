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

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "isal_load.h"
#include "gf_util.h"

/**
 *  gf_util.c
 *  Implementation GF utilities based on ISA-L library.
 *
 */

unsigned char h_gf_mul(unsigned char a, unsigned char b) {
  return isaLoader->gf_mul(a, b);
}

unsigned char h_gf_inv(unsigned char a) {
  return isaLoader->gf_inv(a);
}

void h_gf_gen_rs_matrix(unsigned char *a, int m, int k) {
  isaLoader->gf_gen_rs_matrix(a, m, k);
}

void h_gf_gen_cauchy_matrix(unsigned char *a, int m, int k) {
  isaLoader->gf_gen_cauchy_matrix(a, m, k);
}

int h_gf_invert_matrix(unsigned char *in, unsigned char *out, const int n) {
  return isaLoader->gf_invert_matrix(in, out, n);
}

int h_gf_vect_mul(int len, unsigned char *gftbl, void *src, void *dest) {
  return isaLoader->gf_vect_mul(len, gftbl, src, dest);
}