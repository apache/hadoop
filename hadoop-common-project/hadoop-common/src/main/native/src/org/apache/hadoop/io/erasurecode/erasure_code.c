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
#include "erasure_code.h"

/**
 *  erasure_code.c
 *  Implementation erasure code utilities based on ISA-L library.
 *
 */

void h_ec_init_tables(int k, int rows, unsigned char* a, unsigned char* gftbls) {
  isaLoader->ec_init_tables(k, rows, a, gftbls);
}

void h_ec_encode_data(int len, int k, int rows, unsigned char *gftbls,
    unsigned char **data, unsigned char **coding) {
  isaLoader->ec_encode_data(len, k, rows, gftbls, data, coding);
}

void h_ec_encode_data_update(int len, int k, int rows, int vec_i,
         unsigned char *gftbls, unsigned char *data, unsigned char **coding) {
  isaLoader->ec_encode_data_update(len, k, rows, vec_i, gftbls, data, coding);
}