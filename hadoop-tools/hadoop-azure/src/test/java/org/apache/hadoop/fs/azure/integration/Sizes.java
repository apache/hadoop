/*
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

package org.apache.hadoop.fs.azure.integration;

/**
 * Sizes of data.
 * Checkstyle doesn't like the naming scheme or the fact its an interface.
 */
public interface Sizes {

  int S_256 = 256;
  int S_512 = 512;
  int S_1K = 1024;
  int S_4K = 4 * S_1K;
  int S_8K = 8 * S_1K;
  int S_16K = 16 * S_1K;
  int S_32K = 32 * S_1K;
  int S_64K = 64 * S_1K;
  int S_128K = 128 * S_1K;
  int S_256K = 256 * S_1K;
  int S_1M = S_1K * S_1K;
  int S_2M = 2 * S_1M;
  int S_5M = 5 * S_1M;
  int S_10M = 10* S_1M;
  double NANOSEC = 1.0e9;

}
