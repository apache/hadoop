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

package org.apache.hadoop.test;

/**
 * Sizes of data in KiB/MiB.
 */
public final class Sizes {

  public static final int S_256 = 256;
  public static final int S_512 = 512;
  public static final int S_1K = 1024;
  public static final int S_2K = 2 * S_1K;
  public static final int S_4K = 4 * S_1K;
  public static final int S_8K = 8 * S_1K;
  public static final int S_10K = 10 * S_1K;
  public static final int S_16K = 16 * S_1K;
  public static final int S_32K = 32 * S_1K;
  public static final int S_64K = 64 * S_1K;
  public static final int S_128K = 128 * S_1K;
  public static final int S_256K = 256 * S_1K;
  public static final int S_500K = 500 * S_1K;
  public static final int S_512K = 512 * S_1K;
  public static final int S_1M = S_1K * S_1K;
  public static final int S_2M = 2 * S_1M;
  public static final int S_4M = 4 * S_1M;
  public static final int S_5M = 5 * S_1M;
  public static final int S_8M = 8 * S_1M;
  public static final int S_16M = 16 * S_1M;
  public static final int S_10M = 10 * S_1M;
  public static final int S_32M = 32 * S_1M;
  public static final int S_64M = 64 * S_1M;
  public static final double NANOSEC = 1.0e9;

  private Sizes() {
  }
}
