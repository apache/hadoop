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

package org.apache.hadoop.io;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Sizes of binary values and other some common sizes.
 * This avoids having to remember the larger binary values,
 * and stops IDEs/style checkers complaining about numeric
 * values in source code.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class Sizes {

  /** 2^8 bytes: {@value}. */
  public static final int S_256 = 256;

  /** 2^9 bytes: {@value}. */
  public static final int S_512 = S_256 << 1;

  /** 2^10 bytes - 1 KiB: {@value}. */
  public static final int S_1K = S_512 << 1;

  /** 2^11 bytes - 1 KiB: {@value}. */
  public static final int S_2K = S_1K << 1;

  /** 2^12 bytes - 2 KiB: {@value}. */
  public static final int S_4K = S_2K << 1;

  /** 2^13 bytes: {@value}. */
  public static final int S_8K = S_4K << 1;

  /** 2^14 bytes: {@value}. */
  public static final int S_16K = S_8K << 1;

  /** 2^15 bytes: {@value}. */
  public static final int S_32K = S_16K << 1;

  /** 2^16 bytes: {@value}. */
  public static final int S_64K = S_32K << 1;

  /** 2^17 bytes, 128 KiB: {@value}. */
  public static final int S_128K = S_64K << 1;

  /** 2^18 bytes, 256 KiB: {@value}. */
  public static final int S_256K = S_128K << 1;

  /** 2^19 bytes, 512 KiB: {@value}. */
  public static final int S_512K = S_256K << 1;

  /** 2^20 bytes, 1 MiB: {@value}. */
  public static final int S_1M = S_512K << 1;

  /** 2^21 bytes, 2 MiB: {@value}. */
  public static final int S_2M = S_1M << 1;

  /** 2^22 bytes, 4 MiB:  {@value}. */
  public static final int S_4M = S_2M << 1;

  /** 2^23 bytes,  MiB:  {@value}. */
  public static final int S_8M = S_4M << 1;

  /** 2^24 bytes,  MiB:  {@value}. */
  public static final int S_16M = S_8M << 1;

  /** 2^25 bytes,  MiB:  {@value}. */
  public static final int S_32M = S_16M << 1;

  /** 5 MiB:  {@value}. */
  public static final int S_5M = 5 * S_1M;

  /** 10 MiB:  {@value}. */
  public static final int S_10M = 10 * S_1M;

}
