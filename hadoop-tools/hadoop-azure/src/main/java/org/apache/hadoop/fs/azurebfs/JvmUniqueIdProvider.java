/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs;

import java.util.Random;

/**
 * Provides a JVM-scoped identifier.
 *
 * <p>The identifier is generated once when the class is loaded and remains
 * constant for the lifetime of the JVM. It is derived using a combination of
 * the current system time and random entropy to reduce the likelihood of
 * collisions across JVM instances.</p>
 *
 * <p>The identifier is intended for lightweight JVM-level identification,
 * such as tagging metrics or log entries. It provides best-effort uniqueness
 * and is not guaranteed to be globally unique.</p>
 *
 * <p>This class is utility-only and cannot be instantiated.</p>
 */
public final class JvmUniqueIdProvider {

  /** Lower bound (inclusive) for the generated JVM identifier. */
  private static final int MIN_JVM_ID = 100_000;

  /** Size of the identifier value range. */
  private static final int JVM_ID_RANGE = 900_000;

  /** Upper bound for random entropy mixed into the identifier. */
  private static final int RANDOM_ENTROPY_BOUND = 1_000;

  /** JVM-scoped identifier generated at class initialization time. */
  private static final int JVM_UNIQUE_ID;

  static {
    long time = System.currentTimeMillis();
    int random = new Random().nextInt(RANDOM_ENTROPY_BOUND);
    JVM_UNIQUE_ID = (int) ((time + random) % JVM_ID_RANGE) + MIN_JVM_ID;
  }

  /** Prevents instantiation. */
  private JvmUniqueIdProvider() {
  }

  /**
   * Returns the JVM-scoped identifier.
   *
   * @return an identifier that remains constant for the lifetime of the JVM
   */
  public static int getJvmId() {
    return JVM_UNIQUE_ID;
  }
}

