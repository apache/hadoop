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
 * Provides a JVM-scoped unique identifier.
 *
 * <p>The identifier is generated once when the class is loaded and remains
 * constant for the lifetime of the JVM. The value is a random 6-digit number
 * in the range {@code [100000, 999999]}.</p>
 *
 * <p>This class is utility-only and cannot be instantiated.</p>
 */
public final class JvmIdProvider {

  /**
   * A JVM-wide unique identifier generated at class load time.
   */
  private static final long JVM_UNIQUE_ID;

  static {
    JVM_UNIQUE_ID = 100000L + new Random().nextInt(900000);
  }

  /**
   * Prevents instantiation.
   */
  private JvmIdProvider() {
  }

  /**
   * Returns the JVM-scoped unique identifier.
   *
   * @return the unique ID for this JVM instance
   */
  public static long getJvmId() {
    return JVM_UNIQUE_ID;
  }
}
