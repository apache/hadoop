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
package org.apache.hadoop.hbase.ipc;

/**
 * Utility for managing the flag byte passed in response to a
 * {@link HBaseServer.Call}
 */
class ResponseFlag {
  private static final byte ERROR_BIT = 0x1;
  private static final byte LENGTH_BIT = 0x2;

  private ResponseFlag() {
    // Make it so this class cannot be constructed.
  }

  static boolean isError(final byte flag) {
    return (flag & ERROR_BIT) != 0;
  }

  static boolean isLength(final byte flag) {
    return (flag & LENGTH_BIT) != 0;
  }

  static byte getLengthSetOnly() {
    return LENGTH_BIT;
  }

  static byte getErrorAndLengthSet() {
    return LENGTH_BIT | ERROR_BIT;
  }
}
