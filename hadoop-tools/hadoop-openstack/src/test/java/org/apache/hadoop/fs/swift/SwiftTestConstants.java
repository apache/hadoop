/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.swift;

/**
 * Hard coded constants for the test timeouts
 */
public interface SwiftTestConstants {
  /**
   * Timeout for swift tests: {@value}
   */
  int SWIFT_TEST_TIMEOUT = 5 * 60 * 1000;

  /**
   * Timeout for tests performing bulk operations: {@value}
   */
  int SWIFT_BULK_IO_TEST_TIMEOUT = 12 * 60 * 1000;
}
