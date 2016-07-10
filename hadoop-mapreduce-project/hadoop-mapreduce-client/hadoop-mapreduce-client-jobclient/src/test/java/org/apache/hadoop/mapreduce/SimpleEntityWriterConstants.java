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

package org.apache.hadoop.mapreduce;

/**
 * Constants for simple entity writers.
 */
interface SimpleEntityWriterConstants {
  // constants for mtype = 1
  String KBS_SENT = "kbs sent";
  int KBS_SENT_DEFAULT = 1;
  String TEST_TIMES = "testtimes";
  int TEST_TIMES_DEFAULT = 100;
  String TIMELINE_SERVICE_PERFORMANCE_RUN_ID =
      "timeline.server.performance.run.id";

  /**
   *  To ensure that the compression really gets exercised, generate a
   *  random alphanumeric fixed length payload.
   */
  char[] ALPHA_NUMS = new char[] {'a', 'b', 'c', 'd', 'e', 'f',
      'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
      's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D',
      'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
      'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '1', '2',
      '3', '4', '5', '6', '7', '8', '9', '0', ' '};
}
