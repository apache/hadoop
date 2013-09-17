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
package org.apache.hadoop.test;

import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

/**
 * Class for test units to extend in order that their individual tests will
 * be timed out and fail automatically should they run more than 10 seconds.
 * This provides an automatic regression check for tests that begin running
 * longer than expected.
 */
public class UnitTestcaseTimeLimit {
  public final int timeOutSecs = 10;
  
  @Rule public TestRule globalTimeout = new Timeout(timeOutSecs * 1000);
}
