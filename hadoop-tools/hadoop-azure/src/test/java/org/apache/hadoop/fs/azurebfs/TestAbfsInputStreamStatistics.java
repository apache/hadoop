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

package org.apache.hadoop.fs.azurebfs;

import org.junit.Test;

import org.apache.hadoop.fs.azurebfs.services.AbfsInputStreamStatisticsImpl;

public class TestAbfsInputStreamStatistics extends AbstractAbfsIntegrationTest {

  private static final int OPERATIONS = 100;

  public TestAbfsInputStreamStatistics() throws Exception {
  }

  /**
   * Test to check the bytesReadFromBuffer statistic value from AbfsInputStream.
   */
  @Test
  public void testBytesReadFromBufferStatistic() {
    describe("Testing bytesReadFromBuffer statistics value in AbfsInputStream");

    AbfsInputStreamStatisticsImpl abfsInputStreamStatistics =
        new AbfsInputStreamStatisticsImpl();

    //Increment the bytesReadFromBuffer value.
    for (int i = 0; i < OPERATIONS; i++) {
      abfsInputStreamStatistics.bytesReadFromBuffer(1);
    }

    /*
     * Since we incremented the bytesReadFromBuffer OPERATIONS times, this
     * should be the expected value.
     */
    assertEquals("Mismatch in bytesReadFromBuffer value", OPERATIONS,
        abfsInputStreamStatistics.getBytesReadFromBuffer());

  }
}
