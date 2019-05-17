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

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.fs.azurebfs.utils.CRC64;
/**
 * Test for Crc64 in AzureBlobFileSystem, notice that ABFS CRC64 has its own polynomial.
 * */
public class TestAbfsCrc64 {

  @Test
  public void tesCrc64Compute() {
    CRC64 crc64 = new CRC64();
    final String[] testStr = {"#$", "dir_2_ac83abee", "dir_42_976df1f5"};
    final String[] expected = {"f91f7e6a837dbfa8", "203f9fefc38ae97b", "cc0d56eafe58a855"};
    for (int i = 0; i < testStr.length; i++) {
      Assert.assertEquals(expected[i], Long.toHexString(crc64.compute(testStr[i].getBytes())));
    }
  }
}
