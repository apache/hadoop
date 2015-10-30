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
package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.io.erasurecode.rawcoder.util.RSUtil;

/**
 * Test base for raw Reed-solomon coders.
 */
public abstract class TestRSRawCoderBase extends TestRawCoderBase {

  private static int symbolSize = 0;
  private static int symbolMax = 0;

  private static int RS_FIXED_DATA_GENERATOR = 0;

  static {
    symbolSize = (int) Math.round(Math.log(
        RSUtil.GF.getFieldSize()) / Math.log(2));
    symbolMax = (int) Math.pow(2, symbolSize);
  }

  @Override
  protected byte[] generateData(int len) {
    byte[] buffer = new byte[len];
    for (int i = 0; i < buffer.length; i++) {
      buffer[i] = (byte) RAND.nextInt(symbolMax);
    }
    return buffer;
  }

  @Override
  protected byte[] generateFixedData(int len) {
    byte[] buffer = new byte[len];
    for (int i = 0; i < buffer.length; i++) {
      buffer[i] = (byte) RS_FIXED_DATA_GENERATOR++;
      if (RS_FIXED_DATA_GENERATOR == symbolMax) {
        RS_FIXED_DATA_GENERATOR = 0;
      }
    }
    return buffer;
  }
}
