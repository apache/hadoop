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
package org.apache.hadoop.io.erasurecode.codec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.io.erasurecode.ErasureCodecOptions;
import org.apache.hadoop.io.erasurecode.coder.ErasureCoder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestHHXORErasureCodec {
  private ECSchema schema = new ECSchema("hhxor", 10, 4);
  private ErasureCodecOptions options = new ErasureCodecOptions(schema);

  @Test
  public void testGoodCodec() {
    HHXORErasureCodec codec
        = new HHXORErasureCodec(new Configuration(), options);
    ErasureCoder encoder = codec.createEncoder();
    assertEquals(10, encoder.getNumDataUnits());
    assertEquals(4, encoder.getNumParityUnits());

    ErasureCoder decoder = codec.createDecoder();
    assertEquals(10, decoder.getNumDataUnits());
    assertEquals(4, decoder.getNumParityUnits());
  }
}
