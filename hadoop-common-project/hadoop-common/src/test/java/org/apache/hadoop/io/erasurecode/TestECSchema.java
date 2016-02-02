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
package org.apache.hadoop.io.erasurecode;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertEquals;
import java.util.HashMap;
import java.util.Map;

public class TestECSchema {

   @Rule
   public Timeout globalTimeout = new Timeout(300000);

  @Test
  public void testGoodSchema() {
    int numDataUnits = 6;
    int numParityUnits = 3;
    String codec = "rs";
    String extraOption = "extraOption";
    String extraOptionValue = "extraOptionValue";

    Map<String, String> options = new HashMap<String, String>();
    options.put(ECSchema.NUM_DATA_UNITS_KEY, String.valueOf(numDataUnits));
    options.put(ECSchema.NUM_PARITY_UNITS_KEY, String.valueOf(numParityUnits));
    options.put(ECSchema.CODEC_NAME_KEY, codec);
    options.put(extraOption, extraOptionValue);

    ECSchema schema = new ECSchema(options);
    System.out.println(schema.toString());

    assertEquals(numDataUnits, schema.getNumDataUnits());
    assertEquals(numParityUnits, schema.getNumParityUnits());
    assertEquals(codec, schema.getCodecName());
    assertEquals(extraOptionValue, schema.getExtraOptions().get(extraOption));
  }
}
