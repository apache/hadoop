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
package org.apache.hadoop.yarn.nodelabels;

import static org.junit.Assert.fail;
import org.junit.Test;

/**
 * Test class to verify node label util ops.
 */
public class TestNodeLabelUtil {

  @Test
  public void testAttributeValueAddition() {
    String[] values =
        new String[]{"1_8", "1.8", "ABZ", "ABZ", "az", "a-z", "a_z",
            "123456789"};
    for (String val : values) {
      try {
        NodeLabelUtil.checkAndThrowAttributeValue(val);
      } catch (Exception e) {
        fail("Valid values for NodeAttributeValue :" + val);
      }
    }

    String[] invalidVals = new String[]{"_18", "1,8", "1/5", ".15", "1\\5"};
    for (String val : invalidVals) {
      try {
        NodeLabelUtil.checkAndThrowAttributeValue(val);
        fail("Valid values for NodeAttributeValue :" + val);
      } catch (Exception e) {
        // IGNORE
      }
    }
  }
}
