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

package org.apache.hadoop.sqoop.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import junit.framework.TestCase;

/**
 * Test Hive DDL statement generation.
 */
public class TestTableDefWriter extends TestCase {

  public static final Log LOG = LogFactory.getLog(TestHiveImport.class.getName());


  // Test getHiveOctalCharCode and expect an IllegalArgumentException.
  private void expectExceptionInCharCode(int charCode) {
    try {
      TableDefWriter.getHiveOctalCharCode(charCode);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException iae) {
      // Expected; ok.
    }
  }

  public void testHiveOctalCharCode() {
    assertEquals("\\000", TableDefWriter.getHiveOctalCharCode(0));
    assertEquals("\\001", TableDefWriter.getHiveOctalCharCode(1));
    assertEquals("\\012", TableDefWriter.getHiveOctalCharCode((int) '\n'));
    assertEquals("\\177", TableDefWriter.getHiveOctalCharCode(0177));

    expectExceptionInCharCode(4096);
    expectExceptionInCharCode(0200);
    expectExceptionInCharCode(254);
  }
}
