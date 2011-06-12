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

package org.apache.hadoop.sqoop;

import junit.framework.TestCase;


/**
 * Test aspects of the ImportOptions class
 */
public class TestImportOptions extends TestCase {

  // tests for the toChar() parser
  public void testNormalChar() throws ImportOptions.InvalidOptionsException {
    assertEquals('a', ImportOptions.toChar("a"));
  }

  public void testEmptyString() throws ImportOptions.InvalidOptionsException {
    try {
      ImportOptions.toChar("");
      fail("Expected exception");
    } catch (ImportOptions.InvalidOptionsException ioe) {
      // expect this.
    }
  }

  public void testNullString() throws ImportOptions.InvalidOptionsException {
    try {
      ImportOptions.toChar(null);
      fail("Expected exception");
    } catch (ImportOptions.InvalidOptionsException ioe) {
      // expect this.
    }
  }

  public void testTooLong() throws ImportOptions.InvalidOptionsException {
    // Should just use the first character and log a warning.
    assertEquals('x', ImportOptions.toChar("xyz"));
  }

  public void testHexChar1() throws ImportOptions.InvalidOptionsException {
    assertEquals(0xF, ImportOptions.toChar("\\0xf"));
  }

  public void testHexChar2() throws ImportOptions.InvalidOptionsException {
    assertEquals(0xF, ImportOptions.toChar("\\0xF"));
  }

  public void testHexChar3() throws ImportOptions.InvalidOptionsException {
    assertEquals(0xF0, ImportOptions.toChar("\\0xf0"));
  }

  public void testHexChar4() throws ImportOptions.InvalidOptionsException {
    assertEquals(0xF0, ImportOptions.toChar("\\0Xf0"));
  }

  public void testEscapeChar1() throws ImportOptions.InvalidOptionsException {
    assertEquals('\n', ImportOptions.toChar("\\n"));
  }

  public void testEscapeChar2() throws ImportOptions.InvalidOptionsException {
    assertEquals('\\', ImportOptions.toChar("\\\\"));
  }

  public void testEscapeChar3() throws ImportOptions.InvalidOptionsException {
    assertEquals('\\', ImportOptions.toChar("\\"));
  }

  public void testUnknownEscape1() throws ImportOptions.InvalidOptionsException {
    try {
      ImportOptions.toChar("\\Q");
      fail("Expected exception");
    } catch (ImportOptions.InvalidOptionsException ioe) {
      // expect this.
    }
  }

  public void testUnknownEscape2() throws ImportOptions.InvalidOptionsException {
    try {
      ImportOptions.toChar("\\nn");
      fail("Expected exception");
    } catch (ImportOptions.InvalidOptionsException ioe) {
      // expect this.
    }
  }

  public void testEscapeNul1() throws ImportOptions.InvalidOptionsException {
    assertEquals('\000', ImportOptions.toChar("\\0"));
  }

  public void testEscapeNul2() throws ImportOptions.InvalidOptionsException {
    assertEquals('\000', ImportOptions.toChar("\\00"));
  }

  public void testEscapeNul3() throws ImportOptions.InvalidOptionsException {
    assertEquals('\000', ImportOptions.toChar("\\0000"));
  }

  public void testEscapeNul4() throws ImportOptions.InvalidOptionsException {
    assertEquals('\000', ImportOptions.toChar("\\0x0"));
  }

  public void testOctalChar1() throws ImportOptions.InvalidOptionsException {
    assertEquals(04, ImportOptions.toChar("\\04"));
  }

  public void testOctalChar2() throws ImportOptions.InvalidOptionsException {
    assertEquals(045, ImportOptions.toChar("\\045"));
  }

  public void testErrOctalChar() throws ImportOptions.InvalidOptionsException {
    try {
      ImportOptions.toChar("\\095");
      fail("Expected exception");
    } catch (NumberFormatException nfe) {
      // expected.
    }
  }

  public void testErrHexChar() throws ImportOptions.InvalidOptionsException {
    try {
      ImportOptions.toChar("\\0x9K5");
      fail("Expected exception");
    } catch (NumberFormatException nfe) {
      // expected.
    }
  }

  // test that setting output delimiters also sets input delimiters 
  public void testDelimitersInherit() throws ImportOptions.InvalidOptionsException {
    String [] args = {
        "--fields-terminated-by",
        "|"
    };

    ImportOptions opts = new ImportOptions();
    opts.parse(args);
    assertEquals('|', opts.getInputFieldDelim());
    assertEquals('|', opts.getOutputFieldDelim());
  }

  // test that setting output delimiters and setting input delims separately works
  public void testDelimOverride1() throws ImportOptions.InvalidOptionsException {
    String [] args = {
        "--fields-terminated-by",
        "|",
        "--input-fields-terminated-by",
        "*"
    };

    ImportOptions opts = new ImportOptions();
    opts.parse(args);
    assertEquals('*', opts.getInputFieldDelim());
    assertEquals('|', opts.getOutputFieldDelim());
  }

  // test that the order in which delims are specified doesn't matter
  public void testDelimOverride2() throws ImportOptions.InvalidOptionsException {
    String [] args = {
        "--input-fields-terminated-by",
        "*",
        "--fields-terminated-by",
        "|"
    };

    ImportOptions opts = new ImportOptions();
    opts.parse(args);
    assertEquals('*', opts.getInputFieldDelim());
    assertEquals('|', opts.getOutputFieldDelim());
  }
}
