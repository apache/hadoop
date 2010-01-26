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
 * Test aspects of the SqoopOptions class
 */
public class TestSqoopOptions extends TestCase {

  // tests for the toChar() parser
  public void testNormalChar() throws SqoopOptions.InvalidOptionsException {
    assertEquals('a', SqoopOptions.toChar("a"));
  }

  public void testEmptyString() throws SqoopOptions.InvalidOptionsException {
    try {
      SqoopOptions.toChar("");
      fail("Expected exception");
    } catch (SqoopOptions.InvalidOptionsException ioe) {
      // expect this.
    }
  }

  public void testNullString() throws SqoopOptions.InvalidOptionsException {
    try {
      SqoopOptions.toChar(null);
      fail("Expected exception");
    } catch (SqoopOptions.InvalidOptionsException ioe) {
      // expect this.
    }
  }

  public void testTooLong() throws SqoopOptions.InvalidOptionsException {
    // Should just use the first character and log a warning.
    assertEquals('x', SqoopOptions.toChar("xyz"));
  }

  public void testHexChar1() throws SqoopOptions.InvalidOptionsException {
    assertEquals(0xF, SqoopOptions.toChar("\\0xf"));
  }

  public void testHexChar2() throws SqoopOptions.InvalidOptionsException {
    assertEquals(0xF, SqoopOptions.toChar("\\0xF"));
  }

  public void testHexChar3() throws SqoopOptions.InvalidOptionsException {
    assertEquals(0xF0, SqoopOptions.toChar("\\0xf0"));
  }

  public void testHexChar4() throws SqoopOptions.InvalidOptionsException {
    assertEquals(0xF0, SqoopOptions.toChar("\\0Xf0"));
  }

  public void testEscapeChar1() throws SqoopOptions.InvalidOptionsException {
    assertEquals('\n', SqoopOptions.toChar("\\n"));
  }

  public void testEscapeChar2() throws SqoopOptions.InvalidOptionsException {
    assertEquals('\\', SqoopOptions.toChar("\\\\"));
  }

  public void testEscapeChar3() throws SqoopOptions.InvalidOptionsException {
    assertEquals('\\', SqoopOptions.toChar("\\"));
  }

  public void testUnknownEscape1() throws SqoopOptions.InvalidOptionsException {
    try {
      SqoopOptions.toChar("\\Q");
      fail("Expected exception");
    } catch (SqoopOptions.InvalidOptionsException ioe) {
      // expect this.
    }
  }

  public void testUnknownEscape2() throws SqoopOptions.InvalidOptionsException {
    try {
      SqoopOptions.toChar("\\nn");
      fail("Expected exception");
    } catch (SqoopOptions.InvalidOptionsException ioe) {
      // expect this.
    }
  }

  public void testEscapeNul1() throws SqoopOptions.InvalidOptionsException {
    assertEquals('\000', SqoopOptions.toChar("\\0"));
  }

  public void testEscapeNul2() throws SqoopOptions.InvalidOptionsException {
    assertEquals('\000', SqoopOptions.toChar("\\00"));
  }

  public void testEscapeNul3() throws SqoopOptions.InvalidOptionsException {
    assertEquals('\000', SqoopOptions.toChar("\\0000"));
  }

  public void testEscapeNul4() throws SqoopOptions.InvalidOptionsException {
    assertEquals('\000', SqoopOptions.toChar("\\0x0"));
  }

  public void testOctalChar1() throws SqoopOptions.InvalidOptionsException {
    assertEquals(04, SqoopOptions.toChar("\\04"));
  }

  public void testOctalChar2() throws SqoopOptions.InvalidOptionsException {
    assertEquals(045, SqoopOptions.toChar("\\045"));
  }

  public void testErrOctalChar() throws SqoopOptions.InvalidOptionsException {
    try {
      SqoopOptions.toChar("\\095");
      fail("Expected exception");
    } catch (NumberFormatException nfe) {
      // expected.
    }
  }

  public void testErrHexChar() throws SqoopOptions.InvalidOptionsException {
    try {
      SqoopOptions.toChar("\\0x9K5");
      fail("Expected exception");
    } catch (NumberFormatException nfe) {
      // expected.
    }
  }

  // test that setting output delimiters also sets input delimiters 
  public void testDelimitersInherit() throws SqoopOptions.InvalidOptionsException {
    String [] args = {
        "--fields-terminated-by",
        "|"
    };

    SqoopOptions opts = new SqoopOptions();
    opts.parse(args);
    assertEquals('|', opts.getInputFieldDelim());
    assertEquals('|', opts.getOutputFieldDelim());
  }

  // test that setting output delimiters and setting input delims separately works
  public void testDelimOverride1() throws SqoopOptions.InvalidOptionsException {
    String [] args = {
        "--fields-terminated-by",
        "|",
        "--input-fields-terminated-by",
        "*"
    };

    SqoopOptions opts = new SqoopOptions();
    opts.parse(args);
    assertEquals('*', opts.getInputFieldDelim());
    assertEquals('|', opts.getOutputFieldDelim());
  }

  // test that the order in which delims are specified doesn't matter
  public void testDelimOverride2() throws SqoopOptions.InvalidOptionsException {
    String [] args = {
        "--input-fields-terminated-by",
        "*",
        "--fields-terminated-by",
        "|"
    };

    SqoopOptions opts = new SqoopOptions();
    opts.parse(args);
    assertEquals('*', opts.getInputFieldDelim());
    assertEquals('|', opts.getOutputFieldDelim());
  }

  public void testBadNumMappers1() {
    String [] args = {
      "--num-mappers",
      "x"
    };

    try {
      SqoopOptions opts = new SqoopOptions();
      opts.parse(args);
      fail("Expected InvalidOptionsException");
    } catch (SqoopOptions.InvalidOptionsException ioe) {
      // expected.
    }
  }

  public void testBadNumMappers2() {
    String [] args = {
      "-m",
      "x"
    };

    try {
      SqoopOptions opts = new SqoopOptions();
      opts.parse(args);
      fail("Expected InvalidOptionsException");
    } catch (SqoopOptions.InvalidOptionsException ioe) {
      // expected.
    }
  }

  public void testGoodNumMappers() throws SqoopOptions.InvalidOptionsException {
    String [] args = {
      "-m",
      "4"
    };

    SqoopOptions opts = new SqoopOptions();
    opts.parse(args);
    assertEquals(4, opts.getNumMappers());
  }
}
