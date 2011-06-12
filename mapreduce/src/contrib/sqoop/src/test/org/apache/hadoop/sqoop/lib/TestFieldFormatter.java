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

package org.apache.hadoop.sqoop.lib;

import java.util.ArrayList;
import java.util.List;
import junit.framework.TestCase;


/**
 * Test that the field formatter works in a variety of configurations
 */
public class TestFieldFormatter extends TestCase {
  
  public void testAllEmpty() {
    char [] chars = new char[0];
    String result = FieldFormatter.escapeAndEnclose("", "", "", chars, false);
    assertEquals("", result);
  }

  public void testNullArgs() {
    String result = FieldFormatter.escapeAndEnclose("", null, null, null, false);
    assertEquals("", result);
  }

  public void testBasicStr() {
    String result = FieldFormatter.escapeAndEnclose("foo", null, null, null, false);
    assertEquals("foo", result);
  }

  public void testEscapeSlash() {
    String result = FieldFormatter.escapeAndEnclose("foo\\bar", "\\", "\"", null, false);
    assertEquals("foo\\\\bar", result);
  }

  public void testMustEnclose() {
    String result = FieldFormatter.escapeAndEnclose("foo", null, "\"", null, true);
    assertEquals("\"foo\"", result);
  }

  public void testEncloseComma1() {
    char [] chars = { ',' };

    String result = FieldFormatter.escapeAndEnclose("foo,bar", "\\", "\"", chars, false);
    assertEquals("\"foo,bar\"", result);
  }

  public void testEncloseComma2() {
    char [] chars = { '\n', ',' };

    String result = FieldFormatter.escapeAndEnclose("foo,bar", "\\", "\"", chars, false);
    assertEquals("\"foo,bar\"", result);
  }

  public void testEncloseComma3() {
    char [] chars = { ',', '\n' };

    String result = FieldFormatter.escapeAndEnclose("foo,bar", "\\", "\"", chars, false);
    assertEquals("\"foo,bar\"", result);
  }

  public void testNoNeedToEnclose() {
    char [] chars = { ',', '\n' };

    String result = FieldFormatter.escapeAndEnclose(
        "just another string", "\\", "\"", chars, false);
    assertEquals("just another string", result);
  }

  public void testCannotEnclose1() {
    char [] chars = { ',', '\n' };

    // can't enclose because encloser is ""
    String result = FieldFormatter.escapeAndEnclose("foo,bar", "\\", "", chars, false);
    assertEquals("foo,bar", result);
  }

  public void testCannotEnclose2() {
    char [] chars = { ',', '\n' };

    // can't enclose because encloser is null
    String result = FieldFormatter.escapeAndEnclose("foo,bar", "\\", null, chars, false);
    assertEquals("foo,bar", result);
  }

  public void testEmptyCharToEscapeString() {
    // test what happens when the escape char is null. It should encode the null char.

    char nul = '\000';
    String s = "" + nul;
    assertEquals("\000", s);
  }
  
  public void testEscapeCentralQuote() {
    String result = FieldFormatter.escapeAndEnclose("foo\"bar", "\\", "\"", null, false);
    assertEquals("foo\\\"bar", result);
  }

  public void testEscapeMultiCentralQuote() {
    String result = FieldFormatter.escapeAndEnclose("foo\"\"bar", "\\", "\"", null, false);
    assertEquals("foo\\\"\\\"bar", result);
  }

  public void testDoubleEscape() {
    String result = FieldFormatter.escapeAndEnclose("foo\\\"bar", "\\", "\"", null, false);
    assertEquals("foo\\\\\\\"bar", result);
  }

  public void testReverseEscape() {
    String result = FieldFormatter.escapeAndEnclose("foo\"\\bar", "\\", "\"", null, false);
    assertEquals("foo\\\"\\\\bar", result);
  }

  public void testQuotedEncloser() {
    char [] chars = { ',', '\n' };
    
    String result = FieldFormatter.escapeAndEnclose("foo\",bar", "\\", "\"", chars, false);
    assertEquals("\"foo\\\",bar\"", result);
  }

  public void testQuotedEscape() {
    char [] chars = { ',', '\n' };
    
    String result = FieldFormatter.escapeAndEnclose("foo\\,bar", "\\", "\"", chars, false);
    assertEquals("\"foo\\\\,bar\"", result);
  }
}
