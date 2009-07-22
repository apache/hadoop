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
 * Test that the record parser works in a variety of configurations.
 */
public class TestRecordParser extends TestCase {

  private void assertListsEqual(String msg, List<String> expected, List<String> actual) {
    if (expected == null && actual != null) {
      if (null == msg) {
        msg = "expected null list";
      }

      fail(msg);
    } else if (expected != null && actual == null) {
      if (null == msg) {
        msg = "expected non-null list";
      }

      fail(msg);
    }

    if (expected == null && actual == null) {
      return; // ok. Both null; nothing to do.
    }

    int expectedLen = expected.size();
    int actualLen = actual.size();

    if (expectedLen != actualLen) {
      if (null == msg) {
        msg = "Expected list of length " + expectedLen + "; got " + actualLen;
      }

      fail(msg);
    }

    // Check the list contents.
    for (int i = 0; i < expectedLen; i++) {
      String expectedElem = expected.get(i);
      String actualElem = actual.get(i);

      if (expectedElem == null && actualElem != null) {
        if (null == msg) {
          msg = "Expected null element at position " + i + "; got [" + actualElem + "]";
        }

        fail(msg);
      }

      if (!expectedElem.equals(actualElem)) {
        if (null == msg) {
          msg = "Expected [" + expectedElem + "] at position " + i + "; got [" + actualElem + "]";
        }

        fail(msg);
      }
    }
  }

  private List<String> list(String [] items) {

    if (null == items) {
      return null;
    }

    ArrayList<String> asList = new ArrayList<String>();
    for (int i = 0; i < items.length; i++) {
      asList.add(items[i]);
    }

    return asList;
  }
  
  public void testEmptyLine() throws RecordParser.ParseError {
    // an empty line should return no fields.

    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', false);
    String [] strings = { };
    assertListsEqual(null, list(strings), parser.parseRecord(""));
  }

  public void testJustEOR() throws RecordParser.ParseError {
    // a line with just a newline char should return a single zero-length field.

    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', false);
    String [] strings = { "" };
    assertListsEqual(null, list(strings), parser.parseRecord("\n"));
  }

  public void testOneField() throws RecordParser.ParseError {
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', false);
    String [] strings = { "the field" };
    assertListsEqual(null, list(strings), parser.parseRecord("the field"));
  }

  public void testOneField2() throws RecordParser.ParseError {
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', false);
    String [] strings = { "the field" };
    assertListsEqual(null, list(strings), parser.parseRecord("the field\n"));
  }

  public void testQuotedField1() throws RecordParser.ParseError {
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', false);
    String [] strings = { "the field" };
    assertListsEqual(null, list(strings), parser.parseRecord("\"the field\"\n"));
  }

  public void testQuotedField2() throws RecordParser.ParseError {
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', false);
    String [] strings = { "the field" };
    assertListsEqual(null, list(strings), parser.parseRecord("\"the field\""));
  }

  public void testQuotedField3() throws RecordParser.ParseError {
    // quoted containing EOF
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', false);
    String [] strings = { "the ,field" };
    assertListsEqual(null, list(strings), parser.parseRecord("\"the ,field\""));
  }

  public void testQuotedField4() throws RecordParser.ParseError {
    // quoted containing multiple EOFs
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', false);
    String [] strings = { "the ,,field" };
    assertListsEqual(null, list(strings), parser.parseRecord("\"the ,,field\""));
  }

  public void testQuotedField5() throws RecordParser.ParseError {
    // quoted containing EOF and EOR
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', false);
    String [] strings = { "the ,\nfield" };
    assertListsEqual(null, list(strings), parser.parseRecord("\"the ,\nfield\""));
  }

  public void testQuotedField6() throws RecordParser.ParseError {
    // quoted containing EOR
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', false);
    String [] strings = { "the \nfield" };
    assertListsEqual(null, list(strings), parser.parseRecord("\"the \nfield\""));
  }

  public void testQuotedField7() throws RecordParser.ParseError {
    // quoted containing multiple EORs
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', false);
    String [] strings = { "the \n\nfield" };
    assertListsEqual(null, list(strings), parser.parseRecord("\"the \n\nfield\""));
  }

  public void testQuotedField8() throws RecordParser.ParseError {
    // quoted containing escaped quoted char
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', false);
    String [] strings = { "the \"field" };
    assertListsEqual(null, list(strings), parser.parseRecord("\"the \\\"field\""));
  }

  public void testUnquotedEscape1() throws RecordParser.ParseError {
    // field without quotes with an escaped EOF char.
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', false);
    String [] strings = { "the ,field" };
    assertListsEqual(null, list(strings), parser.parseRecord("the \\,field"));
  }

  public void testUnquotedEscape2() throws RecordParser.ParseError {
    // field without quotes with an escaped escape char.
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', false);
    String [] strings = { "the \\field" };
    assertListsEqual(null, list(strings), parser.parseRecord("the \\\\field"));
  }

  public void testTwoFields1() throws RecordParser.ParseError {
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', false);
    String [] strings = { "field1", "field2" };
    assertListsEqual(null, list(strings), parser.parseRecord("field1,field2"));
  }

  public void testTwoFields2() throws RecordParser.ParseError {
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', false);
    String [] strings = { "field1", "field2" };
    assertListsEqual(null, list(strings), parser.parseRecord("field1,field2\n"));
  }

  public void testTwoFields3() throws RecordParser.ParseError {
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', false);
    String [] strings = { "field1", "field2" };
    assertListsEqual(null, list(strings), parser.parseRecord("\"field1\",field2\n"));
  }

  public void testTwoFields4() throws RecordParser.ParseError {
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', false);
    String [] strings = { "field1", "field2" };
    assertListsEqual(null, list(strings), parser.parseRecord("field1,\"field2\"\n"));
  }

  public void testTwoFields5() throws RecordParser.ParseError {
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', false);
    String [] strings = { "field1", "field2" };
    assertListsEqual(null, list(strings), parser.parseRecord("field1,\"field2\""));
  }
  
  public void testRequiredQuotes0() throws RecordParser.ParseError {
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', true);
    String [] strings = { "field1", "field2" };
    assertListsEqual(null, list(strings), parser.parseRecord("\"field1\",\"field2\"\n"));
  }

  public void testRequiredQuotes1() throws RecordParser.ParseError {
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', true);
    String [] strings = { "field1", "field2" };
    assertListsEqual(null, list(strings), parser.parseRecord("\"field1\",\"field2\""));
  }

  public void testRequiredQuotes2() throws RecordParser.ParseError {
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', true);
    String [] strings = { "field1", "field2" };
    try {
      parser.parseRecord("\"field1\",field2");
      fail("Expected parse error for required quotes");
    } catch (RecordParser.ParseError pe) {
      // ok. expected.
    }
  }

  public void testRequiredQuotes3() throws RecordParser.ParseError {
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', true);
    String [] strings = { "field1", "field2" };
    try {
      parser.parseRecord("field1,\"field2\"");
      fail("Expected parse error for required quotes");
    } catch (RecordParser.ParseError pe) {
      // ok. expected.
    }
  }

  public void testRequiredQuotes4() throws RecordParser.ParseError {
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', true);
    String [] strings = { "field1", "field2" };
    try {
      parser.parseRecord("field1,\"field2\"\n");
      fail("Expected parse error for required quotes");
    } catch (RecordParser.ParseError pe) {
      // ok. expected.
    }
  }

  public void testNull() {
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', true);
    String input = null;
    try {
      parser.parseRecord(input);
      fail("Expected parse error for null string");
    } catch (RecordParser.ParseError pe) {
      // ok. expected.
    }
  }


  public void testEmptyFields1() throws RecordParser.ParseError {
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', false);
    String [] strings = { "", ""};
    assertListsEqual(null, list(strings), parser.parseRecord(","));
  }

  public void testEmptyFields2() throws RecordParser.ParseError {
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', false);
    String [] strings = { "", "" };
    assertListsEqual(null, list(strings), parser.parseRecord(",\n"));
  }

  public void testEmptyFields3() throws RecordParser.ParseError {
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', false);
    String [] strings = { "", "", "" };
    assertListsEqual(null, list(strings), parser.parseRecord(",,\n"));
  }

  public void testEmptyFields4() throws RecordParser.ParseError {
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', false);
    String [] strings = { "", "foo", "" };
    assertListsEqual(null, list(strings), parser.parseRecord(",foo,\n"));
  }

  public void testEmptyFields5() throws RecordParser.ParseError {
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', false);
    String [] strings = { "", "foo", "" };
    assertListsEqual(null, list(strings), parser.parseRecord(",foo,"));
  }

  public void testEmptyFields6() throws RecordParser.ParseError {
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', false);
    String [] strings = { "foo", "" };
    assertListsEqual(null, list(strings), parser.parseRecord("foo,"));
  }

  public void testTrailingText() throws RecordParser.ParseError {
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', false);
    String [] strings = { "foo", "bar" };
    assertListsEqual(null, list(strings), parser.parseRecord("foo,bar\nbaz"));
  }

  public void testTrailingText2() throws RecordParser.ParseError {
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', false);
    String [] strings = { "" };
    assertListsEqual(null, list(strings), parser.parseRecord("\nbaz"));
  }

  public void testLeadingEscape() throws RecordParser.ParseError {
    RecordParser parser = new RecordParser(',', '\n', '\"', '\\', false);
    String [] strings = { "\nbaz" };
    assertListsEqual(null, list(strings), parser.parseRecord("\\\nbaz"));
  }

  public void testEofIsEor() throws RecordParser.ParseError {
    RecordParser parser = new RecordParser(',', ',', '\"', '\\', false);
    String [] strings = { "three", "different", "fields" };
    assertListsEqual(null, list(strings), parser.parseRecord("three,different,fields"));
  }

  public void testEofIsEor2() throws RecordParser.ParseError {
    RecordParser parser = new RecordParser(',', ',', '\"', '\\', false);
    String [] strings = { "three", "different", "fields" };
    assertListsEqual(null, list(strings), parser.parseRecord("three,\"different\",fields"));
  }

  public void testRepeatedParse() throws RecordParser.ParseError {
    RecordParser parser = new RecordParser(',', ',', '\"', '\\', false);
    String [] strings = { "three", "different", "fields" };
    assertListsEqual(null, list(strings), parser.parseRecord("three,\"different\",fields"));

    String [] strings2 = { "foo", "bar" };
    assertListsEqual(null, list(strings2), parser.parseRecord("foo,\"bar\""));
  }

}
