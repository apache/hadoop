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

package org.apache.hadoop.util;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class TestLineReader {

  /**
   * TEST_1: The test scenario is the tail of the buffer equals the starting
   * character/s of delimiter.
   *
   * The Test Data is such that,
   *
   * 1) we will have "&lt;/entity&gt;" as delimiter
   *
   * 2) The tail of the current buffer would be "&lt;/" which matches with the
   * starting character sequence of delimiter.
   *
   * 3) The Head of the next buffer would be "id&gt;" which does NOT match with
   * the remaining characters of delimiter.
   *
   * 4) Input data would be prefixed by char 'a' about
   * numberOfCharToFillTheBuffer times. So that, one iteration to buffer the
   * input data, would end at '&lt;/' ie equals starting 2 char of delimiter
   *
   * 5) For this we would take BufferSize as 64 * 1024;
   *
   * Check Condition In the second key value pair, the value should contain
   * "&lt;/" from currentToken and "id&gt;" from next token
   */
  @Test
  public void testCustomDelimiter1() throws Exception {

    final String delimiter = "</entity>";

    // Ending part of Input Data Buffer
    // It contains '</' ie delimiter character
    final String currentBufferTailToken = "</entity><entity><id>Gelesh</";

    // Supposing the start of next buffer is this
    final String nextBufferHeadToken = "id><name>Omathil</name></entity>";

    // Expected must capture from both the buffer, excluding Delimiter
    final String expected =
        (currentBufferTailToken + nextBufferHeadToken).replace(delimiter, "");

    final String testPartOfInput = currentBufferTailToken + nextBufferHeadToken;

    final int bufferSize = 64 * 1024;
    int numberOfCharToFillTheBuffer =
        bufferSize - currentBufferTailToken.length();

    final char[] fillBuffer = new char[numberOfCharToFillTheBuffer];

    // char 'a' as a filler for the test string
    Arrays.fill(fillBuffer, 'a');

    final StringBuilder fillerString = new StringBuilder();

    final String testData = fillerString + testPartOfInput;

    final LineReader lineReader = new LineReader(
        new ByteArrayInputStream(testData.getBytes(StandardCharsets.UTF_8)),
        delimiter.getBytes(StandardCharsets.UTF_8));

    final Text line = new Text();
    lineReader.readLine(line);
    lineReader.close();

    Assert.assertEquals(fillerString.toString(), line.toString());

    lineReader.readLine(line);
    Assert.assertEquals(expected, line.toString());
  }

  /**
   * TEST_2: The test scenario is such that, the character/s preceding the
   * delimiter, equals the starting character/s of delimiter.
   */
  @Test
  public void testCustomDelimiter2() throws Exception {
    final String delimiter = "record";
    final StringBuilder testStringBuilder = new StringBuilder();

    testStringBuilder.append(delimiter).append("Kerala ");
    testStringBuilder.append(delimiter).append("Bangalore");
    testStringBuilder.append(delimiter).append(" North Korea");
    testStringBuilder.append(delimiter).append(delimiter).append("Guantanamo");

    // ~EOF with 're'
    testStringBuilder.append(delimiter + "ecord" + "recor" + "core");

    final String testData = testStringBuilder.toString();

    final LineReader lineReader = new LineReader(
        new ByteArrayInputStream(testData.getBytes(StandardCharsets.UTF_8)),
        delimiter.getBytes((StandardCharsets.UTF_8)));

    final Text line = new Text();

    lineReader.readLine(line);
    Assert.assertEquals("", line.toString());
    lineReader.readLine(line);
    Assert.assertEquals("Kerala ", line.toString());

    lineReader.readLine(line);
    Assert.assertEquals("Bangalore", line.toString());

    lineReader.readLine(line);
    Assert.assertEquals(" North Korea", line.toString());

    lineReader.readLine(line);
    Assert.assertEquals("", line.toString());
    lineReader.readLine(line);
    Assert.assertEquals("Guantanamo", line.toString());

    lineReader.readLine(line);
    Assert.assertEquals(("ecord" + "recor" + "core"), line.toString());

    lineReader.close();
  }

  /**
   * Test 3: The test scenario is such that, aaabccc split by aaab.
   */
  @Test
  public void testCustomDelimiter3() throws Exception {
    final String testData = "aaaabccc";
    final String delimiter = "aaab";
    final LineReader lineReader = new LineReader(
        new ByteArrayInputStream(testData.getBytes(StandardCharsets.UTF_8)),
        delimiter.getBytes(StandardCharsets.UTF_8));

    final Text line = new Text();

    lineReader.readLine(line);
    Assert.assertEquals("a", line.toString());
    lineReader.readLine(line);
    Assert.assertEquals("ccc", line.toString());

    lineReader.close();
  }
}
