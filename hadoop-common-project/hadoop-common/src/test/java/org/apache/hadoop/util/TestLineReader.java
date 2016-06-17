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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.junit.Test;

import org.junit.Assert;

public class TestLineReader {
  private LineReader lineReader;
  private String TestData;
  private String Delimiter;
  private Text line;

  @Test
  public void testCustomDelimiter() throws Exception {
    /* TEST_1
     * The test scenario is the tail of the buffer
     * equals the starting character/s of delimiter
     * 
     * The Test Data is such that,
     *   
     * 1) we will have "</entity>" as delimiter  
     *  
     * 2) The tail of the current buffer would be "</"
     *    which matches with the starting character sequence of delimiter.
     *    
     * 3) The Head of the next buffer would be   "id>" 
     *    which does NOT match with the remaining characters of delimiter.
     *   
     * 4) Input data would be prefixed by char 'a' 
     *    about numberOfCharToFillTheBuffer times.
     *    So that, one iteration to buffer the input data,
     *    would end at '</' ie equals starting 2 char of delimiter  
     *     
     * 5) For this we would take BufferSize as 64 * 1024;
     * 
     * Check Condition
     *  In the second key value pair, the value should contain 
     *  "</"  from currentToken and
     *  "id>" from next token
     */  
    
    Delimiter="</entity>"; 
    
    String CurrentBufferTailToken=
        "</entity><entity><id>Gelesh</";
    // Ending part of Input Data Buffer
    // It contains '</' ie delimiter character 
    
    String NextBufferHeadToken=
        "id><name>Omathil</name></entity>";
    // Supposing the start of next buffer is this
    
    String Expected = 
        (CurrentBufferTailToken+NextBufferHeadToken)
        .replace(Delimiter, "");                       
    // Expected ,must capture from both the buffer, excluding Delimiter
  
    String TestPartOfInput = CurrentBufferTailToken+NextBufferHeadToken;
  
    int BufferSize=64 * 1024;
    int numberOfCharToFillTheBuffer =
            BufferSize - CurrentBufferTailToken.length();
    StringBuilder fillerString=new StringBuilder();
    for (int i=0; i<numberOfCharToFillTheBuffer; i++) {
      fillerString.append('a'); // char 'a' as a filler for the test string
    }

    TestData = fillerString + TestPartOfInput;
    lineReader = new LineReader(
        new ByteArrayInputStream(TestData.getBytes()), Delimiter.getBytes());
    
    line = new Text();
    
    lineReader.readLine(line);
    Assert.assertEquals(fillerString.toString(), line.toString());
    
    lineReader.readLine(line);
    Assert.assertEquals(Expected, line.toString());
    
    /*TEST_2
     * The test scenario is such that,
     * the character/s preceding the delimiter,
     * equals the starting character/s of delimiter
     */
    
    Delimiter = "record";
    StringBuilder TestStringBuilder = new StringBuilder();
    
    TestStringBuilder.append(Delimiter + "Kerala ");
    TestStringBuilder.append(Delimiter + "Bangalore");
    TestStringBuilder.append(Delimiter + " North Korea");
    TestStringBuilder.append(Delimiter + Delimiter+
                        "Guantanamo");
    TestStringBuilder.append(Delimiter + "ecord"
            + "recor" + "core"); //~EOF with 're'
    
    TestData=TestStringBuilder.toString();
    
    lineReader = new LineReader(
        new ByteArrayInputStream(TestData.getBytes()), Delimiter.getBytes());

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
    Assert.assertEquals(("ecord"+"recor"+"core"), line.toString());

    // Test 3
    // The test scenario is such that,
    // aaaabccc split by aaab
    TestData = "aaaabccc";
    Delimiter = "aaab";
    lineReader = new LineReader(
        new ByteArrayInputStream(TestData.getBytes()), Delimiter.getBytes());

    lineReader.readLine(line);
    Assert.assertEquals("a", line.toString());
    lineReader.readLine(line);
    Assert.assertEquals("ccc", line.toString());
  }
}
