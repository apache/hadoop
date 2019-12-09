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

package org.apache.hadoop.yarn.webapp.view;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.yarn.webapp.ResponseInfo;
import org.apache.hadoop.yarn.webapp.test.WebAppTests;
import org.junit.Before;
import org.junit.Test;

public class TestInfoBlock {

  public static StringWriter sw;

  public static PrintWriter pw;

  static final String JAVASCRIPT = "<script>alert('text')</script>";
  static final String JAVASCRIPT_ESCAPED =
      "&lt;script&gt;alert('text')&lt;/script&gt;";

  public static class JavaScriptInfoBlock extends InfoBlock{

    static ResponseInfo resInfo;

    static {
      resInfo = new ResponseInfo();
      resInfo.__("User_Name", JAVASCRIPT);
    }

    @Override
    public PrintWriter writer() {
      return TestInfoBlock.pw;
    }

    JavaScriptInfoBlock(ResponseInfo info) {
      super(resInfo);
    }

    public JavaScriptInfoBlock() {
      super(resInfo);
    }
  }

  public static class MultilineInfoBlock extends InfoBlock{
    
    static ResponseInfo resInfo;

    static {
      resInfo = new ResponseInfo();
      resInfo.__("Multiple_line_value", "This is one line.");
      resInfo.__("Multiple_line_value", "This is first line.\nThis is second line.");
    }

    @Override
    public PrintWriter writer() {
      return TestInfoBlock.pw;
    }

    MultilineInfoBlock(ResponseInfo info) {
      super(resInfo);
    }

    public MultilineInfoBlock() {
      super(resInfo);
    }
  }

  @Before
  public void setup() {
    sw = new StringWriter();
    pw = new PrintWriter(sw);
  }

  @Test(timeout=60000L)
  public void testMultilineInfoBlock() throws Exception{

    WebAppTests.testBlock(MultilineInfoBlock.class);
    TestInfoBlock.pw.flush();
    String output = TestInfoBlock.sw.toString().replaceAll(" +", " ");
    String expectedMultilineData1 = String.format("<tr class=\"odd\">%n"
      + " <th>%n Multiple_line_value%n </th>%n"
      + " <td>%n This is one line.%n </td>%n");
    String expectedMultilineData2 = String.format("<tr class=\"even\">%n"
      + " <th>%n Multiple_line_value%n </th>%n <td>%n <div>%n"
      + " This is first line.%n </div>%n <div>%n"
      + " This is second line.%n </div>%n");
    assertTrue(output.contains(expectedMultilineData1) && output.contains(expectedMultilineData2));
  }
  
  @Test(timeout=60000L)
  public void testJavaScriptInfoBlock() throws Exception{
    WebAppTests.testBlock(JavaScriptInfoBlock.class);
    TestInfoBlock.pw.flush();
    String output = TestInfoBlock.sw.toString();
    assertFalse(output.contains("<script>"));
    assertTrue(output.contains(JAVASCRIPT_ESCAPED));
  }
}
