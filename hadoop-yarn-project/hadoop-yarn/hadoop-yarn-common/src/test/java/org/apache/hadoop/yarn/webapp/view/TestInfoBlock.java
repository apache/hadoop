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

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.yarn.webapp.ResponseInfo;
import org.apache.hadoop.yarn.webapp.test.WebAppTests;
import org.junit.Before;
import org.junit.Test;

public class TestInfoBlock {

  public static StringWriter sw;

  public static PrintWriter pw;

  public static class MultilineInfoBlock extends InfoBlock{
    
    static ResponseInfo resInfo;

    static {
      resInfo = new ResponseInfo();
      resInfo._("Single_line_value", "This is one line.");
      resInfo._("Multiple_line_value", "This is first line.\nThis is second line.");	
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
    String expectedSinglelineData = "<tr class=\"odd\">\n"
      + " <th>\n Single_line_value\n <td>\n This is one line.\n";
    String expectedMultilineData = "<tr class=\"even\">\n"
      + " <th>\n Multiple_line_value\n <td>\n <div>\n"
      + " This is first line.\n </div>\n <div>\n"
      + " This is second line.\n </div>\n";
    assertTrue(output.contains(expectedSinglelineData) && output.contains(expectedMultilineData));
  }
}