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

import com.google.inject.Injector;

import java.io.PrintWriter;

import org.apache.hadoop.yarn.webapp.MimeType;
import org.apache.hadoop.yarn.webapp.WebAppException;
import org.apache.hadoop.yarn.webapp.test.WebAppTests;
import org.apache.hadoop.yarn.webapp.view.HtmlPage;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class TestHtmlPage {
  
  public static class TestView extends HtmlPage {
    @Override
    public void render(Page.HTML<_> html) {
      html.
        title("test").
        p("#testid")._("test note")._()._();
    }
  }

  public static class ShortView extends HtmlPage {
    @Override
    public void render(Page.HTML<_> html) {
      html.
        title("short test").
        p()._("should throw");
    }
  }

  @Test public void testUsual() {
    Injector injector = WebAppTests.testPage(TestView.class);
    PrintWriter out = injector.getInstance(PrintWriter.class);

    // Verify the HTML page has correct meta tags in the header
    verify(out).print(" http-equiv=\"X-UA-Compatible\"");
    verify(out).print(" content=\"IE=8\"");
    verify(out).print(" http-equiv=\"Content-type\"");
    verify(out).print(String.format(" content=\"%s\"", MimeType.HTML));

    verify(out).print("test");
    verify(out).print(" id=\"testid\"");
    verify(out).print("test note");
  }

  @Test(expected=WebAppException.class) public void testShort() {
    WebAppTests.testPage(ShortView.class);
  }
}
