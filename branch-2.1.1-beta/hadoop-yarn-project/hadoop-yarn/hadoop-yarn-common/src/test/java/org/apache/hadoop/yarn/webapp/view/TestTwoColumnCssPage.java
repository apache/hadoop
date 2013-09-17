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

import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.webapp.Controller;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.test.WebAppTests;
import org.apache.hadoop.yarn.webapp.view.HtmlPage;
import org.apache.hadoop.yarn.webapp.view.TwoColumnCssLayout;
import org.junit.Test;

public class TestTwoColumnCssPage {

  public static class TestController extends Controller {
    @Override
    public void index() {
      set("title", "Testing a Two Column Layout");
      set("ui.accordion.id", "nav");
      render(TwoColumnCssLayout.class);
    }

    public void names() {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < 8; ++i) {
        sb.append(MockApps.newAppName()).append(' ');
      }
      setTitle(sb.toString());
    }

    public void textnames() {
      names();
      renderText($("title"));
    }
  }

  public static class TestView extends HtmlPage {
    @Override
    public void render(Page.HTML<_> html) {
      html.
        title($("title")).
        h1($("title"))._();
    }
  }

  @Test public void shouldNotThrow() {
    WebAppTests.testPage(TwoColumnCssLayout.class);
  }

  public static void main(String[] args) {
    WebApps.$for("test").at(8888).inDevMode().start().joinThread();
  }
}
