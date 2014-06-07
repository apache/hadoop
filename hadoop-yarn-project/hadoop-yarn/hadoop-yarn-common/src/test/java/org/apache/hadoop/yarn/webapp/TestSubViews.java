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

package org.apache.hadoop.yarn.webapp;

import org.apache.hadoop.yarn.webapp.test.WebAppTests;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.HtmlPage;
import java.io.PrintWriter;
import javax.servlet.http.HttpServletResponse;
import com.google.inject.Injector;

import org.junit.Test;
import static org.mockito.Mockito.*;

public class TestSubViews {

  static public class MainView extends HtmlPage {
    @Override
    public void render(Page.HTML<_> html) {
      html.
        body().
          div().
            _(Sub1.class)._().
          div().
            i("inline text").
            _(Sub2.class)._()._()._();
    }
  }

  static public class Sub1 extends HtmlBlock {
    @Override
    public void render(Block html) {
      html.
        div("#sub1").
          _("sub1 text")._();
    }
  }

  static public class Sub2 extends HtmlBlock {
    @Override
    public void render(Block html) {
      html.
        pre().
          _("sub2 text")._();
    }
  }

  @Test public void testSubView() throws Exception {
    Injector injector = WebAppTests.createMockInjector(this);
    injector.getInstance(MainView.class).render();

    PrintWriter out =
        injector.getInstance(HttpServletResponse.class).getWriter();
    out.flush();
    verify(out).print("sub1 text");
    verify(out).print("sub2 text");
    verify(out, times(16)).println(); // test inline transition across views
  }
}
