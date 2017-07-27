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

package org.apache.hadoop.yarn.webapp.example;

import com.google.inject.Inject;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.webapp.Controller;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.view.HtmlPage;

/**
 * The embedded UI serves two pages at:
 * <br>http://localhost:8888/my and
 * <br>http://localhost:8888/my/anythingYouWant
 */
@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
public class MyApp {

  // This is an app API
  public String anyAPI() { return "anything, really!"; }

  // Note this is static so it can be in any files.
  public static class MyController extends Controller {
    final MyApp app;

    // The app injection is optional
    @Inject MyController(MyApp app, RequestContext ctx) {
      super(ctx);
      this.app = app;
    }

    @Override
    public void index() {
      set("anything", "something");
    }

    public void anythingYouWant() {
      set("anything", app.anyAPI());
    }
  }

  // Ditto
  public static class MyView extends HtmlPage {
    // You can inject the app in views if needed.
    @Override
    public void render(Page.HTML<__> html) {
      html.
        title("My App").
        p("#content_id_for_css_styling").
          __("You can have", $("anything")).__().__();
      // Note, there is no __(); (to parent element) method at root level.
      // and IDE provides instant feedback on what level you're on in
      // the auto-completion drop-downs.
    }
  }

  public static void main(String[] args) throws Exception {
    WebApps.$for(new MyApp()).at(8888).inDevMode().start().joinThread();
  }
}
