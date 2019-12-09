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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.webapp.Controller;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.view.HtmlPage;

/**
 * The obligatory example. No xml/jsp/templates/config files! No
 * proliferation of strange annotations either :)
 *
 * <p>3 in 1 example. Check results at
 * <br>http://localhost:8888/hello and
 * <br>http://localhost:8888/hello/html
 * <br>http://localhost:8888/hello/json
 */
@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
public class HelloWorld {
  public static class Hello extends Controller {
    @Override public void index() { renderText("Hello world!"); }
    public void html() { setTitle("Hello world!"); }
    public void json() { renderJSON("Hello world!"); }
  }

  public static class HelloView extends HtmlPage {
    @Override protected void render(Page.HTML<__> html) {
      html. // produces valid html 4.01 strict
        title($("title")).
        p("#hello-for-css").
          __($("title")).__().__();
    }
  }

  public static void main(String[] args) {
    WebApps.$for(new HelloWorld()).at(8888).inDevMode().start().joinThread();
  }
}
