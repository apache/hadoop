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

import java.io.CharArrayWriter;
import java.io.PrintWriter;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * A jquery-ui themeable error page
 */
@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
public class ErrorPage extends HtmlPage {

  @Override
  protected void render(Page.HTML<_> html) {
    set(JQueryUI.ACCORDION_ID, "msg");
    String title = "Sorry, got error "+ status();
    html.
      title(title).
      link(root_url("static","yarn.css")).
      _(JQueryUI.class). // an embedded sub-view
      style("#msg { margin: 1em auto; width: 88%; }",
            "#msg h1 { padding: 0.2em 1.5em; font: bold 1.3em serif; }").
      div("#msg").
        h1(title).
        div().
          _("Please consult").
          a("http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html",
            "RFC 2616")._(" for meanings of the error code.")._().
        h1("Error Details").
        pre().
          _(errorDetails())._()._()._();
  }

  protected String errorDetails() {
    if (!$(ERROR_DETAILS).isEmpty()) {
      return $(ERROR_DETAILS);
    }
    if (error() != null) {
      return toStackTrace(error(), 1024 * 64);
    }
    return "No exception was thrown.";
  }

  public static String toStackTrace(Throwable error, int cutoff) {
    // default initial size is 32 chars
    CharArrayWriter buffer = new CharArrayWriter(8 * 1024);
    error.printStackTrace(new PrintWriter(buffer));
    return buffer.size() < cutoff ? buffer.toString()
        : buffer.toString().substring(0, cutoff);
  }
}
