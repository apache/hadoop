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
import java.util.EnumSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.webapp.MimeType;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.WebAppException;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

/**
 * The parent class of all HTML pages.  Override 
 * {@link #render(org.apache.hadoop.yarn.webapp.hamlet2.Hamlet.HTML)}
 * to actually render the page.
 */
@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
public abstract class HtmlPage extends TextView {

  public static class __ implements Hamlet.__ {
  }

  public class Page extends Hamlet {
    Page(PrintWriter out) {
      super(out, 0, false);
    }

    @Override
    protected void subView(Class<? extends SubView> cls) {
      context().set(nestLevel(), wasInline());
      render(cls);
      setWasInline(context().wasInline());
    }

    public HTML<HtmlPage.__> html() {
      return new HTML<HtmlPage.__>("html", null, EnumSet.of(EOpt.ENDTAG));
    }
  }

  public static final String DOCTYPE =
      "<!DOCTYPE html PUBLIC \"-//W3C//DTD HTML 4.01//EN\""+
      " \"http://www.w3.org/TR/html4/strict.dtd\">";

  private Page page;

  private Page page() {
    if (page == null) {
      page = new Page(writer());
    }
    return page;
  }

  protected HtmlPage() {
    this(null);
  }

  protected HtmlPage(ViewContext ctx) {
    super(ctx, MimeType.HTML);
  }

  @Override
  public void render() {
    putWithoutEscapeHtml(DOCTYPE);
    render(page().html().meta_http("X-UA-Compatible", "IE=8")
        .meta_http("Content-type", MimeType.HTML));
    if (page().nestLevel() != 0) {
      throw new WebAppException("Error rendering page: nestLevel="+
                                page().nestLevel());
    }
  }

  /**
   * Render the the HTML page.
   * @param html the page to render data to.
   */
  protected abstract void render(Page.HTML<__> html);
}

