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

import static org.apache.hadoop.yarn.util.StringHelper.join;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.webapp.SubView;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;

/**
 * A simpler two column layout implementation with a header, a navigation bar
 * on the left, content on the right, and a footer. Works with resizable themes.
 * @see TwoColumnCssLayout
 */
@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
public class TwoColumnLayout extends HtmlPage {

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.yarn.webapp.view.HtmlPage#render(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
   */
  @Override protected void render(Page.HTML<__> html) {
    preHead(html);
    html.
      title($(TITLE)).
      link(root_url("static", "yarn.css")).
      style("#layout { height: 100%; }",
            "#layout thead td { height: 3em; }",
            "#layout #navcell { width: 11em; padding: 0 1em; }",
            "#layout td.content { padding-top: 0 }",
            "#layout tbody { vertical-align: top; }",
            "#layout tfoot td { height: 4em; }").
        __(JQueryUI.class);
    postHead(html);
    JQueryUI.jsnotice(html);
    html.
      table("#layout.ui-widget-content").
        thead().
          tr().
            td().$colspan(2).
        __(header()).__().__().__().
        tfoot().
          tr().
            td().$colspan(2).
        __(footer()).__().__().__().
        tbody().
          tr().
            td().$id("navcell").
        __(nav()).__().
            td().$class("content").
        __(content()).__().__().__().__().__();
  }

  /**
   * Do what needs to be done before the header is rendered.  This usually
   * involves setting page variables for Javascript and CSS rendering.
   * @param html the html to use to render. 
   */
  protected void preHead(Page.HTML<__> html) {
  }

  /**
   * Do what needs to be done after the header is rendered.
   * @param html the html to use to render. 
   */
  protected void postHead(Page.HTML<__> html) {
  }

  /**
   * @return the class that will render the header of the page.
   */
  protected Class<? extends SubView> header() {
    return HeaderBlock.class;
  }

  /**
   * @return the class that will render the content of the page.
   */
  protected Class<? extends SubView> content() {
    return LipsumBlock.class;
  }

  /**
   * @return the class that will render the navigation bar.
   */
  protected Class<? extends SubView> nav() {
    return NavBlock.class;
  }

  /**
   * @return the class that will render the footer.
   */
  protected Class<? extends SubView> footer() {
    return FooterBlock.class;
  }

  /**
   * Sets up a table to be a consistent style.
   * @param html the HTML to use to render.
   * @param tableId the ID of the table to set styles on.
   * @param innerStyles any other styles to add to the table.
   */
  protected void setTableStyles(Page.HTML<__> html, String tableId,
                                String... innerStyles) {
    List<String> styles = Lists.newArrayList();
    styles.add(join('#', tableId, "_paginate span {font-weight:normal}"));
    styles.add(join('#', tableId, " .progress {width:8em}"));
    styles.add(join('#', tableId, "_processing {top:-1.5em; font-size:1em;"));
    styles.add("  color:#000; background:#fefefe}");
    for (String style : innerStyles) {
      styles.add(join('#', tableId, " ", style));
    }
    html.style(styles.toArray());
  }
}
