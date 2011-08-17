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

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import java.util.List;

import static org.apache.hadoop.yarn.util.StringHelper.*;
import static org.apache.hadoop.yarn.webapp.Params.*;

import org.apache.hadoop.yarn.webapp.SubView;

/**
 * A simpler two column layout implementation. Works with resizable themes.
 * @see TwoColumnCssLayout
 */
public class TwoColumnLayout extends HtmlPage {

  @Override protected void render(Page.HTML<_> html) {
    preHead(html);
    html.
      title($(TITLE)).
      link("/static/yarn.css").
      style("#layout { height: 100%; }",
            "#layout thead td { height: 3em; }",
            "#layout #navcell { width: 11em; padding: 0 1em; }",
            "#layout td.content { padding-top: 0 }",
            "#layout tbody { vertical-align: top; }",
            "#layout tfoot td { height: 4em; }").
      _(JQueryUI.class);
    postHead(html);
    JQueryUI.jsnotice(html);
    html.
      table("#layout.ui-widget-content").
        thead().
          tr().
            td().$colspan(2).
              _(header())._()._()._().
        tfoot().
          tr().
            td().$colspan(2).
              _(footer())._()._()._().
        tbody().
          tr().
            td().$id("navcell").
              _(nav())._().
            td().$class("content").
              _(content())._()._()._()._()._();
  }

  protected void preHead(Page.HTML<_> html) {
  }

  protected void postHead(Page.HTML<_> html) {
  }

  protected Class<? extends SubView> header() {
    return HeaderBlock.class;
  }

  protected Class<? extends SubView> content() {
    return LipsumBlock.class;
  }

  protected Class<? extends SubView> nav() {
    return NavBlock.class;
  }

  protected Class<? extends SubView> footer() {
    return FooterBlock.class;
  }

  protected void setTableStyles(Page.HTML<_> html, String tableId,
                                String... innerStyles) {
    List<String> styles = Lists.newArrayList();
    styles.add(join('#', tableId, "_paginate span {font-weight:normal}"));
    styles.add(join('#', tableId, " .progress {width:8em}"));
    styles.add(join('#', tableId, "_processing {top:-1.5em; font-size:1em;"));
    styles.add("  color:#000; background:rgba(255, 255, 255, 0.8)}");
    for (String style : innerStyles) {
      styles.add(join('#', tableId, " ", style));
    }
    html.style(styles.toArray());
  }
}
