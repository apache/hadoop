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

package org.apache.hadoop.yarn.webapp.hamlet;

import java.util.EnumSet;
import java.io.PrintWriter;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import static org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.*;

public class TestHamlet {

  @Test public void testHamlet() {
    Hamlet h = newHamlet().
        title("test").
        h1("heading 1").
        p("#id.class").
          b("hello").
          em("world!")._().
        div("#footer").
          _("Brought to you by").
          a("http://hostname/", "Somebody")._();

    PrintWriter out = h.getWriter();
    out.flush();
    assertEquals(0, h.nestLevel);
    verify(out).print("<title");
    verify(out).print("test");
    verify(out).print("</title>");
    verify(out).print("<h1");
    verify(out).print("heading 1");
    verify(out).print("</h1>");
    verify(out).print("<p");
    verify(out).print(" id=\"id\"");
    verify(out).print(" class=\"class\"");
    verify(out).print("<b");
    verify(out).print("hello");
    verify(out).print("</b>");
    verify(out).print("<em");
    verify(out).print("world!");
    verify(out).print("</em>");
    verify(out).print("<div");
    verify(out).print(" id=\"footer\"");
    verify(out).print("Brought to you by");
    verify(out).print("<a");
    verify(out).print(" href=\"http://hostname/\"");
    verify(out).print("Somebody");
    verify(out).print("</a>");
    verify(out).print("</div>");
    verify(out, never()).print("</p>");
  }

  @Test public void testTable() {
    Hamlet h = newHamlet().
        title("test table").
        link("style.css");

    TABLE t = h.table("#id");

    for (int i = 0; i < 3; ++i) {
      t.tr().td("1").td("2")._();
    }
    t._();

    PrintWriter out = h.getWriter();
    out.flush();
    assertEquals(0, h.nestLevel);
    verify(out).print("<table");
    verify(out).print("</table>");
    verify(out, atLeast(1)).print("</td>");
    verify(out, atLeast(1)).print("</tr>");
  }

  @Test public void testEnumAttrs() {
    Hamlet h = newHamlet().
        meta_http("Content-type", "text/html; charset=utf-8").
        title("test enum attrs").
        link().$rel("stylesheet").
          $media(EnumSet.of(Media.screen, Media.print)).
          $type("text/css").$href("style.css")._().
        link().$rel(EnumSet.of(LinkType.index, LinkType.start)).
          $href("index.html")._();

    h.div("#content")._("content")._();

    PrintWriter out = h.getWriter();
    out.flush();
    assertEquals(0, h.nestLevel);
    verify(out).print(" media=\"screen, print\"");
    verify(out).print(" rel=\"start index\"");
  }

  @Test public void testScriptStyle() {
    Hamlet h = newHamlet().
        script("a.js").script("b.js").
        style("h1 { font-size: 1.2em }");

    PrintWriter out = h.getWriter();
    out.flush();
    assertEquals(0, h.nestLevel);
    verify(out, times(2)).print(" type=\"text/javascript\"");
    verify(out).print(" type=\"text/css\"");
  }

  @Test public void testPreformatted() {
    Hamlet h = newHamlet().
        div().
          i("inline before pre").
          pre().
            _("pre text1\npre text2").
            i("inline in pre").
            _("pre text after inline")._().
          i("inline after pre")._();

    PrintWriter out = h.getWriter();
    out.flush();
    assertEquals(5, h.indents);
  }

  static class TestView1 implements SubView {
    @Override public void renderPartial() {}
  }

  static class TestView2 implements SubView {
    @Override public void renderPartial() {}
  }

  @Test public void testSubViews() {
    Hamlet h = newHamlet().
        title("test sub-views").
        div("#view1")._(TestView1.class)._().
        div("#view2")._(TestView2.class)._();

    PrintWriter out = h.getWriter();
    out.flush();
    assertEquals(0, h.nestLevel);
    verify(out).print("["+ TestView1.class.getName() +"]");
    verify(out).print("["+ TestView2.class.getName() +"]");
  }

  static Hamlet newHamlet() {
    PrintWriter out = spy(new PrintWriter(System.out));
    return new Hamlet(out, 0, false);
  }
}
