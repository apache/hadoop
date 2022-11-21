/*
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
package org.apache.hadoop.yarn.webapp.hamlet2;

import java.io.PrintWriter;

import org.junit.jupiter.api.Test;

import org.apache.hadoop.yarn.webapp.hamlet2.HamletSpec.CoreAttrs;
import org.apache.hadoop.yarn.webapp.hamlet2.HamletSpec.H1;
import org.apache.hadoop.yarn.webapp.hamlet2.HamletSpec.LINK;
import org.apache.hadoop.yarn.webapp.hamlet2.HamletSpec.SCRIPT;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TestHamletImpl {
  /**
   * Test the generic implementation methods
   * @see TestHamlet for Hamlet syntax
   */
  @Test
  void testGeneric() {
    PrintWriter out = spy(new PrintWriter(System.out));
    HamletImpl hi = new HamletImpl(out, 0, false);
    hi.
        root("start")._attr("name", "value").
        __("start text").
        elem("sub")._attr("name", "value").
        __("sub text").__().
        elem("sub1")._noEndTag()._attr("boolean", null).
        __("sub1text").__().
        __("start text2").
        elem("pre")._pre().
        __("pre text").
        elem("i")._inline().__("inline").__().__().
        elem("i")._inline().__("inline after pre").__().
        __("start text3").
        elem("sub2").
        __("sub2text").__().
        elem("sub3")._noEndTag().
        __("sub3text").__().
        elem("sub4")._noEndTag().
        elem("i")._inline().__("inline").__().
        __("sub4text").__().__();

    out.flush();
    assertEquals(0, hi.nestLevel);
    assertEquals(20, hi.indents);
    verify(out).print("<start");
    verify(out, times(2)).print(" name=\"value\"");
    verify(out).print(" boolean");
    verify(out).print("</start>");
    verify(out, never()).print("</sub1>");
    verify(out, never()).print("</sub3>");
    verify(out, never()).print("</sub4>");
  }

  @Test
  void testSetSelector() {
    CoreAttrs e = mock(CoreAttrs.class);
    HamletImpl.setSelector(e, "#id.class");

    verify(e).$id("id");
    verify(e).$class("class");

    H1 t = mock(H1.class);
    HamletImpl.setSelector(t, "#id.class").__("heading");

    verify(t).$id("id");
    verify(t).$class("class");
    verify(t).__("heading");
  }

  @Test
  void testSetLinkHref() {
    LINK link = mock(LINK.class);
    HamletImpl.setLinkHref(link, "uri");
    HamletImpl.setLinkHref(link, "style.css");

    verify(link).$href("uri");
    verify(link).$rel("stylesheet");
    verify(link).$href("style.css");

    verifyNoMoreInteractions(link);
  }

  @Test
  void testSetScriptSrc() {
    SCRIPT script = mock(SCRIPT.class);
    HamletImpl.setScriptSrc(script, "uri");
    HamletImpl.setScriptSrc(script, "script.js");

    verify(script).$src("uri");
    verify(script).$type("text/javascript");
    verify(script).$src("script.js");

    verifyNoMoreInteractions(script);
  }
}