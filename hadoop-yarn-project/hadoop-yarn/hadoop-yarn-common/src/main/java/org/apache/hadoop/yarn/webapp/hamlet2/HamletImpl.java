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

package org.apache.hadoop.yarn.webapp.hamlet2;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.*;
import org.apache.hadoop.thirdparty.com.google.common.base.Splitter;
import org.apache.hadoop.thirdparty.com.google.common.collect.Iterables;

import java.io.PrintWriter;
import java.util.EnumSet;
import static java.util.EnumSet.*;
import java.util.Iterator;

import static org.apache.commons.text.StringEscapeUtils.*;
import static org.apache.hadoop.yarn.webapp.hamlet2.HamletImpl.EOpt.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.WebAppException;


/**
 * A simple unbuffered generic hamlet implementation.
 *
 * Zero copy but allocation on every element, which could be
 * optimized to use a thread-local element pool.
 *
 * Prints HTML as it builds. So the order is important.
 */
@InterfaceAudience.Private
public class HamletImpl extends HamletSpec {
  private static final String INDENT_CHARS = "  ";
  private static final Splitter SS = Splitter.on('.').
      omitEmptyStrings().trimResults();
  private static final Joiner SJ = Joiner.on(' ');
  private static final Joiner CJ = Joiner.on(", ");
  static final int S_ID = 0;
  static final int S_CLASS = 1;

  int nestLevel;
  int indents; // number of indent() called. mostly for testing.
  private final PrintWriter out;
  private final StringBuilder sb = new StringBuilder(); // not shared
  private boolean wasInline = false;

  /**
   * Element options. (whether it needs end tag, is inline etc.)
   */
  public enum EOpt {
    /** needs end(close) tag */
    ENDTAG,
    /** The content is inline */
    INLINE,
    /** The content is preformatted */
    PRE
  };

  /**
   * The base class for elements
   * @param <T> type of the parent (containing) element for the element
   */
  public class EImp<T extends __> implements _Child {
    private final String name;
    private final T parent; // short cut for parent element
    private final EnumSet<EOpt> opts; // element options

    private boolean started = false;
    private boolean attrsClosed = false;

    EImp(String name, T parent, EnumSet<EOpt> opts) {
      this.name = name;
      this.parent = parent;
      this.opts = opts;
    }

    @Override
    public T __() {
      closeAttrs();
      --nestLevel;
      printEndTag(name, opts);
      return parent;
    }

    protected void _p(boolean quote, Object... args) {
      closeAttrs();
      for (Object s : args) {
        if (!opts.contains(PRE)) {
          indent(opts);
        }
        out.print(quote ? escapeHtml4(String.valueOf(s))
                        : String.valueOf(s));
        if (!opts.contains(INLINE) && !opts.contains(PRE)) {
          out.println();
        }
      }
    }

    protected void _v(Class<? extends SubView> cls) {
      closeAttrs();
      subView(cls);
    }

    protected void closeAttrs() {
      if (!attrsClosed) {
        startIfNeeded();
        ++nestLevel;
        out.print('>');
        if (!opts.contains(INLINE) && !opts.contains(PRE)) {
          out.println();
        }
        attrsClosed = true;
      }
    }

    protected void addAttr(String name, String value) {
      checkState(!attrsClosed, "attribute added after content");
      startIfNeeded();
      printAttr(name, value);
    }

    protected void addAttr(String name, Object value) {
      addAttr(name, String.valueOf(value));
    }

    protected void addMediaAttr(String name, EnumSet<Media> media) {
      // 6.13 comma-separated list
      addAttr(name, CJ.join(media));
    }

    protected void addRelAttr(String name, EnumSet<LinkType> types) {
      // 6.12 space-separated list
      addAttr(name, SJ.join(types));
    }

    private void startIfNeeded() {
      if (!started) {
        printStartTag(name, opts);
        started = true;
      }
    }

    protected void _inline(boolean choice) {
      if (choice) {
        opts.add(INLINE);
      } else {
        opts.remove(INLINE);
      }
    }

    protected void _endTag(boolean choice) {
      if (choice) {
        opts.add(ENDTAG);
      } else {
        opts.remove(ENDTAG);
      }
    }

    protected void _pre(boolean choice) {
      if (choice) {
        opts.add(PRE);
      } else {
        opts.remove(PRE);
      }
    }
  }

  public class Generic<T extends __> extends EImp<T> implements PCData {
    Generic(String name, T parent, EnumSet<EOpt> opts) {
      super(name, parent, opts);
    }

    public Generic<T> _inline() {
      super._inline(true);
      return this;
    }

    public Generic<T> _noEndTag() {
      super._endTag(false);
      return this;
    }

    public Generic<T> _pre() {
      super._pre(true);
      return this;
    }

    public Generic<T> _attr(String name, String value) {
      addAttr(name, value);
      return this;
    }

    public Generic<Generic<T>> _elem(String name, EnumSet<EOpt> opts) {
      closeAttrs();
      return new Generic<Generic<T>>(name, this, opts);
    }

    public Generic<Generic<T>> elem(String name) {
      return _elem(name, of(ENDTAG));
    }

    @Override
    public Generic<T> __(Object... lines) {
      _p(true, lines);
      return this;
    }

    @Override
    public Generic<T> _r(Object... lines) {
      _p(false, lines);
      return this;
    }
  }

  public HamletImpl(PrintWriter out, int nestLevel, boolean wasInline) {
    this.out = out;
    this.nestLevel = nestLevel;
    this.wasInline = wasInline;
  }

  public int nestLevel() {
    return nestLevel;
  }

  public boolean wasInline() {
    return wasInline;
  }

  public void setWasInline(boolean state) {
    wasInline = state;
  }

  public PrintWriter getWriter() {
    return out;
  }

  /**
   * Create a root-level generic element.
   * Mostly for testing purpose.
   * @param <T> type of the parent element
   * @param name of the element
   * @param opts {@link EOpt element options}
   * @return the element
   */
  public <T extends __>
  Generic<T> root(String name, EnumSet<EOpt> opts) {
    return new Generic<T>(name, null, opts);
  }

  public <T extends __> Generic<T> root(String name) {
    return root(name, of(ENDTAG));
  }

  protected void printStartTag(String name, EnumSet<EOpt> opts) {
    indent(opts);
    sb.setLength(0);
    out.print(sb.append('<').append(name).toString()); // for easier mock test
  }

  protected void indent(EnumSet<EOpt> opts) {
    if (opts.contains(INLINE) && wasInline) {
      return;
    }
    if (wasInline) {
      out.println();
    }
    wasInline = opts.contains(INLINE) || opts.contains(PRE);
    for (int i = 0; i < nestLevel; ++i) {
      out.print(INDENT_CHARS);
    }
    ++indents;
  }

  protected void printEndTag(String name, EnumSet<EOpt> opts) {
    if (!opts.contains(ENDTAG)) {
      return;
    }
    if (!opts.contains(PRE)) {
      indent(opts);
    } else {
      wasInline = opts.contains(INLINE);
    }
    sb.setLength(0);
    out.print(sb.append("</").append(name).append('>').toString()); // ditto
    if (!opts.contains(INLINE)) {
      out.println();
    }
  }

  protected void printAttr(String name, String value) {
    sb.setLength(0);
    sb.append(' ').append(name);
    if (value != null) {
      sb.append("=\"").append(escapeHtml4(value)).append("\"");
    }
    out.print(sb.toString());
  }

  /**
   * Sub-classes should override this to do something interesting.
   * @param cls the sub-view class
   */
  protected void subView(Class<? extends SubView> cls) {
    indent(of(ENDTAG)); // not an inline view
    sb.setLength(0);
    out.print(sb.append('[').append(cls.getName()).append(']').toString());
    out.println();
  }

  /**
   * Parse selector into id and classes
   * @param selector in the form of (#id)?(.class)*
   * @return an two element array [id, "space-separated classes"].
   *         Either element could be null.
   * @throws WebAppException when both are null or syntax error.
   */
  public static String[] parseSelector(String selector) {
    String[] result = new String[]{null, null};
    Iterable<String> rs = SS.split(selector);
    Iterator<String> it = rs.iterator();
    if (it.hasNext()) {
      String maybeId = it.next();
      if (maybeId.charAt(0) == '#') {
        result[S_ID] = maybeId.substring(1);
        if (it.hasNext()) {
          result[S_CLASS] = SJ.join(Iterables.skip(rs, 1));
        }
      } else {
        result[S_CLASS] = SJ.join(rs);
      }
      return result;
    }
    throw new WebAppException("Error parsing selector: "+ selector);
  }

  /**
   * Set id and/or class attributes for an element.
   * @param <E> type of the element
   * @param e the element
   * @param selector Haml form of "(#id)?(.class)*"
   * @return the element
   */
  public static <E extends CoreAttrs> E setSelector(E e, String selector) {
    String[] res = parseSelector(selector);
    if (res[S_ID] != null) {
      e.$id(res[S_ID]);
    }
    if (res[S_CLASS] != null) {
      e.$class(res[S_CLASS]);
    }
    return e;
  }

  public static <E extends LINK> E setLinkHref(E e, String href) {
    if (href.endsWith(".css")) {
      e.$rel("stylesheet"); // required in html5
    }
    e.$href(href);
    return e;
  }

  public static <E extends SCRIPT> E setScriptSrc(E e, String src) {
    if (src.endsWith(".js")) {
      e.$type("text/javascript"); // required in html4
    }
    e.$src(src);
    return e;
  }
}
