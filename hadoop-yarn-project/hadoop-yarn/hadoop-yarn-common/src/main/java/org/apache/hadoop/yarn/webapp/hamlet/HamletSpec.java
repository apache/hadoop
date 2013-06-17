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

import java.lang.annotation.*;
import java.util.EnumSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.webapp.SubView;

/**
 * HTML5 compatible HTML4 builder interfaces.
 *
 * <p>Generated from HTML 4.01 strict DTD and HTML5 diffs.
 * <br>cf. http://www.w3.org/TR/html4/
 * <br>cf. http://www.w3.org/TR/html5-diff/
 * <p> The omitted attributes and elements (from the 4.01 DTD)
 * are for HTML5 compatibility.
 *
 * <p>Note, the common argument selector uses the same syntax as Haml/Sass:
 * <pre>  selector ::= (#id)?(.class)*</pre>
 * cf. http://haml-lang.com/
 *
 * <p>The naming convention used in this class is slightly different from
 * normal classes. A CamelCase interface corresponds to an entity in the DTD.
 * _CamelCase is for internal refactoring. An element builder interface is in
 * UPPERCASE, corresponding to an element definition in the DTD. $lowercase is
 * used as attribute builder methods to differentiate from element builder
 * methods.
 */
@InterfaceAudience.LimitedPrivate({"YARN", "MapReduce"})
public class HamletSpec {
  // The enum values are lowercase for better compression,
  // while avoiding runtime conversion.
  // cf. http://www.w3.org/Protocols/HTTP/Performance/Compression/HTMLCanon.html
  //     http://www.websiteoptimization.com/speed/tweak/lowercase/
  /** %Shape (case-insensitive) */
  public enum Shape {
    /**
     * rectangle
     */
    rect,
    /**
     * circle
     */
    circle,
    /**
     * polygon
     */
    poly,
    /**
     * default
     */
    Default
  };

  /** Values for the %18n dir attribute (case-insensitive) */
  public enum Dir {
    /**
     * left to right
     */
    ltr,
    /**
     * right to left
     */
    rtl
  };

  /** %MediaDesc (case-sensitive) */
  public enum Media {
    /**
     * computer screen
     */
    screen,
    /**
     * teletype/terminal
     */
    tty,
    /**
     * television
     */
    tv,
    /**
     * projection
     */
    projection,
    /**
     * mobile device
     */
    handheld,
    /**
     * print media
     */
    print,
    /**
     * braille
     */
    braille,
    /**
     * aural
     */
    aural,
    /**
     * suitable all media
     */
    all
  };

  /** %LinkTypes (case-insensitive) */
  public enum LinkType {
    /**
     *
     */
    alternate,
    /**
     *
     */
    stylesheet,
    /**
     *
     */
    start,
    /**
     *
     */
    next,
    /**
     *
     */
    prev,
    /**
     *
     */
    contents,
    /**
     *
     */
    index,
    /**
     *
     */
    glossary,
    /**
     *
     */
    copyright,
    /**
     *
     */
    chapter,
    /**
     *
     */
    section,
    /**
     *
     */
    subsection,
    /**
     *
     */
    appendix,
    /**
     *
     */
    help,
    /**
     *
     */
    bookmark
  };

  /** Values for form methods (case-insensitive) */
  public enum Method {
    /**
     * HTTP GET
     */
    get,
    /**
     * HTTP POST
     */
    post
  };

  /** %InputType (case-insensitive) */
  public enum InputType {
    /**
     *
     */
    text,
    /**
     *
     */
    password,
    /**
     *
     */
    checkbox,
    /**
     *
     */
    radio,
    /**
     *
     */
    submit,
    /**
     *
     */
    reset,
    /**
     *
     */
    file,
    /**
     *
     */
    hidden,
    /**
     *
     */
    image,
    /**
     *
     */
    button
  };

  /** Values for button types */
  public enum ButtonType {
    /**
     *
     */
    button,
    /**
     *
     */
    submit,
    /**
     *
     */
    reset
  };

  /** %Scope (case-insensitive) */
  public enum Scope {
    /**
     *
     */
    row,
    /**
     *
     */
    col,
    /**
     *
     */
    rowgroup,
    /**
     *
     */
    colgroup
  };

  /**
   * The element annotation for specifying element options other than
   * attributes and allowed child elements
   */
  @Target({ElementType.TYPE})
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Element {
    /**
     * Whether the start tag is required for the element.
     * @return true if start tag is required
     */
    boolean startTag() default true;

    /**
     * Whether the end tag is required.
     * @return true if end tag is required
     */
    boolean endTag() default true;
  }

  /**
   *
   */
  public interface _ {}

  /**
   *
   */
  public interface _Child extends _ {
    /**
     * Finish the current element.
     * @return the parent element
     */
    _ _();
  }

  /**
   *
   */
  public interface _Script {
    /**
     * Add a script element.
     * @return a script element builder
     */
    SCRIPT script();

    /**
     * Add a script element
     * @param src uri of the script
     * @return the current element builder
     */
    _Script script(String src);
  }

  /**
   *
   */
  public interface _Object {
      /**
     * Add an object element.
     * @return an object element builder
     */
    OBJECT object();

    /**
     * Add an object element.
     * @param selector as #id.class etc.
     * @return an object element builder
     */
    OBJECT object(String selector);
  }

  /** %head.misc */
  public interface HeadMisc extends _Script, _Object {
    /**
     * Add a style element.
     * @return a style element builder
     */
    STYLE style();

    /**
     * Add a css style element.
     * @param lines content of the style sheet
     * @return the current element builder
     */
    HeadMisc style(Object... lines);

    /**
     * Add a meta element.
     * @return a meta element builder
     */
    META meta();

    /**
     * Add a meta element.
     * Shortcut of <code>meta().$name(name).$content(content)._();</code>
     * @param name of the meta element
     * @param content of the meta element
     * @return the current element builder
     */
    HeadMisc meta(String name, String content);

    /**
     * Add a meta element with http-equiv attribute.
     * Shortcut of <br>
     * <code>meta().$http_equiv(header).$content(content)._();</code>
     * @param header for the http-equiv attribute
     * @param content of the header
     * @return the current element builder
     */
    HeadMisc meta_http(String header, String content);

    /**
     * Add a link element.
     * @return a link element builder
     */
    LINK link();

    /**
     * Add a link element.
     * Implementation should try to figure out type by the suffix of href.
     * So <code>link("style.css");</code> is a shortcut of
     * <code>link().$rel("stylesheet").$type("text/css").$href("style.css")._();
     * </code>
     * @param href of the link
     * @return the current element builder
     */
    HeadMisc link(String href);
  }

  /** %heading */
  public interface Heading {
    /**
     * Add an H1 element.
     * @return a new H1 element builder
     */
    H1 h1();

    /**
     * Add a complete H1 element.
     * @param cdata the content of the element
     * @return the current element builder
     */
    Heading h1(String cdata);

    /**
     * Add a complete H1 element
     * @param selector the css selector in the form of (#id)?(.class)*
     * @param cdata the content of the element
     * @return the current element builder
     */
    Heading h1(String selector, String cdata);

    /**
     * Add an H2 element.
     * @return a new H2 element builder
     */
    H2 h2();

    /**
     * Add a complete H2 element.
     * @param cdata the content of the element
     * @return the current element builder
     */
    Heading h2(String cdata);

    /**
     * Add a complete H1 element
     * @param selector the css selector in the form of (#id)?(.class)*
     * @param cdata the content of the element
     * @return the current element builder
     */
    Heading h2(String selector, String cdata);

    /**
     * Add an H3 element.
     * @return a new H3 element builder
     */
    H3 h3();

    /**
     * Add a complete H3 element.
     * @param cdata the content of the element
     * @return the current element builder
     */
    Heading h3(String cdata);

    /**
     * Add a complete H1 element
     * @param selector the css selector in the form of (#id)?(.class)*
     * @param cdata the content of the element
     * @return the current element builder
     */
    Heading h3(String selector, String cdata);

    /**
     * Add an H4 element.
     * @return a new H4 element builder
     */
    H4 h4();

    /**
     * Add a complete H4 element.
     * @param cdata the content of the element
     * @return the current element builder
     */
    Heading h4(String cdata);

    /**
     * Add a complete H4 element
     * @param selector the css selector in the form of (#id)?(.class)*
     * @param cdata the content of the element
     * @return the current element builder
     */
    Heading h4(String selector, String cdata);

    /**
     * Add an H5 element.
     * @return a new H5 element builder
     */
    H5 h5();

    /**
     * Add a complete H5 element.
     * @param cdata the content of the element
     * @return the current element builder
     */
    Heading h5(String cdata);

    /**
     * Add a complete H5 element
     * @param selector the css selector in the form of (#id)?(.class)*
     * @param cdata the content of the element
     * @return the current element builder
     */
    Heading h5(String selector, String cdata);

    /**
     * Add an H6 element.
     * @return a new H6 element builder
     */
    H6 h6();

    /**
     * Add a complete H6 element.
     * @param cdata the content of the element
     * @return the current element builder
     */
    Heading h6(String cdata);

    /**
     * Add a complete H6 element.
     * @param selector the css selector in the form of (#id)?(.class)*
     * @param cdata the content of the element
     * @return the current element builder
     */
    Heading h6(String selector, String cdata);
  }

  /** %list */
  public interface Listing {

    /**
     * Add a UL (unordered list) element.
     * @return a new UL element builder
     */
    UL ul();

    /**
     * Add a UL (unordered list) element.
     * @param selector the css selector in the form of (#id)?(.class)*
     * @return a new UL element builder
     */
    UL ul(String selector);

    /**
     * Add a OL (ordered list) element.
     * @return a new UL element builder
     */
    OL ol();

    /**
     * Add a OL (ordered list) element.
     * @param selector the css selector in the form of (#id)?(.class)*
     * @return a new UL element builder
     */
    OL ol(String selector);
  }

  /** % preformatted */
  public interface Preformatted {

    /**
     * Add a PRE (preformatted) element.
     * @return a new PRE element builder
     */
    PRE pre();

    /**
     * Add a PRE (preformatted) element.
     * @param selector the css selector in the form of (#id)?(.class)*
     * @return a new PRE element builder
     */
    PRE pre(String selector);
  }

  /** %coreattrs */
  public interface CoreAttrs {
    /** document-wide unique id
     * @param id the id
     * @return the current element builder
     */
    CoreAttrs $id(String id);

    /** space-separated list of classes
     * @param cls the classes
     * @return the current element builder
     */
    CoreAttrs $class(String cls);

    /** associated style info
     * @param style the style
     * @return the current element builder
     */
    CoreAttrs $style(String style);

    /** advisory title
     * @param title the title
     * @return the current element builder
     */
    CoreAttrs $title(String title);
  }

  /** %i18n */
  public interface I18nAttrs {
    /** language code
     * @param lang the code
     * @return the current element builder
     */
    I18nAttrs $lang(String lang);

    /** direction for weak/neutral text
     * @param dir the {@link Dir} value
     * @return the current element builder
     */
    I18nAttrs $dir(Dir dir);
  }

  /** %events */
  public interface EventsAttrs {

    /** a pointer button was clicked
     * @param onclick the script
     * @return the current element builder
     */
    EventsAttrs $onclick(String onclick);

    /** a pointer button was double clicked
     * @param ondblclick the script
     * @return the current element builder
     */
    EventsAttrs $ondblclick(String ondblclick);

    /** a pointer button was pressed down
     * @param onmousedown the script
     * @return the current element builder
     */
    EventsAttrs $onmousedown(String onmousedown);

    /** a pointer button was released
     * @param onmouseup the script
     * @return the current element builder
     */
    EventsAttrs $onmouseup(String onmouseup);

    /** a pointer was moved onto
     * @param onmouseover the script
     * @return the current element builder
     */
    EventsAttrs $onmouseover(String onmouseover);

    /** a pointer was moved within
     * @param onmousemove the script
     * @return the current element builder
     */
    EventsAttrs $onmousemove(String onmousemove);

    /** a pointer was moved away
     * @param onmouseout the script
     * @return the current element builder
     */
    EventsAttrs $onmouseout(String onmouseout);

    /** a key was pressed and released
     * @param onkeypress the script
     * @return the current element builder
     */
    EventsAttrs $onkeypress(String onkeypress);

    /** a key was pressed down
     * @param onkeydown the script
     * @return the current element builder
     */
    EventsAttrs $onkeydown(String onkeydown);

    /** a key was released
     * @param onkeyup the script
     * @return the current element builder
     */
    EventsAttrs $onkeyup(String onkeyup);
  }

  /** %attrs */
  public interface Attrs extends CoreAttrs, I18nAttrs, EventsAttrs {
  }

  /** Part of %pre.exclusion */
  public interface _FontSize extends _Child {
    // BIG omitted cf. http://www.w3.org/TR/html5-diff/

    /**
     * Add a SMALL (small print) element
     * @return a new SMALL element builder
     */
    SMALL small();

    /**
     * Add a complete small (small print) element.
     * Shortcut of: small()._(cdata)._();
     * @param cdata the content of the element
     * @return the current element builder
     */
    _FontSize small(String cdata);

    /**
     * Add a complete small (small print) element.
     * Shortcut of: small().$id(id).$class(class)._(cdata)._();
     * @param selector css selector in the form of (#id)?(.class)*
     * @param cdata the content of the element
     * @return the current element builder
     */
    _FontSize small(String selector, String cdata);
  }

  /** %fontstyle -(%pre.exclusion) */
  public interface _FontStyle extends _Child {
    // TT omitted

    /**
     * Add an I (italic, alt voice/mood) element.
     * @return the new I element builder
     */
    I i();

    /**
     * Add a complete I (italic, alt voice/mood) element.
     * @param cdata the content of the element
     * @return the current element builder
     */
    _FontStyle i(String cdata);

    /**
     * Add a complete I (italic, alt voice/mood) element.
     * @param selector the css selector in the form of (#id)?(.class)*
     * @param cdata the content of the element
     * @return the current element builder
     */
    _FontStyle i(String selector, String cdata);

    /**
     * Add a new B (bold/important) element.
     * @return a new B element builder
     */
    B b();

    /**
     * Add a complete B (bold/important) element.
     * @param cdata the content
     * @return the current element builder
     */
    _FontStyle b(String cdata);

    /**
     * Add a complete B (bold/important) element.
     * @param selector the css select (#id)?(.class)*
     * @param cdata the content
     * @return the current element builder
     */
     _FontStyle b(String selector, String cdata);
  }

  /** %fontstyle */
  public interface FontStyle extends _FontStyle, _FontSize {
  }

  /** %phrase */
  public interface Phrase extends _Child {

    /**
     * Add an EM (emphasized) element.
     * @return a new EM element builder
     */
    EM em();

    /**
     * Add an EM (emphasized) element.
     * @param cdata the content
     * @return the current element builder
     */
    Phrase em(String cdata);

    /**
     * Add an EM (emphasized) element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @param cdata the content
     * @return the current element builder
     */
    Phrase em(String selector, String cdata);

    /**
     * Add a STRONG (important) element.
     * @return a new STRONG element builder
     */
    STRONG strong();

    /**
     * Add a complete STRONG (important) element.
     * @param cdata the content
     * @return the current element builder
     */
    Phrase strong(String cdata);

    /**
     * Add a complete STRONG (important) element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @param cdata the content
     * @return the current element builder
     */
    Phrase strong(String selector, String cdata);

    /**
     * Add a DFN element.
     * @return a new DFN element builder
     */
    DFN dfn();

    /**
     * Add a complete DFN element.
     * @param cdata the content
     * @return the current element builder
     */
    Phrase dfn(String cdata);

    /**
     * Add a complete DFN element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @param cdata the content
     * @return the current element builder
     */
    Phrase dfn(String selector, String cdata);

    /**
     * Add a CODE (code fragment) element.
     * @return a new CODE element builder
     */
    CODE code();

    /**
     * Add a complete CODE element.
     * @param cdata the code
     * @return the current element builder
     */
    Phrase code(String cdata);

    /**
     * Add a complete CODE element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @param cdata the code
     * @return the current element builder
     */
    Phrase code(String selector, String cdata);

    /**
     * Add a SAMP (sample) element.
     * @return a new SAMP element builder
     */
    SAMP samp();

    /**
     * Add a complete SAMP (sample) element.
     * @param cdata the content
     * @return the current element builder
     */
    Phrase samp(String cdata);

    /**
     * Add a complete SAMP (sample) element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @param cdata the content
     * @return the current element builder
     */
    Phrase samp(String selector, String cdata);

    /**
     * Add a KBD (keyboard) element.
     * @return a new KBD element builder
     */
    KBD kbd();

    /**
     * Add a KBD (keyboard) element.
     * @param cdata the content
     * @return the current element builder
     */
    Phrase kbd(String cdata);

    /**
     * Add a KBD (keyboard) element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @param cdata the content
     * @return the current element builder
     */
    Phrase kbd(String selector, String cdata);

    /**
     * Add a VAR (variable) element.
     * @return a new VAR element builder
     */
    VAR var();

    /**
     * Add a VAR (variable) element.
     * @param cdata the content
     * @return the current element builder
     */
    Phrase var(String cdata);

    /**
     * Add a VAR (variable) element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @param cdata the content
     * @return the current element builder
     */
    Phrase var(String selector, String cdata);

    /**
     * Add a CITE element.
     * @return a new CITE element builder
     */
    CITE cite();

    /**
     * Add a CITE element.
     * @param cdata the content
     * @return the current element builder
     */
    Phrase cite(String cdata);

    /**
     * Add a CITE element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @param cdata the content
     * @return the current element builder
     */
    Phrase cite(String selector, String cdata);

    /**
     * Add an ABBR (abbreviation) element.
     * @return a new ABBR element builder
     */
    ABBR abbr();

    /**
     * Add a ABBR (abbreviation) element.
     * @param cdata the content
     * @return the current element builder
     */
    Phrase abbr(String cdata);

    /**
     * Add a ABBR (abbreviation) element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @param cdata the content
     * @return the current element builder
     */
    Phrase abbr(String selector, String cdata);

    // ACRONYM omitted, use ABBR
  }

  /** Part of %pre.exclusion */
  public interface _ImgObject extends _Object, _Child {

    /**
     * Add a IMG (image) element.
     * @return a new IMG element builder
     */
    IMG img();

    /**
     * Add a IMG (image) element.
     * @param src the source URL of the image
     * @return the current element builder
     */
    _ImgObject img(String src);
  }

  /** Part of %pre.exclusion */
  public interface _SubSup extends _Child {

    /**
     * Add a SUB (subscript) element.
     * @return a new SUB element builder
     */
    SUB sub();

    /**
     * Add a complete SUB (subscript) element.
     * @param cdata the content
     * @return the current element builder
     */
    _SubSup sub(String cdata);

    /**
     * Add a complete SUB (subscript) element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @param cdata the content
     * @return the current element builder
     */
    _SubSup sub(String selector, String cdata);

    /**
     * Add a SUP (superscript) element.
     * @return a new SUP element builder
     */
    SUP sup();

    /**
     * Add a SUP (superscript) element.
     * @param cdata the content
     * @return the current element builder
     */
    _SubSup sup(String cdata);

    /**
     * Add a SUP (superscript) element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @param cdata the content
     * @return the current element builder
     */
    _SubSup sup(String selector, String cdata);
  }

  /**
   *
   */
  public interface _Anchor {

    /**
     * Add a A (anchor) element.
     * @return a new A element builder
     */
    A a();

    /**
     * Add a A (anchor) element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @return a new A element builder
     */
    A a(String selector);

    /** Shortcut for <code>a().$href(href)._(anchorText)._();</code>
     * @param href the URI
     * @param anchorText for the URI
     * @return the current element builder
     */
    _Anchor a(String href, String anchorText);

    /** Shortcut for <code>a(selector).$href(href)._(anchorText)._();</code>
     * @param selector in the form of (#id)?(.class)*
     * @param href the URI
     * @param anchorText for the URI
     * @return the current element builder
     */
    _Anchor a(String selector, String href, String anchorText);
  }

  /**
   * INS and DEL are unusual for HTML
   * "in that they may serve as either block-level or inline elements
   * (but not both)".
   * <br>cf. http://www.w3.org/TR/html4/struct/text.html#h-9.4
   * <br>cf. http://www.w3.org/TR/html5/edits.html#edits
   */
  public interface _InsDel {

    /**
     * Add an INS (insert) element.
     * @return an INS element builder
     */
    INS ins();

    /**
     * Add a complete INS element.
     * @param cdata inserted data
     * @return the current element builder
     */
    _InsDel ins(String cdata);

    /**
     * Add a DEL (delete) element.
     * @return a DEL element builder
     */
    DEL del();

    /**
     * Add a complete DEL element.
     * @param cdata deleted data
     * @return the current element builder
     */
    _InsDel del(String cdata);
  }

  /** %special -(A|%pre.exclusion) */
  public interface _Special extends _Script, _InsDel {

    /**
     * Add a BR (line break) element.
     * @return a new BR element builder
     */
    BR br();

    /**
     * Add a BR (line break) element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @return the current element builder
     */
    _Special br(String selector);

    /**
     * Add a MAP element.
     * @return a new MAP element builder
     */
    MAP map();

    /**
     * Add a MAP element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @return a new MAP element builder
     */
    MAP map(String selector);

    /**
     * Add a Q (inline quotation) element.
     * @return a q (inline quotation) element builder
     */
    Q q();

    /**
     * Add a complete Q element.
     * @param cdata the content
     * @return the current element builder
     */
    _Special q(String cdata);

    /**
     * Add a Q element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @param cdata the content
     * @return the current element builder
     */
    _Special q(String selector, String cdata);

    /**
     * Add a SPAN element.
     * @return a new SPAN element builder
     */
    SPAN span();

    /**
     * Add a SPAN element.
     * @param cdata the content
     * @return the current element builder
     */
    _Special span(String cdata);

    /**
     * Add a SPAN element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @param cdata the content
     * @return the current element builder
     */
    _Special span(String selector, String cdata);

    /**
     * Add a bdo (bidirectional override) element
     * @return a bdo element builder
     */
    BDO bdo();

    /**
     * Add a bdo (bidirectional override) element
     * @param dir the direction of the text
     * @param cdata the text
     * @return the current element builder
     */
    _Special bdo(Dir dir, String cdata);
  }

  /** %special */
  public interface Special extends _Anchor, _ImgObject, _SubSup, _Special {
  }

  /**
   *
   */
  public interface _Label extends _Child {

    /**
     * Add a LABEL element.
     * @return a new LABEL element builder
     */
    LABEL label();

    /**
     * Add a LABEL element.
     * Shortcut of <code>label().$for(forId)._(cdata)._();</code>
     * @param forId the for attribute
     * @param cdata the content
     * @return the current element builder
     */
    _Label label(String forId, String cdata);
  }

  /**
   *
   */
  public interface _FormCtrl {

    /**
     * Add a INPUT element.
     * @return a new INPUT element builder
     */
    INPUT input();

    /**
     * Add a INPUT element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @return a new INPUT element builder
     */
    INPUT input(String selector);

    /**
     * Add a SELECT element.
     * @return a new SELECT element builder
     */
    SELECT select();

    /**
     * Add a SELECT element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @return a new SELECT element builder
     */
    SELECT select(String selector);

    /**
     * Add a TEXTAREA element.
     * @return a new TEXTAREA element builder
     */
    TEXTAREA textarea();

    /**
     * Add a TEXTAREA element.
     * @param selector
     * @return a new TEXTAREA element builder
     */
    TEXTAREA textarea(String selector);

    /**
     * Add a complete TEXTAREA element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @param cdata the content
     * @return the current element builder
     */
    _FormCtrl textarea(String selector, String cdata);

    /**
     * Add a BUTTON element.
     * @return a new BUTTON element builder
     */
    BUTTON button();

    /**
     * Add a BUTTON element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @return a new BUTTON element builder
     */
    BUTTON button(String selector);

    /**
     * Add a complete BUTTON element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @param cdata the content
     * @return the current element builder
     */
    _FormCtrl button(String selector, String cdata);
  }

  /** %formctrl */
  public interface FormCtrl extends _Label, _FormCtrl {
  }

  /**
   *
   */
  public interface _Content extends _Child {
    /**
     * Content of the element
     * @param lines of content
     * @return the current element builder
     */
    _Content _(Object... lines);
  }

  /**
   *
   */
  public interface _RawContent extends _Child {
    /**
     * Raw (no need to be HTML escaped) content
     * @param lines of content
     * @return the current element builder
     */
    _RawContent _r(Object... lines);
  }

  /** #PCDATA */
  public interface PCData extends _Content, _RawContent {
  }

  /** %inline */
  public interface Inline extends PCData, FontStyle, Phrase, Special, FormCtrl {
  }

  /**
   *
   */
  public interface I extends Attrs, Inline, _Child {
  }

  /**
   *
   */
  public interface B extends Attrs, Inline, _Child {
  }

  /**
   *
   */
  public interface SMALL extends Attrs, Inline, _Child {
  }

  /**
   *
   */
  public interface EM extends Attrs, Inline, _Child {
  }

  /**
   *
   */
  public interface STRONG extends Attrs, Inline, _Child {
  }

  /**
   *
   */
  public interface DFN extends Attrs, Inline, _Child {
  }

  /**
   *
   */
  public interface CODE extends Attrs, Inline, _Child {
  }

  /**
   *
   */
  public interface SAMP extends Attrs, Inline, _Child {
  }

  /**
   *
   */
  public interface KBD extends Attrs, Inline, _Child {
  }

  /**
   *
   */
  public interface VAR extends Attrs, Inline, _Child {
  }

  /**
   *
   */
  public interface CITE extends Attrs, Inline, _Child {
  }

  /**
   *
   */
  public interface ABBR extends Attrs, Inline, _Child {
  }

  /**
   *
   */
  public interface ACRONYM extends Attrs, Inline, _Child {
  }

  /**
   *
   */
  public interface SUB extends Attrs, Inline, _Child {
  }

  /**
   *
   */
  public interface SUP extends Attrs, Inline, _Child {
  }

  /**
   *
   */
  public interface SPAN extends Attrs, Inline, _Child {
  }

  /** The dir attribute is required for the BDO element */
  public interface BDO extends CoreAttrs, I18nAttrs, Inline, _Child {
  }

  /**
   *
   */
  @Element(endTag=false)
  public interface BR extends CoreAttrs, _Child {
  }

  /**
   *
   */
  public interface _Form {

    /**
     * Add a FORM element.
     * @return a new FORM element builder
     */
    FORM form();

    /**
     * Add a FORM element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @return a new FORM element builder
     */
    FORM form(String selector);
  }

  /**
   *
   */
  public interface _FieldSet {

    /**
     * Add a FIELDSET element.
     * @return a new FIELDSET element builder
     */
    FIELDSET fieldset();

    /**
     * Add a FIELDSET element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @return a new FIELDSET element builder
     */
    FIELDSET fieldset(String selector);
  }

  /** %block -(FORM|FIELDSET) */
  public interface _Block extends Heading, Listing, Preformatted {

    /**
     * Add a P (paragraph) element.
     * @return a new P element builder
     */
    P p();

    /**
     * Add a P (paragraph) element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @return a new P element builder
     */
    P p(String selector);

    /**
     * Add a DL (description list) element.
     * @return a new DL element builder
     */
    DL dl();

    /**
     * Add a DL element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @return a new DL element builder
     */
    DL dl(String selector);

    /**
     * Add a DIV element.
     * @return a new DIV element builder
     */
    DIV div();

    /**
     * Add a DIV element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @return a new DIV element builder
     */
    DIV div(String selector);

    // NOSCRIPT omitted
    // cf. http://www.w3.org/html/wg/tracker/issues/117

    /**
     * Add a BLOCKQUOTE element.
     * @return a new BLOCKQUOTE element builder
     */
    BLOCKQUOTE blockquote();

    /**
     * Alias of blockquote
     * @return a new BLOCKQUOTE element builder
     */
    BLOCKQUOTE bq();

    /**
     * Add a HR (horizontal rule) element.
     * @return a new HR element builder
     */
    HR hr();

    /**
     * Add a HR element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @return a new HR element builder
     */
    _Block hr(String selector);

    /**
     * Add a TABLE element.
     * @return a new TABLE element builder
     */
    TABLE table();

    /**
     * Add a TABLE element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @return a new TABLE element builder
     */
    TABLE table(String selector);

    /**
     * Add a ADDRESS element.
     * @return a new ADDRESS element builder
     */
    ADDRESS address();

    /**
     * Add a complete ADDRESS element.
     * @param cdata the content
     * @return the current element builder
     */
    _Block address(String cdata);

    /**
     * Embed a sub-view.
     * @param cls the sub-view class
     * @return the current element builder
     */
    _Block _(Class<? extends SubView> cls);
  }

  /** %block */
  public interface Block extends _Block, _Form, _FieldSet {
  }

  /** %flow */
  public interface Flow extends Block, Inline {
  }

  /**
   *
   */
  public interface _Body extends Block, _Script, _InsDel {
  }

  /**
   *
   */
  public interface BODY extends Attrs, _Body, _Child {

    /**
     * The document has been loaded.
     * @param script to invoke
     * @return the current element builder
     */
    BODY $onload(String script);

    /**
     * The document has been removed
     * @param script to invoke
     * @return the current element builder
     */
    BODY $onunload(String script);
  }

  /**
   *
   */
  public interface ADDRESS extends Attrs, Inline, _Child {
  }

  /**
   *
   */
  public interface DIV extends Attrs, Flow, _Child {
  }

  /**
   *
   */
  public interface A extends Attrs, _Child, /* %inline -(A) */
                             PCData, FontStyle, Phrase, _ImgObject, _Special,
                             _SubSup, FormCtrl {
    // $charset omitted.

    /** advisory content type
     * @param cdata the content-type
     * @return the current element builder
     */
    A $type(String cdata);

    // $name omitted. use id instead.
    /** URI for linked resource
     * @param uri the URI
     * @return the current element builder
     */
    A $href(String uri);

    /** language code
     * @param cdata the code
     * @return the current element builder
     */
    A $hreflang(String cdata);

    /** forward link types
     * @param linkTypes the types
     * @return the current element builder
     */
    A $rel(EnumSet<LinkType> linkTypes);

    /**
     * forward link types
     * @param linkTypes space-separated list of link types
     * @return the current element builder.
     */
    A $rel(String linkTypes);

    // $rev omitted. Instead of rev="made", use rel="author"

    /** accessibility key character
     * @param cdata the key
     * @return the current element builder
     */
    A $accesskey(String cdata);

    // $shape and coords omitted. use area instead of a for image maps.
    /** position in tabbing order
     * @param index the index
     * @return the current element builder
     */
    A $tabindex(int index);

    /** the element got the focus
     * @param script to invoke
     * @return the current element builder
     */
    A $onfocus(String script);

    /** the element lost the focus
     * @param script to invoke
     * @return the current element builder
     */
    A $onblur(String script);
  }

  /**
   *
   */
  public interface MAP extends Attrs, Block, _Child {

    /**
     * Add a AREA element.
     * @return a new AREA element builder
     */
    AREA area();

    /**
     * Add a AREA element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @return a new AREA element builder
     */
    AREA area(String selector);

    /** for reference by usemap
     * @param name of the map
     * @return the current element builder
     */
    MAP $name(String name);
  }

  /**
   *
   */
  @Element(endTag=false)
  public interface AREA extends Attrs, _Child {

    /** controls interpretation of coords
     * @param shape of the area
     * @return the current element builder
     */
    AREA $shape(Shape shape);

    /** comma-separated list of lengths
     * @param cdata coords of the area
     * @return the current element builder
     */
    AREA $coords(String cdata);

    /** URI for linked resource
     * @param uri the URI
     * @return the current element builder
     */
    AREA $href(String uri);

    // $nohref omitted./
    /** short description
     * @param desc the description
     * @return the current element builder
     */
    AREA $alt(String desc);

    /** position in tabbing order
     * @param index of the order
     * @return the current element builder
     */
    AREA $tabindex(int index);

    /** accessibility key character
     * @param cdata the key
     * @return the current element builder
     */
    AREA $accesskey(String cdata);

    /** the element got the focus
     * @param script to invoke
     * @return the current element builder
     */
    AREA $onfocus(String script);

    /** the element lost the focus
     * @param script to invoke
     * @return the current element builder
     */
    AREA $onblur(String script);
  }

  /**
   *
   */
  @Element(endTag=false)
  public interface LINK extends Attrs, _Child {
    // $charset omitted
    /** URI for linked resource
     * @param uri the URI
     * @return the current element builder
     */
    LINK $href(String uri);

    /** language code
     * @param cdata the code
     * @return the current element builder
     */
    LINK $hreflang(String cdata);

    /** advisory content type
     * @param cdata the type
     * @return the current element builder
     */
    LINK $type(String cdata);

    /** forward link types
     * @param linkTypes the types
     * @return the current element builder
     */
    LINK $rel(EnumSet<LinkType> linkTypes);

    /**
     * forward link types.
     * @param linkTypes space-separated link types
     * @return the current element builder
     */
    LINK $rel(String linkTypes);

    // $rev omitted. Instead of rev="made", use rel="author"

    /** for rendering on these media
     * @param mediaTypes the media types
     * @return the current element builder
     */
    LINK $media(EnumSet<Media> mediaTypes);

    /**
     * for rendering on these media.
     * @param mediaTypes comma-separated list of media
     * @return the current element builder
     */
    LINK $media(String mediaTypes);
  }

  /**
   *
   */
  @Element(endTag=false)
  public interface IMG extends Attrs, _Child {

    /** URI of image to embed
     * @param uri the URI
     * @return the current element builder
     */
    IMG $src(String uri);

    /** short description
     * @param desc the description
     * @return the current element builder
     */
    IMG $alt(String desc);

    // $longdesc omitted. use <a...><img..></a> instead
    // $name omitted. use id instead.

    /** override height
     * @param pixels the height
     * @return the current element builder
     */
    IMG $height(int pixels);

    /**
     * override height
     * @param cdata the height (can use %, * etc.)
     * @return the current element builder
     */
    IMG $height(String cdata);

    /** override width
     * @param pixels the width
     * @return the current element builder
     */
    IMG $width(int pixels);

    /**
     * override width
     * @param cdata the width (can use %, * etc.)
     * @return the current element builder
     */
    IMG $width(String cdata);

    /** use client-side image map
     * @param uri the URI
     * @return the current element builder
     */
    IMG $usemap(String uri);

    /** use server-side image map
     * @return the current element builder
     */
    IMG $ismap();
  }

  /**
   *
   */
  public interface _Param extends _Child {

    /**
     * Add a PARAM (parameter) element.
     * @return a new PARAM element builder
     */
    PARAM param();

    /**
     * Add a PARAM element.
     * Shortcut of <code>param().$name(name).$value(value)._();</code>
     * @param name of the value
     * @param value the value
     * @return the current element builder
     */
    _Param param(String name, String value);
  }

  /**
   *
   */
  public interface OBJECT extends Attrs, _Param, Flow, _Child {
    // $declare omitted. repeat element completely

    // $archive, classid, codebase, codetype ommited. use data and type

    /** reference to object's data
     * @param uri the URI
     * @return the current element builder
     */
    OBJECT $data(String uri);

    /** content type for data
     * @param contentType the type
     * @return the current element builder
     */
    OBJECT $type(String contentType);

    // $standby omitted. fix the resource instead.

    /** override height
     * @param pixels the height
     * @return the current element builder
     */
    OBJECT $height(int pixels);

    /**
     * override height
     * @param length the height (can use %, *)
     * @return the current element builder
     */
    OBJECT $height(String length);

    /** override width
     * @param pixels the width
     * @return the current element builder
     */
    OBJECT $width(int pixels);

    /**
     * override width
     * @param length the height (can use %, *)
     * @return the current element builder
     */
    OBJECT $width(String length);

    /** use client-side image map
     * @param uri the URI/name of the map
     * @return the current element builder
     */
    OBJECT $usemap(String uri);

    /** submit as part of form
     * @param cdata the name of the object
     * @return the current element builder
     */
    OBJECT $name(String cdata);

    /** position in tabbing order
     * @param index of the order
     * @return the current element builder
     */
    OBJECT $tabindex(int index);
  }

  /**
   *
   */
  @Element(endTag=false)
  public interface PARAM {

    /** document-wide unique id
     * @param cdata the id
     * @return the current element builder
     */
    PARAM $id(String cdata);

    /** property name. Required.
     * @param cdata the name
     * @return the current element builder
     */
    PARAM $name(String cdata);

    /** property value
     * @param cdata the value
     * @return the current element builder
     */
    PARAM $value(String cdata);

    // $type and valuetype omitted
  }

  /**
   *
   */
  @Element(endTag=false)
  public interface HR extends Attrs, _Child {
  }

  /**
   *
   */
  @Element(endTag=false)
  public interface P extends Attrs, Inline, _Child {
  }

  /**
   *
   */
  public interface H1 extends Attrs, Inline, _Child {
  }

  /**
   *
   */
  public interface H2 extends Attrs, Inline, _Child {
  }

  /**
   *
   */
  public interface H3 extends Attrs, Inline, _Child {
  }

  /**
   *
   */
  public interface H4 extends Attrs, Inline, _Child {
  }

  /**
   *
   */
  public interface H5 extends Attrs, Inline, _Child {
  }

  /**
   *
   */
  public interface H6 extends Attrs, Inline, _Child {
  }

  /**
   *
   */
  public interface PRE extends Attrs, _Child, /* (%inline;)* -(%pre.exclusion) */
                               PCData, _FontStyle, Phrase, _Anchor, _Special,
                               FormCtrl {
  }

  /**
   *
   */
  public interface Q extends Attrs, Inline, _Child {

    /** URI for source document or msg
     * @param uri the URI
     * @return the current element builder
     */
    Q $cite(String uri);
  }

  /**
   *
   */
  public interface BLOCKQUOTE extends Attrs, Block, _Script, _Child {

    /** URI for source document or msg
     * @param uri the URI
     * @return the current element builder
     */
    BLOCKQUOTE $cite(String uri);
  }

  /**
   * @see _InsDel INS/DEL quirks.
   */
  public interface INS extends Attrs, Flow, _Child {
    /** info on reason for change
     * @param uri
     * @return the current element builder
     */
    INS $cite(String uri);

    /** date and time of change
     * @param datetime
     * @return the current element builder
     */
    INS $datetime(String datetime);
  }

  /**
   * @see _InsDel INS/DEL quirks.
   */
  public interface DEL extends Attrs, Flow, _Child {
    /** info on reason for change
     * @param uri the info URI
     * @return the current element builder
     */
    DEL $cite(String uri);

    /** date and time of change
     * @param datetime the time
     * @return the current element builder
     */
    DEL $datetime(String datetime);
  }

  /**
   *
   */
  public interface _Dl extends _Child {

    /**
     * Add a DT (term of the item) element.
     * @return a new DT element builder
     */
    DT dt();

    /**
     * Add a complete DT element.
     * @param cdata the content
     * @return the current element builder
     */
    _Dl dt(String cdata);

    /**
     * Add a DD (definition/description) element.
     * @return a new DD element builder
     */
    DD dd();

    /**
     * Add a complete DD element.
     * @param cdata the content
     * @return the current element builder
     */
    _Dl dd(String cdata);
  }

  /**
   *
   */
  public interface DL extends Attrs, _Dl, _Child {
  }

  /**
   *
   */
  @Element(endTag=false)
  public interface DT extends Attrs, Inline, _Child {
  }

  /**
   *
   */
  @Element(endTag=false)
  public interface DD extends Attrs, Flow, _Child {
  }

  /**
   *
   */
  public interface _Li extends _Child {

    /**
     * Add a LI (list item) element.
     * @return a new LI element builder
     */
    LI li();

    /**
     * Add a LI element.
     * @param cdata the content
     * @return the current element builder
     */
    _Li li(String cdata);
  }

  /**
   *
   */
  public interface OL extends Attrs, _Li, _Child {
  }

  /**
   *
   */
  public interface UL extends Attrs, _Li, _Child {
  }

  /**
   *
   */
  @Element(endTag=false)
  public interface LI extends Attrs, Flow, _Child {
  }

  /**
   *
   */
  public interface FORM extends Attrs, _Child, /* (%block;|SCRIPT)+ -(FORM) */
                                _Script, _Block, _FieldSet {
    /** server-side form handler
     * @param uri
     * @return the current element builder
     */
    FORM $action(String uri);

    /** HTTP method used to submit the form
     * @param method
     * @return the current element builder
     */
    FORM $method(Method method);

    /**
     * contentype for "POST" method.
     * The default is "application/x-www-form-urlencoded".
     * Use "multipart/form-data" for input type=file
     * @param enctype
     * @return the current element builder
     */
    FORM $enctype(String enctype);

    /** list of MIME types for file upload
     * @param cdata
     * @return the current element builder
     */
    FORM $accept(String cdata);

    /** name of form for scripting
     * @param cdata
     * @return the current element builder
     */
    FORM $name(String cdata);

    /** the form was submitted
     * @param script
     * @return the current element builder
     */
    FORM $onsubmit(String script);

    /** the form was reset
     * @param script
     * @return the current element builder
     */
    FORM $onreset(String script);

    /** (space and/or comma separated) list of supported charsets
     * @param cdata
     * @return the current element builder
     */
    FORM $accept_charset(String cdata);
  }

  /**
   *
   */
  public interface LABEL extends Attrs, _Child, /* (%inline;)* -(LABEL) */
                                 PCData, FontStyle, Phrase, Special, _FormCtrl {
    /** matches field ID value
     * @param cdata
     * @return the current element builder
     */
    LABEL $for(String cdata);

    /** accessibility key character
     * @param cdata
     * @return the current element builder
     */
    LABEL $accesskey(String cdata);

    /** the element got the focus
     * @param script
     * @return the current element builder
     */
    LABEL $onfocus(String script);

    /** the element lost the focus
     * @param script
     * @return the current element builder
     */
    LABEL $onblur(String script);
  }

  /**
   *
   */
  @Element(endTag=false)
  public interface INPUT extends Attrs, _Child {
    /** what kind of widget is needed. default is "text".
     * @param inputType
     * @return the current element builder
     */
    INPUT $type(InputType inputType);

    /** submit as part of form
     * @param cdata
     * @return the current element builder
     */
    INPUT $name(String cdata);

    /** Specify for radio buttons and checkboxes
     * @param cdata
     * @return the current element builder
     */
    INPUT $value(String cdata);

    /** for radio buttons and check boxes
     * @return the current element builder
     */
    INPUT $checked();

    /** unavailable in this context
     * @return the current element builder
     */
    INPUT $disabled();

    /** for text and passwd
     * @return the current element builder
     */
    INPUT $readonly();

    /** specific to each type of field
     * @param cdata
     * @return the current element builder
     */
    INPUT $size(String cdata);

    /** max chars for text fields
     * @param length
     * @return the current element builder
     */
    INPUT $maxlength(int length);

    /** for fields with images
     * @param uri
     * @return the current element builder
     */
    INPUT $src(String uri);

    /** short description
     * @param cdata
     * @return the current element builder
     */
    INPUT $alt(String cdata);

    // $usemap omitted. use img instead of input for image maps.
    /** use server-side image map
     * @return the current element builder
     */
    INPUT $ismap();

    /** position in tabbing order
     * @param index
     * @return the current element builder
     */
    INPUT $tabindex(int index);

    /** accessibility key character
     * @param cdata
     * @return the current element builder
     */
    INPUT $accesskey(String cdata);

    /** the element got the focus
     * @param script
     * @return the current element builder
     */
    INPUT $onfocus(String script);

    /** the element lost the focus
     * @param script
     * @return the current element builder
     */
    INPUT $onblur(String script);

    /** some text was selected
     * @param script
     * @return the current element builder
     */
    INPUT $onselect(String script);

    /** the element value was changed
     * @param script
     * @return the current element builder
     */
    INPUT $onchange(String script);

    /** list of MIME types for file upload (csv)
     * @param contentTypes
     * @return the current element builder
     */
    INPUT $accept(String contentTypes);
  }

  /**
   *
   */
  public interface _Option extends _Child {
    /**
     * Add a OPTION element.
     * @return a new OPTION element builder
     */
    OPTION option();

    /**
     * Add a complete OPTION element.
     * @param cdata the content
     * @return the current element builder
     */
    _Option option(String cdata);
  }

  /**
   *
   */
  public interface SELECT extends Attrs, _Option, _Child {
    /**
     * Add a OPTGROUP element.
     * @return a new OPTGROUP element builder
     */
    OPTGROUP optgroup();

    /** field name
     * @param cdata
     * @return the current element builder
     */
    SELECT $name(String cdata);

    /** rows visible
     * @param rows
     * @return the current element builder
     */
    SELECT $size(int rows);

    /** default is single selection
     * @return the current element builder
     */
    SELECT $multiple();

    /** unavailable in this context
     * @return the current element builder
     */
    SELECT $disabled();

    /** position in tabbing order
     * @param index
     * @return the current element builder
     */
    SELECT $tabindex(int index);

    /** the element got the focus
     * @param script
     * @return the current element builder
     */
    SELECT $onfocus(String script);

    /** the element lost the focus
     * @param script
     * @return the current element builder
     */
    SELECT $onblur(String script);

    /** the element value was changed
     * @param script
     * @return the current element builder
     */
    SELECT $onchange(String script);
  }

  /**
   *
   */
  public interface OPTGROUP extends Attrs, _Option, _Child {
    /** unavailable in this context
     * @return the current element builder
     */
    OPTGROUP $disabled();

    /** for use in hierarchical menus
     * @param cdata
     * @return the current element builder
     */
    OPTGROUP $label(String cdata);
  }

  /**
   *
   */
  @Element(endTag=false)
  public interface OPTION extends Attrs, PCData, _Child {
    /** currently selected option
     * @return the current element builder
     */
    OPTION $selected();

    /** unavailable in this context
     * @return the current element builder
     */
    OPTION $disabled();

    /** for use in hierarchical menus
     * @param cdata
     * @return the current element builder
     */
    OPTION $label(String cdata);

    /** defaults to element content
     * @param cdata
     * @return the current element builder
     */
    OPTION $value(String cdata);
  }

  /**
   *
   */
  public interface TEXTAREA extends Attrs, PCData, _Child {
    /** variable name for the text
     * @param cdata
     * @return the current element builder
     */
    TEXTAREA $name(String cdata);

    /** visible rows
     * @param rows
     * @return the current element builder
     */
    TEXTAREA $rows(int rows);

    /** visible columns
     * @param cols
     * @return the current element builder
     */
    TEXTAREA $cols(int cols);

    /** unavailable in this context
     * @return the current element builder
     */
    TEXTAREA $disabled();

    /** text is readonly
     * @return the current element builder
     */
    TEXTAREA $readonly();

    /** position in tabbing order
     * @param index
     * @return the current element builder
     */
    TEXTAREA $tabindex(int index);

    /** accessibility key character
     * @param cdata
     * @return the current element builder
     */
    TEXTAREA $accesskey(String cdata);

    /** the element got the focus
     * @param script
     * @return the current element builder
     */
    TEXTAREA $onfocus(String script);

    /** the element lost the focus
     * @param script
     * @return the current element builder
     */
    TEXTAREA $onblur(String script);

    /** some text was selected
     * @param script
     * @return the current element builder
     */
    TEXTAREA $onselect(String script);

    /** the element value was changed
     * @param script
     * @return the current element builder
     */
    TEXTAREA $onchange(String script);
  }

  /**
   *
   */
  public interface _Legend extends _Child {
    /**
     * Add a LEGEND element.
     * @return a new LEGEND element builder
     */
    LEGEND legend();

    /**
     * Add a LEGEND element.
     * @param cdata
     * @return the current element builder
     */
    _Legend legend(String cdata);
  }

  /**
   *
   */
  public interface FIELDSET extends Attrs, _Legend, PCData, Flow, _Child {
  }

  /**
   *
   */
  public interface LEGEND extends Attrs, Inline, _Child {
    /** accessibility key character
     * @param cdata
     * @return the current element builder
     */
    LEGEND $accesskey(String cdata);
  }

  /**
   *
   */
  public interface BUTTON extends /* (%flow;)* -(A|%formctrl|FORM|FIELDSET) */
      _Block, PCData, FontStyle, Phrase, _Special, _ImgObject, _SubSup, Attrs {
    /** name of the value
     * @param cdata
     * @return the current element builder
     */
    BUTTON $name(String cdata);

    /** sent to server when submitted
     * @param cdata
     * @return the current element builder
     */
    BUTTON $value(String cdata);

    /** for use as form button
     * @param type
     * @return the current element builder
     */
    BUTTON $type(ButtonType type);

    /** unavailable in this context
     * @return the current element builder
     */
    BUTTON $disabled();

    /** position in tabbing order
     * @param index
     * @return the current element builder
     */
    BUTTON $tabindex(int index);

    /** accessibility key character
     * @param cdata
     * @return the current element builder
     */
    BUTTON $accesskey(String cdata);

    /** the element got the focus
     * @param script
     * @return the current element builder
     */
    BUTTON $onfocus(String script);

    /** the element lost the focus
     * @param script
     * @return the current element builder
     */
    BUTTON $onblur(String script);
  }

  /**
   *
   */
  public interface _TableRow {
    /**
     * Add a TR (table row) element.
     * @return a new TR element builder
     */
    TR tr();

    /**
     * Add a TR element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @return a new TR element builder
     */
    TR tr(String selector);
  }

  /**
   *
   */
  public interface _TableCol extends _Child {
    /**
     * Add a COL element.
     * @return a new COL element builder
     */
    COL col();

    /**
     * Add a COL element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @return the current element builder
     */
    _TableCol col(String selector);
  }

  /**
   *
   */
  public interface _Table extends _TableRow, _TableCol {
    /**
     * Add a CAPTION element.
     * @return a new CAPTION element builder
     */
    CAPTION caption();

    /**
     * Add a CAPTION element.
     * @param cdata
     * @return the current element builder
     */
    _Table caption(String cdata);

    /**
     * Add a COLGROPU element.
     * @return a new COLGROUP element builder
     */
    COLGROUP colgroup();

    /**
     * Add a THEAD element.
     * @return a new THEAD element builder
     */
    THEAD thead();

    /**
     * Add a THEAD element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @return a new THEAD element builder
     */
    THEAD thead(String selector);

    /**
     * Add a TFOOT element.
     * @return a new TFOOT element builder
     */
    TFOOT tfoot();

    /**
     * Add a TFOOT element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @return a new TFOOT element builder
     */
    TFOOT tfoot(String selector);

    /**
     * Add a tbody (table body) element.
     * Must be after thead/tfoot and no tr at the same level.
     * @return a new tbody element builder
     */
    TBODY tbody();

    /**
     * Add a TBODY element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @return a new TBODY element builder
     */
    TBODY tbody(String selector);

    // $summary, width, border, frame, rules, cellpadding, cellspacing omitted
    // use css instead
  }
  /**
   * TBODY should be used after THEAD/TFOOT, iff there're no TABLE.TR elements.
   */
  public interface TABLE extends Attrs, _Table, _Child {
  }

  /**
   *
   */
  public interface CAPTION extends Attrs, Inline, _Child {
  }

  /**
   *
   */
  @Element(endTag=false)
  public interface THEAD extends Attrs, _TableRow, _Child {
  }

  /**
   *
   */
  @Element(endTag=false)
  public interface TFOOT extends Attrs, _TableRow, _Child {
  }

  /**
   *
   */
  public interface TBODY extends Attrs, _TableRow, _Child {
  }

  /**
   *
   */
  @Element(endTag=false)
  public interface COLGROUP extends Attrs, _TableCol, _Child {
    /** default number of columns in group. default: 1
     * @param cols
     * @return the current element builder
     */
    COLGROUP $span(int cols);

    // $width omitted. use css instead.
  }

  /**
   *
   */
  @Element(endTag=false)
  public interface COL extends Attrs, _Child {
    /** COL attributes affect N columns. default: 1
     * @param cols
     * @return the current element builder
     */
    COL $span(int cols);
    // $width omitted. use css instead.
  }

  /**
   *
   */
  public interface _Tr extends _Child {
    /**
     * Add a TH element.
     * @return a new TH element builder
     */
    TH th();

    /**
     * Add a complete TH element.
     * @param cdata the content
     * @return the current element builder
     */
    _Tr th(String cdata);

    /**
     * Add a TH element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @param cdata the content
     * @return the current element builder
     */
    _Tr th(String selector, String cdata);

    /**
     * Add a TD element.
     * @return a new TD element builder
     */
    TD td();

    /**
     * Add a TD element.
     * @param cdata the content
     * @return the current element builder
     */
    _Tr td(String cdata);

    /**
     * Add a TD element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @param cdata the content
     * @return the current element builder
     */
    _Tr td(String selector, String cdata);
  }

  /**
   *
   */
  @Element(endTag=false)
  public interface TR extends Attrs, _Tr, _Child {
  }

  /**
   *
   */
  public interface _Cell extends Attrs, Flow, _Child {
    // $abbr omited. begin cell text with terse text instead.
    // use $title for elaberation, when appropriate.
    // $axis omitted. use scope.
    /** space-separated list of id's for header cells
     * @param cdata
     * @return the current element builder
     */
    _Cell $headers(String cdata);

    /** scope covered by header cells
     * @param scope
     * @return the current element builder
     */
    _Cell $scope(Scope scope);

    /** number of rows spanned by cell. default: 1
     * @param rows
     * @return the current element builder
     */
    _Cell $rowspan(int rows);

    /** number of cols spanned by cell. default: 1
     * @param cols
     * @return the current element builder
     */
    _Cell $colspan(int cols);
  }

  /**
   *
   */
  @Element(endTag=false)
  public interface TH extends _Cell {
  }

  /**
   *
   */
  @Element(endTag=false)
  public interface TD extends _Cell {
  }

  /**
   *
   */
  public interface _Head extends HeadMisc {
    /**
     * Add a TITLE element.
     * @return a new TITLE element builder
     */
    TITLE title();

    /**
     * Add a TITLE element.
     * @param cdata the content
     * @return the current element builder
     */
    _Head title(String cdata);

    /**
     * Add a BASE element.
     * @return a new BASE element builder
     */
    BASE base();

    /**
     * Add a complete BASE element.
     * @param uri
     * @return the current element builder
     */
    _Head base(String uri);
  }

  /**
   *
   */
  public interface HEAD extends I18nAttrs, _Head, _Child {
    // $profile omitted
  }

  /**
   *
   */
  public interface TITLE extends I18nAttrs, PCData, _Child {
  }

  /**
   *
   */
  @Element(endTag=false)
  public interface BASE extends _Child {
    /** URI that acts as base URI
     * @param uri
     * @return the current element builder
     */
    BASE $href(String uri);
  }

  /**
   *
   */
  @Element(endTag=false)
  public interface META extends I18nAttrs, _Child {
    /** HTTP response header name
     * @param header
     * @return the current element builder
     */
    META $http_equiv(String header);

    /** metainformation name
     * @param name
     * @return the current element builder
     */
    META $name(String name);

    /** associated information
     * @param cdata
     * @return the current element builder
     */
    META $content(String cdata);

    // $scheme omitted
  }

  /**
   *
   */
  public interface STYLE extends I18nAttrs, _Content, _Child {
    /** content type of style language
     * @param cdata
     * @return the current element builder
     */
    STYLE $type(String cdata);

    /** designed for use with these media
     * @param media
     * @return the current element builder
     */
    STYLE $media(EnumSet<Media> media);

    /** advisory title
     * @param cdata
     * @return the current element builder
     */
    STYLE $title(String cdata);
  }

  /**
   *
   */
  public interface SCRIPT extends _Content, _Child {
    /** char encoding of linked resource
     * @param cdata
     * @return the current element builder
     */
    SCRIPT $charset(String cdata);

    /** content type of script language
     * @param cdata
     * @return the current element builder
     */
    SCRIPT $type(String cdata);

    /** URI for an external script
     * @param cdata
     * @return the current element builder
     */
    SCRIPT $src(String cdata);

    /** UA may defer execution of script
     * @param cdata
     * @return the current element builder
     */
    SCRIPT $defer(String cdata);
  }

  /**
   *
   */
  public interface _Html extends _Head, _Body, _ {
    /**
     * Add a HEAD element.
     * @return a new HEAD element builder
     */
    HEAD head();

    /**
     * Add a BODY element.
     * @return a new BODY element builder
     */
    BODY body();

    /**
     * Add a BODY element.
     * @param selector the css selector in the form of (#id)*(.class)*
     * @return a new BODY element builder
     */
    BODY body(String selector);
  }

  // There is only one HEAD and BODY, in that order.
  /**
   * The root element
   */
  public interface HTML extends I18nAttrs, _Html {
  }
}
