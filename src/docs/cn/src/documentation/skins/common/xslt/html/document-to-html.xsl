<?xml version="1.0"?>
<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
<!--
This stylesheet contains the majority of templates for converting documentv11
to HTML.  It renders XML as HTML in this form:

  <div class="content">
   ...
  </div>

..which site-to-xhtml.xsl then combines with HTML from the index (book-to-menu.xsl)
and tabs (tab-to-menu.xsl) to generate the final HTML.

Section handling
  - <a name/> anchors are added if the id attribute is specified

-->
<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:param name="dynamic-page" select="'false'"/>
  <xsl:param name="notoc"/>
  <xsl:param name="path"/>
<!-- <xsl:include href="split.xsl"/> -->
  <xsl:include href="dotdots.xsl"/>
  <xsl:include href="pathutils.xsl"/>
<!-- Path to site root, eg '../../' -->
  <xsl:variable name="root">
    <xsl:call-template name="dotdots">
      <xsl:with-param name="path" select="$path"/>
    </xsl:call-template>
  </xsl:variable>
  <xsl:variable name="skin-img-dir" select="concat(string($root), 'skin/images')"/>
  <xsl:template match="/">
    <xsl:apply-templates mode="toc"/>
    <xsl:apply-templates/>
  </xsl:template>
  <xsl:template match="document">
    <div class="content">
      <table summary="" class="title">
        <tr>
          <td valign="middle">
            <xsl:if test="normalize-space(header/title)!=''">
              <h1>
                <xsl:value-of select="header/title"/>
              </h1>
            </xsl:if>
          </td>
          <div id="skinconf-printlink"/>
          <xsl:if test="$dynamic-page='false'">
            <div id="skinconf-pdflink"/>
            <div id="skinconf-xmllink"/>
          </xsl:if>
        </tr>
      </table>
      <xsl:if test="normalize-space(header/subtitle)!=''">
        <h3>
          <xsl:value-of select="header/subtitle"/>
        </h3>
      </xsl:if>
      <xsl:apply-templates select="header/type"/>
      <xsl:apply-templates select="header/notice"/>
      <xsl:apply-templates select="header/abstract"/>
      <xsl:apply-templates select="body"/>
      <div class="attribution">
        <xsl:apply-templates select="header/authors"/>
        <xsl:if test="header/authors and header/version">
<xsl:text>; </xsl:text>
        </xsl:if>
        <xsl:apply-templates select="header/version"/>
      </div>
    </div>
  </xsl:template>
  <xsl:template match="body">
    <div id="skinconf-toc-page"/>
    <xsl:apply-templates/>
  </xsl:template>
<!-- Generate a <a name="..."> tag for an @id -->
  <xsl:template match="@id">
    <xsl:if test="normalize-space(.)!=''"><a name="{.}"/>
    </xsl:if>
  </xsl:template>
  <xsl:template match="section">
<!-- count the number of section in the ancestor-or-self axis to compute
         the title element name later on -->
    <xsl:variable name="sectiondepth" select="count(ancestor-or-self::section)"/><a name="{generate-id()}"/>
    <xsl:apply-templates select="@id"/>
<!-- generate a title element, level 1 -> h3, level 2 -> h4 and so on... -->
    <xsl:element name="{concat('h',$sectiondepth + 2)}">
      <xsl:value-of select="title"/>
      <xsl:if test="$notoc='true' and $sectiondepth = 3">
        <span style="float: right"><a href="#{@id}-menu">^</a>
        </span>
      </xsl:if>
    </xsl:element>
<!-- Indent FAQ entry text 15 pixels -->
    <xsl:variable name="indent">
      <xsl:choose>
        <xsl:when test="$notoc='true' and $sectiondepth = 3">
<xsl:text>15</xsl:text>
        </xsl:when>
        <xsl:otherwise>
<xsl:text>0</xsl:text>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:variable>
    <div style="margin-left: {$indent} ; border: 2px">
      <xsl:apply-templates select="*[not(self::title)]"/>
    </div>
  </xsl:template>
  <xsl:template match="note | warning | fixme">
    <xsl:apply-templates select="@id"/>
    <div>
      <xsl:call-template name="add.class">
        <xsl:with-param name="class">
          <xsl:value-of select="local-name()"/>
        </xsl:with-param>
      </xsl:call-template>
      <div class="label">
        <xsl:choose>
<!-- FIXME: i18n Transformer here -->
          <xsl:when test="@label">
            <xsl:value-of select="@label"/>
          </xsl:when>
          <xsl:when test="local-name() = 'note'">Note</xsl:when>
          <xsl:when test="local-name() = 'warning'">Warning</xsl:when>
          <xsl:otherwise>Fixme (<xsl:value-of select="&#x40;author"/>)</xsl:otherwise>
        </xsl:choose>
      </div>
      <div class="content">
        <xsl:apply-templates/>
      </div>
    </div>
  </xsl:template>
  <xsl:template match="notice">
    <div class="notice">
<!-- FIXME: i18n Transformer here -->
<xsl:text>Notice: </xsl:text>
      <xsl:apply-templates/>
    </div>
  </xsl:template>
  <xsl:template match="link">
    <xsl:apply-templates select="@id"/><a>
    <xsl:if test="@class='jump'">
      <xsl:attribute name="target">_top</xsl:attribute>
    </xsl:if>
    <xsl:if test="@class='fork'">
      <xsl:attribute name="target">_blank</xsl:attribute>
    </xsl:if>
    <xsl:copy-of select="@*"/>
    <xsl:apply-templates/></a>
  </xsl:template>
  <xsl:template match="jump">
    <xsl:apply-templates select="@id"/><a href="{@href}" target="_top">
    <xsl:apply-templates/></a>
  </xsl:template>
  <xsl:template match="fork">
    <xsl:apply-templates select="@id"/><a href="{@href}" target="_blank">
    <xsl:apply-templates/></a>
  </xsl:template>
  <xsl:template match="p[@xml:space='preserve']">
    <xsl:apply-templates select="@id"/>
    <div class="pre">
      <xsl:copy-of select="@id"/>
      <xsl:apply-templates/>
    </div>
  </xsl:template>
  <xsl:template match="source">
    <xsl:apply-templates select="@id"/>
    <pre class="code">
<!-- Temporarily removed long-line-splitter ... gives out-of-memory problems -->
      <xsl:copy-of select="@id"/>
      <xsl:apply-templates/>
<!--
    <xsl:call-template name="format">
    <xsl:with-param select="." name="txt" />
     <xsl:with-param name="width">80</xsl:with-param>
     </xsl:call-template>
-->
    </pre>
  </xsl:template>
  <xsl:template match="anchor"><a name="{@id}">
    <xsl:copy-of select="@id"/></a>
  </xsl:template>
  <xsl:template match="icon">
    <xsl:apply-templates select="@id"/>
    <img class="icon">
      <xsl:copy-of select="@height | @width | @src | @alt | @id"/>
    </img>
  </xsl:template>
  <xsl:template match="code">
    <xsl:apply-templates select="@id"/>
    <span>
      <xsl:call-template name="add.class">
        <xsl:with-param name="class">codefrag</xsl:with-param>
      </xsl:call-template>
      <xsl:copy-of select="@id"/>
      <xsl:value-of select="."/>
    </span>
  </xsl:template>
  <xsl:template match="figure">
    <xsl:apply-templates select="@id"/>
    <div align="center">
      <xsl:copy-of select="@id"/>
      <img class="figure">
        <xsl:copy-of select="@height | @width | @src | @alt | @id"/>
      </img>
    </div>
  </xsl:template>
  <xsl:template match="table">
    <xsl:apply-templates select="@id"/>
    <xsl:choose>
<!-- Limit Forrest specific processing to tables without class -->
      <xsl:when test="not(@class) or @class=''">
        <table cellpadding="4" cellspacing="1" class="ForrestTable">
          <xsl:copy-of select="@cellspacing | @cellpadding | @border | @class | @bgcolor |@id"/>
          <xsl:apply-templates/>
        </table>
      </xsl:when>
      <xsl:otherwise>
<!-- Tables with class are passed without change -->
        <xsl:copy>
          <xsl:copy-of select="@*"/>
          <xsl:apply-templates/>
        </xsl:copy>
      </xsl:otherwise>
    </xsl:choose>
    <xsl:if test="@class = ''"></xsl:if>
  </xsl:template>
  <xsl:template match="acronym/@title">
    <xsl:attribute name="title">
      <xsl:value-of select="normalize-space(.)"/>
    </xsl:attribute>
  </xsl:template>
  <xsl:template match="header/authors">
    <xsl:for-each select="person">
      <xsl:choose>
        <xsl:when test="position()=1">by</xsl:when>
        <xsl:otherwise>,</xsl:otherwise>
      </xsl:choose>
<xsl:text> </xsl:text>
      <xsl:value-of select="@name"/>
    </xsl:for-each>
  </xsl:template>
  <xsl:template match="version">
    <span class="version">
      <xsl:apply-templates select="@major"/>
      <xsl:apply-templates select="@minor"/>
      <xsl:apply-templates select="@fix"/>
      <xsl:apply-templates select="@tag"/>
      <xsl:choose>
        <xsl:when test="starts-with(., '$Revision: ')">
          version <xsl:value-of select="substring(., 12, string-length(.) -11-2)"/>
        </xsl:when>
        <xsl:otherwise>
          <xsl:value-of select="."/>
        </xsl:otherwise>
      </xsl:choose>
    </span>
  </xsl:template>
  <xsl:template match="@major">
     v<xsl:value-of select="."/>
  </xsl:template>
  <xsl:template match="@minor | @fix">
    <xsl:value-of select="concat('.',.)"/>
  </xsl:template>
  <xsl:template match="@tag">
    <xsl:value-of select="concat('-',.)"/>
  </xsl:template>
  <xsl:template match="type">
    <p class="type">
<!-- FIXME: i18n Transformer here -->
<xsl:text>Type: </xsl:text>
      <xsl:value-of select="."/>
    </p>
  </xsl:template>
  <xsl:template match="abstract">
    <p>
      <xsl:apply-templates/>
    </p>
  </xsl:template>
  <xsl:template name="email"><a>
    <xsl:attribute name="href">
      <xsl:value-of select="concat('mailto:',@email)"/>
    </xsl:attribute>
    <xsl:value-of select="@name"/></a>
  </xsl:template>
  <xsl:template name="generate-id">
    <xsl:choose>
      <xsl:when test="@id">
        <xsl:value-of select="@id"/>
      </xsl:when>
      <xsl:when test="@title">
        <xsl:value-of select="@title"/>
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="generate-id(.)"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>
<!--  Templates for "toc" mode.  This will generate a complete
        Table of Contents for the document.  This will then be used
        by the site2xhtml to generate a Menu ToC and a Page ToC -->
  <xsl:template match="document" mode="toc">
    <xsl:apply-templates mode="toc"/>
  </xsl:template>
  <xsl:template match="body" mode="toc">
    <tocitems>
      <xsl:if test="../header/meta[@name='forrest.force-toc'] = 'true'">
        <xsl:attribute name="force">true</xsl:attribute>
      </xsl:if>
      <xsl:apply-templates select="section" mode="toc">
        <xsl:with-param name="level" select="1"/>
      </xsl:apply-templates>
    </tocitems>
  </xsl:template>
  <xsl:template match="section" mode="toc">
    <xsl:param name="level"/>
    <tocitem level="{$level}">
      <xsl:attribute name="href">#<xsl:call-template name="generate-id"/>
      </xsl:attribute>
      <xsl:attribute name="title">
        <xsl:value-of select="title"/>
      </xsl:attribute>
      <xsl:apply-templates mode="toc">
        <xsl:with-param name="level" select="$level+1"/>
      </xsl:apply-templates>
    </tocitem>
  </xsl:template>
  <xsl:template name="add.class">
<!-- use the parameter to set class attribute -->
<!-- if there are already classes set, adds to them -->
    <xsl:param name="class"/>
    <xsl:attribute name="class">
      <xsl:choose>
        <xsl:when test="@class">
          <xsl:value-of select="$class"/>
<xsl:text> </xsl:text>
          <xsl:value-of select="@class"/>
        </xsl:when>
        <xsl:otherwise>
          <xsl:value-of select="$class"/>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:attribute>
  </xsl:template>
  <xsl:template match="node()|@*" mode="toc"/>
<!-- End of "toc" mode templates -->
  <xsl:template match="node()|@*" priority="-1">
<!-- id processing will create its own a-element so processing has to 
         happen outside the copied element 
    -->
    <xsl:apply-templates select="@id"/>
    <xsl:copy>
      <xsl:apply-templates select="@*[name(.) != 'id']"/>
      <xsl:copy-of select="@id"/>
      <xsl:apply-templates/>
    </xsl:copy>
  </xsl:template>
</xsl:stylesheet>
