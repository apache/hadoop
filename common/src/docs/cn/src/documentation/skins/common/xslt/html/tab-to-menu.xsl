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
This stylesheet generates 'tabs' at the top left of the screen.  Tabs are
visual indicators that a certain subsection of the URI space is being browsed.
For example, if we had tabs with paths:

Tab1:  ''
Tab2:  'community'
Tab3:  'community/howto'
Tab4:  'community/howto/xmlform/index.html'

Then if the current path was 'community/howto/foo', Tab3 would be highlighted.
The rule is: the tab with the longest path that forms a prefix of the current
path is enabled.

The output of this stylesheet is HTML of the form:
    <div class="tab">
      ...
    </div>

which is then merged by site-to-xhtml.xsl

-->
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<!-- ================================================================ -->
<!-- These templates SHOULD be overridden                             -->
<!-- ================================================================ -->
<!-- Called before first level 1 tag -->
  <xsl:template name="pre-separator"></xsl:template>
<!-- Called after last level 1 tag -->
  <xsl:template name="post-separator"></xsl:template>
<!-- Called between level 1 tags -->
  <xsl:template name="separator">
<xsl:text> | </xsl:text>
  </xsl:template>
<!-- Called before first level 2 tag -->
  <xsl:template name="level2-pre-separator"></xsl:template>
<!-- Called after last level 2 tag -->
  <xsl:template name="level2-post-separator"></xsl:template>
<!-- Called between level 2 tags -->
  <xsl:template name="level2-separator">
<xsl:text> | </xsl:text>
  </xsl:template>
<!--
  Note: sub-stylesheets can't do apply-imports here, because it would choose
  the 'tags' template and infinitely recurse. Hence call-template used instead.
  -->
<!-- Display a selected level 1 tab node -->
  <xsl:template name="selected">
    <xsl:call-template name="base-selected"/>
  </xsl:template>
<!-- Display an unselected level 1 tab node -->
  <xsl:template name="not-selected">
    <xsl:call-template name="base-not-selected"/>
  </xsl:template>
<!-- Display a selected second level tab node -->
  <xsl:template name="level2-selected">
    <xsl:call-template name="base-selected"/>
  </xsl:template>
<!-- Display an unselected second level tab node -->
  <xsl:template name="level2-not-selected">
    <xsl:call-template name="base-not-selected"/>
  </xsl:template>
<!-- ================================================================ -->
<!-- These templates CAN be overridden                             -->
<!-- ================================================================ -->
  <xsl:template match="tabs">
    <div class="tab">
      <xsl:call-template name="base-tabs"/>
    </div>
  </xsl:template>
<!-- ================================================================ -->
<!-- These templates SHOULD NOT be overridden                         -->
<!-- ================================================================ -->
  <xsl:param name="path"/>
  <xsl:include href="dotdots.xsl"/>
  <xsl:include href="tabutils.xsl"/>
<!-- NOTE: Xalan has a bug (race condition?) where sometimes $root is only half-evaluated -->
  <xsl:variable name="root">
    <xsl:call-template name="dotdots">
      <xsl:with-param name="path" select="$path"/>
    </xsl:call-template>
  </xsl:variable>
  <xsl:variable name="skin-img-dir" select="concat(string($root), 'skin/images')"/>
<!--
    The longest path of any level 1 tab, whose path is a subset of the current URL.  Ie,
    the path of the 'current' level 1 tab.
  -->
  <xsl:variable name="longest-dir">
    <xsl:call-template name="longest-dir">
      <xsl:with-param name="tabfile" select="/"/>
    </xsl:call-template>
  </xsl:variable>
<!--
    The longest path of any level 2 tab, whose path is a subset of the current URL.  Ie,
    the path of the 'current' level 2 tab.
  -->
  <xsl:variable name="level2-longest-dir">
    <xsl:call-template name="level2-longest-dir">
      <xsl:with-param name="tabfile" select="/"/>
    </xsl:call-template>
  </xsl:variable>
  <xsl:variable name="matching-id">
    <xsl:call-template name="matching-id"/>
  </xsl:variable>
<!-- Called from tabs, after it has written the outer 'div class=tabs' and
  any other HTML -->
  <xsl:template name="base-tabs">
    <xsl:call-template name="pre-separator"/>
    <xsl:for-each select="tab">
      <xsl:if test="position()!=1">
        <xsl:call-template name="separator"/>
      </xsl:if>
      <xsl:apply-templates select="." mode="level1"/>
    </xsl:for-each>
    <xsl:call-template name="post-separator"/>
  </xsl:template>
<!-- Called from tabs, after it has written the outer 'div class=tabs' and
  any other HTML -->
  <xsl:template name="level2tabs">
    <xsl:call-template name="level2-pre-separator"/>
    <xsl:for-each select="tab[@dir=$longest-dir]/tab|tab[@href=$longest-dir]/tab|tab[tab/@id=$matching-id]/tab">
      <xsl:if test="position()!=1">
        <xsl:call-template name="level2-separator"/>
      </xsl:if>
      <xsl:apply-templates select="." mode="level2"/>
    </xsl:for-each>
    <xsl:call-template name="level2-post-separator"/>
  </xsl:template>
  <xsl:template match="tab" mode="level1">
    <xsl:choose>
      <xsl:when test="@id and @id = $matching-id">
        <xsl:call-template name="selected"/>
      </xsl:when>
      <xsl:when test="not(@id) and @dir = $longest-dir or @href = $longest-dir">
        <xsl:call-template name="selected"/>
      </xsl:when>
      <xsl:when test="tab[@id = $matching-id]">
        <xsl:call-template name="selected"/>
      </xsl:when>
      <xsl:otherwise>
        <xsl:call-template name="not-selected"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>
  <xsl:template match="tab" mode="level2">
    <xsl:choose>
      <xsl:when test="@id and @id = $matching-id">
        <xsl:call-template name="level2-selected"/>
      </xsl:when>
      <xsl:when test="@dir = $level2-longest-dir">
        <xsl:call-template name="level2-selected"/>
      </xsl:when>
      <xsl:otherwise>
        <xsl:call-template name="level2-not-selected"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>
<!-- Called from 'selected' -->
  <xsl:template name="base-selected"><a class="selected">
    <xsl:attribute name="href">
      <xsl:call-template name="calculate-tab-href">
        <xsl:with-param name="tab" select="."/>
        <xsl:with-param name="path" select="$path"/>
      </xsl:call-template>
    </xsl:attribute>
    <xsl:value-of select="@label"/></a>
  </xsl:template>
<!-- Called from 'not-selected' -->
  <xsl:template name="base-not-selected"><a class="unselected">
    <xsl:attribute name="href">
      <xsl:call-template name="calculate-tab-href">
        <xsl:with-param name="tab" select="."/>
        <xsl:with-param name="path" select="$path"/>
      </xsl:call-template>
    </xsl:attribute>
    <xsl:value-of select="@label"/></a>
  </xsl:template>
</xsl:stylesheet>
