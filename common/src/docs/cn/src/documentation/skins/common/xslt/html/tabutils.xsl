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
Some callable templates useful when dealing with tab paths.  Mostly used in
tab-to-menu.xsl
-->
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:param name="site-file" select="'cocoon://abs-menulinks'"/>
  <xsl:variable name="site" select="document($site-file)"/>
<!-- Given the current path and a tabs.xml entry, returns a relative path to
  the specified tab's URL.  When rendering a set of tabs, this template will be
  called once per tab.
  -->
  <xsl:template name="calculate-tab-href">
    <xsl:param name="dir_index" select="'index.html'"/>
    <xsl:param name="tab"/>
<!-- current 'tab' node -->
    <xsl:param name="path" select="$path"/>
    <xsl:if test="starts-with($tab/@href, 'http')">
<!-- Absolute URL -->
      <xsl:value-of select="$tab/@href"/>
    </xsl:if>
    <xsl:if test="not(starts-with($tab/@href, 'http'))">
<!-- Root-relative path -->
      <xsl:variable name="backpath">
        <xsl:call-template name="dotdots">
          <xsl:with-param name="path" select="$path"/>
        </xsl:call-template>
<xsl:text>/</xsl:text>
        <xsl:value-of select="$tab/@dir | $tab/@href"/>
<!-- If we obviously have a directory, add /index.html -->
        <xsl:if test="$tab/@dir or substring($tab/@href, string-length($tab/@href),
          string-length($tab/@href)) = '/'">
<xsl:text>/</xsl:text>
          <xsl:if test="$tab/@indexfile">
            <xsl:value-of select="$tab/@indexfile"/>
          </xsl:if>
          <xsl:if test="not(@indexfile)">
            <xsl:value-of select="$dir_index"/>
          </xsl:if>
        </xsl:if>
      </xsl:variable>
      <xsl:value-of
        select="translate(normalize-space(translate($backpath, ' /', '/ ')), ' /', '/ ')"/>
<!-- Link to backpath, normalizing slashes -->
    </xsl:if>
  </xsl:template>
<!--
    The id of any tab, whose path is a subset of the current URL.  Ie,
    the path of the 'current' tab.
  -->
  <xsl:template name="matching-id" xmlns:l="http://apache.org/forrest/linkmap/1.0">
    <xsl:value-of select="$site//*[starts-with(@href, $path)]/@tab"/>
  </xsl:template>
<!--
    The longest path of any level 1 tab, whose path is a subset of the current URL.  Ie,
    the path of the 'current' tab.
  -->
  <xsl:template name="longest-dir">
    <xsl:param name="tabfile"/>
    <xsl:for-each select="$tabfile/tabs/tab[starts-with($path, @dir|@href)]">
      <xsl:sort select="string-length(@dir|@href)"
        data-type="number" order="descending"/>
      <xsl:if test="position()=1">
        <xsl:value-of select="@dir|@href"/>
      </xsl:if>
    </xsl:for-each>
  </xsl:template>
<!--
    The longest path of any level 2 tab, whose path is a subset of the current URL.  Ie,
    the path of the 'current' tab.
  -->
  <xsl:template name="level2-longest-dir">
    <xsl:param name="tabfile"/>
    <xsl:for-each select="$tabfile/tabs/tab/tab[starts-with($path, @dir|@href)]">
      <xsl:sort select="string-length(@dir|@href)"
        data-type="number" order="descending"/>
      <xsl:if test="position()=1">
        <xsl:value-of select="@dir|@href"/>
      </xsl:if>
    </xsl:for-each>
  </xsl:template>
</xsl:stylesheet>
