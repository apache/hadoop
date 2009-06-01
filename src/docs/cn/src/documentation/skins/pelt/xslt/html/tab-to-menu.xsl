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
Tab4:  'community/howto/form/index.html'

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
  <xsl:import href="../../../common/xslt/html/tab-to-menu.xsl"/>
  <xsl:template match="tabs">
    <ul id="tabs">
      <xsl:call-template name="base-tabs"/>
    </ul>
    <span id="level2tabs">
      <xsl:call-template name="level2tabs"/>
    </span>
  </xsl:template>
  <xsl:template name="pre-separator"></xsl:template>
  <xsl:template name="post-separator"></xsl:template>
  <xsl:template name="separator"></xsl:template>
  <xsl:template name="level2-separator"></xsl:template>
  <xsl:template name="selected">
    <li class="current"><xsl:call-template name="base-selected"/></li>
  </xsl:template>
  <xsl:template name="not-selected">
    <li><xsl:call-template name="base-not-selected"/></li>
  </xsl:template>
  <xsl:template name="level2-not-selected">
    <xsl:call-template name="base-not-selected"/>
  </xsl:template>
  <xsl:template name="level2-selected">
    <xsl:call-template name="base-selected"/>
  </xsl:template>
</xsl:stylesheet>
