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
book-to-menu.xsl generates the HTML menu.  See the imported book-to-menu.xsl for
details.
-->
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:import href="../../../common/xslt/html/book-to-menu.xsl"/>
  <xsl:template match="book">
    <div id="menu">
      <ul>
        <xsl:apply-templates select="menu"/>
      </ul>
    </div>
  </xsl:template>
  <xsl:template match="menu">
    <li><h1>
        <xsl:value-of select="@label"/>
      </h1>
      <ul>
        <xsl:apply-templates/>
      </ul></li>
  </xsl:template>
  <xsl:template match="menu-item[@type='hidden']"/>
  <xsl:template match="menu-item">
    <li><xsl:apply-imports/></li>
  </xsl:template>
  <xsl:template name="selected">
    <div class="current">
      <xsl:value-of select="@label"/>
    </div>
  </xsl:template>
  <xsl:template name="print-external">
    <font color="#ffcc00">
      <xsl:apply-imports/>
    </font>
  </xsl:template>
</xsl:stylesheet>
