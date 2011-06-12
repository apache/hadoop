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
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" >
  <xsl:output method="xml" media-type="image/svg" omit-xml-declaration="yes" indent="yes"/>
<!-- the skinconf file -->
  <xsl:param name="config-file"/>
  <xsl:variable name="config" select="document($config-file)/skinconfig"/>
<!-- Get the section depth to use when generating the minitoc (default is 2) -->
  <xsl:variable name="toc-max-depth" select="number($config/toc/@max-depth)"/>
  <xsl:param name="numbersections" select="'true'"/>
<!-- Section depth at which we stop numbering and just indent -->
  <xsl:param name="numbering-max-depth" select="'3'"/>
  <xsl:param name="ctxbasedir" select="."/>
  <xsl:param name="xmlbasedir"/>
  <xsl:template match="/">
    <svg width="1305" height="1468" xmlns="http://www.w3.org/2000/svg">
      <g transform="translate(0 0)">
        <xsl:apply-templates/>
      </g>
    </svg>
  </xsl:template>
  <xsl:template match="document">
    <text x="00px" y="30px" style="font-size:20;">
      <xsl:value-of select="header/title"/>
    </text>
    <text x="0px" y="50px" style="font-size:12;">
      <xsl:apply-templates/>
    </text>
  </xsl:template>
</xsl:stylesheet>
