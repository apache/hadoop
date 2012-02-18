<!--
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
-->

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

  <xsl:template name="humanReadableBytes">

    <xsl:param name="number"/>

    <xsl:variable name="kb" select="1024"/>
    <xsl:variable name="Mb" select="$kb * 1024"/>
    <xsl:variable name="Gb" select="$Mb * 1024"/>
    <xsl:variable name="Tb" select="$Gb * 1024"/>
    <xsl:variable name="Pb" select="$Tb * 1024"/>

     
    <xsl:choose>
      <xsl:when test="$number &lt; $kb"><xsl:value-of select="format-number($number, '#,###.##')"/> b</xsl:when>
      <xsl:when test="$number &lt; $Mb"><xsl:value-of select="format-number($number div $kb, '#,###.00')"/> kb</xsl:when>
      <xsl:when test="$number &lt; $Gb"><xsl:value-of select="format-number($number div $Mb, '#,###.00')"/> Mb</xsl:when>
      <xsl:when test="$number &lt; $Tb"><xsl:value-of select="format-number($number div $Gb, '#,###.00')"/> Gb</xsl:when>

      <xsl:when test="$number &lt; $Pb"><xsl:value-of select="format-number($number div $Tb, '#,###.00')"/> Tb</xsl:when>
      <xsl:when test="$number &lt; ($Pb * 1024)"><xsl:value-of select="format-number($number div $Pb, '#,###.00')"/> Pb</xsl:when>
      <xsl:otherwise><xsl:value-of select="format-number($number, '#,###.00')"/> b</xsl:otherwise>
    </xsl:choose>

  </xsl:template>

  <xsl:template name="percentage">
    <xsl:param name="number"/>
    <xsl:value-of select="format-number($number, '0.000%')"/>
  </xsl:template>

  <!--
    Displays value:
      - if it has parameter unit="b" then call humanReadableBytes
      - if it has parameter link then call displayLink
  -->
  <xsl:template name="displayValue">
    <xsl:param name="value"/>
    <xsl:param name="unit"/>

    <xsl:param name="link"/>
    <xsl:choose>
      <xsl:when test="$unit = 'b'">
        <xsl:call-template name="humanReadableBytes">
          <xsl:with-param name="number">
            <xsl:value-of select="@value"/>
          </xsl:with-param>
        </xsl:call-template>
      </xsl:when>

      <xsl:when test="$unit = '%'">
        <xsl:call-template name="percentage">
          <xsl:with-param name="number">
            <xsl:value-of select="@value"/>
          </xsl:with-param>
        </xsl:call-template>
      </xsl:when>
      <xsl:when test="string-length($link) &gt; 0">
        <a href="{$link}"><xsl:value-of select="$value"/></a>

      </xsl:when>
      <xsl:otherwise><xsl:value-of select="$value"/></xsl:otherwise>
    </xsl:choose>

  </xsl:template>

</xsl:stylesheet> 

