<?xml version="1.0"?>
<!--
  Copyright 2002-2004 The Apache Software Foundation or its licensors,
  as applicable.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

    <xsl:param name="orientation-tb"/>
    <xsl:param name="orientation-lr"/>
    <xsl:param name="size"/>
    <xsl:param name="bg-color-name"/>
    <xsl:param name="stroke-color-name"/>
    <xsl:param name="fg-color-name"/>    

   <!-- if not all colors are present, don't even try to render the corners -->
    <xsl:variable name="isize"><xsl:choose>
    	<xsl:when test="$bg-color-name and $stroke-color-name and $fg-color-name"><xsl:value-of select="$size"/></xsl:when>
    	<xsl:otherwise>0</xsl:otherwise>
    </xsl:choose></xsl:variable>
    <xsl:variable name="smallersize" select="number($isize)-1"/>
    <xsl:variable name="biggersize" select="number($isize)+1"/>     
    <xsl:variable name="bg"><xsl:if test="skinconfig/colors/color[@name=$bg-color-name]">fill:<xsl:value-of select="skinconfig/colors/color[@name=$bg-color-name]/@value"/>;</xsl:if></xsl:variable>
    <xsl:variable name="fill"><xsl:if test="skinconfig/colors/color[@name=$stroke-color-name]">fill:<xsl:value-of select="skinconfig/colors/color[@name=$stroke-color-name]/@value"/>;</xsl:if></xsl:variable>
    <xsl:variable name="stroke"><xsl:if test="skinconfig/colors/color[@name=$fg-color-name]">stroke:<xsl:value-of select="skinconfig/colors/color[@name=$fg-color-name]/@value"/>;</xsl:if></xsl:variable>
        
	<xsl:template match="skinconfig">

        	

<svg width="{$isize}" height="{$isize}">
    <!-- background-->
    <rect x="-1" y="-1" width="{$biggersize}" height="{$biggersize}" style="{$bg}stroke-width:0"/>
<!-- 0,0 0,-4 4,0 4,-4-->

    <xsl:variable name="flip-tb-scale">
      <xsl:choose>
    	<xsl:when test="$orientation-tb='t'">1</xsl:when>
    	<xsl:otherwise>-1</xsl:otherwise>
      </xsl:choose>
    </xsl:variable>

    <xsl:variable name="flip-lr-scale">
      <xsl:choose>
    	<xsl:when test="$orientation-lr='l'">1</xsl:when>
    	<xsl:otherwise>-1</xsl:otherwise>
      </xsl:choose>
    </xsl:variable>
    
    <xsl:variable name="flip-tb-translate">
      <xsl:choose>
    	<xsl:when test="$orientation-tb='t'">0</xsl:when>
    	<xsl:otherwise>-<xsl:value-of select="$isize" /></xsl:otherwise>
      </xsl:choose>
    </xsl:variable>

    <xsl:variable name="flip-lr-translate">
      <xsl:choose>
    	<xsl:when test="$orientation-lr='l'">0</xsl:when>
    	<xsl:otherwise>-<xsl:value-of select="$isize" /></xsl:otherwise>
      </xsl:choose>
    </xsl:variable>    
    
    <!-- flip transform -->
    <g transform="scale({$flip-lr-scale},{$flip-tb-scale}) translate({$flip-lr-translate}, {$flip-tb-translate})"> 
      <xsl:call-template name="figure" />
    </g>
</svg>
</xsl:template>

        
  <xsl:template name="figure">
       <!-- Just change shape here -->     
		<g transform="translate(0.5 0.5)">
			<ellipse cx="{$smallersize}" cy="{$smallersize}" rx="{$smallersize}" ry="{$smallersize}"
				 style="{$fill}{$stroke}stroke-width:1"/>
		</g>
	   <!-- end -->	
  </xsl:template>
    
  
  <xsl:template match="*"></xsl:template>
  <xsl:template match="text()"></xsl:template>
  
</xsl:stylesheet>
