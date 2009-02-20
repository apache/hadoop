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
        
    <xsl:template match="skinconfig">

  <xsl:if test="not(colors)">
  <colors>
<!-- Lenya -->
  <color name="header" value="#ffffff"/>

    <color name="tab-selected" value="#4C6C8F" link="#ffffff" vlink="#ffffff" hlink="#ffffff"/>
    <color name="tab-unselected" value="#E5E4D9" link="#000000" vlink="#000000" hlink="#000000"/>
    <color name="subtab-selected" value="#4C6C8F" link="#ffffff" vlink="#ffffff" hlink="#ffffff"/>
    <color name="subtab-unselected" value="#E5E4D9" link="#000000" vlink="#000000" hlink="#000000"/>

    <color name="heading" value="#E5E4D9"/>
    <color name="subheading" value="#E5E4D9"/>
    <color name="published" value="#4C6C8F" font="#FFFFFF"/>
	<color name="feedback" value="#4C6C8F" font="#FFFFFF" align="center"/>
    <color name="navstrip" value="#E5E4D9" font="#000000"/>

    <color name="toolbox" value="#CFDCED" font="#000000"/>

    <color name="border" value="#999999"/>
    <color name="menu" value="#4C6C8F" font="#ffffff" link="#ffffff" vlink="#ffffff" hlink="#ffffff"  />    
    <color name="menuheading" value="#cfdced" font="#000000" />
    <color name="searchbox" value="#E5E4D9" font="#000000"/>
    
    <color name="dialog" value="#E5E4D9" font="#000000"/>
	<color name="body" value="#ffffff" />            
    
    <color name="table" value="#ccc"/>    
    <color name="table-cell" value="#ffffff"/>   
    <color name="highlight" value="#ffff00"/>
    <color name="fixme" value="#cc6600"/>
    <color name="note" value="#006699"/>
    <color name="warning" value="#990000"/>
    <color name="code" value="#003366"/>
        
    <color name="footer" value="#E5E4D9"/>
  </colors>
  </xsl:if>

     <xsl:copy>
      <xsl:copy-of select="@*"/>
      <xsl:copy-of select="node()[not(name(.)='colors')]"/>     
      <xsl:apply-templates select="colors"/>
     </xsl:copy> 

    </xsl:template>

    <xsl:template match="colors">
     <xsl:copy>
      <xsl:copy-of select="@*"/>
      <xsl:copy-of select="node()[name(.)='color']"/> 
      
     <xsl:if test="not(color[@name='header'])">
       <color name="header" value="#FFFFFF"/>
     </xsl:if>  
     <xsl:if test="not(color[@name='tab-selected'])">
      <color name="tab-selected" value="#4C6C8F" link="#ffffff" vlink="#ffffff" hlink="#ffffff"/>
     </xsl:if>  
     <xsl:if test="not(color[@name='tab-unselected'])">
      <color name="tab-unselected" value="#E5E4D9" link="#000000" vlink="#000000" hlink="#000000"/>
     </xsl:if>
     <xsl:if test="not(color[@name='subtab-selected'])">
      <color name="subtab-selected" value="#4C6C8F" link="#ffffff" vlink="#ffffff" hlink="#ffffff"/>
     </xsl:if>  
     <xsl:if test="not(color[@name='subtab-unselected'])">
      <color name="subtab-unselected" value="#E5E4D9" link="#000000" vlink="#000000" hlink="#000000"/>
     </xsl:if>  
     <xsl:if test="not(color[@name='heading'])">
      <color name="heading" value="#E5E4D9"/>
     </xsl:if>  
     <xsl:if test="not(color[@name='subheading'])">
      <color name="subheading" value="#E5E4D9"/>
     </xsl:if>
      <xsl:if test="not(color[@name='published'])">
		<color name="feedback" value="#4C6C8F" font="#FFFFFF" align="center"/>
     </xsl:if> 
     <xsl:if test="not(color[@name='published'])">
        <color name="published" value="#4C6C8F" font="#FFFFFF"/>
     </xsl:if> 
     <xsl:if test="not(color[@name='navstrip'])">
      <color name="navstrip" value="#E5E4D9" font="#000000"/>
     </xsl:if>  
     <xsl:if test="not(color[@name='toolbox'])">
       <color name="toolbox" value="#CFDCED" font="#000000"/>
     </xsl:if>  
     <xsl:if test="not(color[@name='border'])">
       <color name="border" value="#999999"/>
     </xsl:if>       
     <xsl:if test="not(color[@name='menu'])">
      	<color name="menu" value="#4C6C8F" font="#ffffff" link="#ffffff" vlink="#ffffff" hlink="#ffffff"  /> 
     </xsl:if>
     <xsl:if test="not(color[@name='menuheading'])">
	     <color name="menuheading" value="#cfdced" font="#000000" />
     </xsl:if> 
     <xsl:if test="not(color[@name='searchbox'])">
	 	<color name="searchbox" value="#E5E4D9" font="#000000"/>
     </xsl:if> 
     <xsl:if test="not(color[@name='dialog'])">
      <color name="dialog" value="#E5E4D9" font="#000000" link="#000000" vlink="#000000" hlink="#000000"/>
     </xsl:if>  
     <xsl:if test="not(color[@name='body'])">
      <color name="body" value="#ffffff" /> 
     </xsl:if>  
     <xsl:if test="not(color[@name='table'])">
      <color name="table" value="#ccc"/>    
     </xsl:if>  
     <xsl:if test="not(color[@name='table-cell'])">
      <color name="table-cell" value="#ffffff"/>    
     </xsl:if>  
     <xsl:if test="not(color[@name='highlight'])">
       <color name="highlight" value="#ffff00"/>
     </xsl:if>  
     <xsl:if test="not(color[@name='fixme'])">
       <color name="fixme" value="#c60"/>
     </xsl:if>  
     <xsl:if test="not(color[@name='note'])">
       <color name="note" value="#069"/>
     </xsl:if>  
     <xsl:if test="not(color[@name='warning'])">
       <color name="warning" value="#900"/>
     </xsl:if>  
     <xsl:if test="not(color[@name='code'])">
       <color name="code" value="#a5b6c6"/>
     </xsl:if>  
     <xsl:if test="not(color[@name='footer'])">
       <color name="footer" value="#E5E4D9"/>
     </xsl:if>  
    
     </xsl:copy> 

    </xsl:template>
    
</xsl:stylesheet>
