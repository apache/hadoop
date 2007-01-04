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
  <xsl:import href="corner-imports.svg.xslt" />
  
  <!-- Diagonal 45 degrees corner -->
  <xsl:template name="figure">
        <xsl:variable name="biggersize" select="number($size)+number($size)"/>     
		<g transform="translate(0 0.5)">
           <polygon points="0,{$size} {$size},0 {$biggersize},0 {$biggersize},{$biggersize} 0,{$biggersize}"
                    style="{$fill}{$stroke}stroke-width:1"/>
		</g>
  </xsl:template>
      
</xsl:stylesheet>

