<?xml version="1.0" encoding="utf-8"?>
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
Contains the 'dotdots' template, which, given a path, will output a set of
directory traversals to get back to the source directory. Handles both '/' and
'\' directory separators.

Examples:
  Input                           Output 
    index.html                    ""
    dir/index.html                "../"
    dir/subdir/index.html         "../../"
    dir//index.html              "../"
    dir/                          "../"
    dir//                         "../"
    \some\windows\path            "../../"
    \some\windows\path\           "../../../"
    \Program Files\mydir          "../"

Cannot handle ..'s in the path, so don't expect 'dir/subdir/../index.html' to
work.

-->
<xsl:stylesheet
  version="1.0"
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:template name="dotdots">
    <xsl:param name="path"/>
    <xsl:variable name="dirs" select="normalize-space(translate(concat($path, 'x'), ' /\', '_  '))"/>
<!-- The above does the following:
       o Adds a trailing character to the path. This prevents us having to deal
         with the special case of ending with '/'
       o Translates all directory separators to ' ', and normalize spaces,
		 cunningly eliminating duplicate '//'s. We also translate any real
		 spaces into _ to preserve them.
    -->
    <xsl:variable name="remainder" select="substring-after($dirs, ' ')"/>
    <xsl:if test="$remainder">
<xsl:text>../</xsl:text>
      <xsl:call-template name="dotdots">
        <xsl:with-param name="path" select="translate($remainder, ' ', '/')"/>
<!-- Translate back to /'s because that's what the template expects. -->
      </xsl:call-template>
    </xsl:if>
  </xsl:template>
<!--
  Uncomment to test.
  Usage: saxon dotdots.xsl dotdots.xsl path='/my/test/path'

  <xsl:param name="path"/>
  <xsl:template match="/">
    <xsl:message>Path: <xsl:value-of select="$path"/></xsl:message>
    <xsl:call-template name="dotdots">
      <xsl:with-param name="path" select="$path"/>
    </xsl:call-template>
  </xsl:template>
 -->
</xsl:stylesheet>
