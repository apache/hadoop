<?xml version="1.0"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
<xsl:output method="xml"/>
<xsl:template match="configuration">
<!--
/**
 * Copyright 2010 The Apache Software Foundation
 *
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

This stylesheet is used making an html version of hbase-default.xml.
-->
<section xml:id="hbase_default_configurations"
version="5.0" xmlns="http://docbook.org/ns/docbook"
      xmlns:xlink="http://www.w3.org/1999/xlink"
      xmlns:xi="http://www.w3.org/2001/XInclude"
      xmlns:svg="http://www.w3.org/2000/svg"
      xmlns:m="http://www.w3.org/1998/Math/MathML"
      xmlns:html="http://www.w3.org/1999/xhtml"
      xmlns:db="http://docbook.org/ns/docbook">
<title>HBase Default Configuration</title>
<para>
</para>

<glossary xmlns='http://docbook.org/ns/docbook' xml:id="hbase.default.configuration">
<title>HBase Default Configuration</title>
<para>
The documentation below is generated using the default hbase configuration file,
<filename>hbase-default.xml</filename>, as source.
</para>

<xsl:for-each select="property">
<xsl:if test="not(@skipInDoc)">
<glossentry>
  <xsl:attribute name="id">
    <xsl:value-of select="name" />
  </xsl:attribute>
  <glossterm>
    <varname><xsl:value-of select="name"/></varname>
  </glossterm>
  <glossdef>
  <para><xsl:value-of select="description"/></para>
  <para>Default: <varname><xsl:value-of select="value"/></varname></para>
  </glossdef>
</glossentry>
</xsl:if>
</xsl:for-each>

</glossary>

</section>
</xsl:template>
</xsl:stylesheet>
