<?xml version="1.0" encoding="ISO-8859-1"?>
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

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  
  <xsl:template match="/">
    <html>
      <body>
        <h2>Hadoop DFS command-line tests</h2>
        <table border="1">
          <tr bgcolor="#9acd32">
            <th align="left">ID</th>
            <th align="left">Command</th>
            <th align="left">Description</th>
          </tr>
          <xsl:for-each select="configuration/tests/test">
            <!-- <xsl:sort select="description"/> -->
            <tr>
              <td><xsl:value-of select="position()"/></td>
              <td><xsl:value-of select="substring-before(description,':')"/></td>
              <td><xsl:value-of select="substring-after(description,':')"/></td>
            </tr>
          </xsl:for-each>
        </table>
      </body>
    </html>
  </xsl:template>
  
</xsl:stylesheet>
