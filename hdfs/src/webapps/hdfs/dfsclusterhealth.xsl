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

<xsl:stylesheet version="1.0"
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

  <xsl:include href="dfsclusterhealth_utils.xsl" />

  <xsl:output method="html" encoding="UTF-8" />

  <xsl:template match="/">
    <html>
      <head>
        <link rel="stylesheet" type="text/css" href="static/hadoop.css" />
        <style type="text/css">th,span {width:8em;}</style>
        <title>
          Hadoop cluster
          <xsl:value-of select="cluster/@clusterId" />
        </title>
      </head>
      <body>

        <h1>
          Cluster '
          <xsl:value-of select="cluster/@clusterId" />
          '
        </h1>

        <h2>Cluster Summary</h2>
        <xsl:if test="count(cluster/storage/item)">
          <div id="dfstable">
            <table>
              <tbody>
                <xsl:for-each select="cluster/storage/item">
                  <tr class="rowNormal">
                    <td id="col1">
                      <xsl:value-of select="@label" />
                    </td>
                    <td id="col2">:</td>
                    <td id="col3">

                      <xsl:call-template name="displayValue">
                        <xsl:with-param name="value">
                          <xsl:value-of select="@value" />
                        </xsl:with-param>
                        <xsl:with-param name="unit">
                          <xsl:value-of select="@unit" />
                        </xsl:with-param>
                        <xsl:with-param name="link">
                          <xsl:value-of select="@link" />

                        </xsl:with-param>
                      </xsl:call-template>
                    </td>
                  </tr>
                </xsl:for-each>
              </tbody>
            </table>
          </div>

          <br />
          <hr />
        </xsl:if>
        <xsl:if test="count(cluster/namenodes/node)">
          <h2>Namenodes</h2>

          <div id="dfstable">
            <table>
              <tbody>
                <tr class="rowNormal">
                  <td id="col1">Number of namenodes</td>
                  <td id="col2">:</td>
                  <td id="col3">
                    <xsl:value-of select="count(cluster/namenodes/node)" />
                  </td>
                </tr>
              </tbody>
            </table>
          </div>

          <br />

          <div id="dfstable">
            <table border="1" cellpadding="10" cellspacing="0">

              <thead>
                <xsl:for-each select="cluster/namenodes/node[1]/item">
                  <th>
                    <SPAN><xsl:value-of select="@label" /></SPAN>
                  </th>
                </xsl:for-each>
              </thead>

              <tbody>
                <xsl:for-each select="cluster/namenodes/node">
                  <tr>
                    <xsl:for-each select="item">
                      <td>

                        <xsl:call-template name="displayValue">
                          <xsl:with-param name="value">
                            <xsl:value-of select="@value" />
                          </xsl:with-param>
                          <xsl:with-param name="unit">
                            <xsl:value-of select="@unit" />
                          </xsl:with-param>
                          <xsl:with-param name="link">
                            <xsl:value-of select="@link" />

                          </xsl:with-param>
                        </xsl:call-template>
                      </td>
                    </xsl:for-each>
                  </tr>
                </xsl:for-each>
              </tbody>

            </table>
          </div>
        </xsl:if>

        <xsl:if test="count(cluster/unreportedNamenodes/node)">
          <h2>Unreported Namenodes</h2>
          <div id="dfstable">
            <table border="1" cellpadding="10" cellspacing="0">
              <tbody>
                <xsl:for-each select="cluster/unreportedNamenodes/node">
                  <tr class="rowNormal">
                    <td id="col1">
                      <xsl:value-of select="@name" />
                    </td>
                    <td id="col2">
                      <xsl:value-of select="@exception" />
                    </td>
                  </tr>
                </xsl:for-each>
              </tbody>
            </table>
          </div>
        </xsl:if>

        <xsl:if test="count(cluster/message/item)">
          <h4>Exception</h4>
          <xsl:for-each select="cluster/message/item">
            <xsl:value-of select="@msg" />
          </xsl:for-each>
        </xsl:if>

      </body>
    </html>
  </xsl:template>
</xsl:stylesheet> 

