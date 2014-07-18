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
package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.Text;

import com.google.common.annotations.VisibleForTesting;

public class SWebHdfsFileSystem extends WebHdfsFileSystem {

  public static final Text TOKEN_KIND = new Text("SWEBHDFS delegation");
  public static final String SCHEME = "swebhdfs";

  @Override
  public String getScheme() {
    return SCHEME;
  }

  @Override
  protected String getTransportScheme() {
    return "https";
  }

  @Override
  protected Text getTokenKind() {
    return TOKEN_KIND;
  }

  @Override
  @VisibleForTesting
  public int getDefaultPort() {
    return getConf().getInt(DFSConfigKeys.DFS_NAMENODE_HTTPS_PORT_KEY,
        DFSConfigKeys.DFS_NAMENODE_HTTPS_PORT_DEFAULT);
  }
}
