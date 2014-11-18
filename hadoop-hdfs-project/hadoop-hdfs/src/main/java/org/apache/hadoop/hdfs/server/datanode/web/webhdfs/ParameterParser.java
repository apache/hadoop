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
package org.apache.hadoop.hdfs.server.datanode.web.webhdfs;

import io.netty.handler.codec.http.QueryStringDecoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.web.resources.BlockSizeParam;
import org.apache.hadoop.hdfs.web.resources.BufferSizeParam;
import org.apache.hadoop.hdfs.web.resources.DelegationParam;
import org.apache.hadoop.hdfs.web.resources.DoAsParam;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam;
import org.apache.hadoop.hdfs.web.resources.NamenodeAddressParam;
import org.apache.hadoop.hdfs.web.resources.OffsetParam;
import org.apache.hadoop.hdfs.web.resources.OverwriteParam;
import org.apache.hadoop.hdfs.web.resources.PermissionParam;
import org.apache.hadoop.hdfs.web.resources.ReplicationParam;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.HDFS_URI_SCHEME;
import static org.apache.hadoop.hdfs.server.datanode.web.webhdfs.WebHdfsHandler.WEBHDFS_PREFIX_LENGTH;

class ParameterParser {
  private final Configuration conf;
  private final String path;
  private final Map<String, List<String>> params;

  ParameterParser(QueryStringDecoder decoder, Configuration conf) {
    this.path = decoder.path().substring(WEBHDFS_PREFIX_LENGTH);
    this.params = decoder.parameters();
    this.conf = conf;
  }

  String path() { return path; }

  String op() {
    return param(HttpOpParam.NAME);
  }

  long offset() {
    return new OffsetParam(param(OffsetParam.NAME)).getValue();
  }

  String namenodeId() {
    return new NamenodeAddressParam(param(NamenodeAddressParam.NAME))
      .getValue();
  }

  String doAsUser() {
    return new DoAsParam(param(DoAsParam.NAME)).getValue();
  }

  String userName() {
    return new UserParam(param(UserParam.NAME)).getValue();
  }

  int bufferSize() {
    return new BufferSizeParam(param(BufferSizeParam.NAME)).getValue(conf);
  }

  long blockSize() {
    return new BlockSizeParam(param(BlockSizeParam.NAME)).getValue(conf);
  }

  short replication() {
    return new ReplicationParam(param(ReplicationParam.NAME)).getValue(conf);
  }

  FsPermission permission() {
    return new PermissionParam(param(PermissionParam.NAME)).getFsPermission();
  }

  boolean overwrite() {
    return new OverwriteParam(param(OverwriteParam.NAME)).getValue();
  }

  Token<DelegationTokenIdentifier> delegationToken() throws IOException {
    String delegation = param(DelegationParam.NAME);
    final Token<DelegationTokenIdentifier> token = new
      Token<DelegationTokenIdentifier>();
    token.decodeFromUrlString(delegation);
    URI nnUri = URI.create(HDFS_URI_SCHEME + "://" + namenodeId());
    boolean isLogical = HAUtil.isLogicalUri(conf, nnUri);
    if (isLogical) {
      token.setService(HAUtil.buildTokenServiceForLogicalUri(nnUri,
        HDFS_URI_SCHEME));
    } else {
      token.setService(SecurityUtil.buildTokenService(nnUri));
    }
    return token;
  }

  Configuration conf() {
    return conf;
  }

  private String param(String key) {
    List<String> p = params.get(key);
    return p == null ? null : p.get(0);
  }
}
