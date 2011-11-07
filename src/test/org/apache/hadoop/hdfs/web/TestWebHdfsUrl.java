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

import java.io.IOException;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.web.resources.DelegationParam;
import org.apache.hadoop.hdfs.web.resources.TokenArgumentParam;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.junit.Assert;
import org.junit.Test;
import static org.mockito.Mockito.mock;

public class TestWebHdfsUrl {

  @Test
  public void testDelegationTokenInUrl() throws IOException {
    Configuration conf = new Configuration();
    final String uri = WebHdfsFileSystem.SCHEME + "://" + "127.0.0.1:9071";
    // Turn on security
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    DelegationTokenIdentifier dtId = new DelegationTokenIdentifier(new Text(
        ugi.getUserName()), null, null);
    FSNamesystem namesystem = mock(FSNamesystem.class);
    DelegationTokenSecretManager dtSecretManager = new DelegationTokenSecretManager(
        86400000, 86400000, 86400000, 86400000, namesystem);
    dtSecretManager.startThreads();
    Token<DelegationTokenIdentifier> token = new Token<DelegationTokenIdentifier>(
        dtId, dtSecretManager);
    token.setService(new Text("127.0.0.1:9071"));
    token.setKind(WebHdfsFileSystem.TOKEN_KIND);
    ugi.addToken(token);
    final WebHdfsFileSystem webhdfs = (WebHdfsFileSystem) FileSystem.get(
        URI.create(uri), conf);
    String tokenString = token.encodeToUrlString();
    Path fsPath = new Path("/");
    URL renewTokenUrl = webhdfs.toUrl(PutOpParam.Op.RENEWDELEGATIONTOKEN,
        fsPath, new TokenArgumentParam(tokenString));
    URL cancelTokenUrl = webhdfs.toUrl(PutOpParam.Op.CANCELDELEGATIONTOKEN,
        fsPath, new TokenArgumentParam(tokenString));
    Assert.assertEquals(
        generateUrlQueryPrefix(PutOpParam.Op.RENEWDELEGATIONTOKEN,
            ugi.getUserName())
            + "&token=" + tokenString, renewTokenUrl.getQuery());
    Token<DelegationTokenIdentifier> delegationToken = new Token<DelegationTokenIdentifier>(
        token);
    delegationToken.setKind(WebHdfsFileSystem.TOKEN_KIND);
    Assert.assertEquals(
        generateUrlQueryPrefix(PutOpParam.Op.CANCELDELEGATIONTOKEN,
            ugi.getUserName())
            + "&token="
            + tokenString
            + "&"
            + DelegationParam.NAME
            + "="
            + delegationToken.encodeToUrlString(), cancelTokenUrl.getQuery());
  }

  private String generateUrlQueryPrefix(HttpOpParam.Op op, String username) {
    return "op=" + op.toString() + "&user.name=" + username;
  }
}
