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

import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.web.resources.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.SecurityUtilTestHelper;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.*;

public class TestWebHdfsUrl {
  // NOTE: port is never used 
  final URI uri = URI.create(WebHdfsFileSystem.SCHEME + "://" + "127.0.0.1:0");

  @Before
  public void resetUGI() {
    UserGroupInformation.setConfiguration(new Configuration());
  }
  
  @Test(timeout=4000)
  public void testSimpleAuthParamsInUrl() throws IOException {
    Configuration conf = new Configuration();

    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser("test-user");
    UserGroupInformation.setLoginUser(ugi);

    WebHdfsFileSystem webhdfs = getWebHdfsFileSystem(ugi, conf);
    Path fsPath = new Path("/");

    // send user+token
    URL fileStatusUrl = webhdfs.toUrl(GetOpParam.Op.GETFILESTATUS, fsPath);
    checkQueryParams(
        new String[]{
            GetOpParam.Op.GETFILESTATUS.toQueryString(),
            new UserParam(ugi.getShortUserName()).toString()
        },
        fileStatusUrl);
  }

  @Test(timeout=4000)
  public void testSimpleProxyAuthParamsInUrl() throws IOException {
    Configuration conf = new Configuration();

    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser("test-user");
    ugi = UserGroupInformation.createProxyUser("test-proxy-user", ugi);
    UserGroupInformation.setLoginUser(ugi);

    WebHdfsFileSystem webhdfs = getWebHdfsFileSystem(ugi, conf);
    Path fsPath = new Path("/");

    // send real+effective
    URL fileStatusUrl = webhdfs.toUrl(GetOpParam.Op.GETFILESTATUS, fsPath);
    checkQueryParams(
        new String[]{
            GetOpParam.Op.GETFILESTATUS.toQueryString(),
            new UserParam(ugi.getRealUser().getShortUserName()).toString(),
            new DoAsParam(ugi.getShortUserName()).toString()
    },
        fileStatusUrl);
  }

  @Test(timeout=4000)
  public void testSecureAuthParamsInUrl() throws IOException {
    Configuration conf = new Configuration();
    // fake turning on security so api thinks it should use tokens
    SecurityUtil.setAuthenticationMethod(KERBEROS, conf);
    UserGroupInformation.setConfiguration(conf);

    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser("test-user");
    ugi.setAuthenticationMethod(KERBEROS);
    UserGroupInformation.setLoginUser(ugi);

    WebHdfsFileSystem webhdfs = getWebHdfsFileSystem(ugi, conf);
    Path fsPath = new Path("/");
    String tokenString = webhdfs.getRenewToken().encodeToUrlString();

    // send user
    URL getTokenUrl = webhdfs.toUrl(GetOpParam.Op.GETDELEGATIONTOKEN, fsPath);
    checkQueryParams(
        new String[]{
            GetOpParam.Op.GETDELEGATIONTOKEN.toQueryString(),
            new UserParam(ugi.getShortUserName()).toString()
        },
        getTokenUrl);

    // send user
    URL renewTokenUrl = webhdfs.toUrl(PutOpParam.Op.RENEWDELEGATIONTOKEN,
        fsPath, new TokenArgumentParam(tokenString));
    checkQueryParams(
        new String[]{
            PutOpParam.Op.RENEWDELEGATIONTOKEN.toQueryString(),
            new UserParam(ugi.getShortUserName()).toString(),
            new TokenArgumentParam(tokenString).toString(),
        },
        renewTokenUrl);

    // send user+token
    URL cancelTokenUrl = webhdfs.toUrl(PutOpParam.Op.CANCELDELEGATIONTOKEN,
        fsPath, new TokenArgumentParam(tokenString));
    checkQueryParams(
        new String[]{
            PutOpParam.Op.CANCELDELEGATIONTOKEN.toQueryString(),
            new UserParam(ugi.getShortUserName()).toString(),
            new TokenArgumentParam(tokenString).toString(),
            new DelegationParam(tokenString).toString()
        },
        cancelTokenUrl);
    
    // send user+token
    URL fileStatusUrl = webhdfs.toUrl(GetOpParam.Op.GETFILESTATUS, fsPath);
    checkQueryParams(
        new String[]{
            GetOpParam.Op.GETFILESTATUS.toQueryString(),
            new UserParam(ugi.getShortUserName()).toString(),
            new DelegationParam(tokenString).toString()
        },
        fileStatusUrl);

    // wipe out internal token to simulate auth always required
    webhdfs.setDelegationToken(null);

    // send user
    cancelTokenUrl = webhdfs.toUrl(PutOpParam.Op.CANCELDELEGATIONTOKEN,
        fsPath, new TokenArgumentParam(tokenString));
    checkQueryParams(
        new String[]{
            PutOpParam.Op.CANCELDELEGATIONTOKEN.toQueryString(),
            new UserParam(ugi.getShortUserName()).toString(),
            new TokenArgumentParam(tokenString).toString(),
        },
        cancelTokenUrl);

    // send user
    fileStatusUrl = webhdfs.toUrl(GetOpParam.Op.GETFILESTATUS, fsPath);
    checkQueryParams(
        new String[]{
            GetOpParam.Op.GETFILESTATUS.toQueryString(),
            new UserParam(ugi.getShortUserName()).toString()
        },
        fileStatusUrl);    
  }

  @Test(timeout=4000)
  public void testSecureProxyAuthParamsInUrl() throws IOException {
    Configuration conf = new Configuration();
    // fake turning on security so api thinks it should use tokens
    SecurityUtil.setAuthenticationMethod(KERBEROS, conf);
    UserGroupInformation.setConfiguration(conf);

    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser("test-user");
    ugi.setAuthenticationMethod(KERBEROS);
    ugi = UserGroupInformation.createProxyUser("test-proxy-user", ugi);
    UserGroupInformation.setLoginUser(ugi);

    WebHdfsFileSystem webhdfs = getWebHdfsFileSystem(ugi, conf);
    Path fsPath = new Path("/");
    String tokenString = webhdfs.getRenewToken().encodeToUrlString();

    // send real+effective
    URL getTokenUrl = webhdfs.toUrl(GetOpParam.Op.GETDELEGATIONTOKEN, fsPath);
    checkQueryParams(
        new String[]{
            GetOpParam.Op.GETDELEGATIONTOKEN.toQueryString(),
            new UserParam(ugi.getRealUser().getShortUserName()).toString(),
            new DoAsParam(ugi.getShortUserName()).toString()
        },
        getTokenUrl);

    // send real+effective
    URL renewTokenUrl = webhdfs.toUrl(PutOpParam.Op.RENEWDELEGATIONTOKEN,
        fsPath, new TokenArgumentParam(tokenString));
    checkQueryParams(
        new String[]{
            PutOpParam.Op.RENEWDELEGATIONTOKEN.toQueryString(),
            new UserParam(ugi.getRealUser().getShortUserName()).toString(),
            new DoAsParam(ugi.getShortUserName()).toString(),
            new TokenArgumentParam(tokenString).toString(),
        },
        renewTokenUrl);

    // send effective+token
    URL cancelTokenUrl = webhdfs.toUrl(PutOpParam.Op.CANCELDELEGATIONTOKEN,
        fsPath, new TokenArgumentParam(tokenString));
    checkQueryParams(
        new String[]{
            PutOpParam.Op.CANCELDELEGATIONTOKEN.toQueryString(),
            new UserParam(ugi.getShortUserName()).toString(),
            new TokenArgumentParam(tokenString).toString(),
            new DelegationParam(tokenString).toString()
        },
        cancelTokenUrl);
    
    // send effective+token
    URL fileStatusUrl = webhdfs.toUrl(GetOpParam.Op.GETFILESTATUS, fsPath);
    checkQueryParams(
        new String[]{
            GetOpParam.Op.GETFILESTATUS.toQueryString(),
            new UserParam(ugi.getShortUserName()).toString(),
            new DelegationParam(tokenString).toString()
        },
        fileStatusUrl);

    // wipe out internal token to simulate auth always required
    webhdfs.setDelegationToken(null);
    
    // send real+effective
    cancelTokenUrl = webhdfs.toUrl(PutOpParam.Op.CANCELDELEGATIONTOKEN,
        fsPath, new TokenArgumentParam(tokenString));
    checkQueryParams(
        new String[]{
            PutOpParam.Op.CANCELDELEGATIONTOKEN.toQueryString(),
            new UserParam(ugi.getRealUser().getShortUserName()).toString(),
            new DoAsParam(ugi.getShortUserName()).toString(),
            new TokenArgumentParam(tokenString).toString()
        },
        cancelTokenUrl);
    
    // send real+effective
    fileStatusUrl = webhdfs.toUrl(GetOpParam.Op.GETFILESTATUS, fsPath);
    checkQueryParams(
        new String[]{
            GetOpParam.Op.GETFILESTATUS.toQueryString(),
            new UserParam(ugi.getRealUser().getShortUserName()).toString(),
            new DoAsParam(ugi.getShortUserName()).toString()
        },
        fileStatusUrl);    
  }
  
  private void checkQueryParams(String[] expected, URL url) {
    Arrays.sort(expected);
    String[] query = url.getQuery().split("&");
    Arrays.sort(query);
    assertEquals(Arrays.toString(expected), Arrays.toString(query));
  }

  private WebHdfsFileSystem getWebHdfsFileSystem(UserGroupInformation ugi,
      Configuration conf) throws IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      DelegationTokenIdentifier dtId = new DelegationTokenIdentifier(new Text(
          ugi.getUserName()), null, null);
      FSNamesystem namesystem = mock(FSNamesystem.class);
      DelegationTokenSecretManager dtSecretManager = new DelegationTokenSecretManager(
          86400000, 86400000, 86400000, 86400000, namesystem);
      dtSecretManager.startThreads();
      Token<DelegationTokenIdentifier> token = new Token<DelegationTokenIdentifier>(
          dtId, dtSecretManager);
      SecurityUtil.setTokenService(
          token, NetUtils.createSocketAddr(uri.getAuthority()));
      token.setKind(WebHdfsFileSystem.TOKEN_KIND);
      ugi.addToken(token);
    }
    return (WebHdfsFileSystem) FileSystem.get(uri, conf);
  }
  
  @Test(timeout=4000)
  public void testSelectHdfsDelegationToken() throws Exception {
    SecurityUtilTestHelper.setTokenServiceUseIp(true);

    Configuration conf = new Configuration();
    conf.setClass("fs.webhdfs.impl", MyWebHdfsFileSystem.class, FileSystem.class);
    
    // test with implicit default port 
    URI fsUri = URI.create("webhdfs://localhost");
    MyWebHdfsFileSystem fs = (MyWebHdfsFileSystem) FileSystem.get(fsUri, conf);
    checkTokenSelection(fs, conf);

    // test with explicit default port
    fsUri = URI.create("webhdfs://localhost:"+fs.getDefaultPort());
    fs = (MyWebHdfsFileSystem) FileSystem.get(fsUri, conf);
    checkTokenSelection(fs, conf);
    
    // test with non-default port
    fsUri = URI.create("webhdfs://localhost:"+(fs.getDefaultPort()-1));
    fs = (MyWebHdfsFileSystem) FileSystem.get(fsUri, conf);
    checkTokenSelection(fs, conf);

  }
  
  private void checkTokenSelection(MyWebHdfsFileSystem fs,
                                   Configuration conf) throws IOException {
    int port = fs.getCanonicalUri().getPort();
    // can't clear tokens from ugi, so create a new user everytime
    UserGroupInformation ugi =
        UserGroupInformation.createUserForTesting(fs.getUri().getAuthority(), new String[]{});

    // use ip-based tokens
    SecurityUtilTestHelper.setTokenServiceUseIp(true);

    // test fallback to hdfs token
    Token<?> hdfsToken = new Token<TokenIdentifier>(
        new byte[0], new byte[0],
        DelegationTokenIdentifier.HDFS_DELEGATION_KIND,
        new Text("127.0.0.1:8020"));
    ugi.addToken(hdfsToken);

    // test fallback to hdfs token
    Token<?> token = fs.selectDelegationToken(ugi);
    assertNotNull(token);
    assertEquals(hdfsToken, token);

    // test webhdfs is favored over hdfs
    Token<?> webHdfsToken = new Token<TokenIdentifier>(
        new byte[0], new byte[0],
        WebHdfsFileSystem.TOKEN_KIND, new Text("127.0.0.1:"+port));
    ugi.addToken(webHdfsToken);
    token = fs.selectDelegationToken(ugi);
    assertNotNull(token);
    assertEquals(webHdfsToken, token);
    
    // switch to using host-based tokens, no token should match
    SecurityUtilTestHelper.setTokenServiceUseIp(false);
    token = fs.selectDelegationToken(ugi);
    assertNull(token);
    
    // test fallback to hdfs token
    hdfsToken = new Token<TokenIdentifier>(
        new byte[0], new byte[0],
        DelegationTokenIdentifier.HDFS_DELEGATION_KIND,
        new Text("localhost:8020"));
    ugi.addToken(hdfsToken);
    token = fs.selectDelegationToken(ugi);
    assertNotNull(token);
    assertEquals(hdfsToken, token);

    // test webhdfs is favored over hdfs
    webHdfsToken = new Token<TokenIdentifier>(
        new byte[0], new byte[0],
        WebHdfsFileSystem.TOKEN_KIND, new Text("localhost:"+port));
    ugi.addToken(webHdfsToken);
    token = fs.selectDelegationToken(ugi);
    assertNotNull(token);
    assertEquals(webHdfsToken, token);
  }
  
  static class MyWebHdfsFileSystem extends WebHdfsFileSystem {
    @Override
    public URI getCanonicalUri() {
      return super.getCanonicalUri();
    }
    @Override
    public int getDefaultPort() {
      return super.getDefaultPort();
    }
    // don't automatically get a token
    @Override
    protected void initDelegationToken() throws IOException {}
  }
}