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
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.web.resources.DelegationParam;
import org.apache.hadoop.hdfs.web.resources.DoAsParam;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;
import org.apache.hadoop.hdfs.web.resources.TokenArgumentParam;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.apache.hadoop.hdfs.web.resources.FsActionParam;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestWebHdfsUrl {
  // NOTE: port is never used 
  final URI uri = URI.create(WebHdfsFileSystem.SCHEME + "://" + "127.0.0.1:0");

  @Before
  public void resetUGI() {
    UserGroupInformation.setConfiguration(new Configuration());
  }
  
  @Test(timeout=60000)
  public void testEncodedPathUrl() throws IOException, URISyntaxException{
    Configuration conf = new Configuration();

    final WebHdfsFileSystem webhdfs = (WebHdfsFileSystem) FileSystem.get(
        uri, conf);

    // Construct a file path that contains percentage-encoded string
    String pathName = "/hdtest010%2C60020%2C1371000602151.1371058984668";
    Path fsPath = new Path(pathName);
    URL encodedPathUrl = webhdfs.toUrl(PutOpParam.Op.CREATE, fsPath);
    // We should get back the original file path after cycling back and decoding
    Assert.assertEquals(WebHdfsFileSystem.PATH_PREFIX + pathName,
        encodedPathUrl.toURI().getPath());
  }

  @Test(timeout=60000)
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

  @Test(timeout=60000)
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

  @Test(timeout=60000)
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
    String tokenString = webhdfs.getDelegationToken().encodeToUrlString();

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

    // send token
    URL cancelTokenUrl = webhdfs.toUrl(PutOpParam.Op.CANCELDELEGATIONTOKEN,
        fsPath, new TokenArgumentParam(tokenString));
    checkQueryParams(
        new String[]{
            PutOpParam.Op.CANCELDELEGATIONTOKEN.toQueryString(),
            new UserParam(ugi.getShortUserName()).toString(),
            new TokenArgumentParam(tokenString).toString(),
        },
        cancelTokenUrl);
    
    // send token
    URL fileStatusUrl = webhdfs.toUrl(GetOpParam.Op.GETFILESTATUS, fsPath);
    checkQueryParams(
        new String[]{
            GetOpParam.Op.GETFILESTATUS.toQueryString(),
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

  @Test(timeout=60000)
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
    String tokenString = webhdfs.getDelegationToken().encodeToUrlString();

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

    // send token
    URL cancelTokenUrl = webhdfs.toUrl(PutOpParam.Op.CANCELDELEGATIONTOKEN,
        fsPath, new TokenArgumentParam(tokenString));
    checkQueryParams(
        new String[]{
            PutOpParam.Op.CANCELDELEGATIONTOKEN.toQueryString(),
            new UserParam(ugi.getRealUser().getShortUserName()).toString(),
            new DoAsParam(ugi.getShortUserName()).toString(),
            new TokenArgumentParam(tokenString).toString(),
        },
        cancelTokenUrl);
    
    // send token
    URL fileStatusUrl = webhdfs.toUrl(GetOpParam.Op.GETFILESTATUS, fsPath);
    checkQueryParams(
        new String[]{
            GetOpParam.Op.GETFILESTATUS.toQueryString(),
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

  @Test(timeout=60000)
  public void testCheckAccessUrl() throws IOException {
    Configuration conf = new Configuration();

    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser("test-user");
    UserGroupInformation.setLoginUser(ugi);

    WebHdfsFileSystem webhdfs = getWebHdfsFileSystem(ugi, conf);
    Path fsPath = new Path("/p1");

    URL checkAccessUrl = webhdfs.toUrl(GetOpParam.Op.CHECKACCESS,
        fsPath, new FsActionParam(FsAction.READ_WRITE));
    checkQueryParams(
        new String[]{
            GetOpParam.Op.CHECKACCESS.toQueryString(),
            new UserParam(ugi.getShortUserName()).toString(),
            FsActionParam.NAME + "=" + FsAction.READ_WRITE.SYMBOL
        },
        checkAccessUrl);
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
}
