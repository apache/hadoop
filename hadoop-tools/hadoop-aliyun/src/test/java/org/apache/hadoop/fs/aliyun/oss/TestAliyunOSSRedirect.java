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

package org.apache.hadoop.fs.aliyun.oss;

// Standard Java imports
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

// SLF4J imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Apache imports
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import com.aliyun.oss.common.auth.CredentialsProvider;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.VersionInfo;

// Aliyun OSS imports
import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.comm.Protocol;
import com.aliyun.oss.model.RoutingRule;
import com.aliyun.oss.model.SetBucketWebsiteRequest;
import com.aliyun.oss.ClientException;

// JUnit imports
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;
import org.junit.rules.Timeout;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.apache.hadoop.fs.aliyun.oss.Constants.*;

/**
 * This test checks the redirection behavior of Aliyun OSS.
 * When the redirection feature is available, it will access the
 * redirected target file
 */
public class TestAliyunOSSRedirect {

  private FileSystem fs;

  private OSSClient ossClient;

  private String bucketName;

  private URI testURI;

  private static final Logger LOG = LoggerFactory.getLogger(TestAliyunOSSInputStream.class);

  private static String testRootPath = AliyunOSSTestUtils.generateUniqueTestPath();

  @Rule
  public Timeout testTimeout = new Timeout(30 * 60 * 1000);

  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    constructOssClient(conf);
    cleanRedirectRule(ossClient);
    setRedirectRule(ossClient);
  }

  @After
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.delete(new Path(testRootPath), true);
    }

    cleanRedirectRule(ossClient);
    if (ossClient != null) {
      ossClient.shutdown();
    }
  }

  private void constructOssClient(Configuration conf) throws IOException {
    String fsname = conf.getTrimmed(
        TestAliyunOSSFileSystemContract.TEST_FS_OSS_NAME, "");

    if (StringUtils.isEmpty(fsname)) {
      throw new AssumptionViolatedException("No test filesystem in "
          + TestAliyunOSSFileSystemContract.TEST_FS_OSS_NAME);
    }

    testURI = URI.create(fsname);
    assertTrue(testURI.getScheme().equals(Constants.FS_OSS));
    bucketName = testURI.getHost();

    ClientConfiguration clientConf = new ClientConfiguration();
    clientConf.setMaxConnections(conf.getInt(MAXIMUM_CONNECTIONS_KEY,
        MAXIMUM_CONNECTIONS_DEFAULT));
    boolean secureConnections = conf.getBoolean(SECURE_CONNECTIONS_KEY,
        SECURE_CONNECTIONS_DEFAULT);
    clientConf.setProtocol(secureConnections ? Protocol.HTTPS : Protocol.HTTP);
    clientConf.setMaxErrorRetry(conf.getInt(MAX_ERROR_RETRIES_KEY,
        MAX_ERROR_RETRIES_DEFAULT));
    clientConf.setConnectionTimeout(conf.getInt(ESTABLISH_TIMEOUT_KEY,
        ESTABLISH_TIMEOUT_DEFAULT));
    clientConf.setSocketTimeout(conf.getInt(SOCKET_TIMEOUT_KEY,
        SOCKET_TIMEOUT_DEFAULT));
    clientConf.setUserAgent(
        conf.get(USER_AGENT_PREFIX, USER_AGENT_PREFIX_DEFAULT) + ", Hadoop/"
            + VersionInfo.getVersion());

    String proxyHost = conf.getTrimmed(PROXY_HOST_KEY, "");
    int proxyPort = conf.getInt(PROXY_PORT_KEY, -1);
    if (StringUtils.isNotEmpty(proxyHost)) {
      clientConf.setProxyHost(proxyHost);
      if (proxyPort >= 0) {
        clientConf.setProxyPort(proxyPort);
      } else {
        if (secureConnections) {
          LOG.warn("Proxy host set without port. Using HTTPS default 443");
          clientConf.setProxyPort(443);
        } else {
          LOG.warn("Proxy host set without port. Using HTTP default 80");
          clientConf.setProxyPort(80);
        }
      }
      String proxyUsername = conf.getTrimmed(PROXY_USERNAME_KEY);
      String proxyPassword = conf.getTrimmed(PROXY_PASSWORD_KEY);
      if ((proxyUsername == null) != (proxyPassword == null)) {
        String msg = "Proxy error: " + PROXY_USERNAME_KEY + " or " +
            PROXY_PASSWORD_KEY + " set without the other.";
        LOG.error(msg);
        throw new IllegalArgumentException(msg);
      }
      clientConf.setProxyUsername(proxyUsername);
      clientConf.setProxyPassword(proxyPassword);
      clientConf.setProxyDomain(conf.getTrimmed(PROXY_DOMAIN_KEY));
      clientConf.setProxyWorkstation(conf.getTrimmed(PROXY_WORKSTATION_KEY));
    } else if (proxyPort >= 0) {
      String msg = "Proxy error: " + PROXY_PORT_KEY + " set without " +
          PROXY_HOST_KEY;
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    boolean redirectEnable = conf.getBoolean(REDIRECT_ENABLE_KEY,
        REDIRECT_ENABLE_DEFAULT);
    if (!redirectEnable) {
      clientConf.setRedirectEnable(false);
      LOG.info("oss redirectEnable is closed");
    }

    String endPoint = conf.getTrimmed(ENDPOINT_KEY, "");
    if (StringUtils.isEmpty(endPoint)) {
      throw new IllegalArgumentException("Aliyun OSS endpoint should not be " +
          "null or empty. Please set proper endpoint with 'fs.oss.endpoint'.");
    }
    CredentialsProvider provider = AliyunOSSUtils.getCredentialsProvider(testURI, conf);
    ossClient = new OSSClient(endPoint, provider, clientConf);
    LOG.info("constructOssClient success");
  }

  private void cleanRedirectRule(OSSClient ossClient) {
    try {
      // 填写Bucket名称。
      ossClient.deleteBucketWebsite(bucketName);
    } catch (OSSException oe) {
      LOG.error("Caught an OSSException, which means your request made it to OSS, "
          + "but was rejected with an error response for some reason.");
      LOG.error("Error Message:" + oe.getErrorMessage());
      LOG.error("Error Code:" + oe.getErrorCode());
      LOG.error("Request ID:" + oe.getRequestId());
      LOG.error("Host ID:" + oe.getHostId());
      throw oe;
    } catch (ClientException ce) {
      LOG.error("Caught an ClientException, which means the client encountered "
          + "a serious internal problem while trying to communicate with OSS, "
          + "such as not being able to access the network.");
      LOG.error("Error Message:" + ce.getMessage());
      throw ce;
    }
  }

  private void setRedirectRule(OSSClient ossClient) {

    try {
      SetBucketWebsiteRequest request = new SetBucketWebsiteRequest(bucketName);
      List<RoutingRule> routingRules = new ArrayList<RoutingRule>();

      RoutingRule rule = new RoutingRule();
      rule.setNumber(1);
      rule.getCondition().setKeyPrefixEquals("test/redirect_test_");
      rule.getCondition().setHttpErrorCodeReturnedEquals(404);
      rule.getRedirect().setRedirectType(RoutingRule.RedirectType.External);
      rule.getRedirect().setHttpRedirectCode(302);
      rule.getRedirect().setHostName("apache.github.io");
      rule.getRedirect().setProtocol(RoutingRule.Protocol.Http);
      rule.getRedirect().setReplaceKeyWith("hadoop/hadoop-aliyun/tools/hadoop-aliyun/index.html");

      routingRules.add(rule);
      request.setRoutingRules(routingRules);
      ossClient.setBucketWebsite(request);
      LOG.info("setBucketWebsite success");
    } catch (OSSException oe) {
      LOG.info("Caught an OSSException, which means your request made it to OSS, "
          + "but was rejected with an error response for some reason.");
      LOG.info("Error Message:" + oe.getErrorMessage());
      LOG.info("Error Code:" + oe.getErrorCode());
      LOG.info("Request ID:" + oe.getRequestId());
      LOG.info("Host ID:" + oe.getHostId());
      throw oe;
    } catch (ClientException ce) {
      LOG.error("Caught an ClientException, which means the client encountered "
          + "a serious internal problem while trying to communicate with OSS, "
          + "such as not being able to access the network.");
      LOG.error("Error Message:" + ce.getMessage());
      throw ce;
    }
  }

  @Test
  public void testRedirectDisable() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(REDIRECT_ENABLE_KEY, false);
    fs = AliyunOSSTestUtils.createTestFileSystem(conf);

    Path srcFilePath = new Path(testRootPath,"redirect_test_src.txt");
    assertThrows(IOException.class, () -> this.fs.open(srcFilePath));
  }

  @Test
  public void testRedirectEnable() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(REDIRECT_ENABLE_KEY, true);
    fs = AliyunOSSTestUtils.createTestFileSystem(conf);

    Path srcFilePath = new Path(testRootPath, "redirect_test_src.txt");
    FSDataInputStream instream = this.fs.open(srcFilePath);
    byte[] content = IOUtils.readFullyToByteArray(instream);
    IOUtils.closeStream(instream);
    LOG.info("content:" + content);
    assertTrue(content.length >= 0);
  }
}
