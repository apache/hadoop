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
package org.apache.hadoop.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.hadoop.hdfs.server.namenode.ha.IPFailoverProxyProvider;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.hadoop.net.StandardSocketFactory;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import sun.net.spi.nameservice.NameService;

public class TestDFSClientFailover {
  
  private static final Log LOG = LogFactory.getLog(TestDFSClientFailover.class);
  
  private static final Path TEST_FILE = new Path("/tmp/failover-test-file");
  private static final int FILE_LENGTH_TO_VERIFY = 100;
  
  private final Configuration conf = new Configuration();
  private MiniDFSCluster cluster;
  
  @Before
  public void setUpCluster() throws IOException {
    cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHATopology())
      .build();
    cluster.transitionToActive(0);
    cluster.waitActive();
  }
  
  @After
  public void tearDownCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @After
  public void clearConfig() {
    SecurityUtil.setTokenServiceUseIp(true);
  }

  /**
   * Make sure that client failover works when an active NN dies and the standby
   * takes over.
   */
  @Test
  public void testDfsClientFailover() throws IOException, URISyntaxException {
    FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);
    
    DFSTestUtil.createFile(fs, TEST_FILE,
        FILE_LENGTH_TO_VERIFY, (short)1, 1L);
    
    assertEquals(fs.getFileStatus(TEST_FILE).getLen(), FILE_LENGTH_TO_VERIFY);
    cluster.shutdownNameNode(0);
    cluster.transitionToActive(1);
    assertEquals(fs.getFileStatus(TEST_FILE).getLen(), FILE_LENGTH_TO_VERIFY);
    
    // Check that it functions even if the URL becomes canonicalized
    // to include a port number.
    Path withPort = new Path("hdfs://" +
        HATestUtil.getLogicalHostname(cluster) + ":" +
        HdfsClientConfigKeys.DFS_NAMENODE_RPC_PORT_DEFAULT + "/" +
        TEST_FILE.toUri().getPath());
    FileSystem fs2 = withPort.getFileSystem(fs.getConf());
    assertTrue(fs2.exists(withPort));

    fs.close();
  }
  
  /**
   * Test that even a non-idempotent method will properly fail-over if the
   * first IPC attempt times out trying to connect. Regression test for
   * HDFS-4404. 
   */
  @Test
  public void testFailoverOnConnectTimeout() throws Exception {
    conf.setClass(CommonConfigurationKeysPublic.HADOOP_RPC_SOCKET_FACTORY_CLASS_DEFAULT_KEY,
        InjectingSocketFactory.class, SocketFactory.class);
    // Set up the InjectingSocketFactory to throw a ConnectTimeoutException
    // when connecting to the first NN.
    InjectingSocketFactory.portToInjectOn = cluster.getNameNodePort(0);

    FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);
    
    // Make the second NN the active one.
    cluster.shutdownNameNode(0);
    cluster.transitionToActive(1);
    
    // Call a non-idempotent method, and ensure the failover of the call proceeds
    // successfully.
    IOUtils.closeStream(fs.create(TEST_FILE));
  }
  
  private static class InjectingSocketFactory extends StandardSocketFactory {

    static final SocketFactory defaultFactory = SocketFactory.getDefault();

    static int portToInjectOn;
    
    @Override
    public Socket createSocket() throws IOException {
      Socket spy = Mockito.spy(defaultFactory.createSocket());
      // Simplify our spying job by not having to also spy on the channel
      Mockito.doReturn(null).when(spy).getChannel();
      // Throw a ConnectTimeoutException when connecting to our target "bad"
      // host.
      Mockito.doThrow(new ConnectTimeoutException("injected"))
        .when(spy).connect(
            Mockito.argThat(new MatchesPort()),
            Mockito.anyInt());
      return spy;
    }

    private class MatchesPort extends BaseMatcher<SocketAddress> {
      @Override
      public boolean matches(Object arg0) {
        return ((InetSocketAddress)arg0).getPort() == portToInjectOn;
      }

      @Override
      public void describeTo(Description desc) {
        desc.appendText("matches port " + portToInjectOn);
      }
    }
  }
  
  /**
   * Regression test for HDFS-2683.
   */
  @Test
  public void testLogicalUriShouldNotHavePorts() {
    Configuration config = new HdfsConfiguration(conf);
    String logicalName = HATestUtil.getLogicalHostname(cluster);
    HATestUtil.setFailoverConfigurations(cluster, config, logicalName);
    Path p = new Path("hdfs://" + logicalName + ":12345/");
    try {
      p.getFileSystem(config).exists(p);
      fail("Did not fail with fake FS");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "does not use port information", ioe);
    }
  }

  /**
   * Make sure that a helpful error message is shown if a proxy provider is
   * configured for a given URI, but no actual addresses are configured for that
   * URI.
   */
  @Test
  public void testFailureWithMisconfiguredHaNNs() throws Exception {
    String logicalHost = "misconfigured-ha-uri";
    Configuration conf = new Configuration();
    conf.set(HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX + "." + logicalHost,
        ConfiguredFailoverProxyProvider.class.getName());
    
    URI uri = new URI("hdfs://" + logicalHost + "/test");
    try {
      FileSystem.get(uri, conf).exists(new Path("/test"));
      fail("Successfully got proxy provider for misconfigured FS");
    } catch (IOException ioe) {
      LOG.info("got expected exception", ioe);
      assertTrue("expected exception did not contain helpful message",
          StringUtils.stringifyException(ioe).contains(
          "Could not find any configured addresses for URI " + uri));
    }
  }

  /**
   * Spy on the Java DNS infrastructure.
   * This likely only works on Sun-derived JDKs, but uses JUnit's
   * Assume functionality so that any tests using it are skipped on
   * incompatible JDKs.
   */
  private NameService spyOnNameService() {
    try {
      Field f = InetAddress.class.getDeclaredField("nameServices");
      f.setAccessible(true);
      Assume.assumeNotNull(f);
      @SuppressWarnings("unchecked")
      List<NameService> nsList = (List<NameService>) f.get(null);

      NameService ns = nsList.get(0);
      Log log = LogFactory.getLog("NameServiceSpy");
      
      ns = Mockito.mock(NameService.class,
          new GenericTestUtils.DelegateAnswer(log, ns));
      nsList.set(0, ns);
      return ns;
    } catch (Throwable t) {
      LOG.info("Unable to spy on DNS. Skipping test.", t);
      // In case the JDK we're testing on doesn't work like Sun's, just
      // skip the test.
      Assume.assumeNoException(t);
      throw new RuntimeException(t);
    }
  }
  
  /**
   * Test that the client doesn't ever try to DNS-resolve the logical URI.
   * Regression test for HADOOP-9150.
   */
  @Test
  public void testDoesntDnsResolveLogicalURI() throws Exception {
    FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);
    NameService spyNS = spyOnNameService();
    String logicalHost = fs.getUri().getHost();
    Path qualifiedRoot = fs.makeQualified(new Path("/"));
    
    // Make a few calls against the filesystem.
    fs.getCanonicalServiceName();
    fs.listStatus(qualifiedRoot);
    
    // Ensure that the logical hostname was never resolved.
    Mockito.verify(spyNS, Mockito.never()).lookupAllHostAddr(Mockito.eq(logicalHost));
  }
  
  /**
   * Same test as above, but for FileContext.
   */
  @Test
  public void testFileContextDoesntDnsResolveLogicalURI() throws Exception {
    FileSystem fs = HATestUtil.configureFailoverFs(cluster, conf);
    NameService spyNS = spyOnNameService();
    String logicalHost = fs.getUri().getHost();
    Configuration haClientConf = fs.getConf();
    
    FileContext fc = FileContext.getFileContext(haClientConf);
    Path root = new Path("/");
    fc.listStatus(root);
    fc.listStatus(fc.makeQualified(root));
    fc.getDefaultFileSystem().getCanonicalServiceName();

    // Ensure that the logical hostname was never resolved.
    Mockito.verify(spyNS, Mockito.never()).lookupAllHostAddr(Mockito.eq(logicalHost));
  }

  /**
   * Test that creating proxy doesn't ever try to DNS-resolve the logical URI.
   * Regression test for HDFS-9364.
   */
  @Test(timeout=60000)
  public void testCreateProxyDoesntDnsResolveLogicalURI() throws IOException {
    final NameService spyNS = spyOnNameService();
    final Configuration conf = new HdfsConfiguration();
    final String service = "nameservice1";
    final String namenode = "namenode113";
    conf.set(DFSConfigKeys.DFS_NAMESERVICES, service);
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "hdfs://" + service);
    conf.set(
        HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX + "." + service,
        "org.apache.hadoop.hdfs.server.namenode.ha."
        + "ConfiguredFailoverProxyProvider");
    conf.set(DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + "." + service,
        namenode);
    conf.set(DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY + "." + service + "."
        + namenode, "localhost:8020");

    // call createProxy implicitly and explicitly
    Path p = new Path("/");
    p.getFileSystem(conf);
    NameNodeProxiesClient.createProxyWithClientProtocol(conf,
        FileSystem.getDefaultUri(conf), null);
    NameNodeProxies.createProxy(conf, FileSystem.getDefaultUri(conf),
        NamenodeProtocol.class, null);

    // Ensure that the logical hostname was never resolved.
    Mockito.verify(spyNS, Mockito.never()).lookupAllHostAddr(
        Mockito.eq(service));
  }

  /** Dummy implementation of plain FailoverProxyProvider */
  public static class DummyLegacyFailoverProxyProvider<T>
      implements FailoverProxyProvider<T> {
    private Class<T> xface;
    private T proxy;
    public DummyLegacyFailoverProxyProvider(Configuration conf, URI uri,
        Class<T> xface) {
      try {
        this.proxy = NameNodeProxies.createNonHAProxy(conf,
            DFSUtilClient.getNNAddress(uri), xface,
            UserGroupInformation.getCurrentUser(), false).getProxy();
        this.xface = xface;
      } catch (IOException ioe) {
      }
    }

    @Override
    public Class<T> getInterface() {
      return xface;
    }

    @Override
    public ProxyInfo<T> getProxy() {
      return new ProxyInfo<T>(proxy, "dummy");
    }

    @Override
    public void performFailover(T currentProxy) {
    }

    @Override
    public void close() throws IOException {
    }
  }

  /**
   * Test to verify legacy proxy providers are correctly wrapped.
   */
  @Test
  public void testWrappedFailoverProxyProvider() throws Exception {
    // setup the config with the dummy provider class
    Configuration config = new HdfsConfiguration(conf);
    String logicalName = HATestUtil.getLogicalHostname(cluster);
    HATestUtil.setFailoverConfigurations(cluster, config, logicalName);
    config.set(HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX + "." + logicalName,
        DummyLegacyFailoverProxyProvider.class.getName());
    Path p = new Path("hdfs://" + logicalName + "/");

    // not to use IP address for token service
    SecurityUtil.setTokenServiceUseIp(false);

    // Logical URI should be used.
    assertTrue("Legacy proxy providers should use logical URI.",
        HAUtil.useLogicalUri(config, p.toUri()));
  }

  /**
   * Test to verify IPFailoverProxyProvider is not requiring logical URI.
   */
  @Test
  public void testIPFailoverProxyProviderLogicalUri() throws Exception {
    // setup the config with the IP failover proxy provider class
    Configuration config = new HdfsConfiguration(conf);
    URI nnUri = cluster.getURI(0);
    config.set(HdfsClientConfigKeys.Failover.PROXY_PROVIDER_KEY_PREFIX + "." +
        nnUri.getHost(),
        IPFailoverProxyProvider.class.getName());

    assertFalse("IPFailoverProxyProvider should not use logical URI.",
        HAUtil.useLogicalUri(config, nnUri));
  }

}
