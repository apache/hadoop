/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.security;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.*;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;

import javax.security.auth.kerberos.KerberosPrincipal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.security.alias.LocalJavaKeyStoreProvider;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ZKUtil.ZKAuthInfo;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.thirdparty.com.google.common.io.Files;

public class TestSecurityUtil {

  private static final String ZK_AUTH_VALUE = "a_scheme:a_password";

  @BeforeClass
  public static void unsetKerberosRealm() {
    // prevent failures if kinit-ed or on os x with no realm
    System.setProperty("java.security.krb5.kdc", "");
    System.setProperty("java.security.krb5.realm", "NONE");    
  }

  @Test
  public void isOriginalTGTReturnsCorrectValues() {
    assertTrue(SecurityUtil.isTGSPrincipal
        (new KerberosPrincipal("krbtgt/foo@foo")));
    assertTrue(SecurityUtil.isTGSPrincipal
        (new KerberosPrincipal("krbtgt/foo.bar.bat@foo.bar.bat")));
    assertFalse(SecurityUtil.isTGSPrincipal
        (null));
    assertFalse(SecurityUtil.isTGSPrincipal
        (new KerberosPrincipal("blah")));
    assertFalse(SecurityUtil.isTGSPrincipal
        (new KerberosPrincipal("krbtgt/hello")));
    assertFalse(SecurityUtil.isTGSPrincipal
        (new KerberosPrincipal("krbtgt/foo@FOO")));
  }
  
  private void verify(String original, String hostname, String expected)
      throws IOException {
    assertEquals(expected,
                 SecurityUtil.getServerPrincipal(original, hostname));

    InetAddress addr = mockAddr(hostname);
    assertEquals(expected,
                 SecurityUtil.getServerPrincipal(original, addr));
  }

  private InetAddress mockAddr(String reverseTo) {
    InetAddress mock = Mockito.mock(InetAddress.class);
    Mockito.doReturn(reverseTo).when(mock).getCanonicalHostName();
    return mock;
  }
  
  @Test
  public void testGetServerPrincipal() throws IOException {
    String service = "hdfs/";
    String realm = "@REALM";
    String hostname = "foohost";
    String userPrincipal = "foo@FOOREALM";
    String shouldReplace = service + SecurityUtil.HOSTNAME_PATTERN + realm;
    String replaced = service + hostname + realm;
    verify(shouldReplace, hostname, replaced);
    String shouldNotReplace = service + SecurityUtil.HOSTNAME_PATTERN + "NAME"
        + realm;
    verify(shouldNotReplace, hostname, shouldNotReplace);
    verify(userPrincipal, hostname, userPrincipal);
    // testing reverse DNS lookup doesn't happen
    InetAddress notUsed = Mockito.mock(InetAddress.class);
    assertEquals(shouldNotReplace,
                 SecurityUtil.getServerPrincipal(shouldNotReplace, notUsed));
    Mockito.verify(notUsed, Mockito.never()).getCanonicalHostName();
  }

  @Test
  public void testPrincipalsWithLowerCaseHosts() throws IOException {
    String service = "xyz/";
    String realm = "@REALM";
    String principalInConf = service + SecurityUtil.HOSTNAME_PATTERN + realm;
    String hostname = "FooHost";
    String principal =
        service + StringUtils.toLowerCase(hostname) + realm;
    verify(principalInConf, hostname, principal);
  }

  @Test
  public void testLocalHostNameForNullOrWild() throws Exception {
    String local = StringUtils.toLowerCase(SecurityUtil.getLocalHostName(null));
    assertEquals("hdfs/" + local + "@REALM",
                 SecurityUtil.getServerPrincipal("hdfs/_HOST@REALM", (String)null));
    assertEquals("hdfs/" + local + "@REALM",
                 SecurityUtil.getServerPrincipal("hdfs/_HOST@REALM", "0.0.0.0"));
  }
  
  @Test
  public void testStartsWithIncorrectSettings() throws IOException {
    Configuration conf = new Configuration();
    SecurityUtil.setAuthenticationMethod(KERBEROS, conf);
    String keyTabKey="key";
    conf.set(keyTabKey, "");
    UserGroupInformation.setConfiguration(conf);
    boolean gotException = false;
    try {
      SecurityUtil.login(conf, keyTabKey, "", "");
    } catch (IOException e) {
      // expected
      gotException=true;
    }
    assertTrue("Exception for empty keytabfile name was expected", gotException);
  }
  
  @Test
  public void testGetHostFromPrincipal() {
    assertEquals("host", 
        SecurityUtil.getHostFromPrincipal("service/host@realm"));
    assertEquals(null,
        SecurityUtil.getHostFromPrincipal("service@realm"));
  }

  @Test
  public void testBuildDTServiceName() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_TOKEN_SERVICE_USE_IP, true);
    SecurityUtil.setConfiguration(conf);
    assertEquals("127.0.0.1:123",
        SecurityUtil.buildDTServiceName(URI.create("test://LocalHost"), 123)
    );
    assertEquals("127.0.0.1:123",
        SecurityUtil.buildDTServiceName(URI.create("test://LocalHost:123"), 456)
    );
    assertEquals("127.0.0.1:123",
        SecurityUtil.buildDTServiceName(URI.create("test://127.0.0.1"), 123)
    );
    assertEquals("127.0.0.1:123",
        SecurityUtil.buildDTServiceName(URI.create("test://127.0.0.1:123"), 456)
    );
  }
  
  @Test
  public void testBuildTokenServiceSockAddr() {
    Configuration conf = new Configuration(false);
    conf.setBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_TOKEN_SERVICE_USE_IP, true);
    SecurityUtil.setConfiguration(conf);
    assertEquals("127.0.0.1:123",
        SecurityUtil.buildTokenService(new InetSocketAddress("LocalHost", 123)).toString()
    );
    assertEquals("127.0.0.1:123",
        SecurityUtil.buildTokenService(new InetSocketAddress("127.0.0.1", 123)).toString()
    );
    // what goes in, comes out
    assertEquals("127.0.0.1:123",
        SecurityUtil.buildTokenService(NetUtils.createSocketAddr("127.0.0.1", 123)).toString()
    );
  }

  @Test
  public void testGoodHostsAndPorts() {
    InetSocketAddress compare = NetUtils.createSocketAddrForHost("localhost", 123);
    runGoodCases(compare, "localhost", 123);
    runGoodCases(compare, "localhost:", 123);
    runGoodCases(compare, "localhost:123", 456);
  }
  
  void runGoodCases(InetSocketAddress addr, String host, int port) {
    assertEquals(addr, NetUtils.createSocketAddr(host, port));
    assertEquals(addr, NetUtils.createSocketAddr("hdfs://"+host, port));
    assertEquals(addr, NetUtils.createSocketAddr("hdfs://"+host+"/path", port));
  }
  
  @Test
  public void testBadHostsAndPorts() {
    runBadCases("", true);
    runBadCases(":", false);
    runBadCases("hdfs/", false);
    runBadCases("hdfs:/", false);
    runBadCases("hdfs://", true);
  }
  
  void runBadCases(String prefix, boolean validIfPosPort) {
    runBadPortPermutes(prefix, false);
    runBadPortPermutes(prefix+"*", false);
    runBadPortPermutes(prefix+"localhost", validIfPosPort);
    runBadPortPermutes(prefix+"localhost:-1", false);
    runBadPortPermutes(prefix+"localhost:-123", false);
    runBadPortPermutes(prefix+"localhost:xyz", false);
    runBadPortPermutes(prefix+"localhost/xyz", validIfPosPort);
    runBadPortPermutes(prefix+"localhost/:123", validIfPosPort);
    runBadPortPermutes(prefix+":123", false);
    runBadPortPermutes(prefix+":xyz", false);
  }

  void runBadPortPermutes(String arg, boolean validIfPosPort) {
    int ports[] = { -123, -1, 123 };
    boolean bad = false;
    try {
      NetUtils.createSocketAddr(arg);
    } catch (IllegalArgumentException e) {
      bad = true;
    } finally {
      assertTrue("should be bad: '"+arg+"'", bad);
    }
    for (int port : ports) {
      if (validIfPosPort && port > 0) continue;
      
      bad = false;
      try {
        NetUtils.createSocketAddr(arg, port);
      } catch (IllegalArgumentException e) {
        bad = true;
      } finally {
        assertTrue("should be bad: '"+arg+"' (default port:"+port+")", bad);
      }
    }
  }

  // check that the socket addr has:
  // 1) the InetSocketAddress has the correct hostname, ie. exact host/ip given
  // 2) the address is resolved, ie. has an ip
  // 3,4) the socket's InetAddress has the same hostname, and the correct ip
  // 5) the port is correct
  private void
  verifyValues(InetSocketAddress addr, String host, String ip, int port) {
    assertTrue(!addr.isUnresolved());
    // don't know what the standard resolver will return for hostname.
    // should be host for host; host or ip for ip is ambiguous
    if (!SecurityUtil.useIpForTokenService) {
      assertEquals(host, addr.getHostName());
      assertEquals(host, addr.getAddress().getHostName());
    }
    assertEquals(ip, addr.getAddress().getHostAddress());
    assertEquals(port, addr.getPort());    
  }

  // check:
  // 1) buildTokenService honors use_ip setting
  // 2) setTokenService & getService works
  // 3) getTokenServiceAddr decodes to the identical socket addr
  private void
  verifyTokenService(InetSocketAddress addr, String host, String ip, int port, boolean useIp) {
    //LOG.info("address:"+addr+" host:"+host+" ip:"+ip+" port:"+port);

    Configuration conf = new Configuration(false);
    conf.setBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_TOKEN_SERVICE_USE_IP, useIp);
    SecurityUtil.setConfiguration(conf);
    String serviceHost = useIp ? ip : StringUtils.toLowerCase(host);
    
    Token<?> token = new Token<TokenIdentifier>();
    Text service = new Text(serviceHost+":"+port);
    
    assertEquals(service, SecurityUtil.buildTokenService(addr));
    SecurityUtil.setTokenService(token, addr);
    assertEquals(service, token.getService());
    
    InetSocketAddress serviceAddr = SecurityUtil.getTokenServiceAddr(token);
    assertNotNull(serviceAddr);
    verifyValues(serviceAddr, serviceHost, ip, port);
  }

  // check:
  // 1) socket addr is created with fields set as expected
  // 2) token service with ips
  // 3) token service with the given host or ip
  private void
  verifyAddress(InetSocketAddress addr, String host, String ip, int port) {
    verifyValues(addr, host, ip, port);
    //LOG.info("test that token service uses ip");
    verifyTokenService(addr, host, ip, port, true);    
    //LOG.info("test that token service uses host");
    verifyTokenService(addr, host, ip, port, false);
  }

  // check:
  // 1-4) combinations of host and port
  // this will construct a socket addr, verify all the fields, build the
  // service to verify the use_ip setting is honored, set the token service
  // based on addr and verify the token service is set correctly, decode
  // the token service and ensure all the fields of the decoded addr match
  private void verifyServiceAddr(String host, String ip) {
    InetSocketAddress addr;
    int port = 123;

    // test host, port tuple
    //LOG.info("test tuple ("+host+","+port+")");
    addr = NetUtils.createSocketAddrForHost(host, port);
    verifyAddress(addr, host, ip, port);

    // test authority with no default port
    //LOG.info("test authority '"+host+":"+port+"'");
    addr = NetUtils.createSocketAddr(host+":"+port);
    verifyAddress(addr, host, ip, port);

    // test authority with a default port, make sure default isn't used
    //LOG.info("test authority '"+host+":"+port+"' with ignored default port");
    addr = NetUtils.createSocketAddr(host+":"+port, port+1);
    verifyAddress(addr, host, ip, port);

    // test host-only authority, using port as default port
    //LOG.info("test host:"+host+" port:"+port);
    addr = NetUtils.createSocketAddr(host, port);
    verifyAddress(addr, host, ip, port);
  }

  @Test
  public void testSocketAddrWithName() {
    String staticHost = "my";
    NetUtils.addStaticResolution(staticHost, "localhost");
    verifyServiceAddr("LocalHost", "127.0.0.1");
  }

  @Test
  public void testSocketAddrWithIP() {
    String staticHost = "127.0.0.1";
    NetUtils.addStaticResolution(staticHost, "localhost");
    verifyServiceAddr(staticHost, "127.0.0.1");
  }

  @Test
  public void testSocketAddrWithNameToStaticName() {
    String staticHost = "host1";
    NetUtils.addStaticResolution(staticHost, "localhost");
    verifyServiceAddr(staticHost, "127.0.0.1");
  }

  @Test
  public void testSocketAddrWithNameToStaticIP() {
    String staticHost = "host3";
    NetUtils.addStaticResolution(staticHost, "255.255.255.255");
    verifyServiceAddr(staticHost, "255.255.255.255");
  }

  @Test
  public void testSocketAddrWithChangeIP() {
    String staticHost = "host4";
    NetUtils.addStaticResolution(staticHost, "255.255.255.255");
    verifyServiceAddr(staticHost, "255.255.255.255");

    NetUtils.addStaticResolution(staticHost, "255.255.255.254");
    verifyServiceAddr(staticHost, "255.255.255.254");
  }

  // this is a bizarre case, but it's if a test tries to remap an ip address
  @Test
  public void testSocketAddrWithIPToStaticIP() {
    String staticHost = "1.2.3.4";
    NetUtils.addStaticResolution(staticHost, "255.255.255.255");
    verifyServiceAddr(staticHost, "255.255.255.255");
  }
  
  @Test
  public void testGetAuthenticationMethod() {
    Configuration conf = new Configuration();
    // default is simple
    conf.unset(HADOOP_SECURITY_AUTHENTICATION);
    assertEquals(SIMPLE, SecurityUtil.getAuthenticationMethod(conf));
    // simple
    conf.set(HADOOP_SECURITY_AUTHENTICATION, "simple");
    assertEquals(SIMPLE, SecurityUtil.getAuthenticationMethod(conf));
    // kerberos
    conf.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    assertEquals(KERBEROS, SecurityUtil.getAuthenticationMethod(conf));
    // bad value
    conf.set(HADOOP_SECURITY_AUTHENTICATION, "kaboom");
    String error = null;
    try {
      SecurityUtil.getAuthenticationMethod(conf);
    } catch (Exception e) {
      error = e.toString();
    }
    assertEquals("java.lang.IllegalArgumentException: " +
                 "Invalid attribute value for " +
                 HADOOP_SECURITY_AUTHENTICATION + " of kaboom", error);
  }
  
  @Test
  public void testSetAuthenticationMethod() {
    Configuration conf = new Configuration();
    // default
    SecurityUtil.setAuthenticationMethod(null, conf);
    assertEquals("simple", conf.get(HADOOP_SECURITY_AUTHENTICATION));
    // simple
    SecurityUtil.setAuthenticationMethod(SIMPLE, conf);
    assertEquals("simple", conf.get(HADOOP_SECURITY_AUTHENTICATION));
    // kerberos
    SecurityUtil.setAuthenticationMethod(KERBEROS, conf);
    assertEquals("kerberos", conf.get(HADOOP_SECURITY_AUTHENTICATION));
  }

  @Test
  public void testAuthPlainPasswordProperty() throws Exception {
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.ZK_AUTH, ZK_AUTH_VALUE);
    List<ZKAuthInfo> zkAuths = SecurityUtil.getZKAuthInfos(conf,
        CommonConfigurationKeys.ZK_AUTH);
    assertEquals(1, zkAuths.size());
    ZKAuthInfo zkAuthInfo = zkAuths.get(0);
    assertEquals("a_scheme", zkAuthInfo.getScheme());
    assertArrayEquals("a_password".getBytes(), zkAuthInfo.getAuth());
  }

  @Test
  public void testAuthPlainTextFile() throws Exception {
    Configuration conf = new Configuration();
    File passwordTxtFile = File.createTempFile(
        getClass().getSimpleName() +  ".testAuthAtPathNotation-", ".txt");
    Files.asCharSink(passwordTxtFile, StandardCharsets.UTF_8)
        .write(ZK_AUTH_VALUE);
    try {
      conf.set(CommonConfigurationKeys.ZK_AUTH,
          "@" + passwordTxtFile.getAbsolutePath());
      List<ZKAuthInfo> zkAuths = SecurityUtil.getZKAuthInfos(conf,
          CommonConfigurationKeys.ZK_AUTH);
      assertEquals(1, zkAuths.size());
      ZKAuthInfo zkAuthInfo = zkAuths.get(0);
      assertEquals("a_scheme", zkAuthInfo.getScheme());
      assertArrayEquals("a_password".getBytes(), zkAuthInfo.getAuth());
    } finally {
      boolean deleted = passwordTxtFile.delete();
      assertTrue(deleted);
    }
  }

  @Test
  public void testAuthLocalJceks() throws Exception {
    File localJceksFile = File.createTempFile(
        getClass().getSimpleName() +".testAuthLocalJceks-", ".localjceks");
    populateLocalJceksTestFile(localJceksFile.getAbsolutePath());
    try {
      String localJceksUri = "localjceks://file/" +
          localJceksFile.getAbsolutePath();
      Configuration conf = new Configuration();
      conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
          localJceksUri);
      List<ZKAuthInfo> zkAuths = SecurityUtil.getZKAuthInfos(conf,
          CommonConfigurationKeys.ZK_AUTH);
      assertEquals(1, zkAuths.size());
      ZKAuthInfo zkAuthInfo = zkAuths.get(0);
      assertEquals("a_scheme", zkAuthInfo.getScheme());
      assertArrayEquals("a_password".getBytes(), zkAuthInfo.getAuth());
    } finally {
      boolean deleted = localJceksFile.delete();
      assertTrue(deleted);
    }
  }

  private void populateLocalJceksTestFile(String path) throws IOException {
    Configuration conf = new Configuration();
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        "localjceks://file/" + path);
    CredentialProvider provider =
        CredentialProviderFactory.getProviders(conf).get(0);
    assertEquals(LocalJavaKeyStoreProvider.class.getName(),
        provider.getClass().getName());
    provider.createCredentialEntry(CommonConfigurationKeys.ZK_AUTH,
        ZK_AUTH_VALUE.toCharArray());
    provider.flush();
  }
}
