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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.InetAddress;

import javax.security.auth.kerberos.KerberosPrincipal;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.mockito.Mockito;

public class TestSecurityUtil {
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
        (new KerberosPrincipal("")));
    assertFalse(SecurityUtil.isTGSPrincipal
        (new KerberosPrincipal("krbtgt/hello")));
    assertFalse(SecurityUtil.isTGSPrincipal
        (new KerberosPrincipal("/@")));
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
  public void testLocalHostNameForNullOrWild() throws Exception {
    String local = SecurityUtil.getLocalHostName();
    assertEquals("hdfs/" + local + "@REALM",
                 SecurityUtil.getServerPrincipal("hdfs/_HOST@REALM", (String)null));
    assertEquals("hdfs/" + local + "@REALM",
                 SecurityUtil.getServerPrincipal("hdfs/_HOST@REALM", "0.0.0.0"));
  }
  
  @Test
  public void testStartsWithIncorrectSettings() throws IOException {
    Configuration conf = new Configuration();
    conf.set(
        org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
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
}
