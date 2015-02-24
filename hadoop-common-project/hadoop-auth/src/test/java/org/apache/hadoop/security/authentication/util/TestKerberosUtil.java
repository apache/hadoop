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
package org.apache.hadoop.security.authentication.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.directory.server.kerberos.shared.keytab.Keytab;
import org.apache.directory.server.kerberos.shared.keytab.KeytabEntry;
import org.apache.directory.shared.kerberos.KerberosTime;
import org.apache.directory.shared.kerberos.codec.types.EncryptionType;
import org.apache.directory.shared.kerberos.components.EncryptionKey;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestKerberosUtil {
  static String testKeytab = "test.keytab";
  static String[] testPrincipals = new String[]{
      "HTTP@testRealm",
      "test/testhost@testRealm",
      "HTTP/testhost@testRealm",
      "HTTP1/testhost@testRealm",
      "HTTP/testhostanother@testRealm"
  };

  @After
  public void deleteKeytab() {
    File keytabFile = new File(testKeytab);
    if (keytabFile.exists()){
      keytabFile.delete();
    }
  }

  @Test
  public void testGetServerPrincipal() throws IOException {
    String service = "TestKerberosUtil";
    String localHostname = KerberosUtil.getLocalHostName();
    String testHost = "FooBar";

    // send null hostname
    Assert.assertEquals("When no hostname is sent",
        service + "/" + localHostname.toLowerCase(),
        KerberosUtil.getServicePrincipal(service, null));
    // send empty hostname
    Assert.assertEquals("When empty hostname is sent",
        service + "/" + localHostname.toLowerCase(),
        KerberosUtil.getServicePrincipal(service, ""));
    // send 0.0.0.0 hostname
    Assert.assertEquals("When 0.0.0.0 hostname is sent",
        service + "/" + localHostname.toLowerCase(),
        KerberosUtil.getServicePrincipal(service, "0.0.0.0"));
    // send uppercase hostname
    Assert.assertEquals("When uppercase hostname is sent",
        service + "/" + testHost.toLowerCase(),
        KerberosUtil.getServicePrincipal(service, testHost));
    // send lowercase hostname
    Assert.assertEquals("When lowercase hostname is sent",
        service + "/" + testHost.toLowerCase(),
        KerberosUtil.getServicePrincipal(service, testHost.toLowerCase()));
  }
  
  @Test
  public void testGetPrincipalNamesMissingKeytab() {
    try {
      KerberosUtil.getPrincipalNames(testKeytab);
      Assert.fail("Exception should have been thrown");
    } catch (IOException e) {
      //expects exception
    }
  }

  @Test
  public void testGetPrincipalNamesMissingPattern() throws IOException {
    createKeyTab(testKeytab, new String[]{"test/testhost@testRealm"});
    try {
      KerberosUtil.getPrincipalNames(testKeytab, null);
      Assert.fail("Exception should have been thrown");
    } catch (Exception e) {
      //expects exception
    }
  }

  @Test
  public void testGetPrincipalNamesFromKeytab() throws IOException {
    createKeyTab(testKeytab, testPrincipals); 
    // read all principals in the keytab file
    String[] principals = KerberosUtil.getPrincipalNames(testKeytab);
    Assert.assertNotNull("principals cannot be null", principals);
    
    int expectedSize = 0;
    List<String> principalList = Arrays.asList(principals);
    for (String principal : testPrincipals) {
      Assert.assertTrue("missing principal "+principal,
          principalList.contains(principal));
      expectedSize++;
    }
    Assert.assertEquals(expectedSize, principals.length);
  }
  
  @Test
  public void testGetPrincipalNamesFromKeytabWithPattern() throws IOException {
    createKeyTab(testKeytab, testPrincipals); 
    // read the keytab file
    // look for principals with HTTP as the first part
    Pattern httpPattern = Pattern.compile("HTTP/.*");
    String[] httpPrincipals =
        KerberosUtil.getPrincipalNames(testKeytab, httpPattern);
    Assert.assertNotNull("principals cannot be null", httpPrincipals);
    
    int expectedSize = 0;
    List<String> httpPrincipalList = Arrays.asList(httpPrincipals);
    for (String principal : testPrincipals) {
      if (httpPattern.matcher(principal).matches()) {
        Assert.assertTrue("missing principal "+principal,
            httpPrincipalList.contains(principal));
        expectedSize++;
      }
    }
    Assert.assertEquals(expectedSize, httpPrincipals.length);
  }
  
  private void createKeyTab(String fileName, String[] principalNames)
      throws IOException {
    //create a test keytab file
    List<KeytabEntry> lstEntries = new ArrayList<KeytabEntry>();
    for (String principal : principalNames){
      // create 3 versions of the key to ensure methods don't return
      // duplicate principals
      for (int kvno=1; kvno <= 3; kvno++) {
        EncryptionKey key = new EncryptionKey(
            EncryptionType.UNKNOWN, "samplekey1".getBytes(), kvno);
        KeytabEntry keytabEntry = new KeytabEntry(
            principal, 1 , new KerberosTime(), (byte) 1, key);
        lstEntries.add(keytabEntry);      
      }
    }
    Keytab keytab = Keytab.getInstance();
    keytab.setEntries(lstEntries);
    keytab.write(new File(testKeytab));
  }
}