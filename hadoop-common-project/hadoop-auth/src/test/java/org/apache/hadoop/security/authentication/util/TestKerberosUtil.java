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
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.kerby.kerberos.kerb.keytab.Keytab;
import org.apache.kerby.kerberos.kerb.keytab.KeytabEntry;
import org.apache.kerby.kerberos.kerb.type.KerberosTime;
import org.apache.kerby.kerberos.kerb.type.base.EncryptionKey;
import org.apache.kerby.kerberos.kerb.type.base.EncryptionType;
import org.apache.kerby.kerberos.kerb.type.base.PrincipalName;
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
  public void testGetServerPrincipal()
      throws IOException, UnknownHostException {
    String service = "TestKerberosUtil";
    String localHostname = KerberosUtil.getLocalHostName();
    String testHost = "FooBar";
    String defaultRealm = KerberosUtil.getDefaultRealmProtected();

    String atDefaultRealm;
    if (defaultRealm == null || defaultRealm.equals("")) {
      atDefaultRealm = "";
    } else {
      atDefaultRealm = "@" + defaultRealm;
    }
    // check that the test environment is as expected
    Assert.assertEquals("testGetServerPrincipal assumes localhost realm is default",
        KerberosUtil.getDomainRealm(service + "/" + localHostname.toLowerCase(Locale.US)),
        defaultRealm);
    Assert.assertEquals("testGetServerPrincipal assumes realm of testHost 'FooBar' is default",
        KerberosUtil.getDomainRealm(service + "/" + testHost.toLowerCase(Locale.US)),
        defaultRealm);

    // send null hostname
    Assert.assertEquals("When no hostname is sent",
        service + "/" + localHostname.toLowerCase(Locale.US) + atDefaultRealm,
        KerberosUtil.getServicePrincipal(service, null));
    // send empty hostname
    Assert.assertEquals("When empty hostname is sent",
        service + "/" + localHostname.toLowerCase(Locale.US) + atDefaultRealm,
        KerberosUtil.getServicePrincipal(service, ""));
    // send 0.0.0.0 hostname
    Assert.assertEquals("When 0.0.0.0 hostname is sent",
        service + "/" + localHostname.toLowerCase(Locale.US) + atDefaultRealm,
        KerberosUtil.getServicePrincipal(service, "0.0.0.0"));
    // send uppercase hostname
    Assert.assertEquals("When uppercase hostname is sent",
        service + "/" + testHost.toLowerCase(Locale.US) + atDefaultRealm,
        KerberosUtil.getServicePrincipal(service, testHost));
    // send lowercase hostname
    Assert.assertEquals("When lowercase hostname is sent",
        service + "/" + testHost.toLowerCase(Locale.US) + atDefaultRealm,
        KerberosUtil.getServicePrincipal(
            service, testHost.toLowerCase(Locale.US)));
  }

  @Test
  public void testGetPrincipalNamesMissingKeytab() {
    try {
      KerberosUtil.getPrincipalNames(testKeytab);
      Assert.fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      //expects exception
    } catch (IOException e) {
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
            EncryptionType.NONE, "samplekey1".getBytes(), kvno);
        KeytabEntry keytabEntry = new KeytabEntry(
            new PrincipalName(principal), new KerberosTime(), (byte) 1, key);
        lstEntries.add(keytabEntry);      
      }
    }
    Keytab keytab = new Keytab();
    keytab.addKeytabEntries(lstEntries);
    keytab.store(new File(testKeytab));
  }
}