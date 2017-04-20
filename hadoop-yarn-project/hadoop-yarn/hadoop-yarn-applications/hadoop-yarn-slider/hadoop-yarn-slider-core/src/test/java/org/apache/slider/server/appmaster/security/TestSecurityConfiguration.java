/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.server.appmaster.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.slider.api.resource.Application;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.SliderXmlConfKeys;
import org.apache.slider.core.exceptions.SliderException;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test security configuration.
 */
public class TestSecurityConfiguration {

  @Test
  public void testValidLocalConfiguration() throws Throwable {
    Configuration config = new Configuration();
    config.set(CommonConfigurationKeysPublic
        .HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    Map<String, String> compOps = new HashMap<>();
    compOps.put(SliderXmlConfKeys.KEY_KEYTAB_PRINCIPAL, "test");
    compOps.put(SliderXmlConfKeys.KEY_AM_KEYTAB_LOCAL_PATH,
        "/some/local/path");
    Application application = new Application().configuration(new org.apache
        .slider.api.resource.Configuration().properties(compOps));

    SecurityConfiguration securityConfiguration =
        new SecurityConfiguration(config, application, "testCluster");
  }

  @Test
  public void testValidDistributedConfiguration() throws Throwable {
    Configuration config = new Configuration();
    config.set(CommonConfigurationKeysPublic
        .HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    Map<String, String> compOps = new HashMap<>();
    compOps.put(SliderXmlConfKeys.KEY_KEYTAB_PRINCIPAL, "test");
    compOps.put(SliderXmlConfKeys.KEY_AM_LOGIN_KEYTAB_NAME, "some.keytab");
    Application application = new Application().configuration(new org.apache
        .slider.api.resource.Configuration().properties(compOps));

    SecurityConfiguration securityConfiguration =
        new SecurityConfiguration(config, application, "testCluster");
  }

  @Test
  public void testMissingPrincipalNoLoginWithDistributedConfig() throws
      Throwable {
    Configuration config = new Configuration();
    config.set(CommonConfigurationKeysPublic
        .HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    Map<String, String> compOps = new HashMap<>();
    compOps.put(SliderXmlConfKeys.KEY_AM_LOGIN_KEYTAB_NAME, "some.keytab");
    Application application = new Application().configuration(new org.apache
        .slider.api.resource.Configuration().properties(compOps));

    try {
      SecurityConfiguration securityConfiguration =
          new SecurityConfiguration(config, application, "testCluster") {
            @Override
            protected UserGroupInformation getLoginUser() throws
                IOException {
              return null;
            }
          };
      fail("expected SliderException");
    } catch (SliderException e) {
      // expected
    }
  }

  @Test
  public void testMissingPrincipalNoLoginWithLocalConfig() throws Throwable {
    Configuration config = new Configuration();
    config.set(CommonConfigurationKeysPublic
        .HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    Map<String, String> compOps = new HashMap<>();
    compOps.put(SliderXmlConfKeys.KEY_AM_KEYTAB_LOCAL_PATH,
        "/some/local/path");
    Application application = new Application().configuration(new org.apache
        .slider.api.resource.Configuration().properties(compOps));

    try {
      SecurityConfiguration securityConfiguration =
          new SecurityConfiguration(config, application, "testCluster") {
            @Override
            protected UserGroupInformation getLoginUser() throws IOException {
              return null;
            }
          };
      fail("expected SliderException");
    } catch (SliderException e) {
      // expected
    }
  }

  @Test
  public void testBothKeytabMechanismsConfigured() throws Throwable {
    Configuration config = new Configuration();
    config.set(CommonConfigurationKeysPublic
        .HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    Map<String, String> compOps = new HashMap<>();
    compOps.put(SliderXmlConfKeys.KEY_KEYTAB_PRINCIPAL, "test");
    compOps.put(SliderXmlConfKeys.KEY_AM_KEYTAB_LOCAL_PATH,
        "/some/local/path");
    compOps.put(SliderXmlConfKeys.KEY_AM_LOGIN_KEYTAB_NAME, "some.keytab");
    Application application = new Application().configuration(new org.apache
        .slider.api.resource.Configuration().properties(compOps));

    try {
      SecurityConfiguration securityConfiguration =
          new SecurityConfiguration(config, application,
              "testCluster");
      fail("expected SliderException");
    } catch (SliderException e) {
      // expected
    }
  }

  @Test
  public void testMissingPrincipalButLoginWithDistributedConfig() throws
      Throwable {
    Configuration config = new Configuration();
    config.set(CommonConfigurationKeysPublic
        .HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    Map<String, String> compOps = new HashMap<>();
    compOps.put(SliderXmlConfKeys.KEY_AM_LOGIN_KEYTAB_NAME, "some.keytab");
    Application application = new Application().configuration(new org.apache
        .slider.api.resource.Configuration().properties(compOps));

    SecurityConfiguration securityConfiguration =
        new SecurityConfiguration(config, application, "testCluster");
  }

  @Test
  public void testMissingPrincipalButLoginWithLocalConfig() throws Throwable {
    Configuration config = new Configuration();
    config.set(CommonConfigurationKeysPublic
        .HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    Map<String, String> compOps = new HashMap<>();
    compOps.put(SliderXmlConfKeys.KEY_AM_KEYTAB_LOCAL_PATH,
        "/some/local/path");
    Application application = new Application().configuration(new org.apache
        .slider.api.resource.Configuration().properties(compOps));

    SecurityConfiguration securityConfiguration =
        new SecurityConfiguration(config, application, "testCluster");
  }

  @Test
  public void testKeypathLocationOnceLocalized() throws Throwable {
    Configuration config = new Configuration();
    config.set(CommonConfigurationKeysPublic
        .HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    Map<String, String> compOps = new HashMap<>();
    compOps.put(SliderXmlConfKeys.KEY_AM_LOGIN_KEYTAB_NAME, "some.keytab");
    Application application = new Application().configuration(new org.apache
        .slider.api.resource.Configuration().properties(compOps));

    SecurityConfiguration securityConfiguration =
        new SecurityConfiguration(config, application, "testCluster");

    assertEquals(new File(SliderKeys.KEYTAB_DIR, "some.keytab")
            .getAbsolutePath(),
        securityConfiguration.getKeytabFile().getAbsolutePath());
  }

  @Test
  public void testAMKeytabProvided() throws Throwable {
    Configuration config = new Configuration();
    Map<String, String> compOps = new HashMap<>();
    compOps.put(SliderXmlConfKeys.KEY_AM_KEYTAB_LOCAL_PATH, " ");
    Application application = new Application().configuration(new org.apache
        .slider.api.resource.Configuration().properties(compOps));

    SecurityConfiguration securityConfiguration =
        new SecurityConfiguration(config, application, "testCluster");
    assertFalse(securityConfiguration.isKeytabProvided());

    compOps.put(SliderXmlConfKeys.KEY_AM_LOGIN_KEYTAB_NAME, "");
    assertFalse(securityConfiguration.isKeytabProvided());

    compOps.put(SliderXmlConfKeys.KEY_AM_LOGIN_KEYTAB_NAME, "some.keytab");
    assertTrue(securityConfiguration.isKeytabProvided());
  }

}
