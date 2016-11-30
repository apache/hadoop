/*
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

package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.minikdc.MiniKdc;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_TOKEN_FILES;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.security.KDiag.ARG_KEYLEN;
import static org.apache.hadoop.security.KDiag.ARG_KEYTAB;
import static org.apache.hadoop.security.KDiag.ARG_NOFAIL;
import static org.apache.hadoop.security.KDiag.ARG_NOLOGIN;
import static org.apache.hadoop.security.KDiag.ARG_PRINCIPAL;
import static org.apache.hadoop.security.KDiag.ARG_SECURE;
import static org.apache.hadoop.security.KDiag.CAT_CONFIG;
import static org.apache.hadoop.security.KDiag.CAT_KERBEROS;
import static org.apache.hadoop.security.KDiag.CAT_LOGIN;
import static org.apache.hadoop.security.KDiag.CAT_TOKEN;
import static org.apache.hadoop.security.KDiag.KerberosDiagsFailure;
import static org.apache.hadoop.security.KDiag.exec;

public class TestKDiagNoKDC extends Assert {
  private static final Logger LOG = LoggerFactory.getLogger(TestKDiagNoKDC.class);

  public static final String KEYLEN = "128";

  @Rule
  public TestName methodName = new TestName();

  @Rule
  public Timeout testTimeout = new Timeout(30000);

  @BeforeClass
  public static void nameThread() {
    Thread.currentThread().setName("JUnit");
  }

  private static Configuration conf = new Configuration();


  @Before
  public void reset() {
    UserGroupInformation.reset();
  }

  /**
   * Exec KDiag and expect a failure of a given category
   * @param category category
   * @param args args list
   * @throws Exception any unexpected exception
   */
  void kdiagFailure(String category, String ...args) throws Exception {
    try {
      int ex = exec(conf, args);
      LOG.error("Expected an exception in category {}, return code {}",
          category, ex);
    } catch (KerberosDiagsFailure e) {
      if (!e.getCategory().equals(category)) {
        LOG.error("Expected an exception in category {}, got {}",
            category, e, e);
        throw e;
      }
    }
  }

  int kdiag(String... args) throws Exception {
    return KDiag.exec(conf, args);
  }
  /**
   * Test that the core kdiag command works when there's no KDC around.
   * This test produces different outcomes on hosts where there is a default
   * KDC -it needs to work on hosts without kerberos as well as those with it.
   * @throws Throwable
   */
  @Test
  public void testKDiagStandalone() throws Throwable {
    kdiagFailure(CAT_LOGIN, ARG_KEYLEN, KEYLEN);
  }

  @Test
  public void testKDiagNoLogin() throws Throwable {
    kdiagFailure(CAT_LOGIN, ARG_KEYLEN, KEYLEN, ARG_NOLOGIN);
  }

  @Test
  public void testKDiagStandaloneNofail() throws Throwable {
    kdiag(ARG_KEYLEN, KEYLEN, ARG_NOFAIL);
  }

  @Test
  public void testKDiagUsage() throws Throwable {
    assertEquals(-1, kdiag("usage"));
  }

  @Test
  public void testTokenFile() throws Throwable {
    conf.set(HADOOP_TOKEN_FILES, "SomeNonExistentFile");
    kdiagFailure(CAT_TOKEN, ARG_KEYLEN, KEYLEN);
    conf.unset(HADOOP_TOKEN_FILES);
  }
}
