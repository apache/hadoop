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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.test.GenericTestUtils;
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
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.security.KDiag.*;

public class TestKDiag extends Assert {
  private static final Logger LOG = LoggerFactory.getLogger(TestKDiag.class);

  public static final String KEYLEN = "128";
  public static final String HDFS_SITE_XML
      = "org/apache/hadoop/security/secure-hdfs-site.xml";

  @Rule
  public TestName methodName = new TestName();

  @Rule
  public Timeout testTimeout = new Timeout(30000);

  @BeforeClass
  public static void nameThread() {
    Thread.currentThread().setName("JUnit");
  }

  private static MiniKdc kdc;
  private static File workDir;
  private static File keytab;
  private static Properties securityProperties;
  private static Configuration conf;

  @BeforeClass
  public static void startMiniKdc() throws Exception {
    workDir = new File(System.getProperty("test.dir", "target"));
    securityProperties = MiniKdc.createConf();
    kdc = new MiniKdc(securityProperties, workDir);
    kdc.start();
    keytab = createKeytab("foo");
    conf = new Configuration();
    conf.set(HADOOP_SECURITY_AUTHENTICATION, "KERBEROS");
  }

  @AfterClass
  public static synchronized void stopMiniKdc() {
    if (kdc != null) {
      kdc.stop();
      kdc = null;
    }
  }

  @Before
  public void reset() {
    UserGroupInformation.reset();
  }

  private static File createKeytab(String...principals) throws Exception {
    File keytab = new File(workDir, "keytab");
    kdc.createPrincipal(keytab, principals);
    return keytab;
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

  void kdiag(String... args) throws Exception {
    KDiag.exec(conf, args);
  }

  @Test
  public void testBasicLoginFailure() throws Throwable {
    kdiagFailure(CAT_LOGIN, ARG_KEYLEN, KEYLEN);
  }

  @Test
  public void testBasicLoginSkipped() throws Throwable {
    kdiagFailure(CAT_LOGIN, ARG_KEYLEN, KEYLEN, ARG_NOLOGIN);
  }

  /**
   * This fails as the default cluster config is checked along with
   * the CLI
   * @throws Throwable
   */
  @Test
  public void testSecure() throws Throwable {
    kdiagFailure(CAT_CONFIG, ARG_KEYLEN, KEYLEN, ARG_SECURE);
  }

  @Test
  public void testNoKeytab() throws Throwable {
    kdiagFailure(CAT_KERBEROS, ARG_KEYLEN, KEYLEN,
        ARG_KEYTAB, "target/nofile");
  }

  @Test
  public void testKeytabNoPrincipal() throws Throwable {
    kdiagFailure(CAT_KERBEROS, ARG_KEYLEN, KEYLEN,
        ARG_KEYTAB, keytab.getAbsolutePath());
  }

  @Test
  public void testConfIsSecure() throws Throwable {
    Assert.assertFalse(SecurityUtil.getAuthenticationMethod(conf)
        .equals(UserGroupInformation.AuthenticationMethod.SIMPLE));
  }

  @Test
  public void testKeytabAndPrincipal() throws Throwable {
    kdiag(ARG_KEYLEN, KEYLEN,
        ARG_KEYTAB, keytab.getAbsolutePath(),
        ARG_PRINCIPAL, "foo@EXAMPLE.COM");
  }

  @Test
  public void testKerberosName() throws Throwable {
    kdiagFailure(ARG_KEYLEN, KEYLEN,
            ARG_VERIFYSHORTNAME,
            ARG_PRINCIPAL, "foo/foo/foo@BAR.COM");
  }

  @Test
  public void testShortName() throws Throwable {
    kdiag(ARG_KEYLEN, KEYLEN,
            ARG_KEYTAB, keytab.getAbsolutePath(),
            ARG_PRINCIPAL,
            ARG_VERIFYSHORTNAME,
            ARG_PRINCIPAL, "foo@EXAMPLE.COM");
  }

  @Test
  public void testFileOutput() throws Throwable {
    File f = new File("target/kdiag.txt");
    kdiag(ARG_KEYLEN, KEYLEN,
        ARG_KEYTAB, keytab.getAbsolutePath(),
        ARG_PRINCIPAL, "foo@EXAMPLE.COM",
        ARG_OUTPUT, f.getAbsolutePath());
    LOG.info("Output of {}", f);
    dump(f);
  }

  @Test
  public void testLoadResource() throws Throwable {
    kdiag(ARG_KEYLEN, KEYLEN,
        ARG_RESOURCE, HDFS_SITE_XML,
        ARG_KEYTAB, keytab.getAbsolutePath(),
        ARG_PRINCIPAL, "foo@EXAMPLE.COM");
  }

  @Test
  public void testLoadInvalidResource() throws Throwable {
    kdiagFailure(CAT_CONFIG,
        ARG_KEYLEN, KEYLEN,
        ARG_RESOURCE, "no-such-resource.xml",
        ARG_KEYTAB, keytab.getAbsolutePath(),
        ARG_PRINCIPAL, "foo@EXAMPLE.COM");
  }

  @Test
  public void testRequireJAAS() throws Throwable {
    kdiagFailure(CAT_JAAS,
        ARG_KEYLEN, KEYLEN,
        ARG_JAAS,
        ARG_KEYTAB, keytab.getAbsolutePath(),
        ARG_PRINCIPAL, "foo@EXAMPLE.COM");
  }

/*
 commented out as once JVM gets configured, it stays configured
  @Test(expected = IOException.class)
  public void testKeytabUnknownPrincipal() throws Throwable {
    kdiag(ARG_KEYLEN, KEYLEN,
        ARG_KEYTAB, keytab.getAbsolutePath(),
        ARG_PRINCIPAL, "bob@EXAMPLE.COM");
  }
*/

  /**
   * Dump any file to standard out.
   * @param file file to dump
   * @throws IOException IO problems
   */
  private void dump(File file) throws IOException {
    try (FileInputStream in = new FileInputStream(file)) {
      for (String line : IOUtils.readLines(in)) {
        LOG.info(line);
      }
    }
  }
}
