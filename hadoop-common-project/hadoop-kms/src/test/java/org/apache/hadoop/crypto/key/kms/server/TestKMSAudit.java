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
package org.apache.hadoop.crypto.key.kms.server;

import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.kms.server.KMS.KMSOp;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.Whitebox;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;

public class TestKMSAudit {

  private PrintStream originalOut;
  private ByteArrayOutputStream memOut;
  private FilterOut filterOut;
  private PrintStream capturedOut;
  
  private KMSAudit kmsAudit;

  private static class FilterOut extends FilterOutputStream {
    public FilterOut(OutputStream out) {
      super(out);
    }

    public void setOutputStream(OutputStream out) {
      this.out = out;
    }
  }

  @Rule
  public final Timeout testTimeout = new Timeout(180000);

  @Before
  public void setUp() throws IOException {
    originalOut = System.err;
    memOut = new ByteArrayOutputStream();
    filterOut = new FilterOut(memOut);
    capturedOut = new PrintStream(filterOut);
    System.setErr(capturedOut);
    InputStream is =
        ThreadUtil.getResourceAsStream("log4j-kmsaudit.properties");
    PropertyConfigurator.configure(is);
    IOUtils.closeStream(is);
    Configuration conf = new Configuration();
    this.kmsAudit = new KMSAudit(conf);
  }

  @After
  public void cleanUp() {
    System.setErr(originalOut);
    LogManager.resetConfiguration();
    kmsAudit.shutdown();
  }

  private String getAndResetLogOutput() {
    capturedOut.flush();
    String logOutput = new String(memOut.toByteArray());
    memOut = new ByteArrayOutputStream();
    filterOut.setOutputStream(memOut);
    return logOutput;
  }

  @Test
  @SuppressWarnings("checkstyle:linelength")
  public void testAggregation() throws Exception {
    UserGroupInformation luser = Mockito.mock(UserGroupInformation.class);
    Mockito.when(luser.getShortUserName()).thenReturn("luser");
    kmsAudit.ok(luser, KMSOp.DECRYPT_EEK, "k1", "testmsg");
    kmsAudit.ok(luser, KMSOp.DECRYPT_EEK, "k1", "testmsg");
    kmsAudit.ok(luser, KMSOp.DECRYPT_EEK, "k1", "testmsg");
    kmsAudit.ok(luser, KMSOp.DELETE_KEY, "k1", "testmsg");
    kmsAudit.ok(luser, KMSOp.ROLL_NEW_VERSION, "k1", "testmsg");
    kmsAudit.ok(luser, KMSOp.INVALIDATE_CACHE, "k1", "testmsg");
    kmsAudit.ok(luser, KMSOp.DECRYPT_EEK, "k1", "testmsg");
    kmsAudit.ok(luser, KMSOp.DECRYPT_EEK, "k1", "testmsg");
    kmsAudit.ok(luser, KMSOp.DECRYPT_EEK, "k1", "testmsg");
    kmsAudit.evictCacheForTesting();
    kmsAudit.ok(luser, KMSOp.DECRYPT_EEK, "k1", "testmsg");
    kmsAudit.evictCacheForTesting();
    kmsAudit.ok(luser, KMSOp.REENCRYPT_EEK, "k1", "testmsg");
    kmsAudit.ok(luser, KMSOp.REENCRYPT_EEK, "k1", "testmsg");
    kmsAudit.ok(luser, KMSOp.REENCRYPT_EEK, "k1", "testmsg");
    kmsAudit.evictCacheForTesting();
    kmsAudit.ok(luser, KMSOp.REENCRYPT_EEK_BATCH, "k1", "testmsg");
    kmsAudit.ok(luser, KMSOp.REENCRYPT_EEK_BATCH, "k1", "testmsg");
    kmsAudit.evictCacheForTesting();
    String out = getAndResetLogOutput();
    System.out.println(out);
    Assert.assertTrue(
        out.matches(
            "OK\\[op=DECRYPT_EEK, key=k1, user=luser, accessCount=1, interval=[^m]{1,4}ms\\] testmsg"
            // Not aggregated !!
            + "OK\\[op=DELETE_KEY, key=k1, user=luser\\] testmsg"
            + "OK\\[op=ROLL_NEW_VERSION, key=k1, user=luser\\] testmsg"
            + "OK\\[op=INVALIDATE_CACHE, key=k1, user=luser\\] testmsg"
            // Aggregated
            + "OK\\[op=DECRYPT_EEK, key=k1, user=luser, accessCount=6, interval=[^m]{1,4}ms\\] testmsg"
            + "OK\\[op=DECRYPT_EEK, key=k1, user=luser, accessCount=1, interval=[^m]{1,4}ms\\] testmsg"
            + "OK\\[op=REENCRYPT_EEK, key=k1, user=luser, accessCount=1, interval=[^m]{1,4}ms\\] testmsg"
            + "OK\\[op=REENCRYPT_EEK, key=k1, user=luser, accessCount=3, interval=[^m]{1,4}ms\\] testmsg"
            + "OK\\[op=REENCRYPT_EEK_BATCH, key=k1, user=luser\\] testmsg"
            + "OK\\[op=REENCRYPT_EEK_BATCH, key=k1, user=luser\\] testmsg"));
  }

  @Test
  @SuppressWarnings("checkstyle:linelength")
  public void testAggregationUnauth() throws Exception {
    UserGroupInformation luser = Mockito.mock(UserGroupInformation.class);
    Mockito.when(luser.getShortUserName()).thenReturn("luser");
    kmsAudit.unauthorized(luser, KMSOp.GENERATE_EEK, "k2");
    kmsAudit.evictCacheForTesting();
    kmsAudit.ok(luser, KMSOp.GENERATE_EEK, "k3", "testmsg");
    kmsAudit.ok(luser, KMSOp.GENERATE_EEK, "k3", "testmsg");
    kmsAudit.ok(luser, KMSOp.GENERATE_EEK, "k3", "testmsg");
    kmsAudit.ok(luser, KMSOp.GENERATE_EEK, "k3", "testmsg");
    kmsAudit.ok(luser, KMSOp.GENERATE_EEK, "k3", "testmsg");
    kmsAudit.unauthorized(luser, KMSOp.GENERATE_EEK, "k3");
    // wait a bit so the UNAUTHORIZED-triggered cache invalidation happens.
    Thread.sleep(1000);
    kmsAudit.ok(luser, KMSOp.GENERATE_EEK, "k3", "testmsg");
    kmsAudit.evictCacheForTesting();
    String out = getAndResetLogOutput();
    System.out.println(out);

    // The UNAUTHORIZED will trigger cache invalidation, which then triggers
    // the aggregated OK (accessCount=5). But the order of the UNAUTHORIZED and
    // the aggregated OK is arbitrary - no correctness concerns, but flaky here.
    Assert.assertTrue(out.matches(
        "UNAUTHORIZED\\[op=GENERATE_EEK, key=k2, user=luser\\] "
            + "OK\\[op=GENERATE_EEK, key=k3, user=luser, accessCount=1, interval=[^m]{1,4}ms\\] testmsg"
            + "OK\\[op=GENERATE_EEK, key=k3, user=luser, accessCount=5, interval=[^m]{1,4}ms\\] testmsg"
            + "UNAUTHORIZED\\[op=GENERATE_EEK, key=k3, user=luser\\] "
            + "OK\\[op=GENERATE_EEK, key=k3, user=luser, accessCount=1, interval=[^m]{1,4}ms\\] testmsg")
        || out.matches(
        "UNAUTHORIZED\\[op=GENERATE_EEK, key=k2, user=luser\\] "
            + "OK\\[op=GENERATE_EEK, key=k3, user=luser, accessCount=1, interval=[^m]{1,4}ms\\] testmsg"
            + "UNAUTHORIZED\\[op=GENERATE_EEK, key=k3, user=luser\\] "
            + "OK\\[op=GENERATE_EEK, key=k3, user=luser, accessCount=5, interval=[^m]{1,4}ms\\] testmsg"
            + "OK\\[op=GENERATE_EEK, key=k3, user=luser, accessCount=1, interval=[^m]{1,4}ms\\] testmsg"));
  }

  @Test
  @SuppressWarnings("checkstyle:linelength")
  public void testAuditLogFormat() throws Exception {
    UserGroupInformation luser = Mockito.mock(UserGroupInformation.class);
    Mockito.when(luser.getShortUserName()).thenReturn("luser");
    kmsAudit.ok(luser, KMSOp.GENERATE_EEK, "k4", "testmsg");
    kmsAudit.ok(luser, KMSOp.GENERATE_EEK, "testmsg");
    kmsAudit.evictCacheForTesting();
    kmsAudit.unauthorized(luser, KMSOp.DECRYPT_EEK, "k4");
    kmsAudit.error(luser, "method", "url", "testmsg");
    kmsAudit.unauthenticated("remotehost", "method", "url", "testmsg");
    String out = getAndResetLogOutput();
    System.out.println(out);
    Assert.assertTrue(out.matches(
        "OK\\[op=GENERATE_EEK, key=k4, user=luser, accessCount=1, interval=[^m]{1,4}ms\\] testmsg"
            + "OK\\[op=GENERATE_EEK, user=luser\\] testmsg"
            + "OK\\[op=GENERATE_EEK, key=k4, user=luser, accessCount=1, interval=[^m]{1,4}ms\\] testmsg"
            + "UNAUTHORIZED\\[op=DECRYPT_EEK, key=k4, user=luser\\] "
            + "ERROR\\[user=luser\\] Method:'method' Exception:'testmsg'"
            + "UNAUTHENTICATED RemoteHost:remotehost Method:method URL:url ErrorMsg:'testmsg'"));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testInitAuditLoggers() throws Exception {
    // Default should be the simple logger
    List<KMSAuditLogger> loggers = (List<KMSAuditLogger>) Whitebox
        .getInternalState(kmsAudit, "auditLoggers");
    Assert.assertEquals(1, loggers.size());
    Assert.assertEquals(SimpleKMSAuditLogger.class, loggers.get(0).getClass());

    // Explicitly configure the simple logger. Duplicates are ignored.
    final Configuration conf = new Configuration();
    conf.set(KMSConfiguration.KMS_AUDIT_LOGGER_KEY,
        SimpleKMSAuditLogger.class.getName() + ", "
            + SimpleKMSAuditLogger.class.getName());
    final KMSAudit audit = new KMSAudit(conf);
    loggers =
        (List<KMSAuditLogger>) Whitebox.getInternalState(audit, "auditLoggers");
    Assert.assertEquals(1, loggers.size());
    Assert.assertEquals(SimpleKMSAuditLogger.class, loggers.get(0).getClass());

    // If any loggers unable to load, init should fail.
    conf.set(KMSConfiguration.KMS_AUDIT_LOGGER_KEY,
        SimpleKMSAuditLogger.class.getName() + ",unknown");
    try {
      new KMSAudit(conf);
      Assert.fail("loggers configured but invalid, init should fail.");
    } catch (Exception ex) {
      GenericTestUtils
          .assertExceptionContains(KMSConfiguration.KMS_AUDIT_LOGGER_KEY, ex);
    }
  }
}
