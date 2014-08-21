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
import java.io.OutputStream;
import java.io.PrintStream;

import org.apache.hadoop.crypto.key.kms.server.KMS.KMSOp;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
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

  @Before
  public void setUp() {
    originalOut = System.err;
    memOut = new ByteArrayOutputStream();
    filterOut = new FilterOut(memOut);
    capturedOut = new PrintStream(filterOut);
    System.setErr(capturedOut);
    PropertyConfigurator.configure(Thread.currentThread().
        getContextClassLoader()
        .getResourceAsStream("log4j-kmsaudit.properties"));
    this.kmsAudit = new KMSAudit(1000);
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
  public void testAggregation() throws Exception {
    UserGroupInformation luser = Mockito.mock(UserGroupInformation.class);
    Mockito.when(luser.getShortUserName()).thenReturn("luser");
    kmsAudit.ok(luser, KMSOp.DECRYPT_EEK, "k1", "testmsg");
    kmsAudit.ok(luser, KMSOp.DECRYPT_EEK, "k1", "testmsg");
    kmsAudit.ok(luser, KMSOp.DECRYPT_EEK, "k1", "testmsg");
    kmsAudit.ok(luser, KMSOp.DELETE_KEY, "k1", "testmsg");
    kmsAudit.ok(luser, KMSOp.ROLL_NEW_VERSION, "k1", "testmsg");
    kmsAudit.ok(luser, KMSOp.DECRYPT_EEK, "k1", "testmsg");
    kmsAudit.ok(luser, KMSOp.DECRYPT_EEK, "k1", "testmsg");
    kmsAudit.ok(luser, KMSOp.DECRYPT_EEK, "k1", "testmsg");
    Thread.sleep(1500);
    kmsAudit.ok(luser, KMSOp.DECRYPT_EEK, "k1", "testmsg");
    Thread.sleep(1500);
    String out = getAndResetLogOutput();
    System.out.println(out);
    Assert.assertTrue(
        out.matches(
            "OK\\[op=DECRYPT_EEK, key=k1, user=luser, accessCount=1, interval=[^m]{1,4}ms\\] testmsg"
            // Not aggregated !!
            + "OK\\[op=DELETE_KEY, key=k1, user=luser\\] testmsg"
            + "OK\\[op=ROLL_NEW_VERSION, key=k1, user=luser\\] testmsg"
            // Aggregated
            + "OK\\[op=DECRYPT_EEK, key=k1, user=luser, accessCount=6, interval=[^m]{1,4}ms\\] testmsg"
            + "OK\\[op=DECRYPT_EEK, key=k1, user=luser, accessCount=1, interval=[^m]{1,4}ms\\] testmsg"));
  }

  @Test
  public void testAggregationUnauth() throws Exception {
    UserGroupInformation luser = Mockito.mock(UserGroupInformation.class);
    Mockito.when(luser.getShortUserName()).thenReturn("luser");
    kmsAudit.unauthorized(luser, KMSOp.GENERATE_EEK, "k2");
    Thread.sleep(1000);
    kmsAudit.ok(luser, KMSOp.GENERATE_EEK, "k3", "testmsg");
    kmsAudit.ok(luser, KMSOp.GENERATE_EEK, "k3", "testmsg");
    kmsAudit.ok(luser, KMSOp.GENERATE_EEK, "k3", "testmsg");
    kmsAudit.ok(luser, KMSOp.GENERATE_EEK, "k3", "testmsg");
    kmsAudit.ok(luser, KMSOp.GENERATE_EEK, "k3", "testmsg");
    kmsAudit.unauthorized(luser, KMSOp.GENERATE_EEK, "k3");
    kmsAudit.ok(luser, KMSOp.GENERATE_EEK, "k3", "testmsg");
    Thread.sleep(2000);
    String out = getAndResetLogOutput();
    System.out.println(out);
    Assert.assertTrue(
        out.matches(
            "UNAUTHORIZED\\[op=GENERATE_EEK, key=k2, user=luser\\] "
            + "OK\\[op=GENERATE_EEK, key=k3, user=luser, accessCount=1, interval=[^m]{1,4}ms\\] testmsg"
            + "OK\\[op=GENERATE_EEK, key=k3, user=luser, accessCount=5, interval=[^m]{1,4}ms\\] testmsg"
            + "UNAUTHORIZED\\[op=GENERATE_EEK, key=k3, user=luser\\] "
            + "OK\\[op=GENERATE_EEK, key=k3, user=luser, accessCount=1, interval=[^m]{1,4}ms\\] testmsg"));
  }

}
