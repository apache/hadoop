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
package org.apache.hadoop.security;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.security.auth.kerberos.KerberosTicket;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.minikdc.KerberosSecurityTestcase;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SaslRpcServer.QualityOfProtection;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.junit.Before;
import org.junit.Test;

/**
 * Testcase for HADOOP-13433 that confirms that tgt will always be the first
 * ticket after relogin.
 */
public class TestRaceWhenRelogin extends KerberosSecurityTestcase {

  private int numThreads = 10;

  private String clientPrincipal = "client";

  private String serverProtocol = "server";

  private String[] serverProtocols;

  private String host = "localhost";

  private String serverPrincipal = serverProtocol + "/" + host;

  private String[] serverPrincipals;

  private File keytabFile;

  private Configuration conf = new Configuration();

  private Map<String, String> props;

  private UserGroupInformation ugi;

  @Before
  public void setUp() throws Exception {
    keytabFile = new File(getWorkDir(), "keytab");
    serverProtocols = new String[numThreads];
    serverPrincipals = new String[numThreads];
    for (int i = 0; i < numThreads; i++) {
      serverProtocols[i] = serverProtocol + i;
      serverPrincipals[i] = serverProtocols[i] + "/" + host;
    }
    String[] principals =
        Arrays.copyOf(serverPrincipals, serverPrincipals.length + 2);
    principals[numThreads] = serverPrincipal;
    principals[numThreads + 1] = clientPrincipal;
    getKdc().createPrincipal(keytabFile, principals);
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, conf);
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.setShouldRenewImmediatelyForTests(true);
    props = new HashMap<String, String>();
    props.put(Sasl.QOP, QualityOfProtection.AUTHENTICATION.saslQop);
    ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(clientPrincipal,
        keytabFile.getAbsolutePath());
  }

  private void relogin(AtomicBoolean pass) {
    for (int i = 0; i < 100; i++) {
      try {
        ugi.reloginFromKeytab();
      } catch (IOException e) {
      }
      KerberosTicket tgt = ugi.getSubject().getPrivateCredentials().stream()
          .filter(c -> c instanceof KerberosTicket).map(c -> (KerberosTicket) c)
          .findFirst().get();
      if (!tgt.getServer().getName().startsWith("krbtgt")) {
        pass.set(false);
        return;
      }
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
      }
    }
  }

  private void getServiceTicket(AtomicBoolean running, String serverProtocol) {
    while (running.get()) {
      try {
        ugi.doAs(new PrivilegedExceptionAction<Void>() {

          @Override
          public Void run() throws Exception {
            SaslClient client = Sasl.createSaslClient(
                new String[] {AuthMethod.KERBEROS.getMechanismName()},
                clientPrincipal, serverProtocol, host, props, null);
            client.evaluateChallenge(new byte[0]);
            client.dispose();
            return null;
          }
        });
      } catch (Exception e) {
      }
      try {
        Thread.sleep(ThreadLocalRandom.current().nextInt(100));
      } catch (InterruptedException e) {
      }
    }
  }

  @Test
  public void test() throws InterruptedException, IOException {
    AtomicBoolean pass = new AtomicBoolean(true);
    Thread reloginThread = new Thread(() -> relogin(pass), "Relogin");

    AtomicBoolean running = new AtomicBoolean(true);
    Thread[] getServiceTicketThreads = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      String serverProtocol = serverProtocols[i];
      getServiceTicketThreads[i] =
          new Thread(() -> getServiceTicket(running, serverProtocol),
              "GetServiceTicket-" + i);
    }
    for (Thread getServiceTicketThread : getServiceTicketThreads) {
      getServiceTicketThread.start();
    }
    reloginThread.start();
    reloginThread.join();
    running.set(false);
    for (Thread getServiceTicketThread : getServiceTicketThreads) {
      getServiceTicketThread.join();
    }
    assertTrue("tgt is not the first ticket after relogin", pass.get());
  }
}
