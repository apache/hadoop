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

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.minikdc.KerberosSecurityTestcase;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SaslRpcServer.QualityOfProtection;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.junit.Before;
import org.junit.Test;

/**
 * Testcase for HADOOP-13433 that verifies the logic of fixKerberosTicketOrder.
 */
public class TestFixKerberosTicketOrder extends KerberosSecurityTestcase {

  private String clientPrincipal = "client";

  private String server1Protocol = "server1";

  private String server2Protocol = "server2";

  private String host = "localhost";

  private String server1Principal = server1Protocol + "/" + host;

  private String server2Principal = server2Protocol + "/" + host;

  private File keytabFile;

  private Configuration conf = new Configuration();

  private Map<String, String> props;

  @Before
  public void setUp() throws Exception {
    keytabFile = new File(getWorkDir(), "keytab");
    getKdc().createPrincipal(keytabFile, clientPrincipal, server1Principal,
        server2Principal);
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, conf);
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.setShouldRenewImmediatelyForTests(true);
    props = new HashMap<String, String>();
    props.put(Sasl.QOP, QualityOfProtection.AUTHENTICATION.saslQop);
  }

  @Test
  public void test() throws Exception {
    UserGroupInformation ugi =
        UserGroupInformation.loginUserFromKeytabAndReturnUGI(clientPrincipal,
            keytabFile.getCanonicalPath());
    ugi.doAs(new PrivilegedExceptionAction<Void>() {

      @Override
      public Void run() throws Exception {
        SaslClient client = Sasl.createSaslClient(
            new String[] {AuthMethod.KERBEROS.getMechanismName()},
            clientPrincipal, server1Protocol, host, props, null);
        client.evaluateChallenge(new byte[0]);
        client.dispose();
        return null;
      }
    });

    Subject subject = ugi.getSubject();

    // move tgt to the last
    for (KerberosTicket ticket : subject
        .getPrivateCredentials(KerberosTicket.class)) {
      if (ticket.getServer().getName().startsWith("krbtgt")) {
        subject.getPrivateCredentials().remove(ticket);
        subject.getPrivateCredentials().add(ticket);
        break;
      }
    }
    // make sure the first ticket is not tgt
    assertFalse(
        "The first ticket is still tgt, "
            + "the implementation in jdk may have been changed, "
            + "please reconsider the problem in HADOOP-13433",
        subject.getPrivateCredentials().stream()
            .filter(c -> c instanceof KerberosTicket)
            .map(c -> ((KerberosTicket) c).getServer().getName()).findFirst()
            .get().startsWith("krbtgt"));
    // should fail as we send a service ticket instead of tgt to KDC.
    intercept(SaslException.class,
        () -> ugi.doAs(new PrivilegedExceptionAction<Void>() {

          @Override
          public Void run() throws Exception {
            SaslClient client = Sasl.createSaslClient(
                new String[] {AuthMethod.KERBEROS.getMechanismName()},
                clientPrincipal, server2Protocol, host, props, null);
            client.evaluateChallenge(new byte[0]);
            client.dispose();
            return null;
          }
        }));

    ugi.fixKerberosTicketOrder();

    // check if TGT is the first ticket after the fix.
    assertTrue("The first ticket is not tgt",
        subject.getPrivateCredentials().stream()
            .filter(c -> c instanceof KerberosTicket)
            .map(c -> ((KerberosTicket) c).getServer().getName()).findFirst()
            .get().startsWith("krbtgt"));

    // make sure we can still get new service ticket after the fix.
    ugi.doAs(new PrivilegedExceptionAction<Void>() {

      @Override
      public Void run() throws Exception {
        SaslClient client = Sasl.createSaslClient(
            new String[] {AuthMethod.KERBEROS.getMechanismName()},
            clientPrincipal, server2Protocol, host, props, null);
        client.evaluateChallenge(new byte[0]);
        client.dispose();
        return null;
      }
    });
    assertTrue("No service ticket for " + server2Protocol + " found",
        subject.getPrivateCredentials(KerberosTicket.class).stream()
            .filter(t -> t.getServer().getName().startsWith(server2Protocol))
            .findAny().isPresent());
  }

  @Test
  public void testWithDestroyedTGT() throws Exception {
    UserGroupInformation ugi =
        UserGroupInformation.loginUserFromKeytabAndReturnUGI(clientPrincipal,
            keytabFile.getCanonicalPath());
    ugi.doAs(new PrivilegedExceptionAction<Void>() {

      @Override
      public Void run() throws Exception {
        SaslClient client = Sasl.createSaslClient(
            new String[] {AuthMethod.KERBEROS.getMechanismName()},
            clientPrincipal, server1Protocol, host, props, null);
        client.evaluateChallenge(new byte[0]);
        client.dispose();
        return null;
      }
    });

    Subject subject = ugi.getSubject();

    // mark the ticket as destroyed
    for (KerberosTicket ticket : subject
        .getPrivateCredentials(KerberosTicket.class)) {
      if (ticket.getServer().getName().startsWith("krbtgt")) {
        ticket.destroy();
        break;
      }
    }

    ugi.fixKerberosTicketOrder();

    // verify that after fixing, the tgt ticket should be removed
    assertFalse("The first ticket is not tgt",
        subject.getPrivateCredentials().stream()
            .filter(c -> c instanceof KerberosTicket)
            .map(c -> ((KerberosTicket) c).getServer().getName()).findFirst()
            .isPresent());


    // should fail as we send a service ticket instead of tgt to KDC.
    intercept(SaslException.class,
        () -> ugi.doAs(new PrivilegedExceptionAction<Void>() {

          @Override
          public Void run() throws Exception {
            SaslClient client = Sasl.createSaslClient(
                new String[] {AuthMethod.KERBEROS.getMechanismName()},
                clientPrincipal, server2Protocol, host, props, null);
            client.evaluateChallenge(new byte[0]);
            client.dispose();
            return null;
          }
        }));

    // relogin to get a new ticket
    ugi.reloginFromKeytab();

    // make sure we can get new service ticket after the relogin.
    ugi.doAs(new PrivilegedExceptionAction<Void>() {

      @Override
      public Void run() throws Exception {
        SaslClient client = Sasl.createSaslClient(
            new String[] {AuthMethod.KERBEROS.getMechanismName()},
            clientPrincipal, server2Protocol, host, props, null);
        client.evaluateChallenge(new byte[0]);
        client.dispose();
        return null;
      }
    });

    assertTrue("No service ticket for " + server2Protocol + " found",
        subject.getPrivateCredentials(KerberosTicket.class).stream()
            .filter(t -> t.getServer().getName().startsWith(server2Protocol))
            .findAny().isPresent());
  }
}