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

package org.apache.hadoop.fs.azurebfs.extensions;

import java.net.URI;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsTestWithTimeout;
import org.apache.hadoop.fs.azurebfs.security.AbfsDelegationTokenManager;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;

import static org.apache.hadoop.fs.azurebfs.extensions.KerberizedAbfsCluster.newURI;
import static org.apache.hadoop.fs.azurebfs.extensions.StubDelegationTokenManager.createToken;
import static org.apache.hadoop.fs.azurebfs.extensions.StubAbfsTokenIdentifier.decodeIdentifier;

/**
 * Test the lifecycle of custom DT managers.
 */
@SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
public class TestDTManagerLifecycle extends AbstractAbfsTestWithTimeout {

  public static final String RENEWER = "resourcemanager";

  private Configuration conf;

  public static final String ABFS
      = "abfs://testing@account.dfs.core.windows.net";

  public static final URI FSURI = newURI(ABFS);

  public static final Text OWNER = new Text("owner");

  public static final Text KIND2 = new Text("kind2");

  @BeforeEach
  public void setup() throws Exception {
    conf = StubDelegationTokenManager.useStubDTManager(new Configuration());
  }

  @AfterEach
  public void teardown() throws Exception {
  }

  /**
   * Assert that a token is of a specific kind
   * @param kind expected kind
   * @param dt token.
   */
  protected void assertTokenKind(final Text kind,
      final Token<DelegationTokenIdentifier> dt) {
    assertEquals(kind, dt.getKind(), "Token Kind");
  }

  /**
   * Test the classic lifecycle, that is: don't call bind() on the manager,
   * so that it does not attempt to bind the custom DT manager it has created.
   *
   * There'll be no canonical service name from the token manager, which
   * will trigger falling back to the default value.
   */
  @Test
  public void testClassicLifecycle() throws Throwable {
    AbfsDelegationTokenManager manager
        = new AbfsDelegationTokenManager(conf);
    StubDelegationTokenManager stub = getTokenManager(manager);

    // this is automatically inited
    assertTrue(stub.isInitialized(), "Not initialized: " + stub);
    Token<DelegationTokenIdentifier> dt = stub.getDelegationToken(RENEWER);
    assertTokenKind(StubAbfsTokenIdentifier.TOKEN_KIND, dt);

    assertNull(
       manager.getCanonicalServiceName(), "canonicalServiceName in " + stub);
    assertEquals(1, stub.getIssued(), "Issued count number in " + stub);
    StubAbfsTokenIdentifier id = decodeIdentifier(dt);
    assertEquals(1, id.getSequenceNumber(), "Sequence number in " + id);
    stub.renewDelegationToken(dt);
    assertEquals(1, stub.getRenewals(), "Renewal count in " + stub);
    stub.cancelDelegationToken(dt);
    assertEquals(1, stub.getCancellations(), "Cancel count in " + stub);
  }

  protected StubDelegationTokenManager getTokenManager(final AbfsDelegationTokenManager manager) {
    return (StubDelegationTokenManager) manager.getTokenManager();
  }

  /**
   * Instantiate through the manager, but then call direct.
   */
  @Test
  public void testBindingLifecycle() throws Throwable {
    AbfsDelegationTokenManager manager = new AbfsDelegationTokenManager(conf);
    StubDelegationTokenManager stub = getTokenManager(manager);
    assertTrue(stub.isInitialized(), "Not initialized: " + stub);
    stub.bind(FSURI, conf);
    assertEquals(FSURI, stub.getFsURI(), "URI in " + stub);
    decodeIdentifier(stub.getDelegationToken(RENEWER));
    stub.close();
    assertTrue(stub.isClosed(), "Not closed: " + stub);
    // and for resilience
    stub.close();
    assertTrue(stub.isClosed(), "Not closed: " + stub);
  }

  @Test
  public void testBindingThroughManager() throws Throwable {
    AbfsDelegationTokenManager manager = new AbfsDelegationTokenManager(conf);
    manager.bind(FSURI, conf);
    StubDelegationTokenManager stub = getTokenManager(manager);
    assertEquals(ABFS, stub.createServiceText().toString(), "Service in " + manager);
    assertEquals(FSURI, stub.getFsURI(), "Binding URI of " + stub);

    Token<DelegationTokenIdentifier> token = manager.getDelegationToken(
        RENEWER);
    assertEquals(ABFS, token.getService().toString(), "Service in " + token);
    decodeIdentifier(token);
    assertTokenKind(StubAbfsTokenIdentifier.TOKEN_KIND, token);

    // now change the token kind on the stub, verify propagation
    stub.setKind(KIND2);

    Token<DelegationTokenIdentifier> dt2 = manager.getDelegationToken("");
    assertTokenKind(KIND2, dt2);

    // change the token kind and, unless it is registered, it will not decode.
    assertNull(
       dt2.decodeIdentifier(), "Token is of unknown kind, must not decode");

    // closing the manager will close the stub too.
    manager.close();
    assertTrue(stub.isClosed(), "Not closed: " + stub);
  }

  /**
   * Instantiate a DT manager in the renewal workflow: the manager is
   * unbound; tokens must still be issued and cancelled.
   */
  @Test
  public void testRenewalThroughManager() throws Throwable {

    // create without going through the DT manager, which is of course unbound.
    Token<DelegationTokenIdentifier> dt = createToken(0, FSURI, OWNER,
        new Text(RENEWER));

    // create a DT manager in the renewer codepath.
    AbfsDelegationTokenManager manager = new AbfsDelegationTokenManager(conf);
    StubDelegationTokenManager stub = getTokenManager(manager);
    assertNull(stub.getFsURI(), "Stub should not bebound " + stub);

    StubAbfsTokenIdentifier dtId =
        (StubAbfsTokenIdentifier) dt.decodeIdentifier();
    String idStr = dtId.toString();
    assertEquals(FSURI, dtId.getUri(), "URI in " + idStr);
    assertEquals(RENEWER, dtId.getRenewer().toString(), "renewer in " + idStr);
    manager.renewDelegationToken(dt);
    assertEquals(1, stub.getRenewals(), "Renewal count in " + stub);
    manager.cancelDelegationToken(dt);
    assertEquals(1, stub.getCancellations(), "Cancel count in " + stub);

    // closing the manager will close the stub too.
    manager.close();
    assertTrue(stub.isClosed(), "Not closed: " + stub);
  }

}
