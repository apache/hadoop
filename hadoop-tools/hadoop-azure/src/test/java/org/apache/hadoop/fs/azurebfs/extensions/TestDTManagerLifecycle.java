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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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

  @Before
  public void setup() throws Exception {
    conf = StubDelegationTokenManager.useStubDTManager(new Configuration());
  }

  @After
  public void teardown() throws Exception {
  }

  /**
   * Assert that a token is of a specific kind
   * @param kind expected kind
   * @param dt token.
   */
  protected void assertTokenKind(final Text kind,
      final Token<DelegationTokenIdentifier> dt) {
    assertEquals("Token Kind",
        kind, dt.getKind());
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
    assertTrue("Not initialized: " + stub, stub.isInitialized());
    Token<DelegationTokenIdentifier> dt = stub.getDelegationToken(RENEWER);
    assertTokenKind(StubAbfsTokenIdentifier.TOKEN_KIND, dt);

    assertNull("canonicalServiceName in " + stub,
        manager.getCanonicalServiceName());
    assertEquals("Issued count number in " + stub, 1, stub.getIssued());
    StubAbfsTokenIdentifier id = decodeIdentifier(dt);
    assertEquals("Sequence number in " + id, 1, id.getSequenceNumber());
    stub.renewDelegationToken(dt);
    assertEquals("Renewal count in " + stub, 1, stub.getRenewals());
    stub.cancelDelegationToken(dt);
    assertEquals("Cancel count in " + stub, 1, stub.getCancellations());
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
    assertTrue("Not initialized: " + stub, stub.isInitialized());
    stub.bind(FSURI, conf);
    assertEquals("URI in " + stub, FSURI, stub.getFsURI());
    decodeIdentifier(stub.getDelegationToken(RENEWER));
    stub.close();
    assertTrue("Not closed: " + stub, stub.isClosed());
    // and for resilience
    stub.close();
    assertTrue("Not closed: " + stub, stub.isClosed());
  }

  @Test
  public void testBindingThroughManager() throws Throwable {
    AbfsDelegationTokenManager manager = new AbfsDelegationTokenManager(conf);
    manager.bind(FSURI, conf);
    StubDelegationTokenManager stub = getTokenManager(manager);
    assertEquals("Service in " + manager,
        ABFS, stub.createServiceText().toString());
    assertEquals("Binding URI of " + stub, FSURI, stub.getFsURI());

    Token<DelegationTokenIdentifier> token = manager.getDelegationToken(
        RENEWER);
    assertEquals("Service in " + token,
        ABFS, token.getService().toString());
    decodeIdentifier(token);
    assertTokenKind(StubAbfsTokenIdentifier.TOKEN_KIND, token);

    // now change the token kind on the stub, verify propagation
    stub.setKind(KIND2);

    Token<DelegationTokenIdentifier> dt2 = manager.getDelegationToken("");
    assertTokenKind(KIND2, dt2);

    // change the token kind and, unless it is registered, it will not decode.
    assertNull("Token is of unknown kind, must not decode",
        dt2.decodeIdentifier());

    // closing the manager will close the stub too.
    manager.close();
    assertTrue("Not closed: " + stub, stub.isClosed());
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
    assertNull("Stub should not bebound " + stub, stub.getFsURI());

    StubAbfsTokenIdentifier dtId =
        (StubAbfsTokenIdentifier) dt.decodeIdentifier();
    String idStr = dtId.toString();
    assertEquals("URI in " + idStr, FSURI, dtId.getUri());
    assertEquals("renewer in " + idStr,
        RENEWER, dtId.getRenewer().toString());
    manager.renewDelegationToken(dt);
    assertEquals("Renewal count in " + stub, 1, stub.getRenewals());
    manager.cancelDelegationToken(dt);
    assertEquals("Cancel count in " + stub, 1, stub.getCancellations());

    // closing the manager will close the stub too.
    manager.close();
    assertTrue("Not closed: " + stub, stub.isClosed());
  }

}
