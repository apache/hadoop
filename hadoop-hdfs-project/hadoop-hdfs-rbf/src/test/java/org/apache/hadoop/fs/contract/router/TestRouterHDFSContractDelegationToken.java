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

package org.apache.hadoop.fs.contract.router;

import static org.apache.hadoop.fs.contract.router.SecurityConfUtil.initSecurity;
import static org.apache.hadoop.hdfs.server.federation.metrics.TestRBFMetrics.ROUTER_BEAN;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.AbstractFSContractTestBase;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.federation.FederationTestUtils;
import org.apache.hadoop.hdfs.server.federation.metrics.RouterMBean;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Test to verify router contracts for delegation token operations.
 */
public class TestRouterHDFSContractDelegationToken
    extends AbstractFSContractTestBase {

  @BeforeClass
  public static void createCluster() throws Exception {
    RouterHDFSContract.createCluster(false, 1, true);
  }

  @AfterClass
  public static void teardownCluster() throws IOException {
    RouterHDFSContract.destroyCluster();
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new RouterHDFSContract(conf);
  }

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testRouterDelegationToken() throws Exception {
    RouterMBean bean = FederationTestUtils.getBean(
        ROUTER_BEAN, RouterMBean.class);
    // Initially there is no token in memory
    assertEquals(0, bean.getCurrentTokensCount());
    // Generate delegation token
    Token<DelegationTokenIdentifier> token =
        (Token<DelegationTokenIdentifier>) getFileSystem()
        .getDelegationToken("router");
    assertNotNull(token);
    // Verify properties of the token
    assertEquals("HDFS_DELEGATION_TOKEN", token.getKind().toString());
    DelegationTokenIdentifier identifier = token.decodeIdentifier();
    assertNotNull(identifier);
    String owner = identifier.getOwner().toString();
    // Windows will not reverse name lookup "127.0.0.1" to "localhost".
    String host = Path.WINDOWS ? "127.0.0.1" : "localhost";
    String expectedOwner = "router/"+ host + "@EXAMPLE.COM";
    assertEquals(expectedOwner, owner);
    assertEquals("router", identifier.getRenewer().toString());
    int masterKeyId = identifier.getMasterKeyId();
    assertTrue(masterKeyId > 0);
    int sequenceNumber = identifier.getSequenceNumber();
    assertTrue(sequenceNumber > 0);
    long existingMaxTime = token.decodeIdentifier().getMaxDate();
    assertTrue(identifier.getMaxDate() >= identifier.getIssueDate());
    // one token is expected after the generation
    assertEquals(1, bean.getCurrentTokensCount());

    // Renew delegation token
    long expiryTime = token.renew(initSecurity());
    assertNotNull(token);
    assertEquals(existingMaxTime, token.decodeIdentifier().getMaxDate());
    // Expiry time after renewal should never exceed max time of the token.
    assertTrue(expiryTime <= existingMaxTime);
    // Renewal should retain old master key id and sequence number
    identifier = token.decodeIdentifier();
    assertEquals(identifier.getMasterKeyId(), masterKeyId);
    assertEquals(identifier.getSequenceNumber(), sequenceNumber);
    assertEquals(1, bean.getCurrentTokensCount());

    // Cancel delegation token
    token.cancel(initSecurity());
    assertEquals(0, bean.getCurrentTokensCount());

    // Renew a cancelled token
    exceptionRule.expect(SecretManager.InvalidToken.class);
    token.renew(initSecurity());
  }
}
