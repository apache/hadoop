/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.security.authentication.util;

import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import javax.servlet.ServletContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.test.TestingServer;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestZKSignerSecretProvider {

  private TestingServer zkServer;

  // rollover every 2 sec
  private final int timeout = 100;
  private final long rolloverFrequency = timeout / 2;

  static final Log LOG = LogFactory.getLog(TestZKSignerSecretProvider.class);
  {
    LogManager.getLogger( RolloverSignerSecretProvider.LOG.getName() ).setLevel(Level.DEBUG);
  }

  @Before
  public void setup() throws Exception {
    zkServer = new TestingServer();
  }

  @After
  public void teardown() throws Exception {
    if (zkServer != null) {
      zkServer.stop();
      zkServer.close();
    }
  }

  @Test
  // Test just one ZKSignerSecretProvider to verify that it works in the
  // simplest case
  public void testOne() throws Exception {
    // use the same seed so we can predict the RNG
    long seed = System.currentTimeMillis();
    Random rand = new Random(seed);
    byte[] secret2 = Long.toString(rand.nextLong()).getBytes();
    byte[] secret1 = Long.toString(rand.nextLong()).getBytes();
    byte[] secret3 = Long.toString(rand.nextLong()).getBytes();
    MockZKSignerSecretProvider secretProvider =
        spy(new MockZKSignerSecretProvider(seed));
    Properties config = new Properties();
    config.setProperty(
        ZKSignerSecretProvider.ZOOKEEPER_CONNECTION_STRING,
        zkServer.getConnectString());
    config.setProperty(ZKSignerSecretProvider.ZOOKEEPER_PATH,
        "/secret");
    try {
      secretProvider.init(config, getDummyServletContext(), rolloverFrequency);

      byte[] currentSecret = secretProvider.getCurrentSecret();
      byte[][] allSecrets = secretProvider.getAllSecrets();
      Assert.assertArrayEquals(secret1, currentSecret);
      Assert.assertEquals(2, allSecrets.length);
      Assert.assertArrayEquals(secret1, allSecrets[0]);
      Assert.assertNull(allSecrets[1]);
      verify(secretProvider, timeout(timeout).atLeastOnce()).rollSecret();
      secretProvider.realRollSecret();

      currentSecret = secretProvider.getCurrentSecret();
      allSecrets = secretProvider.getAllSecrets();
      Assert.assertArrayEquals(secret2, currentSecret);
      Assert.assertEquals(2, allSecrets.length);
      Assert.assertArrayEquals(secret2, allSecrets[0]);
      Assert.assertArrayEquals(secret1, allSecrets[1]);
      verify(secretProvider, timeout(timeout).atLeast(2)).rollSecret();
      secretProvider.realRollSecret();

      currentSecret = secretProvider.getCurrentSecret();
      allSecrets = secretProvider.getAllSecrets();
      Assert.assertArrayEquals(secret3, currentSecret);
      Assert.assertEquals(2, allSecrets.length);
      Assert.assertArrayEquals(secret3, allSecrets[0]);
      Assert.assertArrayEquals(secret2, allSecrets[1]);
      verify(secretProvider, timeout(timeout).atLeast(3)).rollSecret();
      secretProvider.realRollSecret();
    } finally {
      secretProvider.destroy();
    }
  }

  /**
   * A hack to test ZKSignerSecretProvider.
   * We want to test that ZKSignerSecretProvider.rollSecret() is periodically
   * called at the expected frequency, but we want to exclude the
   * race-condition.
   */
  private class MockZKSignerSecretProvider extends ZKSignerSecretProvider {
    MockZKSignerSecretProvider(long seed) {
      super(seed);
    }
    @Override
    protected synchronized void rollSecret() {
      // this is a no-op: simply used for Mockito to verify that rollSecret()
      // is periodically called at the expected frequency
    }

    public void realRollSecret() {
      // the test code manually calls ZKSignerSecretProvider.rollSecret()
      // to update the state
      super.rollSecret();
    }
  }

  @Test
  public void testMultiple1() throws Exception {
    testMultiple(1);
  }

  @Test
  public void testMultiple2() throws Exception {
    testMultiple(2);
  }

  /**
   * @param order:
   *            1: secretProviderA wins both realRollSecret races
   *            2: secretProviderA wins 1st race, B wins 2nd
   * @throws Exception
   */
  public void testMultiple(int order) throws Exception {
    long seedA = System.currentTimeMillis();
    Random rand = new Random(seedA);
    byte[] secretA2 = Long.toString(rand.nextLong()).getBytes();
    byte[] secretA1 = Long.toString(rand.nextLong()).getBytes();
    byte[] secretA3 = Long.toString(rand.nextLong()).getBytes();
    byte[] secretA4 = Long.toString(rand.nextLong()).getBytes();
    // use the same seed so we can predict the RNG
    long seedB = System.currentTimeMillis() + rand.nextLong();
    rand = new Random(seedB);
    byte[] secretB2 = Long.toString(rand.nextLong()).getBytes();
    byte[] secretB1 = Long.toString(rand.nextLong()).getBytes();
    byte[] secretB3 = Long.toString(rand.nextLong()).getBytes();
    byte[] secretB4 = Long.toString(rand.nextLong()).getBytes();
    MockZKSignerSecretProvider secretProviderA =
        spy(new MockZKSignerSecretProvider(seedA));
    MockZKSignerSecretProvider secretProviderB =
        spy(new MockZKSignerSecretProvider(seedB));
    Properties config = new Properties();
    config.setProperty(
        ZKSignerSecretProvider.ZOOKEEPER_CONNECTION_STRING,
        zkServer.getConnectString());
    config.setProperty(ZKSignerSecretProvider.ZOOKEEPER_PATH,
        "/secret");
    try {
      secretProviderA.init(config, getDummyServletContext(), rolloverFrequency);
      secretProviderB.init(config, getDummyServletContext(), rolloverFrequency);

      byte[] currentSecretA = secretProviderA.getCurrentSecret();
      byte[][] allSecretsA = secretProviderA.getAllSecrets();
      byte[] currentSecretB = secretProviderB.getCurrentSecret();
      byte[][] allSecretsB = secretProviderB.getAllSecrets();
      Assert.assertArrayEquals(secretA1, currentSecretA);
      Assert.assertArrayEquals(secretA1, currentSecretB);
      Assert.assertEquals(2, allSecretsA.length);
      Assert.assertEquals(2, allSecretsB.length);
      Assert.assertArrayEquals(secretA1, allSecretsA[0]);
      Assert.assertArrayEquals(secretA1, allSecretsB[0]);
      Assert.assertNull(allSecretsA[1]);
      Assert.assertNull(allSecretsB[1]);
      verify(secretProviderA, timeout(timeout).atLeastOnce()).rollSecret();
      verify(secretProviderB, timeout(timeout).atLeastOnce()).rollSecret();
      secretProviderA.realRollSecret();
      secretProviderB.realRollSecret();

      currentSecretA = secretProviderA.getCurrentSecret();
      allSecretsA = secretProviderA.getAllSecrets();
      Assert.assertArrayEquals(secretA2, currentSecretA);
      Assert.assertEquals(2, allSecretsA.length);
      Assert.assertArrayEquals(secretA2, allSecretsA[0]);
      Assert.assertArrayEquals(secretA1, allSecretsA[1]);

      currentSecretB = secretProviderB.getCurrentSecret();
      allSecretsB = secretProviderB.getAllSecrets();
      Assert.assertArrayEquals(secretA2, currentSecretB);
      Assert.assertEquals(2, allSecretsA.length);
      Assert.assertArrayEquals(secretA2, allSecretsB[0]);
      Assert.assertArrayEquals(secretA1, allSecretsB[1]);
      verify(secretProviderA, timeout(timeout).atLeast(2)).rollSecret();
      verify(secretProviderB, timeout(timeout).atLeastOnce()).rollSecret();

      switch (order) {
        case 1:
          secretProviderA.realRollSecret();
          secretProviderB.realRollSecret();
          secretProviderA.realRollSecret();
          secretProviderB.realRollSecret();
          break;
        case 2:
          secretProviderB.realRollSecret();
          secretProviderA.realRollSecret();
          secretProviderB.realRollSecret();
          secretProviderA.realRollSecret();
          break;
        default:
          throw new Exception("Invalid order selected");
      }

      currentSecretA = secretProviderA.getCurrentSecret();
      allSecretsA = secretProviderA.getAllSecrets();
      currentSecretB = secretProviderB.getCurrentSecret();
      allSecretsB = secretProviderB.getAllSecrets();
      Assert.assertArrayEquals(currentSecretA, currentSecretB);
      Assert.assertEquals(2, allSecretsA.length);
      Assert.assertEquals(2, allSecretsB.length);
      Assert.assertArrayEquals(allSecretsA[0], allSecretsB[0]);
      Assert.assertArrayEquals(allSecretsA[1], allSecretsB[1]);
      switch (order) {
        case 1:
          Assert.assertArrayEquals(secretA4, allSecretsA[0]);
          break;
        case 2:
          Assert.assertArrayEquals(secretB4, allSecretsA[0]);
          break;
      }
    } finally {
      secretProviderB.destroy();
      secretProviderA.destroy();
    }
  }

  private ServletContext getDummyServletContext() {
    ServletContext servletContext = mock(ServletContext.class);
    when(servletContext.getAttribute(ZKSignerSecretProvider
        .ZOOKEEPER_SIGNER_SECRET_PROVIDER_CURATOR_CLIENT_ATTRIBUTE))
        .thenReturn(null);
    return servletContext;
  }
}
