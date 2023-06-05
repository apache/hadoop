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

import java.nio.charset.Charset;
import java.util.Properties;
import java.util.Random;
import javax.servlet.ServletContext;

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

  // rollover every 50 msec
  private final int timeout = 100;
  private final long rolloverFrequency = timeout / 2;

  {
    LogManager.getLogger(
        RolloverSignerSecretProvider.LOG.getName()).setLevel(Level.DEBUG);
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
    // Use the same seed and a "plain" Random so we can predict the RNG
    long seed = System.currentTimeMillis();
    Random rand = new Random(seed);
    byte[] secret2 = generateNewSecret(rand);
    byte[] secret1 = generateNewSecret(rand);
    byte[] secret3 = generateNewSecret(rand);
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
   * race-condition and not take a long time to run the test.
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
  // HADOOP-14246 increased the length of the secret from 160 bits to 256 bits.
  // This test verifies that the upgrade goes smoothly.
  public void testUpgradeChangeSecretLength() throws Exception {
    // Use the same seed and a "plain" Random so we can predict the RNG
    long seed = System.currentTimeMillis();
    Random rand = new Random(seed);
    byte[] secret2 = Long.toString(rand.nextLong())
        .getBytes(Charset.forName("UTF-8"));
    byte[] secret1 = Long.toString(rand.nextLong())
        .getBytes(Charset.forName("UTF-8"));
    byte[] secret3 = Long.toString(rand.nextLong())
        .getBytes(Charset.forName("UTF-8"));
    rand = new Random(seed);
    // Secrets 4 and 5 get thrown away by ZK when the new secret provider tries
    // to init
    byte[] secret4 = generateNewSecret(rand);
    byte[] secret5 = generateNewSecret(rand);
    byte[] secret6 = generateNewSecret(rand);
    byte[] secret7 = generateNewSecret(rand);
    // Initialize the znode data with the old secret length
    MockZKSignerSecretProvider oldSecretProvider =
        spy(new OldMockZKSignerSecretProvider(seed));
    Properties config = new Properties();
    config.setProperty(
        ZKSignerSecretProvider.ZOOKEEPER_CONNECTION_STRING,
        zkServer.getConnectString());
    config.setProperty(ZKSignerSecretProvider.ZOOKEEPER_PATH,
        "/secret");
    try {
      oldSecretProvider.init(config, getDummyServletContext(),
          rolloverFrequency);

      byte[] currentSecret = oldSecretProvider.getCurrentSecret();
      byte[][] allSecrets = oldSecretProvider.getAllSecrets();
      Assert.assertArrayEquals(secret1, currentSecret);
      Assert.assertEquals(2, allSecrets.length);
      Assert.assertArrayEquals(secret1, allSecrets[0]);
      Assert.assertNull(allSecrets[1]);
      oldSecretProvider.realRollSecret();

      currentSecret = oldSecretProvider.getCurrentSecret();
      allSecrets = oldSecretProvider.getAllSecrets();
      Assert.assertArrayEquals(secret2, currentSecret);
      Assert.assertEquals(2, allSecrets.length);
      Assert.assertArrayEquals(secret2, allSecrets[0]);
      Assert.assertArrayEquals(secret1, allSecrets[1]);
    } finally {
      oldSecretProvider.destroy();
    }
    // Now use a ZKSignerSecretProvider with the newer length
    MockZKSignerSecretProvider newSecretProvider =
        spy(new MockZKSignerSecretProvider(seed));
    try {
      newSecretProvider.init(config, getDummyServletContext(),
          rolloverFrequency);

      byte[] currentSecret = newSecretProvider.getCurrentSecret();
      byte[][] allSecrets = newSecretProvider.getAllSecrets();
      Assert.assertArrayEquals(secret2, currentSecret);
      Assert.assertEquals(2, allSecrets.length);
      Assert.assertArrayEquals(secret2, allSecrets[0]);
      Assert.assertArrayEquals(secret1, allSecrets[1]);
      newSecretProvider.realRollSecret();

      currentSecret = newSecretProvider.getCurrentSecret();
      allSecrets = newSecretProvider.getAllSecrets();
      Assert.assertArrayEquals(secret3, currentSecret);
      Assert.assertEquals(2, allSecrets.length);
      Assert.assertArrayEquals(secret3, allSecrets[0]);
      Assert.assertArrayEquals(secret2, allSecrets[1]);
      newSecretProvider.realRollSecret();

      currentSecret = newSecretProvider.getCurrentSecret();
      allSecrets = newSecretProvider.getAllSecrets();
      Assert.assertArrayEquals(secret6, currentSecret);
      Assert.assertEquals(2, allSecrets.length);
      Assert.assertArrayEquals(secret6, allSecrets[0]);
      Assert.assertArrayEquals(secret3, allSecrets[1]);
      newSecretProvider.realRollSecret();

      currentSecret = newSecretProvider.getCurrentSecret();
      allSecrets = newSecretProvider.getAllSecrets();
      Assert.assertArrayEquals(secret7, currentSecret);
      Assert.assertEquals(2, allSecrets.length);
      Assert.assertArrayEquals(secret7, allSecrets[0]);
      Assert.assertArrayEquals(secret6, allSecrets[1]);
    } finally {
      newSecretProvider.destroy();
    }
  }

  /**
   * A version of {@link MockZKSignerSecretProvider} that uses the old way of
   * generating secrets (160 bit long).
   */
  private class OldMockZKSignerSecretProvider
      extends MockZKSignerSecretProvider {
    private Random rand;
    OldMockZKSignerSecretProvider(long seed) {
      super(seed);
      rand = new Random(seed);
    }

    @Override
    protected byte[] generateRandomSecret() {
      return Long.toString(rand.nextLong()).getBytes(Charset.forName("UTF-8"));
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
    // Use the same seed and a "plain" Random so we can predict the RNG
    long seedA = System.currentTimeMillis();
    Random rand = new Random(seedA);
    byte[] secretA2 = generateNewSecret(rand);
    byte[] secretA1 = generateNewSecret(rand);
    byte[] secretA3 = generateNewSecret(rand);
    byte[] secretA4 = generateNewSecret(rand);
    long seedB = System.currentTimeMillis() + rand.nextLong();
    rand = new Random(seedB);
    byte[] secretB2 = generateNewSecret(rand);
    byte[] secretB1 = generateNewSecret(rand);
    byte[] secretB3 = generateNewSecret(rand);
    byte[] secretB4 = generateNewSecret(rand);
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

  private byte[] generateNewSecret(Random rand) {
    byte[] secret = new byte[32];
    rand.nextBytes(secret);
    return secret;
  }
}
