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
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestZKSignerSecretProvider {

  private TestingServer zkServer;

  // rollover every 2 sec
  private final int timeout = 4000;
  private final long rolloverFrequency = timeout / 2;

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
    ZKSignerSecretProvider secretProvider =
        spy(new ZKSignerSecretProvider(seed));
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
      verify(secretProvider, timeout(timeout).times(1)).rollSecret();

      currentSecret = secretProvider.getCurrentSecret();
      allSecrets = secretProvider.getAllSecrets();
      Assert.assertArrayEquals(secret2, currentSecret);
      Assert.assertEquals(2, allSecrets.length);
      Assert.assertArrayEquals(secret2, allSecrets[0]);
      Assert.assertArrayEquals(secret1, allSecrets[1]);
      verify(secretProvider, timeout(timeout).times(2)).rollSecret();

      currentSecret = secretProvider.getCurrentSecret();
      allSecrets = secretProvider.getAllSecrets();
      Assert.assertArrayEquals(secret3, currentSecret);
      Assert.assertEquals(2, allSecrets.length);
      Assert.assertArrayEquals(secret3, allSecrets[0]);
      Assert.assertArrayEquals(secret2, allSecrets[1]);
      verify(secretProvider, timeout(timeout).times(3)).rollSecret();
    } finally {
      secretProvider.destroy();
    }
  }

  @Test
  public void testMultipleInit() throws Exception {
    // use the same seed so we can predict the RNG
    long seedA = System.currentTimeMillis();
    Random rand = new Random(seedA);
    byte[] secretA2 = Long.toString(rand.nextLong()).getBytes();
    byte[] secretA1 = Long.toString(rand.nextLong()).getBytes();
    // use the same seed so we can predict the RNG
    long seedB = System.currentTimeMillis() + rand.nextLong();
    rand = new Random(seedB);
    byte[] secretB2 = Long.toString(rand.nextLong()).getBytes();
    byte[] secretB1 = Long.toString(rand.nextLong()).getBytes();
    // use the same seed so we can predict the RNG
    long seedC = System.currentTimeMillis() + rand.nextLong();
    rand = new Random(seedC);
    byte[] secretC2 = Long.toString(rand.nextLong()).getBytes();
    byte[] secretC1 = Long.toString(rand.nextLong()).getBytes();
    ZKSignerSecretProvider secretProviderA =
        spy(new ZKSignerSecretProvider(seedA));
    ZKSignerSecretProvider secretProviderB =
        spy(new ZKSignerSecretProvider(seedB));
    ZKSignerSecretProvider secretProviderC =
        spy(new ZKSignerSecretProvider(seedC));
    Properties config = new Properties();
    config.setProperty(
        ZKSignerSecretProvider.ZOOKEEPER_CONNECTION_STRING,
        zkServer.getConnectString());
    config.setProperty(ZKSignerSecretProvider.ZOOKEEPER_PATH,
        "/secret");
    try {
      secretProviderA.init(config, getDummyServletContext(), rolloverFrequency);
      secretProviderB.init(config, getDummyServletContext(), rolloverFrequency);
      secretProviderC.init(config, getDummyServletContext(), rolloverFrequency);

      byte[] currentSecretA = secretProviderA.getCurrentSecret();
      byte[][] allSecretsA = secretProviderA.getAllSecrets();
      byte[] currentSecretB = secretProviderB.getCurrentSecret();
      byte[][] allSecretsB = secretProviderB.getAllSecrets();
      byte[] currentSecretC = secretProviderC.getCurrentSecret();
      byte[][] allSecretsC = secretProviderC.getAllSecrets();
      Assert.assertArrayEquals(currentSecretA, currentSecretB);
      Assert.assertArrayEquals(currentSecretB, currentSecretC);
      Assert.assertEquals(2, allSecretsA.length);
      Assert.assertEquals(2, allSecretsB.length);
      Assert.assertEquals(2, allSecretsC.length);
      Assert.assertArrayEquals(allSecretsA[0], allSecretsB[0]);
      Assert.assertArrayEquals(allSecretsB[0], allSecretsC[0]);
      Assert.assertNull(allSecretsA[1]);
      Assert.assertNull(allSecretsB[1]);
      Assert.assertNull(allSecretsC[1]);
      char secretChosen = 'z';
      if (Arrays.equals(secretA1, currentSecretA)) {
        Assert.assertArrayEquals(secretA1, allSecretsA[0]);
        secretChosen = 'A';
      } else if (Arrays.equals(secretB1, currentSecretB)) {
        Assert.assertArrayEquals(secretB1, allSecretsA[0]);
        secretChosen = 'B';
      }else if (Arrays.equals(secretC1, currentSecretC)) {
        Assert.assertArrayEquals(secretC1, allSecretsA[0]);
        secretChosen = 'C';
      } else {
        Assert.fail("It appears that they all agreed on the same secret, but "
                + "not one of the secrets they were supposed to");
      }
      verify(secretProviderA, timeout(timeout).times(1)).rollSecret();
      verify(secretProviderB, timeout(timeout).times(1)).rollSecret();
      verify(secretProviderC, timeout(timeout).times(1)).rollSecret();

      currentSecretA = secretProviderA.getCurrentSecret();
      allSecretsA = secretProviderA.getAllSecrets();
      currentSecretB = secretProviderB.getCurrentSecret();
      allSecretsB = secretProviderB.getAllSecrets();
      currentSecretC = secretProviderC.getCurrentSecret();
      allSecretsC = secretProviderC.getAllSecrets();
      Assert.assertArrayEquals(currentSecretA, currentSecretB);
      Assert.assertArrayEquals(currentSecretB, currentSecretC);
      Assert.assertEquals(2, allSecretsA.length);
      Assert.assertEquals(2, allSecretsB.length);
      Assert.assertEquals(2, allSecretsC.length);
      Assert.assertArrayEquals(allSecretsA[0], allSecretsB[0]);
      Assert.assertArrayEquals(allSecretsB[0], allSecretsC[0]);
      Assert.assertArrayEquals(allSecretsA[1], allSecretsB[1]);
      Assert.assertArrayEquals(allSecretsB[1], allSecretsC[1]);
      // The second secret used is prechosen by whoever won the init; so it
      // should match with whichever we saw before
      if (secretChosen == 'A') {
        Assert.assertArrayEquals(secretA2, currentSecretA);
      } else if (secretChosen == 'B') {
        Assert.assertArrayEquals(secretB2, currentSecretA);
      } else if (secretChosen == 'C') {
        Assert.assertArrayEquals(secretC2, currentSecretA);
      }
    } finally {
      secretProviderC.destroy();
      secretProviderB.destroy();
      secretProviderA.destroy();
    }
  }

  @Test
  public void testMultipleUnsychnronized() throws Exception {
    long seedA = System.currentTimeMillis();
    Random rand = new Random(seedA);
    byte[] secretA2 = Long.toString(rand.nextLong()).getBytes();
    byte[] secretA1 = Long.toString(rand.nextLong()).getBytes();
    byte[] secretA3 = Long.toString(rand.nextLong()).getBytes();
    // use the same seed so we can predict the RNG
    long seedB = System.currentTimeMillis() + rand.nextLong();
    rand = new Random(seedB);
    byte[] secretB2 = Long.toString(rand.nextLong()).getBytes();
    byte[] secretB1 = Long.toString(rand.nextLong()).getBytes();
    byte[] secretB3 = Long.toString(rand.nextLong()).getBytes();
    ZKSignerSecretProvider secretProviderA =
        spy(new ZKSignerSecretProvider(seedA));
    ZKSignerSecretProvider secretProviderB =
        spy(new ZKSignerSecretProvider(seedB));
    Properties config = new Properties();
    config.setProperty(
        ZKSignerSecretProvider.ZOOKEEPER_CONNECTION_STRING,
        zkServer.getConnectString());
    config.setProperty(ZKSignerSecretProvider.ZOOKEEPER_PATH,
        "/secret");
    try {
      secretProviderA.init(config, getDummyServletContext(), rolloverFrequency);

      byte[] currentSecretA = secretProviderA.getCurrentSecret();
      byte[][] allSecretsA = secretProviderA.getAllSecrets();
      Assert.assertArrayEquals(secretA1, currentSecretA);
      Assert.assertEquals(2, allSecretsA.length);
      Assert.assertArrayEquals(secretA1, allSecretsA[0]);
      Assert.assertNull(allSecretsA[1]);
      verify(secretProviderA, timeout(timeout).times(1)).rollSecret();

      currentSecretA = secretProviderA.getCurrentSecret();
      allSecretsA = secretProviderA.getAllSecrets();
      Assert.assertArrayEquals(secretA2, currentSecretA);
      Assert.assertEquals(2, allSecretsA.length);
      Assert.assertArrayEquals(secretA2, allSecretsA[0]);
      Assert.assertArrayEquals(secretA1, allSecretsA[1]);
      Thread.sleep((rolloverFrequency / 5));

      secretProviderB.init(config, getDummyServletContext(), rolloverFrequency);

      byte[] currentSecretB = secretProviderB.getCurrentSecret();
      byte[][] allSecretsB = secretProviderB.getAllSecrets();
      Assert.assertArrayEquals(secretA2, currentSecretB);
      Assert.assertEquals(2, allSecretsA.length);
      Assert.assertArrayEquals(secretA2, allSecretsB[0]);
      Assert.assertArrayEquals(secretA1, allSecretsB[1]);
      verify(secretProviderA, timeout(timeout).times(2)).rollSecret();
      verify(secretProviderB, timeout(timeout).times(1)).rollSecret();

      currentSecretA = secretProviderA.getCurrentSecret();
      allSecretsA = secretProviderA.getAllSecrets();
      currentSecretB = secretProviderB.getCurrentSecret();
      allSecretsB = secretProviderB.getAllSecrets();
      Assert.assertArrayEquals(currentSecretA, currentSecretB);
      Assert.assertEquals(2, allSecretsA.length);
      Assert.assertEquals(2, allSecretsB.length);
      Assert.assertArrayEquals(allSecretsA[0], allSecretsB[0]);
      Assert.assertArrayEquals(allSecretsA[1], allSecretsB[1]);
      if (Arrays.equals(secretA3, currentSecretA)) {
        Assert.assertArrayEquals(secretA3, allSecretsA[0]);
      } else if (Arrays.equals(secretB3, currentSecretB)) {
        Assert.assertArrayEquals(secretB3, allSecretsA[0]);
      } else {
        Assert.fail("It appears that they all agreed on the same secret, but "
                + "not one of the secrets they were supposed to");
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
