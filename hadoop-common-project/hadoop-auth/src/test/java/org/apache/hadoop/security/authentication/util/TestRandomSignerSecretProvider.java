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

import java.util.Random;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

public class TestRandomSignerSecretProvider {

  // rollover every 250 msec
  private final int timeout = 500;
  private final long rolloverFrequency = timeout / 2;

  {
    LogManager.getLogger(
        RolloverSignerSecretProvider.LOG.getName()).setLevel(Level.DEBUG);
  }

  @Test
  public void testGetAndRollSecrets() throws Exception {
    // Use the same seed and a "plain" Random so we can predict the RNG
    long seed = System.currentTimeMillis();
    Random rand = new Random(seed);
    byte[] secret1 = generateNewSecret(rand);
    byte[] secret2 = generateNewSecret(rand);
    byte[] secret3 = generateNewSecret(rand);
    MockRandomSignerSecretProvider secretProvider =
        spy(new MockRandomSignerSecretProvider(seed));
    try {
      secretProvider.init(null, null, rolloverFrequency);

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
   * A hack to test RandomSignerSecretProvider.
   * We want to test that RandomSignerSecretProvider.rollSecret() is
   * periodically called at the expected frequency, but we want to exclude the
   * race-condition and not take a long time to run the test.
   */
  private class MockRandomSignerSecretProvider
      extends RandomSignerSecretProvider {
    MockRandomSignerSecretProvider(long seed) {
      super(seed);
    }
    @Override
    protected synchronized void rollSecret() {
      // this is a no-op: simply used for Mockito to verify that rollSecret()
      // is periodically called at the expected frequency
    }

    public void realRollSecret() {
      // the test code manually calls RandomSignerSecretProvider.rollSecret()
      // to update the state
      super.rollSecret();
    }
  }

  private byte[] generateNewSecret(Random rand) {
    byte[] secret = new byte[32];
    rand.nextBytes(secret);
    return secret;
  }
}
