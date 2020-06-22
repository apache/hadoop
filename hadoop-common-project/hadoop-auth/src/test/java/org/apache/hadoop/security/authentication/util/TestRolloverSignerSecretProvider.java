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

import org.apache.curator.shaded.com.google.common.annotations.VisibleForTesting;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class TestRolloverSignerSecretProvider {

  @Test
  public void testGetAndRollSecrets() throws Exception {
    byte[] secret1 = "doctor".getBytes();
    byte[] secret2 = "who".getBytes();
    byte[] secret3 = "tardis".getBytes();
    TRolloverSignerSecretProvider secretProvider =
        new TRolloverSignerSecretProvider(
            new byte[][]{secret1, secret2, secret3});
    try {
      secretProvider.init(null, null, TimeUnit.SECONDS.toMillis(2));

      byte[] currentSecret = secretProvider.getCurrentSecret();
      byte[][] allSecrets = secretProvider.getAllSecrets();
      Assert.assertArrayEquals(secret1, currentSecret);
      Assert.assertEquals(2, allSecrets.length);
      Assert.assertArrayEquals(secret1, allSecrets[0]);
      Assert.assertNull(allSecrets[1]);
      secretProvider.waitUntilNewSecret();

      currentSecret = secretProvider.getCurrentSecret();
      allSecrets = secretProvider.getAllSecrets();
      Assert.assertArrayEquals(secret2, currentSecret);
      Assert.assertEquals(2, allSecrets.length);
      Assert.assertArrayEquals(secret2, allSecrets[0]);
      Assert.assertArrayEquals(secret1, allSecrets[1]);
      secretProvider.waitUntilNewSecret();

      currentSecret = secretProvider.getCurrentSecret();
      allSecrets = secretProvider.getAllSecrets();
      Assert.assertArrayEquals(secret3, currentSecret);
      Assert.assertEquals(2, allSecrets.length);
      Assert.assertArrayEquals(secret3, allSecrets[0]);
      Assert.assertArrayEquals(secret2, allSecrets[1]);
    } finally {
      secretProvider.destroy();
    }
  }

  class TRolloverSignerSecretProvider extends RolloverSignerSecretProvider {

    private byte[][] newSecretSequence;
    private int newSecretSequenceIndex;

    /**
     * This monitor objects used to notify the thread waiting for new secret.
     */
    private final Object rolloverMonitor = new Object();

    TRolloverSignerSecretProvider(byte[][] newSecretSequence) {
      super();
      this.newSecretSequence = newSecretSequence;
      this.newSecretSequenceIndex = 0;
    }

    @Override
    protected byte[] generateNewSecret() {
      synchronized (rolloverMonitor) {
        rolloverMonitor.notify();
      }
      return newSecretSequence[newSecretSequenceIndex++];
    }

    /**
     * cause the current thread to sleep until the generateNewSecret method called
     *
     * @throws InterruptedException if any thread interrupted the current thread before or while the current thread
     *                              was waiting for a notification.
     */
    @VisibleForTesting
    private void waitUntilNewSecret() throws InterruptedException {
      synchronized (rolloverMonitor) {
        rolloverMonitor.wait();
      }
    }
  }
}
