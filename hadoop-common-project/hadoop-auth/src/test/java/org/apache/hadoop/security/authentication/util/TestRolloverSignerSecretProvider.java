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

import org.junit.Assert;
import org.junit.Test;

public class TestRolloverSignerSecretProvider {

  @Test
  public void testGetAndRollSecrets() throws Exception {
    long rolloverFrequency = 15 * 1000; // rollover every 15 sec
    byte[] secret1 = "doctor".getBytes();
    byte[] secret2 = "who".getBytes();
    byte[] secret3 = "tardis".getBytes();
    TRolloverSignerSecretProvider secretProvider =
        new TRolloverSignerSecretProvider(
            new byte[][]{secret1, secret2, secret3});
    try {
      secretProvider.init(null, null, rolloverFrequency);

      byte[] currentSecret = secretProvider.getCurrentSecret();
      byte[][] allSecrets = secretProvider.getAllSecrets();
      Assert.assertArrayEquals(secret1, currentSecret);
      Assert.assertEquals(2, allSecrets.length);
      Assert.assertArrayEquals(secret1, allSecrets[0]);
      Assert.assertNull(allSecrets[1]);
      Thread.sleep(rolloverFrequency + 2000);

      currentSecret = secretProvider.getCurrentSecret();
      allSecrets = secretProvider.getAllSecrets();
      Assert.assertArrayEquals(secret2, currentSecret);
      Assert.assertEquals(2, allSecrets.length);
      Assert.assertArrayEquals(secret2, allSecrets[0]);
      Assert.assertArrayEquals(secret1, allSecrets[1]);
      Thread.sleep(rolloverFrequency + 2000);

      currentSecret = secretProvider.getCurrentSecret();
      allSecrets = secretProvider.getAllSecrets();
      Assert.assertArrayEquals(secret3, currentSecret);
      Assert.assertEquals(2, allSecrets.length);
      Assert.assertArrayEquals(secret3, allSecrets[0]);
      Assert.assertArrayEquals(secret2, allSecrets[1]);
      Thread.sleep(rolloverFrequency + 2000);
    } finally {
      secretProvider.destroy();
    }
  }

  class TRolloverSignerSecretProvider extends RolloverSignerSecretProvider {

    private byte[][] newSecretSequence;
    private int newSecretSequenceIndex;

    public TRolloverSignerSecretProvider(byte[][] newSecretSequence)
        throws Exception {
      super();
      this.newSecretSequence = newSecretSequence;
      this.newSecretSequenceIndex = 0;
    }

    @Override
    protected byte[] generateNewSecret() {
      return newSecretSequence[newSecretSequenceIndex++];
    }

  }
}
