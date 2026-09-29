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
package org.apache.hadoop.hdfs.server.balancer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test Dispatcher handling of InvalidEncryptionKeyException.
 */
@Timeout(120)
public class TestDispatcherEncryptionKey {

  /**
   * Verify that the dispatcher refreshes the block keys and clears the cached
   * data encryption key before retrying InvalidEncryptionKeyException.
   */
  @Test
  public void testClearEncryptionKeyOnRetry() throws Exception {
    Configuration conf = new HdfsConfiguration();
    CountingKeyManager km = new CountingKeyManager(conf);

    assertTrue(prepareRetryAfterInvalidEncryptionKey(km, 1));
    assertEquals(1, km.updateBlockKeysCount);
    assertEquals(1, km.clearKeyCount);

    assertFalse(prepareRetryAfterInvalidEncryptionKey(km, 2));
    assertEquals(1, km.updateBlockKeysCount);
    assertEquals(1, km.clearKeyCount);
  }

  private static boolean prepareRetryAfterInvalidEncryptionKey(KeyManager km,
      int retryCount) throws Exception {
    Method method = Dispatcher.class.getDeclaredMethod(
        "prepareRetryAfterInvalidEncryptionKey", KeyManager.class, int.class);
    method.setAccessible(true);
    return (boolean) method.invoke(null, km, retryCount);
  }

  private static final class CountingKeyManager extends KeyManager {
    private int updateBlockKeysCount;
    private int clearKeyCount;

    private CountingKeyManager(Configuration conf) throws Exception {
      super("bp-test", createNamenode(), false, conf);
    }

    @Override
    public void updateBlockKeys() {
      updateBlockKeysCount++;
    }

    @Override
    public synchronized void clearDataEncryptionKey() {
      clearKeyCount++;
    }
  }

  private static NamenodeProtocol createNamenode() {
    return (NamenodeProtocol) Proxy.newProxyInstance(
        NamenodeProtocol.class.getClassLoader(),
        new Class<?>[]{NamenodeProtocol.class},
        (proxy, method, args) -> {
          if ("getBlockKeys".equals(method.getName())) {
            return ExportedBlockKeys.DUMMY_KEYS;
          }
          throw new UnsupportedOperationException(method.getName());
        });
  }

}
