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
package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;

import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.DataEncryptionKeyFactory;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test DataNode.DataTransfer handling of InvalidEncryptionKeyException.
 */
@Timeout(60)
public class TestDataTransferEncryptionKey {

  /**
   * Verify that DataTransfer clears the cached data encryption key before
   * retrying InvalidEncryptionKeyException.
   */
  @Test
  public void testClearEncryptionKeyOnRetry() throws Exception {
    CountingKeyFactory keyFactory = new CountingKeyFactory();

    assertTrue(prepareRetryAfterInvalidEncryptionKey(keyFactory, 1));
    assertEquals(1, keyFactory.clearCount);

    assertFalse(prepareRetryAfterInvalidEncryptionKey(keyFactory, 2));
    assertEquals(1, keyFactory.clearCount);
  }

  private static boolean prepareRetryAfterInvalidEncryptionKey(
      DataEncryptionKeyFactory keyFactory, int retryCount) throws Exception {
    Method method = DataNode.class.getDeclaredMethod(
        "prepareRetryAfterInvalidEncryptionKey",
        DataEncryptionKeyFactory.class, int.class);
    method.setAccessible(true);
    return (boolean) method.invoke(null, keyFactory, retryCount);
  }

  private static final class CountingKeyFactory
      implements DataEncryptionKeyFactory {
    private int clearCount;

    @Override
    public DataEncryptionKey newDataEncryptionKey() {
      return null;
    }

    @Override
    public void clearDataEncryptionKey() {
      clearCount++;
    }
  }
}
