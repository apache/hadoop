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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;

import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestFSDirEncryptionZoneOp {

  @Test
  public void testWarmUpEdekCacheRetries() throws IOException {
    NameNode.initMetrics(new Configuration(), NamenodeRole.NAMENODE);

    final int initialDelay = 100;
    final int retryInterval = 100;
    final int maxRetries = 2;

    KeyProviderCryptoExtension kpMock = mock(KeyProviderCryptoExtension.class);

    doThrow(new IOException())
        .doThrow(new IOException())
        .doAnswer(invocation -> null)
        .when(kpMock).warmUpEncryptedKeys(any());

    FSDirEncryptionZoneOp.EDEKCacheLoader loader =
        new FSDirEncryptionZoneOp.EDEKCacheLoader(new String[] {"edek1", "edek2"}, kpMock,
            initialDelay, retryInterval, maxRetries);

    loader.run();

    verify(kpMock, times(maxRetries)).warmUpEncryptedKeys(any());
  }
}