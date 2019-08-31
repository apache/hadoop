/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon;

import java.io.IOException;

import org.apache.hadoop.ozone.recon.api.types.ContainerKeyPrefix;
import org.apache.hadoop.ozone.recon.spi.impl.ContainerKeyPrefixCodec;
import org.apache.hadoop.utils.db.Codec;
import org.apache.hadoop.utils.db.IntegerCodec;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit Tests for Codecs used in Recon.
 */
public class TestReconCodecs {

  @Test
  public void testContainerKeyPrefixCodec() throws IOException {
    ContainerKeyPrefix containerKeyPrefix = new ContainerKeyPrefix(
        System.currentTimeMillis(), "TestKeyPrefix", 0);

    Codec<ContainerKeyPrefix> codec = new ContainerKeyPrefixCodec();
    byte[] persistedFormat = codec.toPersistedFormat(containerKeyPrefix);
    Assert.assertTrue(persistedFormat != null);
    ContainerKeyPrefix fromPersistedFormat =
        codec.fromPersistedFormat(persistedFormat);
    Assert.assertEquals(containerKeyPrefix, fromPersistedFormat);
  }

  @Test
  public void testIntegerCodec() throws IOException {
    Integer i = 1000;
    Codec<Integer> codec = new IntegerCodec();
    byte[] persistedFormat = codec.toPersistedFormat(i);
    Assert.assertTrue(persistedFormat != null);
    Integer fromPersistedFormat =
        codec.fromPersistedFormat(persistedFormat);
    Assert.assertEquals(i, fromPersistedFormat);
  }
}
