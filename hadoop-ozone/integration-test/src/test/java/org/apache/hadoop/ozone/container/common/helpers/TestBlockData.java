/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.common.helpers;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.common.Checksum;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Tests to test block deleting service.
 */
public class TestBlockData {
  static final Logger LOG = LoggerFactory.getLogger(TestBlockData.class);
  @Rule
  public TestRule timeout = new Timeout(10000);

  static ContainerProtos.ChunkInfo buildChunkInfo(String name, long offset,
      long len) {
    return ContainerProtos.ChunkInfo.newBuilder()
        .setChunkName(name)
        .setOffset(offset)
        .setLen(len)
        .setChecksumData(Checksum.getNoChecksumDataProto())
        .build();
  }

  @Test
  public void testAddAndRemove() {
    final BlockData computed = new BlockData(null);
    final List<ContainerProtos.ChunkInfo> expected = new ArrayList<>();

    assertChunks(expected, computed);
    long offset = 0;
    int n = 5;
    for(int i = 0; i < n; i++) {
      offset += assertAddChunk(expected, computed, offset);
    }

    for(; !expected.isEmpty();) {
      removeChunk(expected, computed);
    }
  }

  private static int chunkCount = 0;
  static ContainerProtos.ChunkInfo addChunk(
      List<ContainerProtos.ChunkInfo> expected, long offset) {
    final long length = ThreadLocalRandom.current().nextLong(1000);
    final ContainerProtos.ChunkInfo info =
        buildChunkInfo("c" + ++chunkCount, offset, length);
    expected.add(info);
    return info;
  }

  static long assertAddChunk(List<ContainerProtos.ChunkInfo> expected,
      BlockData computed, long offset) {
    final ContainerProtos.ChunkInfo info = addChunk(expected, offset);
    LOG.info("addChunk: " + toString(info));
    computed.addChunk(info);
    assertChunks(expected, computed);
    return info.getLen();
  }


  static void removeChunk(List<ContainerProtos.ChunkInfo> expected,
      BlockData computed) {
    final int i = ThreadLocalRandom.current().nextInt(expected.size());
    final ContainerProtos.ChunkInfo info = expected.remove(i);
    LOG.info("removeChunk: " + toString(info));
    computed.removeChunk(info);
    assertChunks(expected, computed);
  }

  static void assertChunks(List<ContainerProtos.ChunkInfo> expected,
      BlockData computed) {
    final List<ContainerProtos.ChunkInfo> computedChunks = computed.getChunks();
    Assert.assertEquals("expected=" + expected + "\ncomputed=" +
        computedChunks, expected, computedChunks);
    Assert.assertEquals(expected.stream().mapToLong(i -> i.getLen()).sum(),
        computed.getSize());
  }

  static String toString(ContainerProtos.ChunkInfo info) {
    return info.getChunkName() + ":" + info.getOffset() + "," + info.getLen();
  }

  static String toString(List<ContainerProtos.ChunkInfo> infos) {
    return infos.stream().map(TestBlockData::toString)
        .reduce((left, right) -> left + ", " + right)
        .orElse("");
  }

  @Test
  public void testSetChunks() {
    final BlockData computed = new BlockData(null);
    final List<ContainerProtos.ChunkInfo> expected = new ArrayList<>();

    assertChunks(expected, computed);
    long offset = 0;
    int n = 5;
    for(int i = 0; i < n; i++) {
      offset += addChunk(expected, offset).getLen();
      LOG.info("setChunk: " + toString(expected));
      computed.setChunks(expected);
      assertChunks(expected, computed);
    }
  }
}
