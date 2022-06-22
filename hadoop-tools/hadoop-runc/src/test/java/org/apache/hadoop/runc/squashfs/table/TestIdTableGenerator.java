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

package org.apache.hadoop.runc.squashfs.table;

import org.apache.hadoop.runc.squashfs.metadata.MetadataBlockRef;
import org.apache.hadoop.runc.squashfs.metadata.MetadataWriter;
import org.apache.hadoop.runc.squashfs.test.MetadataTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestIdTableGenerator {

  private IdTableGenerator gen;

  @Before
  public void setUp() {
    gen = new IdTableGenerator();
  }

  @Test
  public void addingDuplicateEntriesResultsInSingleMapping() {
    int id = gen.addUidGid(1000);
    assertEquals("wrong id", id, gen.addUidGid(1000));
    assertEquals("wrong count", 1, gen.getIdCount());
  }

  @Test
  public void saveEmptyEntryWritesNoData() throws Exception {
    MetadataWriter writer = new MetadataWriter();

    List<MetadataBlockRef> refs = gen.save(writer);
    assertEquals("wrong refs size", 0, refs.size());

    byte[] ser = MetadataTestUtils.saveMetadataBlock(writer);
    assertEquals("wrong data size", 0, ser.length);
  }

  @Test
  public void saveSingleEntryWritesOneRef() throws Exception {
    gen.addUidGid(1000);

    MetadataWriter writer = new MetadataWriter();

    List<MetadataBlockRef> refs = gen.save(writer);
    assertEquals("wrong refs size", 1, refs.size());

    assertEquals("wrong location", 0, refs.get(0).getLocation());
    assertEquals("wrong offset", (short) 0, refs.get(0).getOffset());

    byte[] ser = MetadataTestUtils.saveMetadataBlock(writer);
    byte[] decoded = MetadataTestUtils.decodeMetadataBlock(ser);
    IntBuffer ib =
        ByteBuffer.wrap(decoded).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
    assertEquals("wrong id", 1000, ib.get());
  }

  @Test
  public void saveAllPossibleEntriesShouldWork() throws Exception {
    for (int i = 0; i < 65536; i++) {
      gen.addUidGid(100_000 + i);
    }

    MetadataWriter writer = new MetadataWriter();

    List<MetadataBlockRef> refs = gen.save(writer);
    System.out.println(refs);
    assertEquals("wrong refs size", 32, refs.size());

    byte[] ser = MetadataTestUtils.saveMetadataBlock(writer);
    byte[] decoded = MetadataTestUtils.decodeMetadataBlocks(ser);
    IntBuffer ib =
        ByteBuffer.wrap(decoded).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();

    for (int i = 0; i < 65536; i++) {
      assertEquals(String.format("wrong id for entry %d", i), 100_000 + i,
          ib.get());
    }
  }

  @Test
  public void toStringShouldNotFail() {
    gen.addUidGid(1000);
    System.out.println(gen.toString());
  }

}
