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

package org.apache.hadoop.fs.azurebfs.contract;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ARROW_COL_NAME;

/**
 * Shared Arrow IPC stream builders for the Photon (Apache Arrow based ListBlob)
 * unit tests. Kept in one place so the over-allocator-limit fixtures used by
 * {@code TestArrowListBlobParser} and {@code TestAbfsBlobClientPhotonHeaders}
 * cannot drift apart.
 */
public final class ArrowListBlobTestStreams {

  /**
   * Row count large enough that parsing the resulting stream exhausts a tiny
   * (e.g. 1 KB) Arrow allocator limit and forces an over-limit failure.
   */
  public static final int OVER_LIMIT_ROW_COUNT = 2000;

  private ArrowListBlobTestStreams() {
  }

  /**
   * Build a valid single-column ({@code Name}) Arrow IPC stream carrying
   * {@link #OVER_LIMIT_ROW_COUNT} rows of reasonably long names, used to drive
   * the allocator-limit failure path.
   *
   * @return the serialized Arrow IPC stream bytes.
   * @throws IOException if the stream cannot be written.
   */
  public static byte[] overAllocatorLimitNameStream() throws IOException {
    Field nameField = new Field(ARROW_COL_NAME,
        FieldType.nullable(new ArrowType.Utf8()), null);
    Schema schema = new Schema(Collections.singletonList(nameField));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
      VarCharVector nameVector = (VarCharVector) root.getVector(ARROW_COL_NAME);
      nameVector.allocateNew(OVER_LIMIT_ROW_COUNT);
      for (int i = 0; i < OVER_LIMIT_ROW_COUNT; i++) {
        nameVector.setSafe(i, ("some-reasonably-long-blob-name-" + i)
            .getBytes(StandardCharsets.UTF_8));
      }
      root.setRowCount(OVER_LIMIT_ROW_COUNT);
      writer.start();
      writer.writeBatch();
      writer.end();
    }
    // Read the bytes only after the writer has been closed so the Arrow stream
    // is guaranteed to be fully flushed and terminated.
    return out.toByteArray();
  }
}
