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

package org.apache.hadoop.runc.squashfs.test;

import org.apache.hadoop.runc.squashfs.metadata.MemoryMetadataBlockReader;
import org.apache.hadoop.runc.squashfs.metadata.MetadataBlock;
import org.apache.hadoop.runc.squashfs.metadata.MetadataBlockReader;
import org.apache.hadoop.runc.squashfs.metadata.MetadataReader;
import org.apache.hadoop.runc.squashfs.metadata.MetadataWriter;
import org.apache.hadoop.runc.squashfs.superblock.CompressionId;
import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;
import org.apache.hadoop.runc.squashfs.util.BinUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;

public final class MetadataTestUtils {

  private MetadataTestUtils() {
  }

  public static byte[] saveMetadataBlock(byte[] data) throws IOException {
    MetadataWriter writer = new MetadataWriter();
    writer.write(data);
    return saveMetadataBlock(writer);
  }

  public static byte[] saveMetadataBlock(MetadataWriter writer)
      throws IOException {
    byte[] data;
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      try (DataOutputStream dos = new DataOutputStream(bos)) {
        writer.save(dos);
      }
      data = bos.toByteArray();

      StringBuilder buf = new StringBuilder();
      BinUtils.dumpBin(buf, 15, "serialized-data", data, 0,
          Math.min(256, data.length), 16, 2);
      System.out.println(buf.toString());
    }

    return data;
  }

  public static MetadataBlock block(byte[] content) {
    MetadataBlock block = new MetadataBlock();
    reflectiveSet(block, "data", content);
    reflectiveSet(block, "header",
        (short) ((content.length & 0x7fff) | 0x8000));
    reflectiveSet(block, "fileLength",
        (short) (2 + (content.length & 0x7ffff)));
    return block;
  }

  private static void reflectiveSet(Object obj, String field, Object value) {
    try {
      Field fieldRef = obj.getClass().getDeclaredField(field);
      fieldRef.setAccessible(true);
      fieldRef.set(obj, value);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException("Unable to set " + field, e);
    }
  }

  public static byte[] decodeMetadataBlock(byte[] data) throws IOException {
    return decodeMetadataBlock(data, 0);
  }

  public static byte[] decodeMetadataBlocks(byte[] data) throws IOException {
    return decodeMetadataBlocks(data, 0);
  }

  public static byte[] decodeMetadataBlocks(byte[] data, int offset)
      throws IOException {
    SuperBlock sb = new SuperBlock();
    sb.setCompressionId(CompressionId.ZLIB);
    sb.setBlockSize(131072);
    sb.setBlockLog((short) 17);
    sb.setVersionMajor((short) 4);
    sb.setVersionMinor((short) 0);

    int tag = 0;
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {

      try (
          MetadataBlockReader mbr = new MemoryMetadataBlockReader(tag, sb, data,
              offset, data.length - offset)) {
        MetadataReader reader = mbr.rawReader(tag, 0, (short) 0);
        while (!reader.isEof()) {
          byte[] output = new byte[reader.available()];
          reader.readFully(output);
          bos.write(output);
        }
      }

      byte[] output = bos.toByteArray();
      StringBuilder buf = new StringBuilder();
      BinUtils.dumpBin(buf, 17, "deserialized-data", output, 0,
          Math.min(256, output.length), 16, 2);
      System.out.println(buf.toString());

      return output;
    }
  }

  public static byte[] decodeMetadataBlock(byte[] data, int offset)
      throws IOException {
    SuperBlock sb = new SuperBlock();
    sb.setCompressionId(CompressionId.ZLIB);
    sb.setBlockSize(131072);
    sb.setBlockLog((short) 17);
    sb.setVersionMajor((short) 4);
    sb.setVersionMinor((short) 0);

    int tag = 0;
    try (MetadataBlockReader mbr = new MemoryMetadataBlockReader(tag, sb, data,
        offset, data.length - offset)) {
      MetadataReader reader = mbr.rawReader(tag, 0, (short) 0);
      reader.isEof();
      byte[] output = new byte[reader.available()];
      reader.readFully(output);

      StringBuilder buf = new StringBuilder();
      BinUtils.dumpBin(buf, 17, "deserialized-data", output, 0,
          Math.min(256, output.length), 16, 2);
      System.out.println(buf.toString());

      return output;
    }
  }

}
