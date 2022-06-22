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

import org.apache.hadoop.runc.squashfs.data.FragmentWriter;
import org.apache.hadoop.runc.squashfs.metadata.MemoryMetadataBlockReader;
import org.apache.hadoop.runc.squashfs.metadata.MetadataBlockReader;
import org.apache.hadoop.runc.squashfs.metadata.MetadataReader;
import org.apache.hadoop.runc.squashfs.metadata.MetadataWriter;
import org.apache.hadoop.runc.squashfs.superblock.CompressionId;
import org.apache.hadoop.runc.squashfs.superblock.SuperBlock;
import org.apache.hadoop.runc.squashfs.util.BinUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

public final class DataTestUtils {

  private DataTestUtils() {
  }

  public static byte[] decompress(byte[] data) throws IOException {
    byte[] xfer = new byte[1024];
    try (ByteArrayInputStream bis = new ByteArrayInputStream(data)) {
      try (
          InflaterInputStream iis = new InflaterInputStream(bis, new Inflater(),
              1024)) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(1024)) {
          int c = 0;
          while ((c = iis.read(xfer, 0, 1024)) >= 0) {
            if (c > 0) {
              bos.write(xfer, 0, c);
            }
          }
          return bos.toByteArray();
        }
      }
    }
  }

  public static byte[] compress(byte[] data) throws IOException {
    byte[] xfer = new byte[1024];
    try (ByteArrayInputStream bis = new ByteArrayInputStream(data)) {
      Deflater def = new Deflater(Deflater.BEST_COMPRESSION);
      try (ByteArrayOutputStream bos = new ByteArrayOutputStream(1024)) {
        try (DeflaterOutputStream dos = new DeflaterOutputStream(bos, def,
            1024)) {
          int c = 0;
          while ((c = bis.read(xfer, 0, 1024)) >= 0) {
            if (c > 0) {
              dos.write(xfer, 0, c);
            }
          }
        }
        return bos.toByteArray();
      } finally {
        def.end();
      }
    }
  }

  public static byte[] saveFragmentMetadata(FragmentWriter fw)
      throws IOException {
    MetadataWriter writer = new MetadataWriter();
    fw.save(writer);

    byte[] data;
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      try (DataOutputStream dos = new DataOutputStream(bos)) {
        writer.save(dos);
      }
      data = bos.toByteArray();

      StringBuilder buf = new StringBuilder();
      BinUtils.dumpBin(buf, 15, "serialized-data", data, 0, data.length, 16, 2);
      System.out.println(buf.toString());
    }

    return data;
  }

  public static byte[] decodeMetadataBlock(byte[] data) throws IOException {
    SuperBlock sb = new SuperBlock();
    sb.setCompressionId(CompressionId.ZLIB);
    sb.setBlockSize(131072);
    sb.setBlockLog((short) 17);
    sb.setVersionMajor((short) 4);
    sb.setVersionMinor((short) 0);

    int tag = 0;
    try (MetadataBlockReader mbr = new MemoryMetadataBlockReader(tag, sb,
        data)) {
      MetadataReader reader = mbr.rawReader(tag, 0L, (short) 0);
      reader.isEof();
      byte[] output = new byte[reader.available()];
      reader.readFully(output);

      StringBuilder buf = new StringBuilder();
      BinUtils
          .dumpBin(buf, 17, "deserialized-data", output, 0, output.length, 16,
              2);
      System.out.println(buf.toString());

      return output;
    }
  }

}
