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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.Loader;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.io.compress.CompressionCodec;

@InterfaceAudience.Private
public final class FSImageUtil {
  public static final byte[] MAGIC_HEADER =
      "HDFSIMG1".getBytes(StandardCharsets.UTF_8);
  public static final int FILE_VERSION = 1;

  public static boolean checkFileFormat(RandomAccessFile file)
      throws IOException {
    if (file.length() < Loader.MINIMUM_FILE_LENGTH)
      return false;

    byte[] magic = new byte[MAGIC_HEADER.length];
    file.readFully(magic);
    if (!Arrays.equals(MAGIC_HEADER, magic))
      return false;

    return true;
  }

  public static FileSummary loadSummary(RandomAccessFile file)
      throws IOException {
    final int FILE_LENGTH_FIELD_SIZE = 4;
    long fileLength = file.length();
    file.seek(fileLength - FILE_LENGTH_FIELD_SIZE);
    int summaryLength = file.readInt();

    if (summaryLength <= 0) {
      throw new IOException("Negative length of the file");
    }
    file.seek(fileLength - FILE_LENGTH_FIELD_SIZE - summaryLength);

    byte[] summaryBytes = new byte[summaryLength];
    file.readFully(summaryBytes);

    FileSummary summary = FileSummary
        .parseDelimitedFrom(new ByteArrayInputStream(summaryBytes));
    if (summary.getOndiskVersion() != FILE_VERSION) {
      throw new IOException("Unsupported file version "
          + summary.getOndiskVersion());
    }

    if (!NameNodeLayoutVersion.supports(Feature.PROTOBUF_FORMAT,
        summary.getLayoutVersion())) {
      throw new IOException("Unsupported layout version "
          + summary.getLayoutVersion());
    }
    return summary;
  }

  public static InputStream wrapInputStreamForCompression(
      Configuration conf, String codec, InputStream in) throws IOException {
    if (codec.isEmpty())
      return in;

    FSImageCompression compression = FSImageCompression.createCompression(
        conf, codec);
    CompressionCodec imageCodec = compression.getImageCodec();
    return imageCodec.createInputStream(in);
  }

}
