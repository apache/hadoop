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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileHeader;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.NameSystemSection;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.compress.CompressionCodec;

import com.google.common.base.Preconditions;
import com.google.protobuf.CodedOutputStream;

/**
 * Utility class to read / write fsimage in protobuf format.
 */
final class FSImageFormatProtobuf {
  private static final Log LOG = LogFactory
      .getLog(DelegationTokenSecretManager.class);

  static final byte[] MAGIC_HEADER = "HDFSIMG1".getBytes();
  private static final int FILE_VERSION = 1;
  private static final int PRE_ALLOCATED_HEADER_SIZE = 1024;

  /**
   * Supported section name
   */
  private enum SectionName {
    NS_INFO("NS_INFO");

    private static final SectionName[] values = SectionName.values();
    private final String name;

    private SectionName(String name) {
      this.name = name;
    }

    private static SectionName fromString(String name) {
      for (SectionName n : values) {
        if (n.name.equals(name))
          return n;
      }
      return null;
    }
  }

  // Buffer size of when reading / writing fsimage
  public static final int DEFAULT_BUFFER_SIZE = 16 * 1024 * 1024;

  static final class Loader implements FSImageFormat.AbstractLoader {
    private final Configuration conf;
    private final FSNamesystem fsn;

    /** The transaction ID of the last edit represented by the loaded file */
    private long imgTxId;
    /** The MD5 sum of the loaded file */
    private MD5Hash imgDigest;

    Loader(Configuration conf, FSNamesystem fsn) {
      this.conf = conf;
      this.fsn = fsn;
    }

    @Override
    public MD5Hash getLoadedImageMd5() {
      return imgDigest;
    }

    @Override
    public long getLoadedImageTxId() {
      return imgTxId;
    }

    @SuppressWarnings("resource")
    public void load(FileInputStream fin) throws IOException {
      FileHeader header = loadHeader(new BufferedInputStream(fin));

      fin.getChannel().position(header.getDataOffset());
      MessageDigest digester = MD5Hash.getDigester();
      InputStream in = new DigestInputStream(new BufferedInputStream(fin,
          DEFAULT_BUFFER_SIZE), digester);

      if (header.hasCodec()) {
        // read compression related info
        FSImageCompression compression = FSImageCompression.createCompression(
            conf, header.getCodec());
        CompressionCodec imageCodec = compression.getImageCodec();
        if (header.getCodec() != null) {
          in = imageCodec.createInputStream(in);
        }
      }

      for (FileHeader.Section s : header.getSectionsList()) {
        String n = s.getName();
        switch (SectionName.fromString(n)) {
        case NS_INFO:
          loadNameSystemSection(in, s);
          break;
        default:
          LOG.warn("Unregconized section " + n);
          break;
        }
      }

      updateDigestForFileHeader(header, digester);

      imgDigest = new MD5Hash(digester.digest());
      in.close();
    }

    private FileHeader loadHeader(InputStream fin) throws IOException {
      byte[] magic = new byte[MAGIC_HEADER.length];
      if (fin.read(magic) != magic.length
          || !Arrays.equals(magic, FSImageFormatProtobuf.MAGIC_HEADER)) {
        throw new IOException("Unrecognized FSImage");
      }

      FileHeader header = FileHeader.parseDelimitedFrom(fin);
      if (header.getOndiskVersion() != FILE_VERSION) {
        throw new IOException("Unsupported file version "
            + header.getOndiskVersion());
      }

      if (!LayoutVersion.supports(Feature.PROTOBUF_FORMAT,
          header.getLayoutVersion())) {
        throw new IOException("Unsupported layout version "
            + header.getLayoutVersion());
      }
      return header;
    }

    private void loadNameSystemSection(InputStream in, FileHeader.Section header)
        throws IOException {
      NameSystemSection s = NameSystemSection.parseDelimitedFrom(in);
      fsn.setGenerationStampV1(s.getGenstampV1());
      fsn.setGenerationStampV2(s.getGenstampV2());
      fsn.setGenerationStampV1Limit(s.getGenstampV1Limit());
      fsn.setLastAllocatedBlockId(s.getLastAllocatedBlockId());
      imgTxId = s.getTransactionId();
      long offset = header.getLength() - getOndiskTrunkSize(s);
      Preconditions.checkArgument(offset == 0);
      in.skip(offset);
    }
  }

  static final class Saver {
    private final SaveNamespaceContext context;
    private MD5Hash savedDigest;

    Saver(SaveNamespaceContext context) {
      this.context = context;
    }

    void save(File file, FSImageCompression compression) throws IOException {
      FileHeader.Builder b = FileHeader.newBuilder()
          .setOndiskVersion(FILE_VERSION)
          .setLayoutVersion(LayoutVersion.getCurrentLayoutVersion())
          .setDataOffset(PRE_ALLOCATED_HEADER_SIZE);
      MessageDigest digester = MD5Hash.getDigester();
      OutputStream out = null;
      try {
        FileOutputStream fout = new FileOutputStream(file);
        FileChannel channel = fout.getChannel();

        channel.position(PRE_ALLOCATED_HEADER_SIZE);
        out = new DigestOutputStream(new BufferedOutputStream(fout,
            DEFAULT_BUFFER_SIZE), digester);

        CompressionCodec codec = compression.getImageCodec();
        if (codec != null) {
          b.setCodec(codec.getClass().getCanonicalName());
          out = codec.createOutputStream(out);
        }

        save(out, b);
        out.flush();
        channel.position(0);
        FileHeader header = b.build();
        Preconditions.checkState(MAGIC_HEADER.length
            + getOndiskTrunkSize(header) < PRE_ALLOCATED_HEADER_SIZE,
            "Insufficient space to write file header");
        fout.write(MAGIC_HEADER);
        header.writeDelimitedTo(fout);
        updateDigestForFileHeader(header, digester);
        savedDigest = new MD5Hash(digester.digest());
      } finally {
        IOUtils.cleanup(LOG, out);
      }
    }

    private void save(OutputStream out, FileHeader.Builder headers)
        throws IOException {
      final FSNamesystem fsn = context.getSourceNamesystem();
      FileHeader.Section.Builder sectionHeader = FileHeader.Section
          .newBuilder().setName(SectionName.NS_INFO.name);
      NameSystemSection.Builder b = NameSystemSection.newBuilder()
          .setGenstampV1(fsn.getGenerationStampV1())
          .setGenstampV1Limit(fsn.getGenerationStampV1Limit())
          .setGenstampV2(fsn.getGenerationStampV2())
          .setLastAllocatedBlockId(fsn.getLastAllocatedBlockId())
          .setTransactionId(context.getTxId());

      // We use the non-locked version of getNamespaceInfo here since
      // the coordinating thread of saveNamespace already has read-locked
      // the namespace for us. If we attempt to take another readlock
      // from the actual saver thread, there's a potential of a
      // fairness-related deadlock. See the comments on HDFS-2223.
      b.setNamespaceId(fsn.unprotectedGetNamespaceInfo().getNamespaceID());
      NameSystemSection s = b.build();
      s.writeDelimitedTo(out);
      sectionHeader.setLength(getOndiskTrunkSize(s));
      headers.addSections(sectionHeader);
    }

    public MD5Hash getSavedDigest() {
      return savedDigest;
    }
  }

  private static int getOndiskTrunkSize(com.google.protobuf.GeneratedMessage s) {
    return CodedOutputStream.computeRawVarint32Size(s.getSerializedSize())
        + s.getSerializedSize();
  }

  /**
   * Include the FileHeader when calculating the digest. This is required as the
   * code does not access the FSImage strictly in sequential order.
   */
  private static void updateDigestForFileHeader(FileHeader header,
      MessageDigest digester) throws IOException {
    ByteArrayOutputStream o = new ByteArrayOutputStream();
    o.write(MAGIC_HEADER);
    header.writeDelimitedTo(o);
    digester.update(o.toByteArray());
  }

  private FSImageFormatProtobuf() {
  }

}