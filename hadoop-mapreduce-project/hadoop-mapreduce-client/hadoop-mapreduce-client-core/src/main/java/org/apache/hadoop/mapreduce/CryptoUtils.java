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
package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoCodec;
import org.apache.hadoop.crypto.CryptoInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.crypto.CryptoFSDataInputStream;
import org.apache.hadoop.fs.crypto.CryptoFSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.io.LimitInputStream;

/**
 * This class provides utilities to make it easier to work with Cryptographic
 * Streams. Specifically for dealing with encrypting intermediate data such
 * MapReduce spill files.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CryptoUtils {

  private static final Log LOG = LogFactory.getLog(CryptoUtils.class);

  public static boolean isShuffleEncrypted(Configuration conf) {
    return conf.getBoolean(MRJobConfig.MR_ENCRYPTED_INTERMEDIATE_DATA,
        MRJobConfig.DEFAULT_MR_ENCRYPTED_INTERMEDIATE_DATA);
  }

  /**
   * This method creates and initializes an IV (Initialization Vector)
   * 
   * @param conf
   * @return byte[]
   * @throws IOException
   */
  public static byte[] createIV(Configuration conf) throws IOException {
    CryptoCodec cryptoCodec = CryptoCodec.getInstance(conf);
    if (isShuffleEncrypted(conf)) {
      byte[] iv = new byte[cryptoCodec.getCipherSuite().getAlgorithmBlockSize()];
      cryptoCodec.generateSecureRandom(iv);
      return iv;
    } else {
      return null;
    }
  }

  public static int cryptoPadding(Configuration conf) {
    // Sizeof(IV) + long(start-offset)
    return isShuffleEncrypted(conf) ? CryptoCodec.getInstance(conf)
        .getCipherSuite().getAlgorithmBlockSize() + 8 : 0;
  }

  private static byte[] getEncryptionKey() throws IOException {
    return TokenCache.getShuffleSecretKey(UserGroupInformation.getCurrentUser()
        .getCredentials());
  }

  private static int getBufferSize(Configuration conf) {
    return conf.getInt(MRJobConfig.MR_ENCRYPTED_INTERMEDIATE_DATA_BUFFER_KB,
        MRJobConfig.DEFAULT_MR_ENCRYPTED_INTERMEDIATE_DATA_BUFFER_KB) * 1024;
  }

  /**
   * Wraps a given FSDataOutputStream with a CryptoOutputStream. The size of the
   * data buffer required for the stream is specified by the
   * "mapreduce.job.encrypted-intermediate-data.buffer.kb" Job configuration
   * variable.
   * 
   * @param conf
   * @param out
   * @return FSDataOutputStream
   * @throws IOException
   */
  public static FSDataOutputStream wrapIfNecessary(Configuration conf,
      FSDataOutputStream out) throws IOException {
    if (isShuffleEncrypted(conf)) {
      out.write(ByteBuffer.allocate(8).putLong(out.getPos()).array());
      byte[] iv = createIV(conf);
      out.write(iv);
      if (LOG.isDebugEnabled()) {
        LOG.debug("IV written to Stream ["
            + Base64.encodeBase64URLSafeString(iv) + "]");
      }
      return new CryptoFSDataOutputStream(out, CryptoCodec.getInstance(conf),
          getBufferSize(conf), getEncryptionKey(), iv);
    } else {
      return out;
    }
  }

  /**
   * Wraps a given InputStream with a CryptoInputStream. The size of the data
   * buffer required for the stream is specified by the
   * "mapreduce.job.encrypted-intermediate-data.buffer.kb" Job configuration
   * variable.
   * 
   * If the value of 'length' is > -1, The InputStream is additionally wrapped
   * in a LimitInputStream. CryptoStreams are late buffering in nature. This
   * means they will always try to read ahead if they can. The LimitInputStream
   * will ensure that the CryptoStream does not read past the provided length
   * from the given Input Stream.
   * 
   * @param conf
   * @param in
   * @param length
   * @return InputStream
   * @throws IOException
   */
  public static InputStream wrapIfNecessary(Configuration conf, InputStream in,
      long length) throws IOException {
    if (isShuffleEncrypted(conf)) {
      int bufferSize = getBufferSize(conf);
      if (length > -1) {
        in = new LimitInputStream(in, length);
      }
      byte[] offsetArray = new byte[8];
      IOUtils.readFully(in, offsetArray, 0, 8);
      long offset = ByteBuffer.wrap(offsetArray).getLong();
      CryptoCodec cryptoCodec = CryptoCodec.getInstance(conf);
      byte[] iv = 
          new byte[cryptoCodec.getCipherSuite().getAlgorithmBlockSize()];
      IOUtils.readFully(in, iv, 0, 
          cryptoCodec.getCipherSuite().getAlgorithmBlockSize());
      if (LOG.isDebugEnabled()) {
        LOG.debug("IV read from ["
            + Base64.encodeBase64URLSafeString(iv) + "]");
      }
      return new CryptoInputStream(in, cryptoCodec, bufferSize,
          getEncryptionKey(), iv, offset + cryptoPadding(conf));
    } else {
      return in;
    }
  }

  /**
   * Wraps a given FSDataInputStream with a CryptoInputStream. The size of the
   * data buffer required for the stream is specified by the
   * "mapreduce.job.encrypted-intermediate-data.buffer.kb" Job configuration
   * variable.
   * 
   * @param conf
   * @param in
   * @return FSDataInputStream
   * @throws IOException
   */
  public static FSDataInputStream wrapIfNecessary(Configuration conf,
      FSDataInputStream in) throws IOException {
    if (isShuffleEncrypted(conf)) {
      CryptoCodec cryptoCodec = CryptoCodec.getInstance(conf);
      int bufferSize = getBufferSize(conf);
      // Not going to be used... but still has to be read...
      // Since the O/P stream always writes it..
      IOUtils.readFully(in, new byte[8], 0, 8);
      byte[] iv = 
          new byte[cryptoCodec.getCipherSuite().getAlgorithmBlockSize()];
      IOUtils.readFully(in, iv, 0, 
          cryptoCodec.getCipherSuite().getAlgorithmBlockSize());
      if (LOG.isDebugEnabled()) {
        LOG.debug("IV read from Stream ["
            + Base64.encodeBase64URLSafeString(iv) + "]");
      }
      return new CryptoFSDataInputStream(in, cryptoCodec, bufferSize,
          getEncryptionKey(), iv);
    } else {
      return in;
    }
  }

}
