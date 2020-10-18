/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.contract.s3a;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractContractSeekTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AInputPolicy;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;
import org.apache.hadoop.util.NativeCodeLoader;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.s3a.Constants.INPUT_FADVISE;
import static org.apache.hadoop.fs.s3a.Constants.INPUT_FADV_NORMAL;
import static org.apache.hadoop.fs.s3a.Constants.INPUT_FADV_RANDOM;
import static org.apache.hadoop.fs.s3a.Constants.INPUT_FADV_SEQUENTIAL;
import static org.apache.hadoop.fs.s3a.Constants.READAHEAD_RANGE;
import static org.apache.hadoop.fs.s3a.Constants.SSL_CHANNEL_MODE;
import static org.apache.hadoop.fs.s3a.S3ATestConstants.FS_S3A_IMPL_DISABLE_CACHE;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.maybeEnableS3Guard;
import static org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory.
        SSLChannelMode.Default_JSSE;
import static org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory.
        SSLChannelMode.Default_JSSE_with_GCM;
import static org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory.
        SSLChannelMode.OpenSSL;
import static org.junit.Assume.assumeTrue;


/**
 * S3A contract tests covering file seek.
 */
@RunWith(Parameterized.class)
public class ITestS3AContractSeek extends AbstractContractSeekTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3AContractSeek.class);

  protected static final int READAHEAD = 1024;

  private final String seekPolicy;
  private final DelegatingSSLSocketFactory.SSLChannelMode sslChannelMode;

  public static final int DATASET_LEN = READAHEAD * 2;

  public static final byte[] DATASET = ContractTestUtils.dataset(DATASET_LEN, 'a', 32);

  /**
   * This test suite is parameterized for the different seek policies
   * which S3A Supports.
   * @return a list of seek policies to test.
   */
  @Parameterized.Parameters
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {INPUT_FADV_SEQUENTIAL, Default_JSSE},
        {INPUT_FADV_RANDOM, OpenSSL},
        {INPUT_FADV_NORMAL, Default_JSSE_with_GCM},
    });
  }

  /**
   * Run the test with a chosen seek policy.
   * @param seekPolicy fadvise policy to use.
   */
  public ITestS3AContractSeek(final String seekPolicy,
      final DelegatingSSLSocketFactory.SSLChannelMode sslChannelMode) {
    this.seekPolicy = seekPolicy;
    this.sslChannelMode = sslChannelMode;
  }

  /**
   * Create a configuration, possibly patching in S3Guard options.
   * The FS is set to be uncached and the readahead and seek policies
   * of the bucket itself are removed, so as to guarantee that the
   * parameterized and test settings are
   * @return a configuration
   */
  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    // patch in S3Guard options
    maybeEnableS3Guard(conf);
    // purge any per-bucket overrides.
    try {
      URI bucketURI = new URI(checkNotNull(conf.get("fs.contract.test.fs.s3a")));
      S3ATestUtils.removeBucketOverrides(bucketURI.getHost(), conf,
          READAHEAD_RANGE,
          INPUT_FADVISE,
          SSL_CHANNEL_MODE);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    // the FS is uncached, so will need clearing in test teardowns.
    S3ATestUtils.disableFilesystemCaching(conf);
    conf.setInt(READAHEAD_RANGE, READAHEAD);
    conf.set(INPUT_FADVISE, seekPolicy);
    conf.set(SSL_CHANNEL_MODE, sslChannelMode.name());
    return conf;
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }

  @Override
  public void teardown() throws Exception {
    S3AFileSystem fs = getFileSystem();
    if (fs.getConf().getBoolean(FS_S3A_IMPL_DISABLE_CACHE, false)) {
      fs.close();
    }
    super.teardown();
  }

  /**
   * This subclass of the {@code path(path)} operation adds the seek policy
   * to the end to guarantee uniqueness across different calls of the same
   * method.
   *
   * {@inheritDoc}
   */
  @Override
  protected Path path(final String filepath) throws IOException {
    return super.path(filepath + "-" + seekPolicy);
  }

  /**
   * Go to end, read then seek back to the previous position to force normal
   * seek policy to switch to random IO.
   * This will call readByte to trigger the second GET
   * @param in input stream
   * @return the byte read
   * @throws IOException failure.
   */
  private byte readAtEndAndReturn(final FSDataInputStream in)
      throws IOException {
    long pos = in.getPos();
    in.seek(DATASET_LEN -1);
    in.readByte();
    // go back to start and force a new GET
    in.seek(pos);
    return in.readByte();
  }

  /**
   * Assert that the data read matches the dataset at the given offset.
   * This helps verify that the seek process is moving the read pointer
   * to the correct location in the file.
   * @param readOffset the offset in the file where the read began.
   * @param operation operation name for the assertion.
   * @param data data read in.
   * @param length length of data to check.
   */
  private void assertDatasetEquals(
      final int readOffset, final String operation,
      final byte[] data,
      int length) {
    for (int i = 0; i < length; i++) {
      int o = readOffset + i;
      assertEquals(operation + " with seek policy " + seekPolicy
          + "and read offset " + readOffset
          + ": data[" + i + "] != DATASET[" + o + "]",
          DATASET[o], data[i]);
    }
  }

  @Override
  public S3AFileSystem getFileSystem() {
    return (S3AFileSystem) super.getFileSystem();
  }

  @Before
  public void validateSSLChannelMode() {
    if (this.sslChannelMode == OpenSSL) {
      assumeTrue(NativeCodeLoader.isNativeCodeLoaded() &&
          NativeCodeLoader.buildSupportsOpenssl());
    }
  }

  @Test
  public void testReadPolicyInFS() throws Throwable {
    describe("Verify the read policy is being consistently set");
    S3AFileSystem fs = getFileSystem();
    assertEquals(S3AInputPolicy.getPolicy(seekPolicy), fs.getInputPolicy());
  }

  /**
   * Test for HADOOP-16109: Parquet reading S3AFileSystem causes EOF.
   * This sets up a read which will span the active readahead and,
   * in random IO mode, a subsequent GET.
   */
  @Test
  public void testReadAcrossReadahead() throws Throwable {
    describe("Sets up a read which will span the active readahead"
        + " and the rest of the file.");
    Path path = path("testReadAcrossReadahead");
    writeTestDataset(path);
    FileSystem fs = getFileSystem();
    // forward seek reading across readahead boundary
    try (FSDataInputStream in = fs.open(path)) {
      final byte[] temp = new byte[5];
      in.readByte();
      int offset = READAHEAD - 1;
      in.readFully(offset, temp); // <-- works
      assertDatasetEquals(offset, "read spanning boundary", temp, temp.length);
    }
    // Read exactly on the the boundary
    try (FSDataInputStream in = fs.open(path)) {
      final byte[] temp = new byte[5];
      readAtEndAndReturn(in);
      assertEquals("current position", 1, (int)(in.getPos()));
      in.readFully(READAHEAD, temp);
      assertDatasetEquals(READAHEAD, "read exactly on boundary",
          temp, temp.length);
    }
  }

  /**
   * Read across the end of the read buffer using the readByte call,
   * which will read a single byte only.
   */
  @Test
  public void testReadSingleByteAcrossReadahead() throws Throwable {
    describe("Read over boundary using read()/readByte() calls.");
    Path path = path("testReadSingleByteAcrossReadahead");
    writeTestDataset(path);
    FileSystem fs = getFileSystem();
    try (FSDataInputStream in = fs.open(path)) {
      final byte[] b0 = new byte[1];
      readAtEndAndReturn(in);
      in.seek(READAHEAD - 1);
      b0[0] = in.readByte();
      assertDatasetEquals(READAHEAD - 1, "read before end of boundary", b0,
          b0.length);
      b0[0] = in.readByte();
      assertDatasetEquals(READAHEAD, "read at end of boundary", b0, b0.length);
      b0[0] = in.readByte();
      assertDatasetEquals(READAHEAD + 1, "read after end of boundary", b0,
          b0.length);
    }
  }

  @Test
  public void testSeekToReadaheadAndRead() throws Throwable {
    describe("Seek to just before readahead limit and call"
        + " InputStream.read(byte[])");
    Path path = path("testSeekToReadaheadAndRead");
    FileSystem fs = getFileSystem();
    writeTestDataset(path);
    try (FSDataInputStream in = fs.open(path)) {
      readAtEndAndReturn(in);
      final byte[] temp = new byte[5];
      int offset = READAHEAD - 1;
      in.seek(offset);
      // expect to read at least one byte.
      int l = in.read(temp);
      assertTrue("Reading in temp data", l > 0);
      LOG.info("Read of byte array at offset {} returned {} bytes", offset, l);
      assertDatasetEquals(offset, "read at end of boundary", temp, l);
    }
  }

  @Test
  public void testSeekToReadaheadExactlyAndRead() throws Throwable {
    describe("Seek to exactly the readahead limit and call"
        + " InputStream.read(byte[])");
    Path path = path("testSeekToReadaheadExactlyAndRead");
    FileSystem fs = getFileSystem();
    writeTestDataset(path);
    try (FSDataInputStream in = fs.open(path)) {
      readAtEndAndReturn(in);
      final byte[] temp = new byte[5];
      int offset = READAHEAD;
      in.seek(offset);
      // expect to read at least one byte.
      int l = in.read(temp);
      LOG.info("Read of byte array at offset {} returned {} bytes", offset, l);
      assertTrue("Reading in temp data", l > 0);
      assertDatasetEquals(offset, "read at end of boundary", temp, l);
    }
  }

  @Test
  public void testSeekToReadaheadExactlyAndReadByte() throws Throwable {
    describe("Seek to exactly the readahead limit and call"
        + " readByte()");
    Path path = path("testSeekToReadaheadExactlyAndReadByte");
    FileSystem fs = getFileSystem();
    writeTestDataset(path);
    try (FSDataInputStream in = fs.open(path)) {
      readAtEndAndReturn(in);
      final byte[] temp = new byte[1];
      int offset = READAHEAD;
      in.seek(offset);
      // expect to read a byte successfully.
      temp[0] = in.readByte();
      assertDatasetEquals(READAHEAD, "read at end of boundary", temp, 1);
      LOG.info("Read of byte at offset {} returned expected value", offset);
    }
  }

  /**
   * Write the standard {@link #DATASET} dataset to the given path.
   * @param path path to write to.
   * @throws IOException failure
   */
  private void writeTestDataset(final Path path) throws IOException {
    ContractTestUtils.writeDataset(getFileSystem(), path,
        DATASET, DATASET_LEN, READAHEAD, true);
  }

}
