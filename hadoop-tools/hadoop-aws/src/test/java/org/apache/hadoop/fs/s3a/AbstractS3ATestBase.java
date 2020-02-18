/*
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

package org.apache.hadoop.fs.s3a;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.AbstractFSContractTestBase;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.contract.s3a.S3AContract;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import com.amazonaws.services.s3.model.ObjectMetadata;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;
import static org.apache.hadoop.fs.s3a.Constants.SERVER_SIDE_ENCRYPTION_KEY;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestDynamoTablePrefix;

/**
 * An extension of the contract test base set up for S3A tests.
 */
public abstract class AbstractS3ATestBase extends AbstractFSContractTestBase
    implements S3ATestConstants {

  protected static final String AWS_KMS_SSE_ALGORITHM = "aws:kms";

  protected static final String SSE_C_ALGORITHM = "AES256";

  protected static final Logger LOG =
      LoggerFactory.getLogger(AbstractS3ATestBase.class);

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf, false);
  }

  @Override
  public void setup() throws Exception {
    Thread.currentThread().setName("setup");
    // force load the local FS -not because we want the FS, but we need all
    // filesystems which add default configuration resources to do it before
    // our tests start adding/removing options. See HADOOP-16626.
    FileSystem.getLocal(new Configuration());
    super.setup();
  }

  @Override
  public void teardown() throws Exception {
    Thread.currentThread().setName("teardown");
    super.teardown();
    describe("closing file system");
    IOUtils.closeStream(getFileSystem());
  }

  @Before
  public void nameThread() {
    Thread.currentThread().setName("JUnit-" + getMethodName());
  }

  protected String getMethodName() {
    return methodName.getMethodName();
  }

  @Override
  protected int getTestTimeoutMillis() {
    return S3A_TEST_TIMEOUT;
  }

  /**
   * Create a configuration, possibly patching in S3Guard options.
   * @return a configuration
   */
  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    return S3ATestUtils.prepareTestConfiguration(conf);
  }

  protected Configuration getConfiguration() {
    return getContract().getConf();
  }

  /**
   * Get the filesystem as an S3A filesystem.
   * @return the typecast FS
   */
  @Override
  public S3AFileSystem getFileSystem() {
    return (S3AFileSystem) super.getFileSystem();
  }

  /**
   * Describe a test in the logs.
   * @param text text to print
   * @param args arguments to format in the printing
   */
  protected void describe(String text, Object... args) {
    LOG.info("\n\n{}: {}\n",
        getMethodName(),
        String.format(text, args));
  }

  /**
   * Write a file, read it back, validate the dataset. Overwrites the file
   * if it is present
   * @param name filename (will have the test path prepended to it)
   * @param len length of file
   * @return the full path to the file
   * @throws IOException any IO problem
   */
  protected Path writeThenReadFile(String name, int len) throws IOException {
    Path path = path(name);
    writeThenReadFile(path, len);
    return path;
  }

  /**
   * Write a file, read it back, validate the dataset. Overwrites the file
   * if it is present
   * @param path path to file
   * @param len length of file
   * @throws IOException any IO problem
   */
  protected void writeThenReadFile(Path path, int len) throws IOException {
    byte[] data = dataset(len, 'a', 'z');
    writeDataset(getFileSystem(), path, data, data.length, 1024 * 1024, true);
    ContractTestUtils.verifyFileContents(getFileSystem(), path, data);
  }

  protected String getTestTableName(String suffix) {
    return getTestDynamoTablePrefix(getConfiguration()) + suffix;
  }

  /**
   * Assert that an exception failed with a specific status code.
   * @param e exception
   * @param code expected status code
   * @throws AWSServiceIOException rethrown if the status code does not match.
   */
  protected void assertStatusCode(AWSServiceIOException e, int code)
      throws AWSServiceIOException {
    if (e.getStatusCode() != code) {
      throw e;
    }
  }

  /**
   * Assert that a path is encrypted with right encryption settings.
   * @param path file path.
   * @param algorithm encryption algorithm.
   * @param kmsKeyArn
   * @throws IOException
   */
  protected void assertEncrypted(final Path path,
                                 final S3AEncryptionMethods algorithm,
                                 final String kmsKeyArn)
          throws IOException {
    ObjectMetadata md = getFileSystem().getObjectMetadata(path);
    String details = String.format(
            "file %s with encryption algorthm %s and key %s",
            path,
            md.getSSEAlgorithm(),
            md.getSSEAwsKmsKeyId());
    switch(algorithm) {
      case SSE_C:
        assertNull("Metadata algorithm should have been null in "
                        + details,
                md.getSSEAlgorithm());
        assertEquals("Wrong SSE-C algorithm in "
                        + details,
                SSE_C_ALGORITHM, md.getSSECustomerAlgorithm());
        String md5Key = convertKeyToMd5();
        assertEquals("getSSECustomerKeyMd5() wrong in " + details,
                md5Key, md.getSSECustomerKeyMd5());
        break;
      case SSE_KMS:
        assertEquals("Wrong algorithm in " + details,
                AWS_KMS_SSE_ALGORITHM, md.getSSEAlgorithm());
        assertEquals("Wrong KMS key in " + details,
                kmsKeyArn,
                md.getSSEAwsKmsKeyId());
        break;
      default:
        assertEquals("AES256", md.getSSEAlgorithm());
    }
  }

  /**
   * Decodes the SERVER_SIDE_ENCRYPTION_KEY from base64 into an AES key, then
   * gets the md5 of it, then encodes it in base64 so it will match the version
   * that AWS returns to us.
   *
   * @return md5'd base64 encoded representation of the server side encryption
   * key
   */
  private String convertKeyToMd5() {
    String base64Key = getFileSystem().getConf().getTrimmed(
            SERVER_SIDE_ENCRYPTION_KEY
    );
    byte[] key = Base64.decodeBase64(base64Key);
    byte[] md5 =  DigestUtils.md5(key);
    return Base64.encodeBase64String(md5).trim();
  }

}
