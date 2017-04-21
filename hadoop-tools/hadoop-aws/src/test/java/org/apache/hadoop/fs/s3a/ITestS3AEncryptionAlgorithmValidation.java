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

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.s3a.S3AContract;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.URI;

/**
 * Test whether or not encryption settings propagate by choosing an invalid
 * one. We expect the S3AFileSystem to fail to initialize.
 */
@Ignore
public class ITestS3AEncryptionAlgorithmValidation
    extends AbstractS3ATestBase {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testEncryptionAlgorithmSetToDES() throws Throwable {
    expectedException.expect(IOException.class);
    expectedException.expectMessage("Unknown Server Side algorithm DES");

    Configuration conf = super.createConfiguration();
    //DES is an invalid encryption algorithm
    conf.set(Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM, "DES");
    S3AContract contract = (S3AContract) createContract(conf);
    contract.init();
    //extract the test FS
    FileSystem fileSystem = contract.getTestFileSystem();
    assertNotNull("null filesystem", fileSystem);
    URI fsURI = fileSystem.getUri();
    LOG.info("Test filesystem = {} implemented by {}", fsURI, fileSystem);
    assertEquals("wrong filesystem of " + fsURI,
        contract.getScheme(), fsURI.getScheme());
    fileSystem.initialize(fsURI, conf);
    throw new Exception("Do not reach here");
  }

  @Test
  public void testEncryptionAlgorithmSSECWithNoEncryptionKey() throws
    Throwable {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("The value of property " +
        Constants.SERVER_SIDE_ENCRYPTION_KEY + " must not be null");

    Configuration conf = super.createConfiguration();
    //SSE-C must be configured with an encryption key
    conf.set(Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM,
        S3AEncryptionMethods.SSE_C.getMethod());
    conf.set(Constants.SERVER_SIDE_ENCRYPTION_KEY, null);
    S3AContract contract = (S3AContract) createContract(conf);
    contract.init();
    //extract the test FS
    FileSystem fileSystem = contract.getTestFileSystem();
    assertNotNull("null filesystem", fileSystem);
    URI fsURI = fileSystem.getUri();
    LOG.info("Test filesystem = {} implemented by {}", fsURI, fileSystem);
    assertEquals("wrong filesystem of " + fsURI,
        contract.getScheme(), fsURI.getScheme());
    fileSystem.initialize(fsURI, conf);
    throw new Exception("Do not reach here");
  }

  @Test
  public void testEncryptionAlgorithmSSECWithBlankEncryptionKey() throws
    Throwable {
    expectedException.expect(IOException.class);
    expectedException.expectMessage(S3AUtils.SSE_C_NO_KEY_ERROR);

    Configuration conf = super.createConfiguration();
    //SSE-C must be configured with an encryption key
    conf.set(Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM,
        S3AEncryptionMethods.SSE_C.getMethod());
    conf.set(Constants.SERVER_SIDE_ENCRYPTION_KEY, "");
    S3AContract contract = (S3AContract) createContract(conf);
    contract.init();
    //extract the test FS
    FileSystem fileSystem = contract.getTestFileSystem();
    assertNotNull("null filesystem", fileSystem);
    URI fsURI = fileSystem.getUri();
    LOG.info("Test filesystem = {} implemented by {}", fsURI, fileSystem);
    assertEquals("wrong filesystem of " + fsURI,
        contract.getScheme(), fsURI.getScheme());
    fileSystem.initialize(fsURI, conf);
    throw new Exception("Do not reach here");
  }

  @Test
  public void testEncryptionAlgorithmSSES3WithEncryptionKey() throws
    Throwable {
    expectedException.expect(IOException.class);
    expectedException.expectMessage(S3AUtils.SSE_S3_WITH_KEY_ERROR);

    Configuration conf = super.createConfiguration();
    //SSE-S3 cannot be configured with an encryption key
    conf.set(Constants.SERVER_SIDE_ENCRYPTION_ALGORITHM, S3AEncryptionMethods
        .SSE_S3.getMethod());
    conf.set(Constants.SERVER_SIDE_ENCRYPTION_KEY,
        "4niV/jPK5VFRHY+KNb6wtqYd4xXyMgdJ9XQJpcQUVbs=");
    S3AContract contract = (S3AContract) createContract(conf);
    contract.init();
    //skip tests if they aren't enabled
    assumeEnabled();
    //extract the test FS
    FileSystem fileSystem = contract.getTestFileSystem();
    assertNotNull("null filesystem", fileSystem);
    URI fsURI = fileSystem.getUri();
    LOG.info("Test filesystem = {} implemented by {}", fsURI, fileSystem);
    assertEquals("wrong filesystem of " + fsURI,
        contract.getScheme(), fsURI.getScheme());
    fileSystem.initialize(fsURI, conf);
  }

  /**
   * Make this a no-op so test setup doesn't fail.
   * @param path path path
   * @throws IOException on any failure
   */
  @Override
  protected void mkdirs(Path path) throws IOException {

  }

}
