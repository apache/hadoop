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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.fs.s3a.s3guard.S3Guard;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3ATestUtils.MetricDiff;
import org.apache.hadoop.fs.s3a.s3guard.BulkOperationState;
import org.apache.hadoop.fs.s3a.s3guard.LocalMetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.NullMetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.PathMetadata;
import org.apache.hadoop.io.IOUtils;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Tests failed writes to metadata store generate the expected
 * MetadataPersistenceException.
 */
@RunWith(Parameterized.class)
public class ITestS3AMetadataPersistenceException extends AbstractS3ATestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3AMetadataPersistenceException.class);

  private S3AFileSystem fs;
  private IOException ioException;
  private final boolean failOnError;

  public ITestS3AMetadataPersistenceException(boolean failOnError) {
    this.failOnError = failOnError;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {true},
        {false}
    });
  }

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.set(Constants.FAIL_ON_METADATA_WRITE_ERROR,
        Boolean.toString(failOnError));
    // replaced in setup() by IOExceptionMetadataStore
    conf.setClass(Constants.S3_METADATA_STORE_IMPL,
        NullMetadataStore.class,
        MetadataStore.class);
    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    S3AFileSystem contractFs = getFileSystem();
    fs = (S3AFileSystem) FileSystem.newInstance(
        contractFs.getUri(), contractFs.getConf());
    ioException = new IOException();
    IOExceptionMetadataStore metadataStore =
        new IOExceptionMetadataStore(ioException);
    metadataStore.initialize(getConfiguration(),
        new S3Guard.TtlTimeProvider(getConfiguration()));
    fs.setMetadataStore(metadataStore);
  }

  @Override
  public void teardown() throws Exception {
    IOUtils.cleanupWithLogger(LOG, fs);
    super.teardown();
  }

  @Test
  public void testFailedMetadataUpdate() throws Throwable {
    // write a trivial file
    Path testFile = path("testFailedMetadataUpdate");
    try {
      FSDataOutputStream outputStream = fs.create(testFile);
      outputStream.write(1);

      if (failOnError) {
        // close should throw the expected exception
        MetadataPersistenceException thrown =
            intercept(
                MetadataPersistenceException.class,
                outputStream::close);
        assertEquals("cause didn't match original exception",
            ioException, thrown.getCause());
      } else {
        MetricDiff ignoredCount = new MetricDiff(fs, Statistic.IGNORED_ERRORS);

        // close should merely log and increment the statistic
        outputStream.close();
        ignoredCount.assertDiffEquals("ignored errors", 1);
      }
    } finally {
      // turn off the store and forcibly delete from the raw bucket.
      fs.setMetadataStore(new NullMetadataStore());
      fs.delete(testFile, false);
    }
  }

  private static class IOExceptionMetadataStore extends LocalMetadataStore {
    private final IOException ioException;

    private IOExceptionMetadataStore(IOException ioException) {
      this.ioException = ioException;
    }

    @Override
    public void put(PathMetadata meta,
        final BulkOperationState operationState) throws IOException {
      throw ioException;
    }

    @Override
    public void put(final PathMetadata meta) throws IOException {
      put(meta, null);
    }

  }
}
