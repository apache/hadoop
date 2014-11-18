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

package org.apache.hadoop.fs.s3a.scale;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

public class TestS3ADeleteManyFiles extends S3AScaleTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestS3ADeleteManyFiles.class);


  @Rule
  public Timeout testTimeout = new Timeout(30 * 60 * 1000);

  @Test
  public void testBulkRenameAndDelete() throws Throwable {
    final Path scaleTestDir = getTestPath();
    final Path srcDir = new Path(scaleTestDir, "src");
    final Path finalDir = new Path(scaleTestDir, "final");
    final long count = getOperationCount();
    ContractTestUtils.rm(fs, scaleTestDir, true, false);

    fs.mkdirs(srcDir);
    fs.mkdirs(finalDir);

    int testBufferSize = fs.getConf()
        .getInt(ContractTestUtils.IO_CHUNK_BUFFER_SIZE,
            ContractTestUtils.DEFAULT_IO_CHUNK_BUFFER_SIZE);
    // use Executor to speed up file creation
    ExecutorService exec = Executors.newFixedThreadPool(16);
    final ExecutorCompletionService<Boolean> completionService =
        new ExecutorCompletionService<Boolean>(exec);
    try {
      final byte[] data = ContractTestUtils.dataset(testBufferSize, 'a', 'z');

      for (int i = 0; i < count; ++i) {
        final String fileName = "foo-" + i;
        completionService.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() throws IOException {
            ContractTestUtils.createFile(fs, new Path(srcDir, fileName),
                false, data);
            return fs.exists(new Path(srcDir, fileName));
          }
        });
      }
      for (int i = 0; i < count; ++i) {
        final Future<Boolean> future = completionService.take();
        try {
          if (!future.get()) {
            LOG.warn("cannot create file");
          }
        } catch (ExecutionException e) {
          LOG.warn("Error while uploading file", e.getCause());
          throw e;
        }
      }
    } finally {
      exec.shutdown();
    }

    int nSrcFiles = fs.listStatus(srcDir).length;
    fs.rename(srcDir, finalDir);
    assertEquals(nSrcFiles, fs.listStatus(finalDir).length);
    ContractTestUtils.assertPathDoesNotExist(fs, "not deleted after rename",
        new Path(srcDir, "foo-" + 0));
    ContractTestUtils.assertPathDoesNotExist(fs, "not deleted after rename",
        new Path(srcDir, "foo-" + count / 2));
    ContractTestUtils.assertPathDoesNotExist(fs, "not deleted after rename",
        new Path(srcDir, "foo-" + (count - 1)));
    ContractTestUtils.assertPathExists(fs, "not renamed to dest dir",
        new Path(finalDir, "foo-" + 0));
    ContractTestUtils.assertPathExists(fs, "not renamed to dest dir",
        new Path(finalDir, "foo-" + count/2));
    ContractTestUtils.assertPathExists(fs, "not renamed to dest dir",
        new Path(finalDir, "foo-" + (count-1)));

    ContractTestUtils.assertDeleted(fs, finalDir, true, false);
  }

  @Test
  public void testOpenCreate() throws IOException {
    Path dir = new Path("/tests3a");
    ContractTestUtils.createAndVerifyFile(fs, dir, 1024);
    ContractTestUtils.createAndVerifyFile(fs, dir, 5 * 1024 * 1024);
    ContractTestUtils.createAndVerifyFile(fs, dir, 20 * 1024 * 1024);


    /*
    Enable to test the multipart upload
    try {
      ContractTestUtils.createAndVerifyFile(fs, dir,
          (long)6 * 1024 * 1024 * 1024);
    } catch (IOException e) {
      fail(e.getMessage());
    }
    */
  }
}
