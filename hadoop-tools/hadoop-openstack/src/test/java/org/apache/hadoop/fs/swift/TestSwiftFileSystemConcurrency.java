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

package org.apache.hadoop.fs.swift;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.swift.util.SwiftTestUtils;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Test Swift FS concurrency logic. This isn't a very accurate test,
 * because it is hard to consistently generate race conditions.
 * Consider it "best effort"
 */
public class TestSwiftFileSystemConcurrency extends SwiftFileSystemBaseTest {
  protected static final Log LOG =
    LogFactory.getLog(TestSwiftFileSystemConcurrency.class);
  private Exception thread1Ex, thread2Ex;
  public static final String TEST_RACE_CONDITION_ON_DELETE_DIR =
    "/test/testraceconditionondirdeletetest";

  /**
   * test on concurrent file system changes
   */
  @Test(timeout = SWIFT_TEST_TIMEOUT)
  public void testRaceConditionOnDirDeleteTest() throws Exception {
    SwiftTestUtils.skip("Skipping unreliable test");

    final String message = "message";
    final Path fileToRead = new Path(
      TEST_RACE_CONDITION_ON_DELETE_DIR +"/files/many-files/file");
    final ExecutorService executorService = Executors.newFixedThreadPool(2);
    fs.create(new Path(TEST_RACE_CONDITION_ON_DELETE_DIR +"/file/test/file1"));
    fs.create(new Path(TEST_RACE_CONDITION_ON_DELETE_DIR + "/documents/doc1"));
    fs.create(new Path(
      TEST_RACE_CONDITION_ON_DELETE_DIR + "/pictures/picture"));


    executorService.execute(new Runnable() {
      @Override
      public void run() {
        try {
          assertDeleted(new Path(TEST_RACE_CONDITION_ON_DELETE_DIR), true);
        } catch (IOException e) {
          LOG.warn("deletion thread:" + e, e);
          thread1Ex = e;
          throw new RuntimeException(e);
        }
      }
    });
    executorService.execute(new Runnable() {
      @Override
      public void run() {
        try {
          final FSDataOutputStream outputStream = fs.create(fileToRead);
          outputStream.write(message.getBytes());
          outputStream.close();
        } catch (IOException e) {
          LOG.warn("writer thread:" + e, e);
          thread2Ex = e;
          throw new RuntimeException(e);
        }
      }
    });

    executorService.awaitTermination(1, TimeUnit.MINUTES);
    if (thread1Ex != null) {
      throw thread1Ex;
    }
    if (thread2Ex != null) {
      throw thread2Ex;
    }
    try {
      fs.open(fileToRead);
      LOG.info("concurrency test failed to trigger a failure");
    } catch (FileNotFoundException expected) {

    }

  }
}
