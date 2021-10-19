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
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.Test;

import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.getCurrentThreadNames;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.listInitialThreadsForLifecycleChecks;
import static org.apache.hadoop.test.LambdaTestUtils.*;
import static org.apache.hadoop.fs.s3a.S3AUtils.E_FS_CLOSED;

/**
 * Tests of the S3A FileSystem which is closed.
 */
public class ITestS3AClosedFS extends AbstractS3ATestBase {

  private Path root = new Path("/");

  @Override
  public void setup() throws Exception {
    super.setup();
    root = getFileSystem().makeQualified(new Path("/"));
    getFileSystem().close();
  }

  @Override
  public void teardown()  {
    // no op, as the FS is closed
  }

  private static final Set<String> THREAD_SET =
      listInitialThreadsForLifecycleChecks();

  @AfterClass
  public static void checkForThreadLeakage() {
    Assertions.assertThat(getCurrentThreadNames())
        .describedAs("The threads at the end of the test run")
        .isSubsetOf(THREAD_SET);
  }

  @Test
  public void testClosedGetFileStatus() throws Exception {
    intercept(IOException.class, E_FS_CLOSED,
        () -> getFileSystem().getFileStatus(root));
  }

  @Test
  public void testClosedListStatus() throws Exception {
    intercept(IOException.class, E_FS_CLOSED,
        () -> getFileSystem().listStatus(root));
  }

  @Test
  public void testClosedListFile() throws Exception {
    intercept(IOException.class, E_FS_CLOSED,
        () -> getFileSystem().listFiles(root, false));
  }

  @Test
  public void testClosedListLocatedStatus() throws Exception {
    intercept(IOException.class, E_FS_CLOSED,
        () -> getFileSystem().listLocatedStatus(root));
  }

  @Test
  public void testClosedCreate() throws Exception {
    intercept(IOException.class, E_FS_CLOSED,
        () -> getFileSystem().create(path("to-create")).close());
  }

  @Test
  public void testClosedDelete() throws Exception {
    intercept(IOException.class, E_FS_CLOSED,
        () ->  getFileSystem().delete(path("to-delete"), false));
  }

  @Test
  public void testClosedOpen() throws Exception {
    intercept(IOException.class, E_FS_CLOSED,
        () ->  getFileSystem().open(path("to-open")));
  }

}
