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

package org.apache.hadoop.fs.s3a.performance;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_LIST_REQUEST;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_METADATA_REQUESTS;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.FILESTATUS_DIR_PROBE_L;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.FILESTATUS_FILE_PROBE_H;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.FILESTATUS_FILE_PROBE_L;

/**
 * Use metrics to assert about the cost of mkdirs.
 * Parameterized directory marker keep vs delete
 */
@RunWith(Parameterized.class)
public class ITestS3AMkdirCost extends AbstractS3ACostTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3AMkdirCost.class);

  /**
   * Parameterization.
   */
  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {"keep-markers", true},
        {"delete-markers", false}
    });
  }

  public ITestS3AMkdirCost(final String name,
      final boolean keepMarkers) {
    super(false, true, false);
  }

  /**
   * Common operation which should be low cost as possible.
   */
  @Test
  public void testMkdirOverDir() throws Throwable {
    describe("create a dir over a dir");
    S3AFileSystem fs = getFileSystem();
    // create base dir with marker
    Path baseDir = dir(methodPath());

    // create the child; only assert on HEAD/GET IO
    verifyMetrics(() -> fs.mkdirs(baseDir),
        // full probe on dest plus list only on parent.
        with(OBJECT_METADATA_REQUESTS,
            0),
        with(OBJECT_LIST_REQUEST,  FILESTATUS_DIR_PROBE_L));
  }

  /**
   * Mkdir with a parent dir will check dest (list+HEAD)
   * then do a list on the parent to find the marker.
   * Once the dir is created, creating a sibling will
   * have the same cost.
   */
  @Test
  public void testMkdirWithParent() throws Throwable {
    describe("create a dir under a dir with a parent");
    S3AFileSystem fs = getFileSystem();
    // create base dir with marker
    Path baseDir = dir(methodPath());
    Path childDir = new Path(baseDir, "child");

    // create the child; only assert on HEAD/GET IO
    verifyMetrics(() -> fs.mkdirs(childDir),
        // full probe on dest plus list only on parent.
        with(OBJECT_METADATA_REQUESTS,
            FILESTATUS_FILE_PROBE_H),

        with(OBJECT_LIST_REQUEST,
            FILESTATUS_FILE_PROBE_L + 2 * FILESTATUS_DIR_PROBE_L));

    // now include a sibling; cost will be the same.
    Path sibling = new Path(baseDir, "sibling");
    verifyMetrics(() -> fs.mkdirs(sibling),
        // full probe on dest plus list only on parent.
        with(OBJECT_METADATA_REQUESTS,
            FILESTATUS_FILE_PROBE_H),

        with(OBJECT_LIST_REQUEST,
            FILESTATUS_FILE_PROBE_L + 2 * FILESTATUS_DIR_PROBE_L));
  }

  /**
   * Mkdir with a grandparent dir will check dest (list+HEAD)
   * then do a list + HEAD on the parent and ultimately find the
   * marker with a list of the parent.
   * That's three list calls and two head requsts.
   * Once the dir is created, creating a sibling will
   * cost less as the list of the parent path will find
   * a directory.
   */
  @Test
  public void testMkdirWithGrandparent() throws Throwable {
    describe("create a dir under a dir with a parent");
    S3AFileSystem fs = getFileSystem();
    // create base dir with marker
    Path baseDir = dir(methodPath());
    Path subDir = new Path(baseDir, "child/grandchild");

    // create the child; only assert on HEAD/GET IO
    verifyMetrics(() -> fs.mkdirs(subDir),
        // full probe on dest plus list only on parent.
        with(OBJECT_METADATA_REQUESTS,
            2 * FILESTATUS_FILE_PROBE_H),

        with(OBJECT_LIST_REQUEST,
            3 * FILESTATUS_DIR_PROBE_L));


    // now include a sibling; cost will be less because
    // now the immediate parent check will succeed on the list call.
    Path sibling = new Path(baseDir, "child/sibling");

    verifyMetrics(() -> fs.mkdirs(sibling),

        // full probe on dest plus list only on parent.
        with(OBJECT_METADATA_REQUESTS,
            FILESTATUS_FILE_PROBE_H),

        with(OBJECT_LIST_REQUEST,
            FILESTATUS_FILE_PROBE_L + 2 * FILESTATUS_DIR_PROBE_L));
  }


  /**
   * When calling mkdir over a file, the list happens first, so
   * is always billed for.
   * @throws Throwable failure.
   */
  @Test
  public void testMkdirOverFile() throws Throwable {
    describe("create a dir over a file; expect dir and file probes");
    S3AFileSystem fs = getFileSystem();
    // create base dir with marker
    Path baseDir = dir(methodPath());
    Path childDir = new Path(baseDir, "child");
    touch(fs, childDir);

    // create the child; only assert on HEAD/GET IO
    verifyMetricsIntercepting(
        FileAlreadyExistsException.class, "",
        () -> fs.mkdirs(childDir),
        // full probe on dest plus list only on parent.
        with(OBJECT_METADATA_REQUESTS,
            FILESTATUS_FILE_PROBE_H),
        with(OBJECT_LIST_REQUEST, FILESTATUS_DIR_PROBE_L));
  }

}
