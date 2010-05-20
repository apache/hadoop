/**
 * Copyright 2008 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.util;

import junit.framework.TestCase;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.Path;

/**
 * Test requirement that root directory must be a URI
 */
public class TestRootPath extends TestCase {
  private static final Log LOG = LogFactory.getLog(TestRootPath.class);

  /** The test */
  public void testRootPath() {
    try {
      // Try good path
      FSUtils.validateRootPath(new Path("file:///tmp/hbase/hbase"));
    } catch (IOException e) {
      LOG.fatal("Unexpected exception checking valid path:", e);
      fail();
    }
    try {
      // Try good path
      FSUtils.validateRootPath(new Path("hdfs://a:9000/hbase"));
    } catch (IOException e) {
      LOG.fatal("Unexpected exception checking valid path:", e);
      fail();
    }
    try {
      // bad path
      FSUtils.validateRootPath(new Path("/hbase"));
      fail();
    } catch (IOException e) {
      // Expected.
      LOG.info("Got expected exception when checking invalid path:", e);
    }
  }
}