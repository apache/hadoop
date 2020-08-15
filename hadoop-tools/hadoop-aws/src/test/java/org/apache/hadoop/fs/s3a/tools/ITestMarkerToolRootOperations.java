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

package org.apache.hadoop.fs.s3a.tools;

import java.io.File;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.s3a.tools.MarkerTool.AUDIT;
import static org.apache.hadoop.fs.s3a.tools.MarkerTool.CLEAN;
import static org.apache.hadoop.fs.s3a.tools.MarkerTool.MARKERS;
import static org.apache.hadoop.fs.s3a.tools.MarkerTool.OPT_OUT;

/**
 * Marker tool tests against the root FS; run in the sequential phase.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ITestMarkerToolRootOperations extends AbstractMarkerToolTest {

  private Path rootPath;

  @Override
  public void setup() throws Exception {
    super.setup();
    rootPath = getFileSystem().makeQualified(new Path("/"));
  }

  @Test
  public void test_100_audit_root_noauth() throws Throwable {
    describe("Run a verbose audit");
    final File audit = tempAuditFile();
    run(MARKERS, V,
        AUDIT,
        m(OPT_OUT), audit,
        rootPath);
    readOutput(audit);
  }

  @Test
  public void test_200_clean_root() throws Throwable {
    describe("Clean the root path");
    final File audit = tempAuditFile();
    run(MARKERS, V,
        CLEAN,
        m(OPT_OUT), audit,
        rootPath);
    readOutput(audit);
  }

}
