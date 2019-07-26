/**
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

package org.apache.hadoop.mapreduce;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * Test class for MRResource.
 */
public class TestMRResource {

  @Test
  public void testGetResourceSubmissionParentDir() {
    Path jobDir = new Path("/jobdir");
    MRResource jobJar = new MRResource(
        MRResourceType.JOBJAR, "/testjojar", MRResourceVisibility.PUBLIC);
    assertTrue(jobJar.
        getResourceSubmissionParentDir(jobDir)
        .toString()
        .equals("/jobdir/public"));
    MRResource file = new MRResource(
        MRResourceType.FILE, "/file1", MRResourceVisibility.APPLICATION);
    assertTrue(file.getResourceSubmissionParentDir(jobDir).
        toString().equals("/jobdir/application/files"));
    MRResource archive = new MRResource(
        MRResourceType.ARCHIVE, "/archive1", MRResourceVisibility.PRIVATE);
    assertTrue(archive.getResourceSubmissionParentDir(jobDir).
        toString().equals("/jobdir/private/archives"));
    MRResource libjar = new MRResource(
        MRResourceType.LIBJAR, "/jar0", MRResourceVisibility.APPLICATION);
    assertTrue(libjar.getResourceSubmissionParentDir(jobDir).
        toString().equals("/jobdir/application/libjars"));
  }

  @Test
  public void testGetLocalResourceUri() throws Exception {
    Configuration conf = new Configuration();
    // Local resource
    String localPathStr = "/testjojar";
    MRResource jobJar = new MRResource(
        MRResourceType.JOBJAR, localPathStr, MRResourceVisibility.PUBLIC);
    Path expectPath = FileSystem.getLocal(conf).makeQualified(
        new Path(localPathStr));
    assertTrue(jobJar.getResourceUri(conf).toString().equals(
        expectPath.toString()));
  }

  @Test
  public void testGetNonLocalResourceUri() throws Exception {
    Configuration conf = new Configuration();
    String pathStr = "viewfs://nn1/testjojar/";
    MRResource jobJar = new MRResource(
        MRResourceType.JOBJAR, pathStr, MRResourceVisibility.APPLICATION);
    assertTrue(jobJar.getResourceUri(conf).toString().equals(
        "viewfs://nn1/testjojar"));
  }
}
