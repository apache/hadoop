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
package org.apache.hadoop.fs.viewfs;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * 
 * Test the ViewFileSystemBaseTest using a viewfs with authority: 
 *    viewfs://mountTableName/
 *    ie the authority is used to load a mount table.
 *    The authority name used is "default"
 *
 */

public class TestViewFileSystemLocalFileSystem extends ViewFileSystemBaseTest {
  private static final Log LOG =
      LogFactory.getLog(TestViewFileSystemLocalFileSystem.class);

  @Override
  @Before
  public void setUp() throws Exception {
    // create the test root on local_fs
    fsTarget = FileSystem.getLocal(new Configuration());
    super.setUp();
    
  }

  @Test
  public void testNflyWriteSimple() throws IOException {
    LOG.info("Starting testNflyWriteSimple");
    final URI[] testUris = new URI[] {
        URI.create(targetTestRoot + "/nfwd1"),
        URI.create(targetTestRoot + "/nfwd2")
    };
    final String testFileName = "test.txt";
    final Configuration testConf = new Configuration(conf);
    final String testString = "Hello Nfly!";
    final Path nflyRoot = new Path("/nflyroot");
    ConfigUtil.addLinkNfly(testConf, nflyRoot.toString(), testUris);
    final FileSystem nfly = FileSystem.get(URI.create("viewfs:///"), testConf);

    final FSDataOutputStream fsDos = nfly.create(
        new Path(nflyRoot, "test.txt"));
    try {
      fsDos.writeUTF(testString);
    } finally {
      fsDos.close();
    }

    FileStatus[] statuses = nfly.listStatus(nflyRoot);

    FileSystem lfs = FileSystem.getLocal(testConf);
    for (final URI testUri : testUris) {
      final Path testFile = new Path(new Path(testUri), testFileName);
      assertTrue(testFile + " should exist!",  lfs.exists(testFile));
      final FSDataInputStream fsdis = lfs.open(testFile);
      try {
        assertEquals("Wrong file content", testString, fsdis.readUTF());
      } finally {
        fsdis.close();
      }
    }
  }


  @Test
  public void testNflyInvalidMinReplication() throws Exception {
    LOG.info("Starting testNflyInvalidMinReplication");
    final URI[] testUris = new URI[] {
        URI.create(targetTestRoot + "/nfwd1"),
        URI.create(targetTestRoot + "/nfwd2")
    };

    final Configuration conf = new Configuration();
    ConfigUtil.addLinkNfly(conf, "mt", "/nflyroot", "minReplication=4",
        testUris);
    try {
      FileSystem.get(URI.create("viewfs://mt/"), conf);
      fail("Expected bad minReplication exception.");
    } catch (IOException ioe) {
      assertTrue("No minReplication message",
          ioe.getMessage().contains("Minimum replication"));
    }
  }


  @Override
  @After
  public void tearDown() throws Exception {
    fsTarget.delete(fileSystemTestHelper.getTestRootPath(fsTarget), true);
    super.tearDown();
  }
}
