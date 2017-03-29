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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;

/**
 * The FileStatus is being serialized in MR as jobs are submitted.
 * Since viewfs has overlayed ViewFsFileStatus, we ran into
 * serialization problems. THis test is test the fix.
 */
public class TestViewfsFileStatus {

  private static final File TEST_DIR = GenericTestUtils.getTestDir(
      TestViewfsFileStatus.class.getSimpleName());

  @Test
  public void testFileStatusSerialziation()
      throws IOException, URISyntaxException {
    String testfilename = "testFileStatusSerialziation";
    TEST_DIR.mkdirs();
    File infile = new File(TEST_DIR, testfilename);
    final byte[] content = "dingos".getBytes();

    FileOutputStream fos = null;
    try {
      fos = new FileOutputStream(infile);
      fos.write(content);
    } finally {
      if (fos != null) {
        fos.close();
      }
    }
    assertEquals((long)content.length, infile.length());

    Configuration conf = new Configuration();
    ConfigUtil.addLink(conf, "/foo/bar/baz", TEST_DIR.toURI());
    FileSystem vfs = FileSystem.get(FsConstants.VIEWFS_URI, conf);
    assertEquals(ViewFileSystem.class, vfs.getClass());
    Path path = new Path("/foo/bar/baz", testfilename);
    FileStatus stat = vfs.getFileStatus(path);
    assertEquals(content.length, stat.getLen());
    ContractTestUtils.assertNotErasureCoded(vfs, path);
    assertTrue(path + " should have erasure coding unset in " +
            "FileStatus#toString(): " + stat,
        stat.toString().contains("isErasureCoded=false"));

    // check serialization/deserialization
    DataOutputBuffer dob = new DataOutputBuffer();
    stat.write(dob);
    DataInputBuffer dib = new DataInputBuffer();
    dib.reset(dob.getData(), 0, dob.getLength());
    FileStatus deSer = new FileStatus();
    deSer.readFields(dib);
    assertEquals(content.length, deSer.getLen());
    assertFalse(deSer.isErasureCoded());
  }

  // Tests that ViewFileSystem.getFileChecksum calls res.targetFileSystem
  // .getFileChecksum with res.remainingPath and not with f
  @Test
  public void testGetFileChecksum() throws IOException {
    final Path path = new Path("/tmp/someFile");
    FileSystem mockFS = Mockito.mock(FileSystem.class);
    InodeTree.ResolveResult<FileSystem> res =
      new InodeTree.ResolveResult<FileSystem>(null, mockFS , null,
        new Path("someFile"));
    @SuppressWarnings("unchecked")
    InodeTree<FileSystem> fsState = Mockito.mock(InodeTree.class);
    Mockito.when(fsState.resolve(path.toString(), true)).thenReturn(res);
    ViewFileSystem vfs = Mockito.mock(ViewFileSystem.class);
    vfs.fsState = fsState;

    Mockito.when(vfs.getFileChecksum(path)).thenCallRealMethod();
    Mockito.when(vfs.getUriPath(path)).thenCallRealMethod();
    vfs.getFileChecksum(path);

    Mockito.verify(mockFS).getFileChecksum(new Path("someFile"));
  }

  @AfterClass
  public static void cleanup() throws IOException {
    FileUtil.fullyDelete(TEST_DIR);
  }

}
