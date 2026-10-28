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
package org.apache.hadoop.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that {@link FileContext.Util#copy(Path, Path)} rejects copying a
 * directory into its own subdirectory ({@code checkDependencies}) when both
 * paths are on the same file system - i.e. same scheme AND same authority,
 * such as the same HDFS cluster - while still allowing it across different
 * authorities (different clusters).
 */
public class TestFileContextCopyDependencies {

  private static final int DEFAULT_PORT = 1234;

  private static Configuration getTestConfiguration() {
    Configuration conf = new Configuration(false);
    conf.set("fs.defaultFS", "file:///");
    conf.setClass("fs.AbstractFileSystem.file.impl",
        org.apache.hadoop.fs.local.LocalFs.class, AbstractFileSystem.class);
    conf.setClass("fs.AbstractFileSystem.dummy.impl",
        DummyAuthorityFileSystem.class, AbstractFileSystem.class);
    return conf;
  }

  @Test
  public void testCopyToSubdirectoryOfSameAuthority() throws Exception {
    FileContext fc = FileContext.getFileContext(getTestConfiguration());
    Path src = new Path("dummy://cluster-a/src");
    Path dst = new Path("dummy://cluster-a/src/subdir");
    IOException e = assertThrows(IOException.class,
        () -> fc.util().copy(src, dst));
    assertTrue(e.getMessage().contains("to its subdirectory"),
        "Expected 'Cannot copy ... to its subdirectory' but got: "
            + e.getMessage());
  }

  @Test
  public void testCopyToSubdirectoryOfDifferentAuthority() throws Exception {
    FileContext fc = FileContext.getFileContext(getTestConfiguration());
    Path src = new Path("dummy://cluster-a/src");
    Path dst = new Path("dummy://cluster-b/src/subdir");
    // The dependency check must not throw for different authorities; the
    // copy proceeds until the stub file system rejects the first write
    // operation (mkdir) on the destination.
    UnsupportedOperationException e = assertThrows(
        UnsupportedOperationException.class, () -> fc.util().copy(src, dst));
    assertTrue(e.getMessage().contains("mkdir"),
        "Expected copy to proceed past the dependency check to mkdir, "
            + "but got: " + e);
  }

  @Test
  public void testCopyToItselfSameAuthority() throws Exception {
    FileContext fc = FileContext.getFileContext(getTestConfiguration());
    Path src = new Path("dummy://cluster-a/src");
    IOException e = assertThrows(IOException.class,
        () -> fc.util().copy(src, src));
    assertTrue(e.getMessage().contains("to itself"),
        "Expected 'Cannot copy ... to itself' but got: " + e.getMessage());
  }

  /**
   * The local file system has a null authority; two null authorities are
   * the same file system, so the guard must keep firing for local copies.
   */
  @Test
  public void testCopyToItsSubdirectoryOnLocalFs() throws Exception {
    FileContext fc = FileContext.getLocalFSFileContext();
    Path dir = new FileContextTestHelper("testCopyToItsSubdirectoryOnLocalFs")
        .getTestRootPath(fc, "dir");
    fc.mkdir(dir, FsPermission.getDirDefault(), true);
    try {
      Path sub = new Path(dir, "sub");
      IOException e = assertThrows(IOException.class,
          () -> fc.util().copy(dir, sub));
      assertTrue(e.getMessage().contains("to its subdirectory"),
          "Expected 'Cannot copy ... to its subdirectory' but got: "
              + e.getMessage());
    } finally {
      fc.delete(dir, true);
    }
  }

  /**
   * A stub file system that accepts any authority it is instantiated with
   * and exposes a single existing directory "/src". Data operations are
   * unsupported: {@link FileContext.Util#copy(Path, Path)} invokes
   * getFileStatus on src and dst and the dependency check before any data
   * transfer, so only those are needed.
   */
  private static class DummyAuthorityFileSystem extends AbstractFileSystem {

    private static final String DIR_PATH = "/src";

    DummyAuthorityFileSystem(URI uri, Configuration conf)
        throws URISyntaxException {
      super(uri, "dummy", true, DEFAULT_PORT);
    }

    @Override
    public int getUriDefaultPort() {
      return DEFAULT_PORT;
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      if (DIR_PATH.equals(f.toUri().getPath())) {
        return new FileStatus(0, true, 1, 0, 0, 0, FsPermission.getDefault(),
            "dummy", "dummy", f);
      }
      throw new FileNotFoundException(f.toString());
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
      if (DIR_PATH.equals(f.toUri().getPath())) {
        return new FileStatus[0];
      }
      throw new FileNotFoundException(f.toString());
    }

    @Override
    public void mkdir(Path dir, FsPermission permission, boolean createParent)
        throws IOException {
      throw new UnsupportedOperationException("mkdir: " + dir);
    }

    @Override
    public FSDataOutputStream createInternal(Path f,
        EnumSet<CreateFlag> flag, FsPermission absolutePermission,
        int bufferSize, short replication, long blockSize,
        Progressable progress, ChecksumOpt checksumOpt, boolean createParent)
        throws IOException {
      throw new UnsupportedOperationException("createInternal: " + f);
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
      throw new UnsupportedOperationException("open: " + f);
    }

    @Override
    public boolean delete(Path f, boolean recursive)
        throws AccessControlException, FileNotFoundException,
        UnresolvedLinkException, IOException {
      throw new UnsupportedOperationException("delete: " + f);
    }

    @Override
    public void renameInternal(Path src, Path dst) throws IOException {
      throw new UnsupportedOperationException(
          "renameInternal: " + src + " -> " + dst);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(Path f, long start, long len)
        throws IOException {
      throw new UnsupportedOperationException("getFileBlockLocations: " + f);
    }

    @Override
    public FileChecksum getFileChecksum(Path f) throws IOException {
      throw new UnsupportedOperationException("getFileChecksum: " + f);
    }

    @Override
    public FsStatus getFsStatus() throws IOException {
      throw new UnsupportedOperationException("getFsStatus");
    }

    @Override
    @Deprecated
    public FsServerDefaults getServerDefaults() throws IOException {
      throw new UnsupportedOperationException("getServerDefaults");
    }

    @Override
    public void setOwner(Path f, String username, String groupname)
        throws IOException {
      throw new UnsupportedOperationException("setOwner: " + f);
    }

    @Override
    public void setPermission(Path f, FsPermission permission)
        throws IOException {
      throw new UnsupportedOperationException("setPermission: " + f);
    }

    @Override
    public boolean setReplication(Path f, short replication)
        throws IOException {
      throw new UnsupportedOperationException("setReplication: " + f);
    }

    @Override
    public void setTimes(Path f, long mtime, long atime) throws IOException {
      throw new UnsupportedOperationException("setTimes: " + f);
    }

    @Override
    public void setVerifyChecksum(boolean verifyChecksum) throws IOException {
      throw new UnsupportedOperationException("setVerifyChecksum");
    }
  }
}
