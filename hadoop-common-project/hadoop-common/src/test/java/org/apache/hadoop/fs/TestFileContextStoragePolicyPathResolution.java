/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests that FileContext hands the resolved (working-dir qualified and
 * symlink-resolved) absolute path to the AbstractFileSystem for the
 * storage policy APIs, like it does for every other operation.
 */
public class TestFileContextStoragePolicyPathResolution {
  static {
    FileSystem.enableSymlinks();
  }

  private static final String SCHEME = "stub";
  private static final Path QUALIFIED_WD = new Path("stub:///base/dir");
  private static final Path QUALIFIED_FILE = new Path("stub:///base/dir/file-warm");

  /**
   * Minimal AbstractFileSystem that records the path arguments of the
   * storage policy calls. It simulates a symlink by throwing
   * UnresolvedLinkException when the recorded path is addressed.
   */
  public static class RecordingStoragePolicyFs extends AbstractFileSystem {

    private static volatile Path lastSetStoragePolicyPath;
    private static volatile Path lastUnsetStoragePolicyPath;
    private static volatile Path lastSatisfyStoragePolicyPath;
    private static volatile String lastPolicyName;

    /** Simulated symlink; null when the symlink test is not active. */
    private static volatile Path linkPath;
    private static volatile Path linkTarget;

    public RecordingStoragePolicyFs(URI uri, Configuration conf)
        throws URISyntaxException {
      super(uri, SCHEME, false, -1);
    }

    static void reset() {
      lastSetStoragePolicyPath = null;
      lastUnsetStoragePolicyPath = null;
      lastSatisfyStoragePolicyPath = null;
      lastPolicyName = null;
      linkPath = null;
      linkTarget = null;
    }

    private static void checkLink(Path f) throws IOException {
      if (linkPath != null && f.equals(linkPath)) {
        throw new UnresolvedLinkException("Unresolved link " + f);
      }
    }

    @Override
    public void setStoragePolicy(Path path, String policyName)
        throws IOException {
      checkLink(path);
      lastSetStoragePolicyPath = path;
      lastPolicyName = policyName;
    }

    @Override
    public void unsetStoragePolicy(Path src) throws IOException {
      checkLink(src);
      lastUnsetStoragePolicyPath = src;
    }

    @Override
    public void satisfyStoragePolicy(Path path) throws IOException {
      checkLink(path);
      lastSatisfyStoragePolicyPath = path;
    }

    @Override
    public Path getLinkTarget(Path f) throws IOException {
      return linkTarget;
    }

    @Override
    public Path getInitialWorkingDirectory() {
      return new Path("/").makeQualified(this.getUri(), null);
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      return new FileStatus(0, true, 1, 1024, 0, f);
    }

    @Override
    public int getUriDefaultPort() {
      return -1;
    }

    @Override
    public FsServerDefaults getServerDefaults() throws IOException {
      return new FsServerDefaults();
    }

    @Override
    public FSDataOutputStream createInternal(Path f,
        EnumSet<CreateFlag> flag, FsPermission absolutePermission,
        int bufferSize, short replication, long blockSize,
        Progressable progress, ChecksumOpt checksumOpt, boolean createParent)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
      return false;
    }

    @Override
    public BlockLocation[] getFileBlockLocations(Path f, long start, long len)
        throws IOException {
      return new BlockLocation[0];
    }

    @Override
    public FileChecksum getFileChecksum(Path f) throws IOException {
      return null;
    }

    @Override
    public FsStatus getFsStatus() throws IOException {
      return new FsStatus(0, 0, 0);
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException {
      return new FileStatus[0];
    }

    @Override
    public void mkdir(Path dir, FsPermission permission,
        boolean createParent) throws IOException {
      // deliberately empty
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
      throw new FileNotFoundException();
    }

    @Override
    public void renameInternal(Path src, Path dst) throws IOException {
      // deliberately empty
    }

    @Override
    public boolean setReplication(Path f, short replication)
        throws IOException {
      return false;
    }

    @Override
    public void setPermission(Path f, FsPermission permission)
        throws IOException {
      // deliberately empty
    }

    @Override
    public void setOwner(Path f, String username, String groupname)
        throws IOException {
      // deliberately empty
    }

    @Override
    public void setTimes(Path f, long mtime, long atime) throws IOException {
      // deliberately empty
    }

    @Override
    public void setVerifyChecksum(boolean verifyChecksum)
        throws IOException {
      // deliberately empty
    }
  }

  private FileContext fc;

  @BeforeEach
  public void setup() throws IOException {
    RecordingStoragePolicyFs.reset();
    Configuration conf = new Configuration();
    conf.set("fs.AbstractFileSystem." + SCHEME + ".impl",
        RecordingStoragePolicyFs.class.getName());
    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, SCHEME + ":///");
    fc = FileContext.getFileContext(conf);
    fc.setWorkingDirectory(new Path("/base/dir"));
  }

  @Test
  @Timeout(value = 30)
  public void testSetStoragePolicyReceivesQualifiedPath() throws IOException {
    fc.setStoragePolicy(new Path("file-warm"), "WARM");
    assertEquals(QUALIFIED_FILE,
        RecordingStoragePolicyFs.lastSetStoragePolicyPath,
        "setStoragePolicy must receive the working-dir qualified path");
    assertEquals("WARM", RecordingStoragePolicyFs.lastPolicyName);
  }

  @Test
  @Timeout(value = 30)
  public void testUnsetStoragePolicyReceivesQualifiedPath()
      throws IOException {
    fc.unsetStoragePolicy(new Path("file-warm"));
    assertEquals(QUALIFIED_FILE,
        RecordingStoragePolicyFs.lastUnsetStoragePolicyPath,
        "unsetStoragePolicy must receive the working-dir qualified path");
  }

  @Test
  @Timeout(value = 30)
  public void testSatisfyStoragePolicyReceivesQualifiedPath()
      throws IOException {
    fc.satisfyStoragePolicy(new Path("file-warm"));
    assertEquals(QUALIFIED_FILE,
        RecordingStoragePolicyFs.lastSatisfyStoragePolicyPath,
        "satisfyStoragePolicy must receive the working-dir qualified path");
  }

  @Test
  @Timeout(value = 30)
  public void testSetStoragePolicyResolvesSymlink() throws IOException {
    RecordingStoragePolicyFs.linkPath = new Path("stub:///base/dir/link");
    RecordingStoragePolicyFs.linkTarget = new Path("/base/dir/target-file");
    fc.setStoragePolicy(new Path("link"), "COLD");
    assertEquals(new Path("stub:///base/dir/target-file"),
        RecordingStoragePolicyFs.lastSetStoragePolicyPath,
        "setStoragePolicy must receive the symlink-resolved path");
  }
}
