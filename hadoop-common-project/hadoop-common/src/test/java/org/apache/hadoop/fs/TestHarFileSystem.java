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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.fs.Options.ChecksumOpt;
import static org.apache.hadoop.fs.Options.CreateOpts;
import static org.apache.hadoop.fs.Options.Rename;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("deprecation")
public class TestHarFileSystem {
  public static final Log LOG = LogFactory.getLog(TestHarFileSystem.class);

  /**
   * FileSystem methods that must not be overwritten by
   * {@link HarFileSystem}. Either because there is a default implementation
   * already available or because it is not relevant.
   */
  @SuppressWarnings("deprecation")
  private interface MustNotImplement {
    public BlockLocation[] getFileBlockLocations(Path p, long start, long len);
    public long getLength(Path f);
    public FSDataOutputStream append(Path f, int bufferSize);
    public void rename(Path src, Path dst, Rename... options);
    public boolean exists(Path f);
    public boolean isDirectory(Path f);
    public boolean isFile(Path f);
    public boolean createNewFile(Path f);

    public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
        boolean overwrite, int bufferSize, short replication, long blockSize,
        Progressable progress) throws IOException;

    public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
        EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize,
        Progressable progress) throws IOException;

    public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
        EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize,
        Progressable progress, ChecksumOpt checksumOpt);

    public boolean mkdirs(Path f);
    public FSDataInputStream open(Path f);
    public FSDataOutputStream create(Path f);
    public FSDataOutputStream create(Path f, boolean overwrite);
    public FSDataOutputStream create(Path f, Progressable progress);
    public FSDataOutputStream create(Path f, short replication);
    public FSDataOutputStream create(Path f, short replication,
        Progressable progress);

    public FSDataOutputStream create(Path f, boolean overwrite,
        int bufferSize);

    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize,
        Progressable progress);

    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize,
        short replication, long blockSize);

    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize,
        short replication, long blockSize, Progressable progress);

    public FSDataOutputStream create(Path f, FsPermission permission,
        EnumSet<CreateFlag> flags, int bufferSize, short replication,
        long blockSize, Progressable progress) throws IOException;

    public FSDataOutputStream create(Path f, FsPermission permission,
        EnumSet<CreateFlag> flags, int bufferSize, short replication,
        long blockSize, Progressable progress, ChecksumOpt checksumOpt)
        throws IOException;

    public String getName();
    public boolean delete(Path f);
    public short getReplication(Path src);
    public void processDeleteOnExit();
    public ContentSummary getContentSummary(Path f);
    public FsStatus getStatus();
    public FileStatus[] listStatus(Path f, PathFilter filter);
    public FileStatus[] listStatus(Path[] files);
    public FileStatus[] listStatus(Path[] files, PathFilter filter);
    public FileStatus[] globStatus(Path pathPattern);
    public FileStatus[] globStatus(Path pathPattern, PathFilter filter);

    public Iterator<LocatedFileStatus> listFiles(Path path,
        boolean isRecursive);

    public Iterator<LocatedFileStatus> listLocatedStatus(Path f);
    public Iterator<LocatedFileStatus> listLocatedStatus(Path f,
        PathFilter filter);
    public void copyFromLocalFile(Path src, Path dst);
    public void moveFromLocalFile(Path[] srcs, Path dst);
    public void moveFromLocalFile(Path src, Path dst);
    public void copyToLocalFile(Path src, Path dst);
    public void copyToLocalFile(boolean delSrc, Path src, Path dst,
        boolean useRawLocalFileSystem);
    public void moveToLocalFile(Path src, Path dst);
    public long getBlockSize(Path f);
    public FSDataOutputStream primitiveCreate(Path f,
        EnumSet<CreateFlag> createFlag, CreateOpts... opts);
    public void primitiveMkdir(Path f, FsPermission absolutePermission,
        boolean createParent);
    public int getDefaultPort();
    public String getCanonicalServiceName();
    public Token<?> getDelegationToken(String renewer) throws IOException;
    public FileChecksum getFileChecksum(Path f) throws IOException;
    public boolean deleteOnExit(Path f) throws IOException;
    public boolean cancelDeleteOnExit(Path f) throws IOException;
    public Token<?>[] addDelegationTokens(String renewer, Credentials creds)
        throws IOException;
    public Path fixRelativePart(Path p);
    public void concat(Path trg, Path [] psrcs) throws IOException;
    public FSDataOutputStream primitiveCreate(Path f,
        FsPermission absolutePermission, EnumSet<CreateFlag> flag, int bufferSize,
        short replication, long blockSize, Progressable progress,
        ChecksumOpt checksumOpt) throws IOException;
    public boolean primitiveMkdir(Path f, FsPermission absolutePermission)
        throws IOException;
    public RemoteIterator<Path> listCorruptFileBlocks(Path path)
        throws IOException;
    public void copyFromLocalFile(boolean delSrc, Path src, Path dst)
        throws IOException;
    public void createSymlink(Path target, Path link, boolean createParent)
        throws IOException;
    public FileStatus getFileLinkStatus(Path f) throws IOException;
    public boolean supportsSymlinks();
    public Path getLinkTarget(Path f) throws IOException;
    public Path resolveLink(Path f) throws IOException;
    public void setVerifyChecksum(boolean verifyChecksum);
    public void setWriteChecksum(boolean writeChecksum);
    public Path createSnapshot(Path path, String snapshotName) throws
        IOException;
    public void renameSnapshot(Path path, String snapshotOldName,
        String snapshotNewName) throws IOException;
    public void deleteSnapshot(Path path, String snapshotName)
        throws IOException;

    public void modifyAclEntries(Path path, List<AclEntry> aclSpec)
        throws IOException;

    public void removeAclEntries(Path path, List<AclEntry> aclSpec)
        throws IOException;

    public void removeDefaultAcl(Path path) throws IOException;

    public void removeAcl(Path path) throws IOException;

    public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException;

    public void setXAttr(Path path, String name, byte[] value)
        throws IOException;

    public void setXAttr(Path path, String name, byte[] value,
        EnumSet<XAttrSetFlag> flag) throws IOException;

    public byte[] getXAttr(Path path, String name) throws IOException;

    public Map<String, byte[]> getXAttrs(Path path) throws IOException;

    public Map<String, byte[]> getXAttrs(Path path, List<String> names)
        throws IOException;

    public List<String> listXAttrs(Path path) throws IOException;

    public void removeXAttr(Path path, String name) throws IOException;

    public AclStatus getAclStatus(Path path) throws IOException;

    public void access(Path path, FsAction mode) throws IOException;
  }

  @Test
  public void testHarUri() {
    final Configuration conf = new Configuration();
    checkInvalidPath("har://hdfs-/foo.har", conf);
    checkInvalidPath("har://hdfs/foo.har", conf);
    checkInvalidPath("har://-hdfs/foo.har", conf);
    checkInvalidPath("har://-/foo.har", conf);
    checkInvalidPath("har://127.0.0.1-/foo.har", conf);
    checkInvalidPath("har://127.0.0.1/foo.har", conf);
  }

  static void checkInvalidPath(String s, Configuration conf) {
    System.out.println("\ncheckInvalidPath: " + s);
    final Path p = new Path(s);
    try {
      p.getFileSystem(conf);
      Assert.fail(p + " is an invalid path.");
    } catch (IOException e) {
      // Expected
    }
  }

  @Test
  public void testFileChecksum() throws Exception {
    final Path p = new Path("har://file-localhost/foo.har/file1");
    final HarFileSystem harfs = new HarFileSystem();
    try {
      Assert.assertEquals(null, harfs.getFileChecksum(p));
    } finally {
      if (harfs != null) {
        harfs.close();
      }
    }
  }

  /**
   * Test how block location offsets and lengths are fixed.
   */
  @Test
  public void testFixBlockLocations() {
    // do some tests where start == 0
    {
      // case 1: range starts before current har block and ends after
      BlockLocation[] b = { new BlockLocation(null, null, 10, 10) };
      HarFileSystem.fixBlockLocations(b, 0, 20, 5);
      assertEquals(b[0].getOffset(), 5);
      assertEquals(b[0].getLength(), 10);
    }
    {
      // case 2: range starts in current har block and ends after
      BlockLocation[] b = { new BlockLocation(null, null, 10, 10) };
      HarFileSystem.fixBlockLocations(b, 0, 20, 15);
      assertEquals(b[0].getOffset(), 0);
      assertEquals(b[0].getLength(), 5);
    }
    {
      // case 3: range starts before current har block and ends in
      // current har block
      BlockLocation[] b = { new BlockLocation(null, null, 10, 10) };
      HarFileSystem.fixBlockLocations(b, 0, 10, 5);
      assertEquals(b[0].getOffset(), 5);
      assertEquals(b[0].getLength(), 5);
    }
    {
      // case 4: range starts and ends in current har block
      BlockLocation[] b = { new BlockLocation(null, null, 10, 10) };
      HarFileSystem.fixBlockLocations(b, 0, 6, 12);
      assertEquals(b[0].getOffset(), 0);
      assertEquals(b[0].getLength(), 6);
    }

    // now try a range where start == 3
    {
      // case 5: range starts before current har block and ends after
      BlockLocation[] b = { new BlockLocation(null, null, 10, 10) };
      HarFileSystem.fixBlockLocations(b, 3, 20, 5);
      assertEquals(b[0].getOffset(), 5);
      assertEquals(b[0].getLength(), 10);
    }
    {
      // case 6: range starts in current har block and ends after
      BlockLocation[] b = { new BlockLocation(null, null, 10, 10) };
      HarFileSystem.fixBlockLocations(b, 3, 20, 15);
      assertEquals(b[0].getOffset(), 3);
      assertEquals(b[0].getLength(), 2);
    }
    {
      // case 7: range starts before current har block and ends in
      // current har block
      BlockLocation[] b = { new BlockLocation(null, null, 10, 10) };
      HarFileSystem.fixBlockLocations(b, 3, 7, 5);
      assertEquals(b[0].getOffset(), 5);
      assertEquals(b[0].getLength(), 5);
    }
    {
      // case 8: range starts and ends in current har block
      BlockLocation[] b = { new BlockLocation(null, null, 10, 10) };
      HarFileSystem.fixBlockLocations(b, 3, 3, 12);
      assertEquals(b[0].getOffset(), 3);
      assertEquals(b[0].getLength(), 3);
    }

    // test case from JIRA MAPREDUCE-1752
    {
      BlockLocation[] b = { new BlockLocation(null, null, 512, 512),
                            new BlockLocation(null, null, 1024, 512) };
      HarFileSystem.fixBlockLocations(b, 0, 512, 896);
      assertEquals(b[0].getOffset(), 0);
      assertEquals(b[0].getLength(), 128);
      assertEquals(b[1].getOffset(), 128);
      assertEquals(b[1].getLength(), 384);
    }
  }

  @Test
  public void testInheritedMethodsImplemented() throws Exception {
    int errors = 0;
    for (Method m : FileSystem.class.getDeclaredMethods()) {
      if (Modifier.isStatic(m.getModifiers()) ||
          Modifier.isPrivate(m.getModifiers()) ||
          Modifier.isFinal(m.getModifiers())) {
        continue;
      }

      try {
        MustNotImplement.class.getMethod(m.getName(), m.getParameterTypes());
        try {
          HarFileSystem.class.getDeclaredMethod(m.getName(), m.getParameterTypes());
          LOG.error("HarFileSystem MUST not implement " + m);
          errors++;
        } catch (NoSuchMethodException ex) {
          // Expected
        }
      } catch (NoSuchMethodException exc) {
        try {
          HarFileSystem.class.getDeclaredMethod(m.getName(), m.getParameterTypes());
        } catch (NoSuchMethodException exc2) {
          LOG.error("HarFileSystem MUST implement " + m);
          errors++;
        }
      }
    }
    assertTrue((errors + " methods were not overridden correctly - see log"),
        errors <= 0);
  }
}
