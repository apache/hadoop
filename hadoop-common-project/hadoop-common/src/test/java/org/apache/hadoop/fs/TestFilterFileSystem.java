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

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.util.EnumSet;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFilterFileSystem {

  private static final Log LOG = FileSystem.LOG;
  private static final Configuration conf = new Configuration();

  @BeforeClass
  public static void setup() {
    conf.set("fs.flfs.impl", FilterLocalFileSystem.class.getName());
    conf.setBoolean("fs.flfs.impl.disable.cache", true);
    conf.setBoolean("fs.file.impl.disable.cache", true);
  }

  /**
   * FileSystem methods that must not be overwritten by
   * {@link FilterFileSystem}. Either because there is a default implementation
   * already available or because it is not relevant.
   */
  public static interface MustNotImplement {
    public BlockLocation[] getFileBlockLocations(Path p,  long start,
        long len);
    public FSDataOutputStream append(Path f) throws IOException;
    public FSDataOutputStream append(Path f, int bufferSize) throws
        IOException;
    public long getLength(Path f);
    public boolean exists(Path f);
    public boolean isDirectory(Path f);
    public boolean isFile(Path f);
    public boolean createNewFile(Path f);

    public FSDataOutputStream createNonRecursive(Path f,
        boolean overwrite, int bufferSize, short replication, long blockSize,
        Progressable progress) throws IOException;

    public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
        boolean overwrite, int bufferSize, short replication, long blockSize,
        Progressable progress) throws IOException;

    public boolean mkdirs(Path f);
    public FSDataInputStream open(Path f);
    public FSDataOutputStream create(Path f);
    public FSDataOutputStream create(Path f, boolean overwrite);
    public FSDataOutputStream create(Path f, Progressable progress);
    public FSDataOutputStream create(Path f, short replication);
    public FSDataOutputStream create(Path f, short replication,
        Progressable progress);
    public FSDataOutputStream create(Path f,  boolean overwrite,
        int bufferSize);
    public FSDataOutputStream create(Path f,  boolean overwrite,
        int bufferSize, Progressable progress);
    public FSDataOutputStream create(Path f,  boolean overwrite,
        int bufferSize, short replication, long blockSize);
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize,
        short replication, long blockSize, Progressable progress);
    public FSDataOutputStream create(Path f, FsPermission permission,
        EnumSet<CreateFlag> flags, int bufferSize, short replication,
        long blockSize, Progressable progress);
    public String getName();
    public boolean delete(Path f);
    public short getReplication(Path src);
    public void processDeleteOnExit();
    public FsStatus getStatus();
    public FileStatus[] listStatus(Path f, PathFilter filter);
    public FileStatus[] listStatusBatch(Path f, byte[] token);
    public FileStatus[] listStatus(Path[] files);
    public FileStatus[] listStatus(Path[] files, PathFilter filter);
    public FileStatus[] globStatus(Path pathPattern);
    public FileStatus[] globStatus(Path pathPattern, PathFilter filter);
    public Iterator<LocatedFileStatus> listFiles(Path path,
        boolean isRecursive);
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
    public boolean deleteOnExit(Path f) throws IOException;
    public boolean cancelDeleteOnExit(Path f) throws IOException;
    public Token<?>[] addDelegationTokens(String renewer, Credentials creds)
        throws IOException;
    public String getScheme();
    public Path fixRelativePart(Path p);
    public ContentSummary getContentSummary(Path f);
    public QuotaUsage getQuotaUsage(Path f);
    StorageStatistics getStorageStatistics();
  }

  @Test
  public void testFilterFileSystem() throws Exception {
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
          FilterFileSystem.class.getDeclaredMethod(m.getName(),
              m.getParameterTypes());
          LOG.error("FilterFileSystem MUST NOT implement " + m);
          errors++;
        } catch (NoSuchMethodException ex) {
          // Expected
        }
      } catch (NoSuchMethodException exc) {
        try{
          FilterFileSystem.class.getDeclaredMethod(m.getName(), m.getParameterTypes());
        } catch(NoSuchMethodException exc2){
          LOG.error("FilterFileSystem MUST implement " + m);
          errors++;
        }
      }
    }
    assertTrue((errors + " methods were not overridden correctly - see" +
        " log"), errors <= 0);
  }
  
  @Test
  public void testFilterEmbedInit() throws Exception {
    FileSystem mockFs = createMockFs(false); // no conf = need init
    checkInit(new FilterFileSystem(mockFs), true);
  }

  @Test
  public void testFilterEmbedNoInit() throws Exception {
    FileSystem mockFs = createMockFs(true); // has conf = skip init
    checkInit(new FilterFileSystem(mockFs), false);
  }

  @Test
  public void testLocalEmbedInit() throws Exception {
    FileSystem mockFs = createMockFs(false); // no conf = need init
    checkInit(new LocalFileSystem(mockFs), true);
  }  
  
  @Test
  public void testLocalEmbedNoInit() throws Exception {
    FileSystem mockFs = createMockFs(true); // has conf = skip init
    checkInit(new LocalFileSystem(mockFs), false);
  }
  
  private FileSystem createMockFs(boolean useConf) {
    FileSystem mockFs = mock(FileSystem.class);
    when(mockFs.getUri()).thenReturn(URI.create("mock:/"));
    when(mockFs.getConf()).thenReturn(useConf ? conf : null);
    return mockFs;
  }

  @Test
  public void testGetLocalFsSetsConfs() throws Exception {
    LocalFileSystem lfs = FileSystem.getLocal(conf);
    checkFsConf(lfs, conf, 2);
  }

  @Test
  public void testGetFilterLocalFsSetsConfs() throws Exception {
    FilterFileSystem flfs =
        (FilterFileSystem) FileSystem.get(URI.create("flfs:/"), conf);
    checkFsConf(flfs, conf, 3);
  }

  @Test
  public void testInitLocalFsSetsConfs() throws Exception {
    LocalFileSystem lfs = new LocalFileSystem();
    checkFsConf(lfs, null, 2);
    lfs.initialize(lfs.getUri(), conf);
    checkFsConf(lfs, conf, 2);
  }

  @Test
  public void testInitFilterFsSetsEmbedConf() throws Exception {
    LocalFileSystem lfs = new LocalFileSystem();
    checkFsConf(lfs, null, 2);
    FilterFileSystem ffs = new FilterFileSystem(lfs);
    assertEquals(lfs, ffs.getRawFileSystem());
    checkFsConf(ffs, null, 3);
    ffs.initialize(URI.create("filter:/"), conf);
    checkFsConf(ffs, conf, 3);
  }

  @Test
  public void testInitFilterLocalFsSetsEmbedConf() throws Exception {
    FilterFileSystem flfs = new FilterLocalFileSystem();
    assertEquals(LocalFileSystem.class, flfs.getRawFileSystem().getClass());
    checkFsConf(flfs, null, 3);
    flfs.initialize(URI.create("flfs:/"), conf);
    checkFsConf(flfs, conf, 3);
  }

  @Test
  public void testVerifyChecksumPassthru() {
    FileSystem mockFs = mock(FileSystem.class);
    FileSystem fs = new FilterFileSystem(mockFs);

    fs.setVerifyChecksum(false);
    verify(mockFs).setVerifyChecksum(eq(false));
    reset(mockFs);
    fs.setVerifyChecksum(true);
    verify(mockFs).setVerifyChecksum(eq(true));
  }

  @Test
  public void testWriteChecksumPassthru() {
    FileSystem mockFs = mock(FileSystem.class);
    FileSystem fs = new FilterFileSystem(mockFs);

    fs.setWriteChecksum(false);
    verify(mockFs).setWriteChecksum(eq(false));
    reset(mockFs);
    fs.setWriteChecksum(true);
    verify(mockFs).setWriteChecksum(eq(true));
  }

  @Test
  public void testRenameOptions() throws Exception {
    FileSystem mockFs = mock(FileSystem.class);
    FileSystem fs = new FilterFileSystem(mockFs);
    Path src = new Path("/src");
    Path dst = new Path("/dest");
    Rename opt = Rename.TO_TRASH;
    fs.rename(src, dst, opt);
    verify(mockFs).rename(eq(src), eq(dst), eq(opt));
  }

  private void checkInit(FilterFileSystem fs, boolean expectInit)
      throws Exception {
    URI uri = URI.create("filter:/");
    fs.initialize(uri, conf);
    
    FileSystem embedFs = fs.getRawFileSystem();
    if (expectInit) {
      verify(embedFs, times(1)).initialize(eq(uri), eq(conf));
    } else {
      verify(embedFs, times(0)).initialize(any(URI.class), any(Configuration.class));
    }
  }

  // check the given fs's conf, and all its filtered filesystems
  private void checkFsConf(FileSystem fs, Configuration conf, int expectDepth) {
    int depth = 0;
    while (true) {
      depth++; 
      assertFalse("depth "+depth+">"+expectDepth, depth > expectDepth);
      assertEquals(conf, fs.getConf());
      if (!(fs instanceof FilterFileSystem)) {
        break;
      }
      fs = ((FilterFileSystem) fs).getRawFileSystem();
    }
    assertEquals(expectDepth, depth);
  }
  
  private static class FilterLocalFileSystem extends FilterFileSystem {
    FilterLocalFileSystem() {
      super(new LocalFileSystem());
    }
  }
}
