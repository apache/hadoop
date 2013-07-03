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
import org.apache.hadoop.fs.Options.ChecksumOpt;
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
  
  public static class DontCheck {
    public BlockLocation[] getFileBlockLocations(Path p, 
        long start, long len) { return null; }
    public FsServerDefaults getServerDefaults() { return null; }
    public long getLength(Path f) { return 0; }
    public FSDataOutputStream append(Path f) { return null; }
    public FSDataOutputStream append(Path f, int bufferSize) { return null; }
    public void rename(final Path src, final Path dst, final Rename... options) { }
    public boolean exists(Path f) { return false; }
    public boolean isDirectory(Path f) { return false; }
    public boolean isFile(Path f) { return false; }
    public boolean createNewFile(Path f) { return false; }
    public FSDataOutputStream createNonRecursive(Path f,
        boolean overwrite,
        int bufferSize, short replication, long blockSize,
        Progressable progress) throws IOException {
      return null;
    }
    public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
        boolean overwrite, int bufferSize, short replication, long blockSize,
        Progressable progress) throws IOException {
      return null;
    }
    public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
            EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize,
            Progressable progress) throws IOException {
      return null;
    }
    public FSDataOutputStream createNonRecursive(Path f, FsPermission permission,
            EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize,
            Progressable progress, ChecksumOpt checksumOpt) throws IOException {
      return null;
    }
    public boolean mkdirs(Path f) { return false; }
    public FSDataInputStream open(Path f) { return null; }
    public FSDataOutputStream create(Path f) { return null; }
    public FSDataOutputStream create(Path f, boolean overwrite) { return null; }
    public FSDataOutputStream create(Path f, Progressable progress) {
      return null;
    }
    public FSDataOutputStream create(Path f, short replication) {
      return null;
    }
    public FSDataOutputStream create(Path f, short replication, 
        Progressable progress) {
      return null;
    }
    public FSDataOutputStream create(Path f, 
        boolean overwrite,
        int bufferSize) {
      return null;
    }
    public FSDataOutputStream create(Path f, 
        boolean overwrite,
        int bufferSize,
        Progressable progress) {
      return null;
    }
    public FSDataOutputStream create(Path f, 
        boolean overwrite,
        int bufferSize,
        short replication,
        long blockSize) {
      return null;
    }
    public FSDataOutputStream create(Path f,
        boolean overwrite,
        int bufferSize,
        short replication,
        long blockSize,
        Progressable progress) {
      return null;
    }
    public FSDataOutputStream create(Path f,
        FsPermission permission,
        boolean overwrite,
        int bufferSize,
        short replication,
        long blockSize,
        Progressable progress) {
      return null;
    }
    public FSDataOutputStream create(Path f,
        FsPermission permission,
        EnumSet<CreateFlag> flags,
        int bufferSize,
        short replication,
        long blockSize,
        Progressable progress) throws IOException {
      return null;
    }
    public FSDataOutputStream create(Path f,
        FsPermission permission,
        EnumSet<CreateFlag> flags,
        int bufferSize,
        short replication,
        long blockSize,
        Progressable progress,
        ChecksumOpt checksumOpt) throws IOException {
      return null;
    }
    public String getName() { return null; }
    public boolean delete(Path f) { return false; }
    public short getReplication(Path src) { return 0 ; }
    public void processDeleteOnExit() { }
    public ContentSummary getContentSummary(Path f) { return null; }
    public FsStatus getStatus() { return null; }
    public FileStatus[] listStatus(Path f, PathFilter filter) { return null; }
    public FileStatus[] listStatus(Path[] files) { return null; }
    public FileStatus[] listStatus(Path[] files, PathFilter filter) { return null; }
    public FileStatus[] globStatus(Path pathPattern) { return null; }
    public FileStatus[] globStatus(Path pathPattern, PathFilter filter) {
      return null;
    }
    public Iterator<LocatedFileStatus> listFiles(
        final Path path, final boolean isRecursive) {
      return null;
    }
    public Iterator<LocatedFileStatus> listLocatedStatus(Path f) {
      return null;
    }
    public Iterator<LocatedFileStatus> listLocatedStatus(Path f,
        final PathFilter filter) {
      return null;
    }
    public void copyFromLocalFile(Path src, Path dst) { }
    public void moveFromLocalFile(Path[] srcs, Path dst) { }
    public void moveFromLocalFile(Path src, Path dst) { }
    public void copyToLocalFile(Path src, Path dst) { }
    public void copyToLocalFile(boolean delSrc, Path src, Path dst, 
            boolean useRawLocalFileSystem) { }
    public void moveToLocalFile(Path src, Path dst) { }
    public long getBlockSize(Path f) { return 0; }
    public FSDataOutputStream primitiveCreate(final Path f,
        final EnumSet<CreateFlag> createFlag,
        CreateOpts... opts) { return null; }
    public void primitiveMkdir(Path f, FsPermission absolutePermission, 
                      boolean createParent) { }
    public int getDefaultPort() { return 0; }
    public String getCanonicalServiceName() { return null; }
    public Token<?> getDelegationToken(String renewer) throws IOException {
      return null;
    }
    public boolean deleteOnExit(Path f) throws IOException {
      return false;
    }
    public boolean cancelDeleteOnExit(Path f) throws IOException {
      return false;
    }
    public Token<?>[] addDelegationTokens(String renewer, Credentials creds)
        throws IOException {
      return null;
    }
    public String getScheme() {
      return "dontcheck";
    }
    public Path fixRelativePart(Path p) { return null; }
  }
  
  @Test
  public void testFilterFileSystem() throws Exception {
    for (Method m : FileSystem.class.getDeclaredMethods()) {
      if (Modifier.isStatic(m.getModifiers()))
        continue;
      if (Modifier.isPrivate(m.getModifiers()))
        continue;
      if (Modifier.isFinal(m.getModifiers()))
        continue;
      
      try {
        DontCheck.class.getMethod(m.getName(), m.getParameterTypes());
        LOG.info("Skipping " + m);
      } catch (NoSuchMethodException exc) {
        LOG.info("Testing " + m);
        try{
          FilterFileSystem.class.getDeclaredMethod(m.getName(), m.getParameterTypes());
        }
        catch(NoSuchMethodException exc2){
          LOG.error("FilterFileSystem doesn't implement " + m);
          throw exc2;
        }
      }
    }
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
