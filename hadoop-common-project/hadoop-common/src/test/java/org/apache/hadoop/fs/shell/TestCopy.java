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

package org.apache.hadoop.fs.shell;

import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.shell.CopyCommands.Put;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;

public class TestCopy {
  static Configuration conf;
  static Path path = new Path("mockfs:/file");
  static Path tmpPath = new Path("mockfs:/file._COPYING_");
  static Put cmd;
  static FileSystem mockFs;
  static PathData target;
  static FileStatus fileStat;
  
  @BeforeClass
  public static void setup() throws IOException {
    conf = new Configuration();
    conf.setClass("fs.mockfs.impl", MockFileSystem.class, FileSystem.class);
    mockFs = mock(FileSystem.class);
    fileStat = mock(FileStatus.class);
    when(fileStat.isDirectory()).thenReturn(false);
  }
  
  @Before
  public void resetMock() throws IOException {
    reset(mockFs);
    target = new PathData(path.toString(), conf);
    cmd = new CopyCommands.Put();
    cmd.setConf(conf);
  }

  @Test
  public void testCopyStreamTarget() throws Exception {
    FSDataOutputStream out = mock(FSDataOutputStream.class);
    whenFsCreate().thenReturn(out);
    when(mockFs.getFileStatus(eq(tmpPath))).thenReturn(fileStat);
    when(mockFs.rename(eq(tmpPath), eq(path))).thenReturn(true);
    FSInputStream in = mock(FSInputStream.class);
    when(in.read(any(byte[].class), anyInt(), anyInt())).thenReturn(-1);
    
    tryCopyStream(in, true);
    verify(in).close();
    verify(out, times(2)).close();
    // no data was written.
    verify(out, never()).write(any(byte[].class), anyInt(), anyInt());
    verify(mockFs, never()).delete(eq(path), anyBoolean());
    verify(mockFs).rename(eq(tmpPath), eq(path));
    verify(mockFs, never()).delete(eq(tmpPath), anyBoolean());
    verify(mockFs, never()).close();
    // temp path never had is existence checked. This is critical for S3 as it
    // avoids the successful path accidentally getting a 404 into the S3 load
    // balancer cache
    verify(mockFs, never()).exists(eq(tmpPath));
    verify(mockFs, never()).exists(eq(path));
  }

  @Test
  public void testCopyStreamTargetExists() throws Exception {
    FSDataOutputStream out = mock(FSDataOutputStream.class);
    whenFsCreate().thenReturn(out);
    when(mockFs.getFileStatus(eq(path))).thenReturn(fileStat);
    target.refreshStatus(); // so it's updated as existing
    cmd.setOverwrite(true);
    when(mockFs.getFileStatus(eq(tmpPath))).thenReturn(fileStat);
    when(mockFs.delete(eq(path), eq(false))).thenReturn(true);
    when(mockFs.rename(eq(tmpPath), eq(path))).thenReturn(true);
    FSInputStream in = mock(FSInputStream.class);
    when(in.read(any(byte[].class), anyInt(), anyInt())).thenReturn(-1);
    
    tryCopyStream(in, true);
    verify(mockFs).delete(eq(path), anyBoolean());
    verify(mockFs).rename(eq(tmpPath), eq(path));
    verify(mockFs, never()).delete(eq(tmpPath), anyBoolean());
    verify(mockFs, never()).close();
  }

  @Test
  public void testInterruptedCreate() throws Exception {
    whenFsCreate().thenThrow(new InterruptedIOException());
    when(mockFs.getFileStatus(eq(tmpPath))).thenReturn(fileStat);
    FSDataInputStream in = mock(FSDataInputStream.class);

    tryCopyStream(in, false);
    verify(mockFs, never()).rename(any(Path.class), any(Path.class));
    verify(mockFs).delete(eq(tmpPath), anyBoolean());
    verify(mockFs, never()).delete(eq(path), anyBoolean());
    verify(mockFs, never()).close();
  }

  /**
   * Create a file but fail in the write.
   * The copy operation should attempt to clean up by
   * closing the output stream then deleting it.
   */
  @Test
  public void testFailedWrite() throws Exception {
    FSDataOutputStream out = mock(FSDataOutputStream.class);
    doThrow(new IOException("mocked"))
        .when(out).write(any(byte[].class), anyInt(), anyInt());
    whenFsCreate().thenReturn(out);
    when(mockFs.getFileStatus(eq(tmpPath))).thenReturn(fileStat);
    FSInputStream in = mock(FSInputStream.class);
    doReturn(0)
        .when(in).read(any(byte[].class), anyInt(), anyInt());
    Throwable thrown = tryCopyStream(in, false);
    assertExceptionContains("mocked", thrown);
    verify(in).close();
    verify(out, times(2)).close();
    verify(mockFs).delete(eq(tmpPath), anyBoolean());
    verify(mockFs, never()).rename(any(Path.class), any(Path.class));
    verify(mockFs, never()).delete(eq(path), anyBoolean());
    verify(mockFs, never()).close();
  }

  @Test
  public void testInterruptedCopyBytes() throws Exception {
    FSDataOutputStream out = mock(FSDataOutputStream.class);
    whenFsCreate().thenReturn(out);
    when(mockFs.getFileStatus(eq(tmpPath))).thenReturn(fileStat);
    FSInputStream in = mock(FSInputStream.class);
    // make IOUtils.copyBytes fail
    when(in.read(any(byte[].class), anyInt(), anyInt())).thenThrow(
        new InterruptedIOException());
    
    tryCopyStream(in, false);
    verify(mockFs).delete(eq(tmpPath), anyBoolean());
    verify(mockFs, never()).rename(any(Path.class), any(Path.class));
    verify(mockFs, never()).delete(eq(path), anyBoolean());
    verify(mockFs, never()).close();
  }

  @Test
  public void testInterruptedRename() throws Exception {
    FSDataOutputStream out = mock(FSDataOutputStream.class);
    whenFsCreate().thenReturn(out);
    when(mockFs.getFileStatus(eq(tmpPath))).thenReturn(fileStat);
    when(mockFs.rename(eq(tmpPath), eq(path))).thenThrow(
        new InterruptedIOException());
    FSInputStream in = mock(FSInputStream.class);
    when(in.read(any(byte[].class), anyInt(), anyInt())).thenReturn(-1);
    
    tryCopyStream(in, false);
    verify(mockFs).delete(eq(tmpPath), anyBoolean());
    verify(mockFs).rename(eq(tmpPath), eq(path));
    verify(mockFs, never()).delete(eq(path), anyBoolean());
    verify(mockFs, never()).close();
  }

  private OngoingStubbing<FSDataOutputStream> whenFsCreate() throws IOException {
    return when(mockFs.create(eq(tmpPath), any(FsPermission.class),
        anyBoolean(), anyInt(), anyShort(), anyLong(), any()));
  }
  
  private Throwable tryCopyStream(InputStream in, boolean shouldPass) {
    try {
      cmd.copyStreamToTarget(new FSDataInputStream(in), target);
      return null;
    } catch (InterruptedIOException e) {
      if (shouldPass) {
        throw new AssertionError("copy failed", e);
      }
      return e;
    } catch (Throwable e) {
      if (shouldPass) {
        throw new AssertionError(e.getMessage(), e);
      }
      return e;
    }
  }
  
  static class MockFileSystem extends FilterFileSystem {
    Configuration conf;
    MockFileSystem() {
      super(mockFs);
    }
    @Override
    public void initialize(URI uri, Configuration conf) {
      this.conf = conf;
    }
    @Override
    public Path makeQualified(Path path) {
      return path;
    }
    @Override
    public Configuration getConf() {
      return conf;
    }
  }
}
