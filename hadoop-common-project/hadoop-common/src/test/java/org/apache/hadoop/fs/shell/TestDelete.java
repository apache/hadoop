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

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.PathIsNotInTrashException;
import org.apache.hadoop.fs.TrashOptionNotExistsException;
import org.apache.hadoop.fs.Options.Rename;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDelete {
  static Configuration conf;
  static FileSystem mockFs;

  @BeforeClass
  public static void setup() throws IOException, URISyntaxException {
    mockFs = mock(FileSystem.class);
    conf = new Configuration();
    conf.setClass("fs.mockfs.impl", MockFileSystem.class, FileSystem.class);
  }

  @Before
  public void resetMock() throws IOException {
    reset(mockFs);
  }

  @Test
  public void testDeleteFileInTrashWithoutTrashOption() throws Exception {
    Path fileInTrash = new Path("mockfs://user/someone/.Trash/Current/someone/file");
    InstrumentedRM cmd;
    String[] cmdargs = new String[]{"mockfs://user/someone/.Trash/Current/someone/file"};
    FileStatus fileInTrash_Stat = mock(FileStatus.class);

    when(fileInTrash_Stat.isDirectory()).thenReturn(false);
    when(fileInTrash_Stat.getPath()).thenReturn(fileInTrash);
    when(mockFs.getFileStatus(eq(fileInTrash))).thenReturn(fileInTrash_Stat);

    cmd = new InstrumentedRM();
    cmd.setConf(conf);
    cmd.run(cmdargs);

    // make sure command failed with the proper exception
    assertTrue("Rename should have failed with trash option not exists exception",
                         cmd.error instanceof TrashOptionNotExistsException);
  }

  @Test
  public void testDeleteDirectoryInTrashWithoutTrashOption() throws Exception {
    Path directoryInTrash = new Path("mockfs://user/someone/.Trash/Current/someone/fold1");
    InstrumentedRM cmd;
    String[] cmdargs = new String[]{"-r", "mockfs://user/someone/.Trash/Current/someone/fold1"};
    FileStatus directoryInTrash_Stat = mock(FileStatus.class);

    when(directoryInTrash_Stat.isDirectory()).thenReturn(true);
    when(directoryInTrash_Stat.getPath()).thenReturn(directoryInTrash);
    when(mockFs.getFileStatus(eq(directoryInTrash))).thenReturn(directoryInTrash_Stat);

    cmd = new InstrumentedRM();
    cmd.setConf(conf);
    cmd.run(cmdargs);

    // make sure command failed with the proper exception
    assertTrue("RM should have failed with trash option not exists exception",
                         cmd.error instanceof TrashOptionNotExistsException);
  }

  @Test
  public void testDeleteFileNotInTrashWithTrashOption() throws Exception {
    Path fileNotInTrash = new Path("mockfs://user/someone/fold0/file0");
    InstrumentedRM cmd;
    String[] cmdargs = new String[]{"-T", "mockfs://user/someone/fold0/file0"};
    FileStatus fileNotInTrash_Stat = mock(FileStatus.class);

    when(fileNotInTrash_Stat.isDirectory()).thenReturn(false);
    when(fileNotInTrash_Stat.getPath()).thenReturn(fileNotInTrash);
    when(mockFs.getFileStatus(eq(fileNotInTrash))).thenReturn(fileNotInTrash_Stat);

    cmd = new InstrumentedRM();
    cmd.setConf(conf);
    cmd.run(cmdargs);

    // make sure command failed with the proper exception
    assertTrue("Rename should have failed with pathIsNotInTrash exception",
                         cmd.error instanceof PathIsNotInTrashException);
  }

  @Test
  public void testDeleteDirectoryNotInTrashWithTrashOption() throws Exception {
    Path directoryNotInTrash = new Path("mockfs://user/someone/fold0/fold1");
    InstrumentedRM cmd;
    String[] cmdargs = new String[]{"-r", "-T", "mockfs://user/someone/fold0/fold1"};
    FileStatus directoryNotInTrash_Stat = mock(FileStatus.class);

    when(directoryNotInTrash_Stat.isDirectory()).thenReturn(true);
    when(directoryNotInTrash_Stat.getPath()).thenReturn(directoryNotInTrash);
    when(mockFs.getFileStatus(eq(directoryNotInTrash))).thenReturn(directoryNotInTrash_Stat);

    cmd = new InstrumentedRM();
    cmd.setConf(conf);
    cmd.run(cmdargs);

    // make sure command failed with the proper exception
    assertTrue("Rename should have failed with pathIsNotInTrash exception",
                         cmd.error instanceof PathIsNotInTrashException);
  }

  /*
   * hadoop -fs -rm -r /user/someone/ .Trash
   * The purpose is to clean trash for saving space.
   * But a blank space added before dot by mistake.
   * That will delete all data under /user/someone permanently.
   * Below test shows that HDFS-11111 can help to avoid this mistake.
   */
  @Test
  public void testMixedUseCase() throws Exception {
    Path trash = new Path("mockfs://user/someone/.Trash");
    Path fileNotInTrash = new Path("mockfs://user/someone/fold0/file0");
    Path directoryInTrash = new Path("mockfs://user/someone/.Trash/Current/someone/fold1");
    Path fileAfterDelete = new Path("mockfs://user/someone/.Trash/Current/someone/fold0/file0");

    InstrumentedRM cmd;
    String[] cmdargs = new String[]{"-r", "mockfs://user/someone/fold0/file0",
        "mockfs://user/someone/.Trash/Current/someone/fold1"};
    FileStatus fileNotInTrash_Stat = mock(FileStatus.class);
    FileStatus directoryInTrash_Stat = mock(FileStatus.class);

    when(fileNotInTrash_Stat.isDirectory()).thenReturn(false);
    when(fileNotInTrash_Stat.getPath()).thenReturn(fileNotInTrash);
    when(mockFs.getFileStatus(eq(fileNotInTrash))).thenReturn(fileNotInTrash_Stat);
    when(directoryInTrash_Stat.isDirectory()).thenReturn(true);
    when(directoryInTrash_Stat.getPath()).thenReturn(directoryInTrash);
    when(mockFs.getFileStatus(eq(directoryInTrash))).thenReturn(directoryInTrash_Stat);

    when(mockFs.getTrashRoot(any())).thenReturn(trash);
    when(mockFs.mkdirs(eq(new Path("mockfs://user/someone/.Trash/Current/someone/fold0")), any())).thenReturn(true);
    when(mockFs.rename(eq(fileNotInTrash), eq(fileAfterDelete))).thenReturn(true);

    cmd = new InstrumentedRM();
    cmd.setConf(conf);
    cmd.run(cmdargs);

    verify(mockFs).mkdirs(eq(new Path("mockfs://user/someone/.Trash/Current/someone/fold0")), any());
    verify(mockFs).rename(eq(fileNotInTrash), eq(fileAfterDelete));
    verify(mockFs, never()).delete(eq(directoryInTrash), anyBoolean());
    // make sure command failed with the proper exception
    assertTrue("Rename should have failed with trash option not exists exception",
                         cmd.error instanceof TrashOptionNotExistsException);
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
    @Override
    public Path resolvePath(Path p) {
      return p;
    }
    @Override
    public FsServerDefaults getServerDefaults(Path f) throws IOException {
      FsServerDefaults mockFsServerDefaults = mock(FsServerDefaults.class);
      when(mockFsServerDefaults.getTrashInterval()).thenReturn(1000L);
      return mockFsServerDefaults;
    }
    @Override
    protected void rename(final Path src, final Path dst,
        final Rename... options) throws IOException {
      mockFs.rename(src, dst);
    }
  }

  private static class InstrumentedRM extends Delete.Rm {
    public static String NAME = "InstrumentedRM";
    private Exception error = null;
    @Override
    public void displayError(Exception e) {
      error = e;
    }
  }
}
