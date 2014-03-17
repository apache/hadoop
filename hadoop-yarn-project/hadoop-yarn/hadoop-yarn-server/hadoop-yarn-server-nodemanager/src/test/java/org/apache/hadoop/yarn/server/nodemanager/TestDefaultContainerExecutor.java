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

package org.apache.hadoop.yarn.server.nodemanager;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.FakeFSDataInputStream;

import static org.apache.hadoop.fs.CreateFlag.*;


import org.junit.AfterClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
import static org.mockito.Mockito.*;

public class TestDefaultContainerExecutor {

  /*
  // XXX FileContext cannot be mocked to do this
  static FSDataInputStream getRandomStream(Random r, int len)
      throws IOException {
    byte[] bytes = new byte[len];
    r.nextBytes(bytes);
    DataInputBuffer buf = new DataInputBuffer();
    buf.reset(bytes, 0, bytes.length);
    return new FSDataInputStream(new FakeFSDataInputStream(buf));
  }

  class PathEndsWith extends ArgumentMatcher<Path> {
    final String suffix;
    PathEndsWith(String suffix) {
      this.suffix = suffix;
    }
    @Override
    public boolean matches(Object o) {
      return
      suffix.equals(((Path)o).getName());
    }
  }

  DataOutputBuffer mockStream(
      AbstractFileSystem spylfs, Path p, Random r, int len) 
      throws IOException {
    DataOutputBuffer dob = new DataOutputBuffer();
    doReturn(getRandomStream(r, len)).when(spylfs).open(p);
    doReturn(new FileStatus(len, false, -1, -1L, -1L, p)).when(
        spylfs).getFileStatus(argThat(new PathEndsWith(p.getName())));
    doReturn(new FSDataOutputStream(dob)).when(spylfs).createInternal(
        argThat(new PathEndsWith(p.getName())),
        eq(EnumSet.of(OVERWRITE)),
        Matchers.<FsPermission>anyObject(), anyInt(), anyShort(), anyLong(),
        Matchers.<Progressable>anyObject(), anyInt(), anyBoolean());
    return dob;
  }
  */

  private static final Path BASE_TMP_PATH = new Path("target",
      TestDefaultContainerExecutor.class.getSimpleName());

  @AfterClass
  public static void deleteTmpFiles() throws IOException {
    FileContext lfs = FileContext.getLocalFSFileContext();
    try {
      lfs.delete(BASE_TMP_PATH, true);
    } catch (FileNotFoundException e) {
    }
  }

  byte[] createTmpFile(Path dst, Random r, int len)
      throws IOException {
    // use unmodified local context
    FileContext lfs = FileContext.getLocalFSFileContext();
    dst = lfs.makeQualified(dst);
    lfs.mkdir(dst.getParent(), null, true);
    byte[] bytes = new byte[len];
    FSDataOutputStream out = null;
    try {
      out = lfs.create(dst, EnumSet.of(CREATE, OVERWRITE));
      r.nextBytes(bytes);
      out.write(bytes);
    } finally {
      if (out != null) out.close();
    }
    return bytes;
  }

  @Test
  public void testDirPermissions() throws Exception {
    deleteTmpFiles();

    final String user = "somebody";
    final String appId = "app_12345_123";
    final FsPermission userCachePerm = new FsPermission(
        DefaultContainerExecutor.USER_PERM);
    final FsPermission appCachePerm = new FsPermission(
        DefaultContainerExecutor.APPCACHE_PERM);
    final FsPermission fileCachePerm = new FsPermission(
        DefaultContainerExecutor.FILECACHE_PERM);
    final FsPermission appDirPerm = new FsPermission(
        DefaultContainerExecutor.APPDIR_PERM);
    final FsPermission logDirPerm = new FsPermission(
        DefaultContainerExecutor.LOGDIR_PERM);
    List<String> localDirs = new ArrayList<String>();
    localDirs.add(new Path(BASE_TMP_PATH, "localDirA").toString());
    localDirs.add(new Path(BASE_TMP_PATH, "localDirB").toString());
    List<String> logDirs = new ArrayList<String>();
    logDirs.add(new Path(BASE_TMP_PATH, "logDirA").toString());
    logDirs.add(new Path(BASE_TMP_PATH, "logDirB").toString());

    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "077");
    FileContext lfs = FileContext.getLocalFSFileContext(conf);
    DefaultContainerExecutor executor = new DefaultContainerExecutor(lfs);
    executor.init();

    try {
      executor.createUserLocalDirs(localDirs, user);
      executor.createUserCacheDirs(localDirs, user);
      executor.createAppDirs(localDirs, user, appId);

      for (String dir : localDirs) {
        FileStatus stats = lfs.getFileStatus(
            new Path(new Path(dir, ContainerLocalizer.USERCACHE), user));
        Assert.assertEquals(userCachePerm, stats.getPermission());
      }

      for (String dir : localDirs) {
        Path userCachePath = new Path(
            new Path(dir, ContainerLocalizer.USERCACHE), user);
        Path appCachePath = new Path(userCachePath,
            ContainerLocalizer.APPCACHE);
        FileStatus stats = lfs.getFileStatus(appCachePath);
        Assert.assertEquals(appCachePerm, stats.getPermission());
        stats = lfs.getFileStatus(
            new Path(userCachePath, ContainerLocalizer.FILECACHE));
        Assert.assertEquals(fileCachePerm, stats.getPermission());
        stats = lfs.getFileStatus(new Path(appCachePath, appId));
        Assert.assertEquals(appDirPerm, stats.getPermission());
      }

      executor.createAppLogDirs(appId, logDirs);

      for (String dir : logDirs) {
        FileStatus stats = lfs.getFileStatus(new Path(dir, appId));
        Assert.assertEquals(logDirPerm, stats.getPermission());
      }
    } finally {
      deleteTmpFiles();
    }
  }

//  @Test
//  public void testInit() throws IOException, InterruptedException {
//    Configuration conf = new Configuration();
//    AbstractFileSystem spylfs =
//      spy(FileContext.getLocalFSFileContext().getDefaultFileSystem());
//    // don't actually create dirs
//    //doNothing().when(spylfs).mkdir(Matchers.<Path>anyObject(),
//    //    Matchers.<FsPermission>anyObject(), anyBoolean());
//    FileContext lfs = FileContext.getFileContext(spylfs, conf);
//
//    Path basedir = new Path("target",
//        TestDefaultContainerExecutor.class.getSimpleName());
//    List<String> localDirs = new ArrayList<String>();
//    List<Path> localPaths = new ArrayList<Path>();
//    for (int i = 0; i < 4; ++i) {
//      Path p = new Path(basedir, i + "");
//      lfs.mkdir(p, null, true);
//      localPaths.add(p);
//      localDirs.add(p.toString());
//    }
//    final String user = "yak";
//    final String appId = "app_RM_0";
//    final Path logDir = new Path(basedir, "logs");
//    final Path nmLocal = new Path(basedir, "nmPrivate/" + user + "/" + appId);
//    final InetSocketAddress nmAddr = new InetSocketAddress("foobar", 8040);
//    System.out.println("NMLOCAL: " + nmLocal);
//    Random r = new Random();
//
//    /*
//    // XXX FileContext cannot be reasonably mocked to do this
//    // mock jobFiles copy
//    long fileSeed = r.nextLong();
//    r.setSeed(fileSeed);
//    System.out.println("SEED: " + seed);
//    Path fileCachePath = new Path(nmLocal, ApplicationLocalizer.FILECACHE_FILE);
//    DataOutputBuffer fileCacheBytes = mockStream(spylfs, fileCachePath, r, 512);
//
//    // mock jobTokens copy
//    long jobSeed = r.nextLong();
//    r.setSeed(jobSeed);
//    System.out.println("SEED: " + seed);
//    Path jobTokenPath = new Path(nmLocal, ApplicationLocalizer.JOBTOKEN_FILE);
//    DataOutputBuffer jobTokenBytes = mockStream(spylfs, jobTokenPath, r, 512);
//    */
//
//    // create jobFiles
//    long fileSeed = r.nextLong();
//    r.setSeed(fileSeed);
//    System.out.println("SEED: " + fileSeed);
//    Path fileCachePath = new Path(nmLocal, ApplicationLocalizer.FILECACHE_FILE);
//    byte[] fileCacheBytes = createTmpFile(fileCachePath, r, 512);
//
//    // create jobTokens
//    long jobSeed = r.nextLong();
//    r.setSeed(jobSeed);
//    System.out.println("SEED: " + jobSeed);
//    Path jobTokenPath = new Path(nmLocal, ApplicationLocalizer.JOBTOKEN_FILE);
//    byte[] jobTokenBytes = createTmpFile(jobTokenPath, r, 512);
//
//    DefaultContainerExecutor dce = new DefaultContainerExecutor(lfs);
//    Localization mockLocalization = mock(Localization.class);
//    ApplicationLocalizer spyLocalizer =
//      spy(new ApplicationLocalizer(lfs, user, appId, logDir,
//            localPaths));
//    // ignore cache localization
//    doNothing().when(spyLocalizer).localizeFiles(
//        Matchers.<Localization>anyObject(), Matchers.<Path>anyObject());
//    Path workingDir = lfs.getWorkingDirectory();
//    dce.initApplication(spyLocalizer, nmLocal, mockLocalization, localPaths);
//    lfs.setWorkingDirectory(workingDir);
//
//    for (Path localdir : localPaths) {
//      Path userdir = lfs.makeQualified(new Path(localdir,
//            new Path(ApplicationLocalizer.USERCACHE, user)));
//      // $localdir/$user
//      verify(spylfs).mkdir(userdir,
//          new FsPermission(ApplicationLocalizer.USER_PERM), true);
//      // $localdir/$user/appcache
//      Path jobdir = new Path(userdir, ApplicationLocalizer.appcache);
//      verify(spylfs).mkdir(jobdir,
//          new FsPermission(ApplicationLocalizer.appcache_PERM), true);
//      // $localdir/$user/filecache
//      Path filedir = new Path(userdir, ApplicationLocalizer.FILECACHE);
//      verify(spylfs).mkdir(filedir,
//          new FsPermission(ApplicationLocalizer.FILECACHE_PERM), true);
//      // $localdir/$user/appcache/$appId
//      Path appdir = new Path(jobdir, appId);
//      verify(spylfs).mkdir(appdir,
//          new FsPermission(ApplicationLocalizer.APPDIR_PERM), true);
//      // $localdir/$user/appcache/$appId/work
//      Path workdir = new Path(appdir, ApplicationLocalizer.WORKDIR);
//      verify(spylfs, atMost(1)).mkdir(workdir, FsPermission.getDefault(), true);
//    }
//    // $logdir/$appId
//    Path logdir = new Path(lfs.makeQualified(logDir), appId);
//    verify(spylfs).mkdir(logdir,
//        new FsPermission(ApplicationLocalizer.LOGDIR_PERM), true);
//  }

}
