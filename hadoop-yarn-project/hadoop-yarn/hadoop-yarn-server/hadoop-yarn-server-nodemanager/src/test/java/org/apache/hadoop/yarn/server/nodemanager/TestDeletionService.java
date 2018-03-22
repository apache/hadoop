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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.FileDeletionTask;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMMemoryStateStoreService;
import org.junit.AfterClass;
import org.junit.Test;
import org.mockito.Mockito;


public class TestDeletionService {

  private static final FileContext lfs = getLfs();
  private static final FileContext getLfs() {
    try {
      return FileContext.getLocalFSFileContext();
    } catch (UnsupportedFileSystemException e) {
      throw new RuntimeException(e);
    }
  }
  private static final Path base =
    lfs.makeQualified(new Path("target", TestDeletionService.class.getName()));

  @AfterClass
  public static void removeBase() throws IOException {
    lfs.delete(base, true);
  }

  public List<Path> buildDirs(Random r, Path root, int numpaths)
      throws IOException {
    ArrayList<Path> ret = new ArrayList<Path>();
    for (int i = 0; i < numpaths; ++i) {
      Path p = root;
      long name = r.nextLong();
      do {
        p = new Path(p, "" + name);
        name = r.nextLong();
      } while (0 == (name % 2));
      ret.add(p);
    }
    return ret;
  }

  public void createDirs(Path base, List<Path> dirs) throws IOException {
    for (Path dir : dirs) {
      lfs.mkdir(new Path(base, dir), null, true);
    }
  }

  static class FakeDefaultContainerExecutor extends DefaultContainerExecutor {
    @Override
    public void deleteAsUser(DeletionAsUserContext ctx)
        throws IOException, InterruptedException {
      String user = ctx.getUser();
      Path subDir = ctx.getSubDir();
      List<Path> basedirs = ctx.getBasedirs();

      if ((Long.parseLong(subDir.getName()) % 2) == 0) {
        assertNull(user);
      } else {
        assertEquals("dingo", user);
      }

      DeletionAsUserContext.Builder builder = new DeletionAsUserContext
          .Builder()
          .setUser(user)
          .setSubDir(subDir);

      if (basedirs != null) {
        builder.setBasedirs(basedirs.toArray(new Path[basedirs.size()]));
      }

      super.deleteAsUser(builder.build());
      assertFalse(lfs.util().exists(subDir));
    }
  }

  @Test
  public void testAbsDelete() throws Exception {
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("SEED: " + seed);
    List<Path> dirs = buildDirs(r, base, 20);
    createDirs(new Path("."), dirs);
    FakeDefaultContainerExecutor exec = new FakeDefaultContainerExecutor();
    Configuration conf = new Configuration();
    exec.setConf(conf);
    DeletionService del = new DeletionService(exec);
    del.init(conf);
    del.start();
    try {
      for (Path p : dirs) {
        FileDeletionTask deletionTask = new FileDeletionTask(del,
            (Long.parseLong(p.getName()) % 2) == 0 ? null : "dingo", p, null);
        del.delete(deletionTask);
      }

      int msecToWait = 20 * 1000;
      for (Path p : dirs) {
        while (msecToWait > 0 && lfs.util().exists(p)) {
          Thread.sleep(100);
          msecToWait -= 100;
        }
        assertFalse(lfs.util().exists(p));
      }
    } finally {
      del.stop();
    }
  }

  @Test
  public void testRelativeDelete() throws Exception {
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("SEED: " + seed);
    List<Path> baseDirs = buildDirs(r, base, 4);
    createDirs(new Path("."), baseDirs);
    List<Path> content = buildDirs(r, new Path("."), 10);
    for (Path b : baseDirs) {
      createDirs(b, content);
    }
    DeletionService del =
      new DeletionService(new FakeDefaultContainerExecutor());
    try {
      del.init(new Configuration());
      del.start();
      for (Path p : content) {
        assertTrue(lfs.util().exists(new Path(baseDirs.get(0), p)));
        FileDeletionTask deletionTask = new FileDeletionTask(del,
            (Long.parseLong(p.getName()) % 2) == 0 ? null : "dingo", p,
            baseDirs);
        del.delete(deletionTask);
      }

      int msecToWait = 20 * 1000;
      for (Path p : baseDirs) {
        for (Path q : content) {
          Path fp = new Path(p, q);
          while (msecToWait > 0 && lfs.util().exists(fp)) {
            Thread.sleep(100);
            msecToWait -= 100;
          }
          assertFalse(lfs.util().exists(fp));
        }
      }
    } finally {
      del.stop();
    }
  }

  @Test
  public void testNoDelete() throws Exception {
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("SEED: " + seed);
    List<Path> dirs = buildDirs(r, base, 20);
    createDirs(new Path("."), dirs);
    FakeDefaultContainerExecutor exec = new FakeDefaultContainerExecutor();
    Configuration conf = new Configuration();
    conf.setInt(YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, -1);
    exec.setConf(conf);
    DeletionService del = new DeletionService(exec);
    try {
      del.init(conf);
      del.start();
      for (Path p : dirs) {
        FileDeletionTask deletionTask = new FileDeletionTask(del,
            (Long.parseLong(p.getName()) % 2) == 0 ? null : "dingo", p, null);
        del.delete(deletionTask);
      }
      int msecToWait = 20 * 1000;
      for (Path p : dirs) {
        while (msecToWait > 0 && lfs.util().exists(p)) {
          Thread.sleep(100);
          msecToWait -= 100;
        }
        assertTrue(lfs.util().exists(p));
      }
    } finally {
      del.stop();
    }
  }

  @Test
  public void testStopWithDelayedTasks() throws Exception {
    DeletionService del = new DeletionService(Mockito.mock(ContainerExecutor.class));
    Configuration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, 60);
    try {
      del.init(conf);
      del.start();
      FileDeletionTask deletionTask = new FileDeletionTask(del, "dingo",
          new Path("/does/not/exist"), null);
      del.delete(deletionTask);
    } finally {
      del.stop();
    }
    assertTrue(del.isTerminated());
  }
  
  @Test (timeout=60000)
  public void testFileDeletionTaskDependency() throws Exception {
    FakeDefaultContainerExecutor exec = new FakeDefaultContainerExecutor();
    Configuration conf = new Configuration();
    exec.setConf(conf);
    DeletionService del = new DeletionService(exec);
    del.init(conf);
    del.start();
    
    try {
      Random r = new Random();
      long seed = r.nextLong();
      r.setSeed(seed);
      System.out.println("SEED: " + seed);
      List<Path> dirs = buildDirs(r, base, 2);
      createDirs(new Path("."), dirs);
      
      // first we will try to delete sub directories which are present. This
      // should then trigger parent directory to be deleted.
      List<Path> subDirs = buildDirs(r, dirs.get(0), 2);

      FileDeletionTask dependentDeletionTask =
          new FileDeletionTask(del, null, dirs.get(0), new ArrayList<Path>());
      List<FileDeletionTask> deletionTasks = new ArrayList<FileDeletionTask>();
      for (Path subDir : subDirs) {
        List<Path> subDirList = new ArrayList<>();
        subDirList.add(subDir);
        FileDeletionTask deletionTask =
            new FileDeletionTask(del, null, dirs.get(0), subDirList);
        deletionTask.addDeletionTaskDependency(dependentDeletionTask);
        deletionTasks.add(deletionTask);
      }
      for (FileDeletionTask task : deletionTasks) {
        del.delete(task);
      }

      int msecToWait = 20 * 1000;
      while (msecToWait > 0 && (lfs.util().exists(dirs.get(0)))) {
        Thread.sleep(100);
        msecToWait -= 100;
      }
      assertFalse(lfs.util().exists(dirs.get(0)));
      

      // Now we will try to delete sub directories; one of the deletion task we
      // will mark as failure and then parent directory should not be deleted.
      subDirs = buildDirs(r, dirs.get(1), 2);
      subDirs.add(new Path(dirs.get(1), "absentFile"));
      
      dependentDeletionTask = new FileDeletionTask(del, null, dirs.get(1),
          new ArrayList<Path>());
      deletionTasks = new ArrayList<FileDeletionTask>();
      for (Path subDir : subDirs) {
        List<Path> subDirList = new ArrayList<>();
        subDirList.add(subDir);
        FileDeletionTask deletionTask = new FileDeletionTask(del, null, null,
            subDirList);
        deletionTask.addDeletionTaskDependency(dependentDeletionTask);
        deletionTasks.add(deletionTask);
      }
      // marking one of the tasks as a failure.
      deletionTasks.get(2).setSuccess(false);
      for (FileDeletionTask task : deletionTasks) {
        del.delete(task);
      }

      msecToWait = 20 * 1000;
      while (msecToWait > 0
          && (lfs.util().exists(subDirs.get(0)) || lfs.util().exists(
            subDirs.get(1)))) {
        Thread.sleep(100);
        msecToWait -= 100;
      }
      assertTrue(lfs.util().exists(dirs.get(1)));
    } finally {
      del.stop();
    }
  }

  @Test
  public void testRecovery() throws Exception {
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("SEED: " + seed);
    List<Path> baseDirs = buildDirs(r, base, 4);
    createDirs(new Path("."), baseDirs);
    List<Path> content = buildDirs(r, new Path("."), 10);
    for (Path b : baseDirs) {
      createDirs(b, content);
    }
    Configuration conf = new YarnConfiguration();
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, true);
    conf.setInt(YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, 1);
    NMMemoryStateStoreService stateStore = new NMMemoryStateStoreService();
    stateStore.init(conf);
    stateStore.start();
    DeletionService del =
      new DeletionService(new FakeDefaultContainerExecutor(), stateStore);
    try {
      del.init(conf);
      del.start();
      for (Path p : content) {
        assertTrue(lfs.util().exists(new Path(baseDirs.get(0), p)));
        FileDeletionTask deletionTask = new FileDeletionTask(del,
            (Long.parseLong(p.getName()) % 2) == 0 ? null : "dingo", p,
            baseDirs);
        del.delete(deletionTask);
      }

      // restart the deletion service
      del.stop();
      del = new DeletionService(new FakeDefaultContainerExecutor(),
          stateStore);
      del.init(conf);
      del.start();

      // verify paths are still eventually deleted
      int msecToWait = 10 * 1000;
      for (Path p : baseDirs) {
        System.out.println("TEST Basedir: " + p.getName());
        for (Path q : content) {
          Path fp = new Path(p, q);
          System.out.println("TEST Path: " + fp.toString());
          while (msecToWait > 0 && lfs.util().exists(fp)) {
            Thread.sleep(100);
            msecToWait -= 100;
          }
          assertFalse(lfs.util().exists(fp));
        }
      }
    } finally {
      del.close();
      stateStore.close();
    }
  }
}
