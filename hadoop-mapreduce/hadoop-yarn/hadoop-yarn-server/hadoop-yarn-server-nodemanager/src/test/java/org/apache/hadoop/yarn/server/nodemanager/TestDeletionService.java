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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;


import org.junit.AfterClass;
import org.junit.Test;
import static org.junit.Assert.*;

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
    public void deleteAsUser(String user, Path subDir, Path... basedirs)
        throws IOException, InterruptedException {
      if ((Long.parseLong(subDir.getName()) % 2) == 0) {
        assertNull(user);
      } else {
        assertEquals("dingo", user);
      }
      super.deleteAsUser(user, subDir, basedirs);
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
        del.delete((Long.parseLong(p.getName()) % 2) == 0 ? null : "dingo",
            p, null);
      }
    } finally {
      del.stop();
    }
    for (Path p : dirs) {
      assertFalse(lfs.util().exists(p));
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
    del.init(new Configuration());
    del.start();
    try {
      for (Path p : content) {
        assertTrue(lfs.util().exists(new Path(baseDirs.get(0), p)));
        del.delete((Long.parseLong(p.getName()) % 2) == 0 ? null : "dingo",
            p, baseDirs.toArray(new Path[4]));
      }
    } finally {
      del.stop();
    }
    for (Path p : baseDirs) {
      for (Path q : content) {
        assertFalse(lfs.util().exists(new Path(p, q)));
      }
    }
  }

}
