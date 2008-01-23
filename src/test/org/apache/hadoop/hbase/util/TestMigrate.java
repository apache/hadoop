/**
 * Copyright 2007 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.util;

import java.io.IOException;

import java.util.zip.ZipInputStream;
import java.util.zip.ZipEntry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.StaticTestEnvironment;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * 
 */
public class TestMigrate extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestMigrate.class);

  /**
   * 
   */
  public TestMigrate() {
    super();
    Logger.getRootLogger().setLevel(Level.WARN);
    Logger.getLogger(this.getClass().getPackage().getName()).
      setLevel(Level.DEBUG);
  }

  /** {@inheritDoc} */
  @Override
  protected void setUp() throws Exception {
    super.setUp();
  }

  /** {@inheritDoc} */
  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }
  
  /**
   * 
   */
  public void testUpgrade() {
    MiniDFSCluster dfsCluster = null;
    try {
      dfsCluster = new MiniDFSCluster(conf, 2, true, (String[])null);
      FileSystem dfs = dfsCluster.getFileSystem();
      Path root = dfs.makeQualified(new Path(
          conf.get(HConstants.HBASE_DIR, HConstants.DEFAULT_HBASE_DIR)));
      dfs.mkdirs(root);

      /*
       * First load files from an old style HBase file structure
       */
      
      // Current directory is .../workspace/project/build/contrib/hbase/test/data
      
      FileSystem localfs = FileSystem.getLocal(conf);
      
      // Get path for zip file

      FSDataInputStream hs = localfs.open(new Path(Path.CUR_DIR,
          
          // this path is for running test with ant
          
          "../../../../../src/contrib/hbase/src/testdata/HADOOP-2478-testdata.zip")
      
          // and this path is for when you want to run inside eclipse
      
          /*"src/contrib/hbase/src/testdata/HADOOP-2478-testdata.zip")*/
      );
      
      ZipInputStream zip = new ZipInputStream(hs);
      
      unzip(zip, dfs, root);
      
      zip.close();
      hs.close();
      
      listPaths(dfs, root, root.toString().length() + 1);
      
      Migrate u = new Migrate(conf);
      u.run(new String[] {"check"});

      listPaths(dfs, root, root.toString().length() + 1);
      
      u = new Migrate(conf);
      u.run(new String[] {"upgrade"});

      listPaths(dfs, root, root.toString().length() + 1);
      
      // Remove version file and try again
      
      dfs.delete(new Path(root, HConstants.VERSION_FILE_NAME));
      u = new Migrate(conf);
      u.run(new String[] {"upgrade"});

      listPaths(dfs, root, root.toString().length() + 1);
      
      // Try again. No upgrade should be necessary
      
      u = new Migrate(conf);
      u.run(new String[] {"check"});
      u = new Migrate(conf);
      u.run(new String[] {"upgrade"});

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (dfsCluster != null) {
        StaticTestEnvironment.shutdownDfs(dfsCluster);
      }
    }
  }

  private void unzip(ZipInputStream zip, FileSystem dfs, Path root)
  throws IOException {

    ZipEntry e = null;
    while ((e = zip.getNextEntry()) != null)  {
      if (e.isDirectory()) {
        dfs.mkdirs(new Path(root, e.getName()));
        
      } else {
        FSDataOutputStream out = dfs.create(new Path(root, e.getName()));
        byte[] buffer = new byte[4096];
        int len;
        do {
          len = zip.read(buffer);
          if (len > 0) {
            out.write(buffer, 0, len);
          }
        } while (len > 0);
        out.close();
      }
      zip.closeEntry();
    }
  }
  
  private void listPaths(FileSystem fs, Path dir, int rootdirlength)
  throws IOException {
    FileStatus[] stats = fs.listStatus(dir);
    if (stats == null || stats.length == 0) {
      return;
    }
    for (int i = 0; i < stats.length; i++) {
      String relativePath =
        stats[i].getPath().toString().substring(rootdirlength);
      if (stats[i].isDir()) {
        System.out.println("d " + relativePath);
        listPaths(fs, stats[i].getPath(), rootdirlength);
      } else {
        System.out.println("f " + relativePath + " size=" + stats[i].getLen());
      }
    }
  }
}
