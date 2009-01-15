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
package org.apache.hadoop.hdfs;

import junit.framework.TestCase;
import java.io.IOException;
import java.io.DataOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FsShell;

/**
 * This class tests commands from Trash.
 */
public class TestTrash extends TestCase {

  static private Path writeFile(FileSystem fs, Path f) throws IOException {
    DataOutputStream out = fs.create(f);
    out.writeBytes("dhruba: " + f);
    out.close();
    assertTrue(fs.exists(f));
    return f;
  }

  static private Path mkdir(FileSystem fs, Path p) throws IOException {
    assertTrue(fs.mkdirs(p));
    assertTrue(fs.exists(p));
    assertTrue(fs.getFileStatus(p).isDir());
    return p;
  }

  // check that the specified file is in Trash
  static void checkTrash(FileSystem fs, Path trashRoot, String pathname)
                         throws IOException {
    Path p = new Path(trashRoot+"/"+new Path(pathname).getName());
    assertTrue(fs.exists(p));
  }

  // check that the specified file is not in Trash
  static void checkNotInTrash(FileSystem fs, Path trashRoot, String pathname)
                              throws IOException {
    Path p = new Path(trashRoot+"/"+ new Path(pathname).getName());
    assertTrue(!fs.exists(p));
  }

  static void show(String s) {
    System.out.println(Thread.currentThread().getStackTrace()[2] + " " + s);
  }

  /**
   * Tests Trash
   */
  public void testTrash() throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.trash.interval", "10"); // 10 minute
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    FileSystem fs = cluster.getFileSystem();
    FsShell shell = new FsShell();
    shell.setConf(conf);
    Path trashRoot = null;

    try {
      // First create a new directory with mkdirs
      Path myPath = new Path("/test/mkdirs");
      mkdir(fs, myPath);

      // Second, create a file in that directory.
      Path myFile = new Path("/test/mkdirs/myFile");
      writeFile(fs, myFile);

      // Verify that expunge without Trash directory 
      // won't throw Exception
      {
        String[] args = new String[1];
        args[0] = "-expunge";
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from Trash.run " +
                             e.getLocalizedMessage()); 
        }
        assertTrue(val == 0);
      }

      // Verify that we succeed in removing the file we created.
      // This should go into Trash.
      {
        String[] args = new String[2];
        args[0] = "-rm";
        args[1] = "/test/mkdirs/myFile";
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from Trash.run " +
                             e.getLocalizedMessage()); 
        }
        assertTrue(val == 0);

        trashRoot = shell.getCurrentTrashDir();
        checkTrash(fs, trashRoot, args[1]);
      }

      // Verify that we can recreate the file
      writeFile(fs, myFile);

      // Verify that we succeed in removing the file we re-created
      {
        String[] args = new String[2];
        args[0] = "-rm";
        args[1] = "/test/mkdirs/myFile";
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from Trash.run " +
                             e.getLocalizedMessage()); 
        }
        assertTrue(val == 0);
      }

      // Verify that we can recreate the file
      writeFile(fs, myFile);

      // Verify that we succeed in removing the whole directory
      // along with the file inside it.
      {
        String[] args = new String[2];
        args[0] = "-rmr";
        args[1] = "/test/mkdirs";
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from Trash.run " +
                             e.getLocalizedMessage()); 
        }
        assertTrue(val == 0);
      }

      // recreate directory 
      mkdir(fs, myPath);

      // Verify that we succeed in removing the whole directory
      {
        String[] args = new String[2];
        args[0] = "-rmr";
        args[1] = "/test/mkdirs";
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from Trash.run " +
                             e.getLocalizedMessage()); 
        }
        assertTrue(val == 0);
      }

      // Check that we can delete a file from the trash
      {
          Path toErase = new Path(trashRoot, "toErase");
          int retVal = -1;
          writeFile(fs, toErase);
          try {
            retVal = shell.run(new String[] {"-rm", toErase.toString()});
          } catch (Exception e) {
            System.err.println("Exception raised from Trash.run " +
                               e.getLocalizedMessage()); 
          }
          assertTrue(retVal == 0);
          checkNotInTrash (fs, trashRoot, toErase.toString());
          checkNotInTrash (fs, trashRoot, toErase.toString()+".1");
      }

      // simulate Trash removal
      {
        String[] args = new String[1];
        args[0] = "-expunge";
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from Trash.run " +
                             e.getLocalizedMessage()); 
        }
        assertTrue(val == 0);
      }

      // verify that after expunging the Trash, it really goes away
      checkNotInTrash(fs, trashRoot, "/test/mkdirs/myFile");

      // recreate directory and file
      mkdir(fs, myPath);
      writeFile(fs, myFile);

      // remove file first, then remove directory
      {
        String[] args = new String[2];
        args[0] = "-rm";
        args[1] = "/test/mkdirs/myFile";
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from Trash.run " +
                             e.getLocalizedMessage()); 
        }
        assertTrue(val == 0);
        checkTrash(fs, trashRoot, args[1]);

        args = new String[2];
        args[0] = "-rmr";
        args[1] = "/test/mkdirs";
        val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from Trash.run " +
                             e.getLocalizedMessage()); 
        }
        assertTrue(val == 0);
        checkTrash(fs, trashRoot, args[1]);
      }

      // attempt to remove parent of trash
      {
        String[] args = new String[2];
        args[0] = "-rmr";
        args[1] = trashRoot.getParent().getParent().toString();
        int val = -1;
        try {
          val = shell.run(args);
        } catch (Exception e) {
          System.err.println("Exception raised from Trash.run " +
                             e.getLocalizedMessage()); 
        }
        assertTrue(val == -1);
        assertTrue(fs.exists(trashRoot));
      }
    } finally {
      try {
        fs.close();
      } catch (Exception e) {
      }
      cluster.shutdown();
    }
  }
}
