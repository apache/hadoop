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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * This class tests snapshot functionality. One or multiple snapshots are
 * created. The snapshotted directory is changed and verification is done to
 * ensure snapshots remain unchanges.
 */
public class TestSnapshot {
  protected static final long seed = 0;
  protected static final short REPLICATION = 3;
  protected static final long BLOCKSIZE = 1024;
  public static final int SNAPSHOTNUMBER = 10;

  private final Path dir = new Path("/TestSnapshot");
  private final Path sub1 = new Path(dir, "sub1");
  private final Path subsub1 = new Path(sub1, "subsub1");
  
  protected Configuration conf;
  protected MiniDFSCluster cluster;
  protected FSNamesystem fsn;
  protected DistributedFileSystem hdfs;

  /**
   * The list recording all previous snapshots. Each element in the array
   * records a snapshot root.
   */
  protected static ArrayList<Path> snapshotList = new ArrayList<Path>();
  
  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
        .build();
    cluster.waitActive();

    fsn = cluster.getNamesystem();
    hdfs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Make changes (modification, deletion, creation) to the current files/dir.
   * Then check if the previous snapshots are still correct.
   * 
   * @param modifications Modifications that to be applied to the current dir.
   */
  public void modifyCurrentDirAndCheckSnapshots(Modification[] modifications)
      throws Exception {
    for (Modification modification : modifications) {
      modification.loadSnapshots();
      modification.modify();
      modification.checkSnapshots();
    }
  }

  /**
   * Generate the snapshot name based on its index.
   * 
   * @param snapshotIndex The index of the snapshot
   * @return The snapshot name
   */
  private String genSnapshotName(int snapshotIndex) {
    return "s" + snapshotIndex;
  }

  /**
   * Main test, where we will go in the following loop:
   * 
   * Create snapshot <----------------------+ -> Check snapshot creation | ->
   * Change the current/live files/dir | -> Check previous snapshots
   * -----------+
   * 
   * @param snapshottedDir The dir to be snapshotted
   * @param modificiationsList The list of modifications. Each element in the
   *          list is a group of modifications applied to current dir.
   */
  protected void testSnapshot(Path snapshottedDir,
      ArrayList<Modification[]> modificationsList) throws Exception {
    int snapshotIndex = 0;
    for (Modification[] modifications : modificationsList) {
      // 1. create snapshot
      // TODO: we also need to check creating snapshot for a directory under a
      // snapshottable directory
      Path snapshotRoot = SnapshotTestHelper.createSnapshot(hdfs,
          snapshottedDir, genSnapshotName(snapshotIndex++));
      snapshotList.add(snapshotRoot);
      // 2. Check the basic functionality of the snapshot(s)
      SnapshotTestHelper.checkSnapshotCreation(hdfs, snapshotRoot,
          snapshottedDir);
      // 3. Make changes to the current directory
      for (Modification m : modifications) {
        m.loadSnapshots();
        m.modify();
        m.checkSnapshots();
      }
    }
  }

  /**
   * Prepare a list of modifications. A modification may be a file creation,
   * file deletion, or a modification operation such as appending to an existing
   * file.
   * 
   * @param number
   *          Number of times that we make modifications to the current
   *          directory.
   * @return A list of modifications. Each element in the list is a group of
   *         modifications that will be apply to the "current" directory.
   * @throws Exception
   */
  private ArrayList<Modification[]> prepareModifications(int number)
      throws Exception {
    final Path[] files = new Path[3];
    files[0] = new Path(sub1, "file0");
    files[1] = new Path(sub1, "file1");
    files[2] = new Path(sub1, "file2");
    DFSTestUtil.createFile(hdfs, files[0], BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, files[1], BLOCKSIZE, REPLICATION, seed);

    ArrayList<Modification[]> mList = new ArrayList<Modification[]>();
    //
    // Modification iterations are as follows:
    // Iteration 0 - delete:file0, append:file1, create:file2
    // Iteration 1 - delete:file1, append:file2, create:file0
    // Iteration 3 - delete:file2, append:file0, create:file1
    // ...
    //
    for (int i = 0; i < number; i++) {
      Modification[] mods = new Modification[3];
      // delete files[i % 3]
      mods[0] = new FileDeletion(files[i % 3], hdfs);
      // modify files[(i+1) % 3]
      mods[1] = new FileAppend(files[(i + 1) % 3], hdfs, (int) BLOCKSIZE);
      // create files[(i+2) % 3]
      mods[2] = new FileCreation(files[(i + 2) % 3], hdfs, (int) BLOCKSIZE);
      mList.add(mods);
    }
    return mList;
  }

  @Test
  public void testSnapshot() throws Exception {
    ArrayList<Modification[]> mList = prepareModifications(SNAPSHOTNUMBER);
    testSnapshot(sub1, mList);
  }

  /**
   * Creating snapshots for a directory that is not snapshottable must fail.
   * 
   * TODO: Listing/Deleting snapshots for a directory that is not snapshottable
   * should also fail.
   */
  @Test
  public void testSnapshottableDirectory() throws Exception {
    Path file0 = new Path(sub1, "file0");
    Path file1 = new Path(sub1, "file1");
    DFSTestUtil.createFile(hdfs, file0, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPLICATION, seed);

    exception.expect(RemoteException.class);
    hdfs.createSnapshot("s1", sub1.toString());
  }
    
  /**
   * Deleting snapshottable directory with snapshots must fail.
   */
  @Test
  public void testDeleteDirectoryWithSnapshot() throws Exception {
    Path file0 = new Path(sub1, "file0");
    Path file1 = new Path(sub1, "file1");
    DFSTestUtil.createFile(hdfs, file0, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPLICATION, seed);

    // Allow snapshot for sub1, and create snapshot for it
    hdfs.allowSnapshot(sub1.toString());
    hdfs.createSnapshot("s1", sub1.toString());

    // Deleting a snapshottable dir with snapshots should fail
    exception.expect(RemoteException.class);
    hdfs.delete(sub1, true);
  }
  
  /**
   * Deleting directory with snapshottable descendant with snapshots must fail.
   */
  @Test
  public void testDeleteDirectoryWithSnapshot2() throws Exception {
    Path file0 = new Path(sub1, "file0");
    Path file1 = new Path(sub1, "file1");
    DFSTestUtil.createFile(hdfs, file0, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPLICATION, seed);
    
    Path subfile1 = new Path(subsub1, "file0");
    Path subfile2 = new Path(subsub1, "file1");
    DFSTestUtil.createFile(hdfs, subfile1, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, subfile2, BLOCKSIZE, REPLICATION, seed);

    // Allow snapshot for subsub1, and create snapshot for it
    hdfs.allowSnapshot(subsub1.toString());
    hdfs.createSnapshot("s1", subsub1.toString());

    // Deleting dir while its descedant subsub1 having snapshots should fail
    exception.expect(RemoteException.class);
    hdfs.delete(dir, true);
  }  
  
  /**
   * Base class to present changes applied to current file/dir. A modification
   * can be file creation, deletion, or other modifications such as appending on
   * an existing file. Three abstract methods need to be implemented by
   * subclasses: loadSnapshots() captures the states of snapshots before the
   * modification, modify() applies the modification to the current directory,
   * and checkSnapshots() verifies the snapshots do not change after the
   * modification.
   */
  static abstract class Modification {
    protected final Path file;
    protected final FileSystem fs;
    final String type;
    protected final Random random;

    Modification(Path file, FileSystem fs, String type) {
      this.file = file;
      this.fs = fs;
      this.type = type;
      this.random = new Random();
    }

    abstract void loadSnapshots() throws Exception;

    abstract void modify() throws Exception;

    abstract void checkSnapshots() throws Exception;
  }

  /**
   * Appending a specified length to an existing file
   */
  static class FileAppend extends Modification {
    final int appendLen;
    private final HashMap<Path, Long> snapshotFileLengthMap;

    FileAppend(Path file, FileSystem fs, int len) throws Exception {
      super(file, fs, "append");
      assert len >= 0;
      this.appendLen = len;
      this.snapshotFileLengthMap = new HashMap<Path, Long>();
    }

    @Override
    void loadSnapshots() throws Exception {
      for (Path snapshotRoot : snapshotList) {
        Path snapshotFile = new Path(snapshotRoot, file.getName());
        if (fs.exists(snapshotFile)) {
          long snapshotFileLen = fs.getFileStatus(snapshotFile).getLen();
          snapshotFileLengthMap.put(snapshotFile, snapshotFileLen);
        } else {
          snapshotFileLengthMap.put(snapshotFile, -1L);
        }
      }
    }

    @Override
    void modify() throws Exception {
      assert fs.exists(file);
      FSDataOutputStream out = fs.append(file);
      byte[] buffer = new byte[appendLen];
      random.nextBytes(buffer);
      out.write(buffer);
      out.close();
    }

    @Override
    void checkSnapshots() throws Exception {
      byte[] buffer = new byte[32];
      for (Path snapshotRoot : snapshotList) {
        Path snapshotFile = new Path(snapshotRoot, file.getName());
        long currentSnapshotFileLen = -1L;
        if (fs.exists(snapshotFile)) {
          currentSnapshotFileLen = fs.getFileStatus(snapshotFile).getLen();
        }
        long originalSnapshotFileLen = snapshotFileLengthMap.get(snapshotFile);
        assertEquals(currentSnapshotFileLen, originalSnapshotFileLen);
        // Read the snapshot file out of the boundary
        if (fs.exists(snapshotFile)) {
          FSDataInputStream input = fs.open(snapshotFile);
          int readLen = input.read(currentSnapshotFileLen, buffer, 0, 1);
          assertEquals(readLen, -1);
        }
      }
    }
  }

  /**
   * New file creation
   */
  static class FileCreation extends Modification {
    final int fileLen;
    private final HashMap<Path, FileStatus> fileStatusMap;

    FileCreation(Path file, FileSystem fs, int len) {
      super(file, fs, "creation");
      assert len >= 0;
      this.fileLen = len;
      fileStatusMap = new HashMap<Path, FileStatus>();
    }

    @Override
    void loadSnapshots() throws Exception {
      for (Path snapshotRoot : snapshotList) {
        Path snapshotFile = new Path(snapshotRoot, file.getName());
        boolean exist = fs.exists(snapshotFile);
        if (exist) {
          fileStatusMap.put(snapshotFile, fs.getFileStatus(snapshotFile));
        } else {
          fileStatusMap.put(snapshotFile, null);
        }
      }
    }

    @Override
    void modify() throws Exception {
      DFSTestUtil.createFile(fs, file, fileLen, fileLen, BLOCKSIZE,
          REPLICATION, seed);
    }

    @Override
    void checkSnapshots() throws Exception {
      for (Path snapshotRoot : snapshotList) {
        Path snapshotFile = new Path(snapshotRoot, file.getName());
        boolean currentSnapshotFileExist = fs.exists(snapshotFile);
        boolean originalSnapshotFileExist = !(fileStatusMap.get(snapshotFile) == null);
        assertEquals(currentSnapshotFileExist, originalSnapshotFileExist);
        if (currentSnapshotFileExist) {
          FileStatus currentSnapshotStatus = fs.getFileStatus(snapshotFile);
          FileStatus originalStatus = fileStatusMap.get(snapshotFile);
          assertEquals(currentSnapshotStatus, originalStatus);
        }
      }
    }
  }

  /**
   * File deletion
   */
  static class FileDeletion extends Modification {
    private final HashMap<Path, Boolean> snapshotFileExistenceMap;

    FileDeletion(Path file, FileSystem fs) {
      super(file, fs, "deletion");
      snapshotFileExistenceMap = new HashMap<Path, Boolean>();
    }

    @Override
    void loadSnapshots() throws Exception {
      for (Path snapshotRoot : snapshotList) {
        Path snapshotFile = new Path(snapshotRoot, file.getName());
        boolean existence = fs.exists(snapshotFile);
        snapshotFileExistenceMap.put(snapshotFile, existence);
      }
    }

    @Override
    void modify() throws Exception {
      fs.delete(file, true);
    }

    @Override
    void checkSnapshots() throws Exception {
      for (Path snapshotRoot : snapshotList) {
        Path snapshotFile = new Path(snapshotRoot, file.getName());
        boolean currentSnapshotFileExist = fs.exists(snapshotFile);
        boolean originalSnapshotFileExist = snapshotFileExistenceMap
            .get(snapshotFile);
        assertEquals(currentSnapshotFileExist, originalSnapshotFileExist);
      }
    }
  }
}
