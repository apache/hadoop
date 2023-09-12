/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class TestDirCopyFilter {

  private static final String SRCROOT = "/src";
  private static final long BLOCK_SIZE = 1024;
  private static MiniDFSCluster cluster;
  private static Configuration conf;
  private static FileSystem fs;
  public static final Path DIR1 = new Path(SRCROOT, "dir1");
  public static final Path FILE0 = new Path(SRCROOT, "file0");
  public static final Path DIR1_FILE1 = new Path(DIR1, "file1");
  public static final Path DIR1_FILE2 = new Path(DIR1, "file2");
  public static final Path DIR1_DIR3 = new Path(DIR1, "dir3");
  public static final Path DIR1_DIR3_DIR4 = new Path(DIR1_DIR3, "dir4");
  public static final Path DIR1_DIR3_DIR4_FILE_3 = new Path(DIR1_DIR3_DIR4, "file1");
  public static final Path[] FILE_PATHS = new Path[]{FILE0, DIR1_FILE1,
          DIR1_FILE2, DIR1_DIR3_DIR4_FILE_3};
  public static final Path[] DIR_PATHS = new Path[]{DIR1, DIR1_DIR3, DIR1_DIR3_DIR4};
  @BeforeClass
  public static void setupClass() throws IOException {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();

    for(Path dirPath : DIR_PATHS){
      createDir(dirPath);
    }

    for(Path filePath : FILE_PATHS){
      createFile(filePath);
    }
  }

  @AfterClass
  public static void teardownClass() throws IOException {
    if (fs != null){
      fs.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }


  @Test
  public void testSupportFileStatus() {
    Configuration configuration = new Configuration(false);
    DirCopyFilter dirCopyFilter = new DirCopyFilter(configuration);
    assertThat(dirCopyFilter.supportFileStatus()).isTrue();
  }


  @Test
  public void testShouldCopyTrue() throws IOException {
    DirCopyFilter copyFilter = new DirCopyFilter(conf);
    Path[] dirPaths = new Path[]{DIR1, DIR1_DIR3, DIR1_DIR3_DIR4};

    for (Path dirPath : dirPaths) {
      assertThat(copyFilter.shouldCopy(dirStatus(dirPath)))
              .describedAs("should copy dir: " + dirPath)
              .isTrue();
      assertThat(copyFilter.shouldCopy(dirStatus(dirPath).getPath()))
              .describedAs("should copy dir: " + dirPath)
              .isTrue();
    }
  }

  @Test
  public void testShouldCopyFalse() {
    DirCopyFilter copyFilter = new DirCopyFilter(conf);


    for (Path filePath : FILE_PATHS) {
      assertThat(copyFilter.shouldCopy(fileStatus(filePath)))
              .describedAs("should copy file: " + filePath)
              .isFalse();
      assertThat(copyFilter.shouldCopy(filePath))
              .describedAs("should copy file: " + filePath)
              .isFalse();
    }
  }


  private CopyListingFileStatus newStatus(final Path path,
                                          final boolean isDir) {
    return new CopyListingFileStatus(new FileStatus(0, isDir, 0, 0, 0, path));
  }

  private CopyListingFileStatus dirStatus(final Path path) throws IOException {
    CopyListingFileStatus dirFileStatus = newStatus(path, true);
    return dirFileStatus;
  }

  private CopyListingFileStatus fileStatus(final Path path) {
    CopyListingFileStatus fileStatus = newStatus(path, false);
    return fileStatus;
  }

  private static void createFile(Path filePath) throws IOException {
    fs.createFile(filePath);
    Random random = new Random();

    DataOutputStream outputStream = null;
    try {
      outputStream = fs.create(filePath, true, 0);
      outputStream.write(new byte[random.nextInt((int)BLOCK_SIZE * 2)]);
    } finally {
      IOUtils.cleanupWithLogger(null, outputStream);
    }
  }

  private static void createDir(Path fileStatus) throws IOException {
    fs.mkdirs(fileStatus);
  }

}
