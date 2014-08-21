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

package org.apache.hadoop.tools.util;

import org.apache.hadoop.tools.DistCpOptions.FileAttribute;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;

import java.util.EnumSet;
import java.util.Random;
import java.util.Stack;
import java.io.IOException;
import java.io.OutputStream;

public class TestDistCpUtils {
  private static final Log LOG = LogFactory.getLog(TestDistCpUtils.class);

  private static final Configuration config = new Configuration();
  private static MiniDFSCluster cluster;

  @BeforeClass
  public static void create() throws IOException {
    cluster = new MiniDFSCluster.Builder(config).numDataNodes(1).format(true)
                                                .build(); 
  }

  @AfterClass
  public static void destroy() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testGetRelativePathRoot() {
    Path root = new Path("/tmp/abc");
    Path child = new Path("/tmp/abc/xyz/file");
    Assert.assertEquals(DistCpUtils.getRelativePath(root, child), "/xyz/file");

    root = new Path("/");
    child = new Path("/a");
    Assert.assertEquals(DistCpUtils.getRelativePath(root, child), "/a");
  }

  @Test
  public void testPackAttributes() {
    EnumSet<FileAttribute> attributes = EnumSet.noneOf(FileAttribute.class);
    Assert.assertEquals(DistCpUtils.packAttributes(attributes), "");

    attributes.add(FileAttribute.REPLICATION);
    Assert.assertEquals(DistCpUtils.packAttributes(attributes), "R");
    Assert.assertEquals(attributes, DistCpUtils.unpackAttributes("R"));

    attributes.add(FileAttribute.BLOCKSIZE);
    Assert.assertEquals(DistCpUtils.packAttributes(attributes), "RB");
    Assert.assertEquals(attributes, DistCpUtils.unpackAttributes("RB"));

    attributes.add(FileAttribute.USER);
    Assert.assertEquals(DistCpUtils.packAttributes(attributes), "RBU");
    Assert.assertEquals(attributes, DistCpUtils.unpackAttributes("RBU"));

    attributes.add(FileAttribute.GROUP);
    Assert.assertEquals(DistCpUtils.packAttributes(attributes), "RBUG");
    Assert.assertEquals(attributes, DistCpUtils.unpackAttributes("RBUG"));

    attributes.add(FileAttribute.PERMISSION);
    Assert.assertEquals(DistCpUtils.packAttributes(attributes), "RBUGP");
    Assert.assertEquals(attributes, DistCpUtils.unpackAttributes("RBUGP"));
  }

  @Test
  public void testPreserve() {
    try {
      FileSystem fs = FileSystem.get(config);
      EnumSet<FileAttribute> attributes = EnumSet.noneOf(FileAttribute.class);


      Path path = new Path("/tmp/abc");
      Path src = new Path("/tmp/src");
      fs.mkdirs(path);
      fs.mkdirs(src);
      CopyListingFileStatus srcStatus = new CopyListingFileStatus(
        fs.getFileStatus(src));

      FsPermission noPerm = new FsPermission((short) 0);
      fs.setPermission(path, noPerm);
      fs.setOwner(path, "nobody", "nobody");

      DistCpUtils.preserve(fs, path, srcStatus, attributes, false);
      FileStatus target = fs.getFileStatus(path);
      Assert.assertEquals(target.getPermission(), noPerm);
      Assert.assertEquals(target.getOwner(), "nobody");
      Assert.assertEquals(target.getGroup(), "nobody");

      attributes.add(FileAttribute.PERMISSION);
      DistCpUtils.preserve(fs, path, srcStatus, attributes, false);
      target = fs.getFileStatus(path);
      Assert.assertEquals(target.getPermission(), srcStatus.getPermission());
      Assert.assertEquals(target.getOwner(), "nobody");
      Assert.assertEquals(target.getGroup(), "nobody");

      attributes.add(FileAttribute.GROUP);
      attributes.add(FileAttribute.USER);
      DistCpUtils.preserve(fs, path, srcStatus, attributes, false);
      target = fs.getFileStatus(path);
      Assert.assertEquals(target.getPermission(), srcStatus.getPermission());
      Assert.assertEquals(target.getOwner(), srcStatus.getOwner());
      Assert.assertEquals(target.getGroup(), srcStatus.getGroup());

      fs.delete(path, true);
      fs.delete(src, true);
    } catch (IOException e) {
      LOG.error("Exception encountered ", e);
      Assert.fail("Preserve test failure");
    }
  }

  private static Random rand = new Random();

  public static String createTestSetup(FileSystem fs) throws IOException {
    return createTestSetup("/tmp1", fs, FsPermission.getDefault());
  }
  
  public static String createTestSetup(FileSystem fs,
                                       FsPermission perm) throws IOException {
    return createTestSetup("/tmp1", fs, perm);
  }

  public static String createTestSetup(String baseDir,
                                       FileSystem fs,
                                       FsPermission perm) throws IOException {
    String base = getBase(baseDir);
    fs.mkdirs(new Path(base + "/newTest/hello/world1"));
    fs.mkdirs(new Path(base + "/newTest/hello/world2/newworld"));
    fs.mkdirs(new Path(base + "/newTest/hello/world3/oldworld"));
    fs.setPermission(new Path(base + "/newTest"), perm);
    fs.setPermission(new Path(base + "/newTest/hello"), perm);
    fs.setPermission(new Path(base + "/newTest/hello/world1"), perm);
    fs.setPermission(new Path(base + "/newTest/hello/world2"), perm);
    fs.setPermission(new Path(base + "/newTest/hello/world2/newworld"), perm);
    fs.setPermission(new Path(base + "/newTest/hello/world3"), perm);
    fs.setPermission(new Path(base + "/newTest/hello/world3/oldworld"), perm);
    createFile(fs, base + "/newTest/1");
    createFile(fs, base + "/newTest/hello/2");
    createFile(fs, base + "/newTest/hello/world3/oldworld/3");
    createFile(fs, base + "/newTest/hello/world2/4");
    return base;
  }

  private static String getBase(String base) {
    String location = String.valueOf(rand.nextLong());
    return base + "/" + location;
  }

  public static void delete(FileSystem fs, String path) {
    try {
      if (fs != null) {
        if (path != null) {
          fs.delete(new Path(path), true);
        }
      }
    } catch (IOException e) {
      LOG.warn("Exception encountered ", e);
    }
  }

  public static void createFile(FileSystem fs, String filePath) throws IOException {
    OutputStream out = fs.create(new Path(filePath));
    IOUtils.closeStream(out);
  }

  public static boolean checkIfFoldersAreInSync(FileSystem fs, String targetBase, String sourceBase)
      throws IOException {
    Path base = new Path(targetBase);

     Stack<Path> stack = new Stack<Path>();
     stack.push(base);
     while (!stack.isEmpty()) {
       Path file = stack.pop();
       if (!fs.exists(file)) continue;
       FileStatus[] fStatus = fs.listStatus(file);
       if (fStatus == null || fStatus.length == 0) continue;

       for (FileStatus status : fStatus) {
         if (status.isDirectory()) {
           stack.push(status.getPath());
         }
         Assert.assertTrue(fs.exists(new Path(sourceBase + "/" +
             DistCpUtils.getRelativePath(new Path(targetBase), status.getPath()))));
       }
     }
     return true;
  }
}
