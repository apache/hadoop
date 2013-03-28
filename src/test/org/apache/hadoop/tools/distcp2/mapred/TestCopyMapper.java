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

package org.apache.hadoop.tools.distcp2.mapred;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.distcp2.DistCpConstants;
import org.apache.hadoop.tools.distcp2.DistCpOptionSwitch;
import org.apache.hadoop.tools.distcp2.DistCpOptions;
import org.apache.hadoop.tools.distcp2.StubContext;
import org.apache.hadoop.tools.distcp2.util.DistCpUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCopyMapper {
  private static final Log LOG = LogFactory.getLog(TestCopyMapper.class);
  private static List<Path> pathList = new ArrayList<Path>();
  private static int nFiles = 0;
  private static final int FILE_SIZE = 1024;

  private static MiniDFSCluster cluster;

  private static final String SOURCE_PATH = "/tmp/source";
  private static final String TARGET_PATH = "/tmp/target";

  private static Configuration configuration;

  @BeforeClass
  public static void setup() throws Exception {
    configuration = getConfigurationForCluster();
    cluster = new MiniDFSCluster(configuration, 1, true, null);
  }

  private static Configuration getConfigurationForCluster() throws IOException {
    Configuration configuration = new Configuration();
    System.setProperty("test.build.data", "target/tmp/build/TEST_COPY_MAPPER/data");
    configuration.set("hadoop.log.dir", "target/tmp");
    LOG.debug("fs.default.name  == " + configuration.get("fs.default.name"));
    LOG.debug("dfs.http.address == " + configuration.get("dfs.http.address"));
    return configuration;
  }

  private static Configuration getConfiguration() throws IOException {
    Configuration configuration = getConfigurationForCluster();
    final FileSystem fs = cluster.getFileSystem();
    Path workPath = new Path(TARGET_PATH)
            .makeQualified(fs.getUri(), fs.getWorkingDirectory());
    configuration.set(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH,
            workPath.toString());
    configuration.set(DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH,
            workPath.toString());
    configuration.setBoolean(DistCpOptionSwitch.OVERWRITE.getConfigLabel(),
            false);
    configuration.setBoolean(DistCpOptionSwitch.SKIP_CRC.getConfigLabel(),
            true);
    configuration.setBoolean(DistCpOptionSwitch.SYNC_FOLDERS.getConfigLabel(),
            true);
    configuration.set(DistCpOptionSwitch.PRESERVE_STATUS.getConfigLabel(),
            "br");
    return configuration;
  }

  private static void createSourceData() throws Exception {
    mkdirs(SOURCE_PATH + "/1");
    mkdirs(SOURCE_PATH + "/2");
    mkdirs(SOURCE_PATH + "/2/3/4");
    mkdirs(SOURCE_PATH + "/2/3");
    mkdirs(SOURCE_PATH + "/5");
    touchFile(SOURCE_PATH + "/5/6");
    mkdirs(SOURCE_PATH + "/7");
    mkdirs(SOURCE_PATH + "/7/8");
    touchFile(SOURCE_PATH + "/7/8/9");
  }

  private static void mkdirs(String path) throws Exception {
    FileSystem fileSystem = cluster.getFileSystem();
    final Path qualifiedPath = new Path(path).makeQualified(fileSystem.getUri(),
                                              fileSystem.getWorkingDirectory());
    pathList.add(qualifiedPath);
    fileSystem.mkdirs(qualifiedPath);
  }

  private static void touchFile(String path) throws Exception {
    FileSystem fs;
    DataOutputStream outputStream = null;
    try {
      fs = cluster.getFileSystem();
      final Path qualifiedPath = new Path(path).makeQualified(fs.getUri(),
                                                      fs.getWorkingDirectory());
      final long blockSize = fs.getDefaultBlockSize() * 2;
      outputStream = fs.create(qualifiedPath, true, 0,
              (short)(fs.getDefaultReplication()*2),
              blockSize);
      outputStream.write(new byte[FILE_SIZE]);
      pathList.add(qualifiedPath);
      ++nFiles;

      FileStatus fileStatus = fs.getFileStatus(qualifiedPath);
      System.out.println(fileStatus.getBlockSize());
      System.out.println(fileStatus.getReplication());
    }
    finally {
      IOUtils.cleanup(null, outputStream);
    }
  }

  @Test
  public void testRun() {
    try {
      deleteState();
      createSourceData();

      FileSystem fs = cluster.getFileSystem();
      CopyMapper copyMapper = new CopyMapper();
      StubContext stubContext = new StubContext(getConfiguration(), null, 0);
      Mapper<Text, FileStatus, Text, Text>.Context context
              = stubContext.getContext();
      copyMapper.setup(context);

      for (Path path: pathList) {
        copyMapper.map(new Text(DistCpUtils.getRelativePath(new Path(SOURCE_PATH), path)),
                fs.getFileStatus(path), context);
      }

      // Check that the maps worked.
      for (Path path : pathList) {
        final Path targetPath = new Path(path.toString()
                .replaceAll(SOURCE_PATH, TARGET_PATH));
        Assert.assertTrue(fs.exists(targetPath));
        Assert.assertTrue(fs.isFile(targetPath) == fs.isFile(path));
        Assert.assertEquals(fs.getFileStatus(path).getReplication(),
                fs.getFileStatus(targetPath).getReplication());
        Assert.assertEquals(fs.getFileStatus(path).getBlockSize(),
                fs.getFileStatus(targetPath).getBlockSize());
        Assert.assertTrue(!fs.isFile(targetPath) ||
                fs.getFileChecksum(targetPath).equals(
                        fs.getFileChecksum(path)));
      }

      Assert.assertEquals(pathList.size(),
              stubContext.getReporter().getCounter(CopyMapper.Counter.COPY).getValue());
      Assert.assertEquals(nFiles * FILE_SIZE,
              stubContext.getReporter().getCounter(CopyMapper.Counter.BYTESCOPIED).getValue());

      testCopyingExistingFiles(fs, copyMapper, context);
      for (Text value : stubContext.getWriter().values()) {
        Assert.assertTrue(value.toString() + " is not skipped", value.toString().startsWith("SKIP:"));
      }
    }
    catch (Exception e) {
      LOG.error("Unexpected exception: ", e);
      Assert.assertTrue(false);
    }
  }

  private void testCopyingExistingFiles(FileSystem fs, CopyMapper copyMapper,
                                        Mapper<Text, FileStatus, Text, Text>.Context context) {

    try {
      for (Path path : pathList) {
        copyMapper.map(new Text(DistCpUtils.getRelativePath(new Path(SOURCE_PATH), path)),
                fs.getFileStatus(path), context);
      }

      Assert.assertEquals(nFiles,
              context.getCounter(CopyMapper.Counter.SKIP).getValue());
    }
    catch (Exception exception) {
      Assert.assertTrue("Caught unexpected exception:" + exception.getMessage(),
              false);
    }
  }

  @Test
  public void testMakeDirFailure() {
    try {
      deleteState();
      createSourceData();

      FileSystem fs = cluster.getFileSystem();
      CopyMapper copyMapper = new CopyMapper();
      StubContext stubContext = new StubContext(getConfiguration(), null, 0);
      Mapper<Text, FileStatus, Text, Text>.Context context
              = stubContext.getContext();

      Configuration configuration = context.getConfiguration();
      String workPath = new Path("hftp://localhost:1234/*/*/*/?/")
              .makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString();
      configuration.set(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH,
              workPath);
      copyMapper.setup(context);

      copyMapper.map(new Text(DistCpUtils.getRelativePath(new Path(SOURCE_PATH), pathList.get(0))),
              fs.getFileStatus(pathList.get(0)), context);

      Assert.assertTrue("There should have been an exception.", false);
    }
    catch (Exception ignore) {
    }
  }

  @Test
  public void testIgnoreFailures() {
    doTestIgnoreFailures(true);
    doTestIgnoreFailures(false);
  }

  @Test
  public void testDirToFile() {
    try {
      deleteState();
      createSourceData();

      FileSystem fs = cluster.getFileSystem();
      CopyMapper copyMapper = new CopyMapper();
      StubContext stubContext = new StubContext(getConfiguration(), null, 0);
      Mapper<Text, FileStatus, Text, Text>.Context context
              = stubContext.getContext();

      mkdirs(SOURCE_PATH + "/src/file");
      touchFile(TARGET_PATH + "/src/file");
      try {
        copyMapper.setup(context);
        copyMapper.map(new Text("/src/file"),
            fs.getFileStatus(new Path(SOURCE_PATH + "/src/file")),
            context);
      } catch (IOException e) {
        Assert.assertTrue(e.getMessage().startsWith("Can't replace"));
      }
    } catch (Exception e) {
      LOG.error("Exception encountered ", e);
      Assert.fail("Test failed: " + e.getMessage());
    }
  }

  @Test
  public void testPreserve() {
    try {
      deleteState();
      createSourceData();

      UserGroupInformation tmpUser = UserGroupInformation.createRemoteUser("guest");

      final CopyMapper copyMapper = new CopyMapper();
      
      final Mapper<Text, FileStatus, Text, Text>.Context context =  tmpUser.
          doAs(new PrivilegedAction<Mapper<Text, FileStatus, Text, Text>.Context>() {
        @Override
        public Mapper<Text, FileStatus, Text, Text>.Context run() {
          try {
            StubContext stubContext = new StubContext(getConfiguration(), null, 0);
            return stubContext.getContext();
          } catch (Exception e) {
            LOG.error("Exception encountered ", e);
            throw new RuntimeException(e);
          }
        }
      });

      EnumSet<DistCpOptions.FileAttribute> preserveStatus =
          EnumSet.allOf(DistCpOptions.FileAttribute.class);

      context.getConfiguration().set(DistCpConstants.CONF_LABEL_PRESERVE_STATUS,
        DistCpUtils.packAttributes(preserveStatus));

      touchFile(SOURCE_PATH + "/src/file");
      mkdirs(TARGET_PATH);
      cluster.getFileSystem().setPermission(new Path(TARGET_PATH), new FsPermission((short)511));

      final FileSystem tmpFS = tmpUser.doAs(new PrivilegedAction<FileSystem>() {
        @Override
        public FileSystem run() {
          try {
            return FileSystem.get(configuration);
          } catch (IOException e) {
            LOG.error("Exception encountered ", e);
            Assert.fail("Test failed: " + e.getMessage());
            throw new RuntimeException("Test ought to fail here");
          }
        }
      });

      tmpUser.doAs(new PrivilegedAction<Integer>() {
        @Override
        public Integer run() {
          try {
            copyMapper.setup(context);
            copyMapper.map(new Text("/src/file"),
                tmpFS.getFileStatus(new Path(SOURCE_PATH + "/src/file")),
                context);
            Assert.fail("Expected copy to fail");
          } catch (AccessControlException e) {
            Assert.assertTrue("Got exception: " + e.getMessage(), true);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          return null;
        }
      });
    } catch (Exception e) {
      LOG.error("Exception encountered ", e);
      Assert.fail("Test failed: " + e.getMessage());
    }
  }

  @Test
  public void testCopyReadableFiles() {
    try {
      deleteState();
      createSourceData();

      UserGroupInformation tmpUser = UserGroupInformation.createRemoteUser("guest");

      final CopyMapper copyMapper = new CopyMapper();

      final Mapper<Text, FileStatus, Text, Text>.Context context =  tmpUser.
          doAs(new PrivilegedAction<Mapper<Text, FileStatus, Text, Text>.Context>() {
        @Override
        public Mapper<Text, FileStatus, Text, Text>.Context run() {
          try {
            StubContext stubContext = new StubContext(getConfiguration(), null, 0);
            return stubContext.getContext();
          } catch (Exception e) {
            LOG.error("Exception encountered ", e);
            throw new RuntimeException(e);
          }
        }
      });

      touchFile(SOURCE_PATH + "/src/file");
      mkdirs(TARGET_PATH);
      cluster.getFileSystem().setPermission(new Path(SOURCE_PATH + "/src/file"),
          new FsPermission(FsAction.READ, FsAction.READ, FsAction.READ));
      cluster.getFileSystem().setPermission(new Path(TARGET_PATH), new FsPermission((short)511));

      final FileSystem tmpFS = tmpUser.doAs(new PrivilegedAction<FileSystem>() {
        @Override
        public FileSystem run() {
          try {
            return FileSystem.get(configuration);
          } catch (IOException e) {
            LOG.error("Exception encountered ", e);
            Assert.fail("Test failed: " + e.getMessage());
            throw new RuntimeException("Test ought to fail here");
          }
        }
      });

      tmpUser.doAs(new PrivilegedAction<Integer>() {
        @Override
        public Integer run() {
          try {
            copyMapper.setup(context);
            copyMapper.map(new Text("/src/file"),
                tmpFS.getFileStatus(new Path(SOURCE_PATH + "/src/file")),
                context);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          return null;
        }
      });
    } catch (Exception e) {
      LOG.error("Exception encountered ", e);
      Assert.fail("Test failed: " + e.getMessage());
    }
  }

  @Test
  public void testSkipCopyNoPerms() {
    try {
      deleteState();
      createSourceData();

      UserGroupInformation tmpUser = UserGroupInformation.createRemoteUser("guest");

      final CopyMapper copyMapper = new CopyMapper();

      final StubContext stubContext =  tmpUser.
          doAs(new PrivilegedAction<StubContext>() {
        @Override
        public StubContext run() {
          try {
            return new StubContext(getConfiguration(), null, 0);
          } catch (Exception e) {
            LOG.error("Exception encountered ", e);
            throw new RuntimeException(e);
          }
        }
      });

      final Mapper<Text, FileStatus, Text, Text>.Context context = stubContext.getContext();
      EnumSet<DistCpOptions.FileAttribute> preserveStatus =
          EnumSet.allOf(DistCpOptions.FileAttribute.class);

      context.getConfiguration().set(DistCpConstants.CONF_LABEL_PRESERVE_STATUS,
        DistCpUtils.packAttributes(preserveStatus));

      touchFile(SOURCE_PATH + "/src/file");
      touchFile(TARGET_PATH + "/src/file");
      cluster.getFileSystem().setPermission(new Path(SOURCE_PATH + "/src/file"),
          new FsPermission(FsAction.READ, FsAction.READ, FsAction.READ));
      cluster.getFileSystem().setPermission(new Path(TARGET_PATH + "/src/file"),
          new FsPermission(FsAction.READ, FsAction.READ, FsAction.READ));

      final FileSystem tmpFS = tmpUser.doAs(new PrivilegedAction<FileSystem>() {
        @Override
        public FileSystem run() {
          try {
            return FileSystem.get(configuration);
          } catch (IOException e) {
            LOG.error("Exception encountered ", e);
            Assert.fail("Test failed: " + e.getMessage());
            throw new RuntimeException("Test ought to fail here");
          }
        }
      });

      tmpUser.doAs(new PrivilegedAction<Integer>() {
        @Override
        public Integer run() {
          try {
            copyMapper.setup(context);
            copyMapper.map(new Text("/src/file"),
                tmpFS.getFileStatus(new Path(SOURCE_PATH + "/src/file")),
                context);
            Assert.assertEquals(stubContext.getWriter().values().size(), 1);
            Assert.assertTrue(stubContext.getWriter().values().get(0).toString().startsWith("SKIP"));
            Assert.assertTrue(stubContext.getWriter().values().get(0).toString().
                contains(SOURCE_PATH + "/src/file"));
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          return null;
        }
      });
    } catch (Exception e) {
      LOG.error("Exception encountered ", e);
      Assert.fail("Test failed: " + e.getMessage());
    }
  }

  @Test
  public void testFailCopyWithAccessControlException() {
    try {
      deleteState();
      createSourceData();

      UserGroupInformation tmpUser = UserGroupInformation.createRemoteUser("guest");

      final CopyMapper copyMapper = new CopyMapper();

      final StubContext stubContext =  tmpUser.
          doAs(new PrivilegedAction<StubContext>() {
        @Override
        public StubContext run() {
          try {
            return new StubContext(getConfiguration(), null, 0);
          } catch (Exception e) {
            LOG.error("Exception encountered ", e);
            throw new RuntimeException(e);
          }
        }
      });

      EnumSet<DistCpOptions.FileAttribute> preserveStatus =
          EnumSet.allOf(DistCpOptions.FileAttribute.class);

      final Mapper<Text, FileStatus, Text, Text>.Context context
              = stubContext.getContext();
      
      context.getConfiguration().set(DistCpConstants.CONF_LABEL_PRESERVE_STATUS,
        DistCpUtils.packAttributes(preserveStatus));

      touchFile(SOURCE_PATH + "/src/file");
      OutputStream out = cluster.getFileSystem().create(new Path(TARGET_PATH + "/src/file"));
      out.write("hello world".getBytes());
      out.close();
      cluster.getFileSystem().setPermission(new Path(SOURCE_PATH + "/src/file"),
          new FsPermission(FsAction.READ, FsAction.READ, FsAction.READ));
      cluster.getFileSystem().setPermission(new Path(TARGET_PATH + "/src/file"),
          new FsPermission(FsAction.READ, FsAction.READ, FsAction.READ));

      final FileSystem tmpFS = tmpUser.doAs(new PrivilegedAction<FileSystem>() {
        @Override
        public FileSystem run() {
          try {
            return FileSystem.get(configuration);
          } catch (IOException e) {
            LOG.error("Exception encountered ", e);
            Assert.fail("Test failed: " + e.getMessage());
            throw new RuntimeException("Test ought to fail here");
          }
        }
      });

      tmpUser.doAs(new PrivilegedAction<Integer>() {
        @Override
        public Integer run() {
          try {
            copyMapper.setup(context);
            copyMapper.map(new Text("/src/file"),
                tmpFS.getFileStatus(new Path(SOURCE_PATH + "/src/file")),
                context);
            Assert.fail("Didn't expect the file to be copied");
          } catch (AccessControlException ignore) {
          } catch (Exception e) {
            if (e.getCause() == null || !(e.getCause() instanceof AccessControlException)) {
              throw new RuntimeException(e);
            }
          }
          return null;
        }
      });
    } catch (Exception e) {
      LOG.error("Exception encountered ", e);
      Assert.fail("Test failed: " + e.getMessage());
    }
  }

  @Test
  public void testFileToDir() {
    try {
      deleteState();
      createSourceData();

      FileSystem fs = cluster.getFileSystem();
      CopyMapper copyMapper = new CopyMapper();
      StubContext stubContext = new StubContext(getConfiguration(), null, 0);
      Mapper<Text, FileStatus, Text, Text>.Context context
              = stubContext.getContext();

      touchFile(SOURCE_PATH + "/src/file");
      mkdirs(TARGET_PATH + "/src/file");
      try {
        copyMapper.setup(context);
        copyMapper.map(new Text("/src/file"),
            fs.getFileStatus(new Path(SOURCE_PATH + "/src/file")),
            context);
      } catch (IOException e) {
        Assert.assertTrue(e.getMessage().startsWith("Can't replace"));
      }
    } catch (Exception e) {
      LOG.error("Exception encountered ", e);
      Assert.fail("Test failed: " + e.getMessage());
    }
  }

  private void doTestIgnoreFailures(boolean ignoreFailures) {
    try {
      deleteState();
      createSourceData();

      FileSystem fs = cluster.getFileSystem();
      CopyMapper copyMapper = new CopyMapper();
      StubContext stubContext = new StubContext(getConfiguration(), null, 0);
      Mapper<Text, FileStatus, Text, Text>.Context context
              = stubContext.getContext();

      Configuration configuration = context.getConfiguration();
      configuration.setBoolean(
              DistCpOptionSwitch.IGNORE_FAILURES.getConfigLabel(),ignoreFailures);
      configuration.setBoolean(DistCpOptionSwitch.OVERWRITE.getConfigLabel(),
              true);
      configuration.setBoolean(DistCpOptionSwitch.SKIP_CRC.getConfigLabel(),
              true);
      copyMapper.setup(context);

      for (Path path : pathList) {
        final FileStatus fileStatus = fs.getFileStatus(path);
        if (!fileStatus.isDir()) {
          fs.delete(path, true);
          copyMapper.map(new Text(DistCpUtils.getRelativePath(new Path(SOURCE_PATH), path)),
                  fileStatus, context);
        }
      }
      if (ignoreFailures) {
        for (Text value : stubContext.getWriter().values()) {
          Assert.assertTrue(value.toString() + " is not skipped", value.toString().startsWith("FAIL:"));
        }
      }
      Assert.assertTrue("There should have been an exception.", ignoreFailures);
    }
    catch (Exception e) {
      Assert.assertTrue("Unexpected exception: " + e.getMessage(),
              !ignoreFailures);
      e.printStackTrace();
    }
  }

  private static void deleteState() throws IOException {
    pathList.clear();
    nFiles = 0;
    cluster.getFileSystem().delete(new Path(SOURCE_PATH), true);
    cluster.getFileSystem().delete(new Path(TARGET_PATH), true);
  }

  @Test
  public void testPreserveBlockSizeAndReplication() {
    testPreserveBlockSizeAndReplicationImpl(true);
    testPreserveBlockSizeAndReplicationImpl(false);
  }

  private void testPreserveBlockSizeAndReplicationImpl(boolean preserve){
    try {

      deleteState();
      createSourceData();

      FileSystem fs = cluster.getFileSystem();
      CopyMapper copyMapper = new CopyMapper();
      StubContext stubContext = new StubContext(getConfiguration(), null, 0);
      Mapper<Text, FileStatus, Text, Text>.Context context
              = stubContext.getContext();

      Configuration configuration = context.getConfiguration();
      EnumSet<DistCpOptions.FileAttribute> fileAttributes
              = EnumSet.noneOf(DistCpOptions.FileAttribute.class);
      if (preserve) {
        fileAttributes.add(DistCpOptions.FileAttribute.BLOCKSIZE);
        fileAttributes.add(DistCpOptions.FileAttribute.REPLICATION);
      }
      configuration.set(DistCpOptionSwitch.PRESERVE_STATUS.getConfigLabel(),
              DistCpUtils.packAttributes(fileAttributes));

      copyMapper.setup(context);

      for (Path path : pathList) {
        final FileStatus fileStatus = fs.getFileStatus(path);
        copyMapper.map(new Text(DistCpUtils.getRelativePath(new Path(SOURCE_PATH), path)),
                fileStatus, context);
      }

      // Check that the block-size/replication aren't preserved.
      for (Path path : pathList) {
        final Path targetPath = new Path(path.toString()
                .replaceAll(SOURCE_PATH, TARGET_PATH));
        final FileStatus source = fs.getFileStatus(path);
        final FileStatus target = fs.getFileStatus(targetPath);
        if (!source.isDir() ) {
          Assert.assertTrue(preserve ||
                  source.getBlockSize() != target.getBlockSize());
          Assert.assertTrue(preserve ||
                  source.getReplication() != target.getReplication());
          Assert.assertTrue(!preserve ||
                  source.getBlockSize() == target.getBlockSize());
          Assert.assertTrue(!preserve ||
                  source.getReplication() == target.getReplication());
        }
      }
    }
    catch (Exception e) {
      Assert.assertTrue("Unexpected exception: " + e.getMessage(), false);
      e.printStackTrace();
    }
  }

  private static void changeUserGroup(String user, String group)
          throws IOException {
    FileSystem fs = cluster.getFileSystem();
    FsPermission changedPermission = new FsPermission(
            FsAction.ALL, FsAction.ALL, FsAction.ALL
    );
    for (Path path : pathList)
      if (fs.isFile(path)) {
        fs.setOwner(path, user, group);
        fs.setPermission(path, changedPermission);
      }
  }

  /**
   * If a single file is being copied to a location where the file (of the same
   * name) already exists, then the file shouldn't be skipped.
   */
  @Test
  public void testSingleFileCopy() {
    try {
      deleteState();
      touchFile(SOURCE_PATH + "/1");
      Path sourceFilePath = pathList.get(0);
      Path targetFilePath = new Path(sourceFilePath.toString().replaceAll(
              SOURCE_PATH, TARGET_PATH));
      touchFile(targetFilePath.toString());

      FileSystem fs = cluster.getFileSystem();
      CopyMapper copyMapper = new CopyMapper();
      StubContext stubContext = new StubContext(getConfiguration(), null, 0);
      Mapper<Text, FileStatus, Text, Text>.Context context
              = stubContext.getContext();

      context.getConfiguration().set(
              DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH,
              targetFilePath.getParent().toString()); // Parent directory.
      copyMapper.setup(context);

      final FileStatus sourceFileStatus = fs.getFileStatus(sourceFilePath);

      long before = fs.getFileStatus(targetFilePath).getModificationTime();
      copyMapper.map(new Text(DistCpUtils.getRelativePath(
              new Path(SOURCE_PATH), sourceFilePath)), sourceFileStatus, context);
      long after = fs.getFileStatus(targetFilePath).getModificationTime();

      Assert.assertTrue("File should have been skipped", before == after);

      context.getConfiguration().set(
              DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH,
              targetFilePath.toString()); // Specify the file path.
      copyMapper.setup(context);

      before = fs.getFileStatus(targetFilePath).getModificationTime();
      try { Thread.sleep(2); } catch (Throwable ignore) {}
      copyMapper.map(new Text(DistCpUtils.getRelativePath(
              new Path(SOURCE_PATH), sourceFilePath)), sourceFileStatus, context);
      after = fs.getFileStatus(targetFilePath).getModificationTime();

      Assert.assertTrue("File should have been overwritten.", before < after);

    } catch (Exception exception) {
      Assert.fail("Unexpected exception: " + exception.getMessage());
      exception.printStackTrace();
    }
  }

  @Test
  public void testPreserveUserGroup() {
    testPreserveUserGroupImpl(true);
    testPreserveUserGroupImpl(false);
  }

  private void testPreserveUserGroupImpl(boolean preserve){
    try {

      deleteState();
      createSourceData();
      changeUserGroup("Michael", "Corleone");

      FileSystem fs = cluster.getFileSystem();
      CopyMapper copyMapper = new CopyMapper();
      StubContext stubContext = new StubContext(getConfiguration(), null, 0);
      Mapper<Text, FileStatus, Text, Text>.Context context
              = stubContext.getContext();

      Configuration configuration = context.getConfiguration();
      EnumSet<DistCpOptions.FileAttribute> fileAttributes
              = EnumSet.noneOf(DistCpOptions.FileAttribute.class);
      if (preserve) {
        fileAttributes.add(DistCpOptions.FileAttribute.USER);
        fileAttributes.add(DistCpOptions.FileAttribute.GROUP);
        fileAttributes.add(DistCpOptions.FileAttribute.PERMISSION);
      }

      configuration.set(DistCpOptionSwitch.PRESERVE_STATUS.getConfigLabel(),
              DistCpUtils.packAttributes(fileAttributes));
      copyMapper.setup(context);

      for (Path path : pathList) {
        final FileStatus fileStatus = fs.getFileStatus(path);
        copyMapper.map(new Text(DistCpUtils.getRelativePath(new Path(SOURCE_PATH), path)),
                fileStatus, context);
      }

      // Check that the user/group attributes are preserved
      // (only) as necessary.
      for (Path path : pathList) {
        final Path targetPath = new Path(path.toString()
                .replaceAll(SOURCE_PATH, TARGET_PATH));
        final FileStatus source = fs.getFileStatus(path);
        final FileStatus target = fs.getFileStatus(targetPath);
        if (!source.isDir()) {
          Assert.assertTrue(!preserve || source.getOwner().equals(target.getOwner()));
          Assert.assertTrue(!preserve || source.getGroup().equals(target.getGroup()));
          Assert.assertTrue(!preserve || source.getPermission().equals(target.getPermission()));
          Assert.assertTrue( preserve || !source.getOwner().equals(target.getOwner()));
          Assert.assertTrue( preserve || !source.getGroup().equals(target.getGroup()));
          Assert.assertTrue( preserve || !source.getPermission().equals(target.getPermission()));
          Assert.assertTrue(source.isDir() ||
                  source.getReplication() != target.getReplication());
        }
      }
    }
    catch (Exception e) {
      Assert.assertTrue("Unexpected exception: " + e.getMessage(), false);
      e.printStackTrace();
    }
  }
}
