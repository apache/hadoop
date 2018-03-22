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

package org.apache.hadoop.tools.mapred;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.DistCpOptionSwitch;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.StubContext;
import org.apache.hadoop.tools.util.DistCpUtils;
import org.apache.hadoop.util.DataChecksum;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;

public class TestCopyMapper {
  private static final Log LOG = LogFactory.getLog(TestCopyMapper.class);
  private static List<Path> pathList = new ArrayList<Path>();
  private static int nFiles = 0;
  private static final int DEFAULT_FILE_SIZE = 1024;
  private static final long NON_DEFAULT_BLOCK_SIZE = 4096;

  private static MiniDFSCluster cluster;

  private static final String SOURCE_PATH = "/tmp/source";
  private static final String TARGET_PATH = "/tmp/target";

  private static Configuration configuration;

  @BeforeClass
  public static void setup() throws Exception {
    configuration = getConfigurationForCluster();
    cluster = new MiniDFSCluster.Builder(configuration)
                .numDataNodes(1)
                .format(true)
                .build();
  }

  private static Configuration getConfigurationForCluster() throws IOException {
    Configuration configuration = new Configuration();
    System.setProperty("test.build.data", "target/tmp/build/TEST_COPY_MAPPER/data");
    configuration.set("hadoop.log.dir", "target/tmp");
    configuration.set("dfs.namenode.fs-limits.min-block-size", "0");
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
            false);
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

  private static void appendSourceData() throws Exception {
    FileSystem fs = cluster.getFileSystem();
    for (Path source : pathList) {
      if (fs.getFileStatus(source).isFile()) {
        // append 2048 bytes per file
        appendFile(source, DEFAULT_FILE_SIZE * 2);
      }
    }
  }

  private static void createSourceDataWithDifferentBlockSize() throws Exception {
    mkdirs(SOURCE_PATH + "/1");
    mkdirs(SOURCE_PATH + "/2");
    mkdirs(SOURCE_PATH + "/2/3/4");
    mkdirs(SOURCE_PATH + "/2/3");
    mkdirs(SOURCE_PATH + "/5");
    touchFile(SOURCE_PATH + "/5/6", true, null);
    mkdirs(SOURCE_PATH + "/7");
    mkdirs(SOURCE_PATH + "/7/8");
    touchFile(SOURCE_PATH + "/7/8/9");
  }

  private static void createSourceDataWithDifferentChecksumType()
      throws Exception {
    mkdirs(SOURCE_PATH + "/1");
    mkdirs(SOURCE_PATH + "/2");
    mkdirs(SOURCE_PATH + "/2/3/4");
    mkdirs(SOURCE_PATH + "/2/3");
    mkdirs(SOURCE_PATH + "/5");
    touchFile(SOURCE_PATH + "/5/6", new ChecksumOpt(DataChecksum.Type.CRC32,
        512));
    mkdirs(SOURCE_PATH + "/7");
    mkdirs(SOURCE_PATH + "/7/8");
    touchFile(SOURCE_PATH + "/7/8/9", new ChecksumOpt(DataChecksum.Type.CRC32C,
        512));
  }

  private static void mkdirs(String path) throws Exception {
    FileSystem fileSystem = cluster.getFileSystem();
    final Path qualifiedPath = new Path(path).makeQualified(fileSystem.getUri(),
                                              fileSystem.getWorkingDirectory());
    pathList.add(qualifiedPath);
    fileSystem.mkdirs(qualifiedPath);
  }

  private static void touchFile(String path) throws Exception {
    touchFile(path, false, null);
  }

  private static void touchFile(String path, ChecksumOpt checksumOpt)
      throws Exception {
    // create files with specific checksum opt and non-default block size
    touchFile(path, true, checksumOpt);
  }

  private static void touchFile(String path, boolean createMultipleBlocks,
      ChecksumOpt checksumOpt) throws Exception {
    FileSystem fs;
    DataOutputStream outputStream = null;
    try {
      fs = cluster.getFileSystem();
      final Path qualifiedPath = new Path(path).makeQualified(fs.getUri(),
          fs.getWorkingDirectory());
      final long blockSize = createMultipleBlocks ? NON_DEFAULT_BLOCK_SIZE : fs
          .getDefaultBlockSize(qualifiedPath) * 2;
      FsPermission permission = FsPermission.getFileDefault().applyUMask(
          FsPermission.getUMask(fs.getConf()));
      outputStream = fs.create(qualifiedPath, permission,
          EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE), 0,
          (short) (fs.getDefaultReplication(qualifiedPath) * 2), blockSize,
          null, checksumOpt);
      byte[] bytes = new byte[DEFAULT_FILE_SIZE];
      outputStream.write(bytes);
      long fileSize = DEFAULT_FILE_SIZE;
      if (createMultipleBlocks) {
        while (fileSize < 2*blockSize) {
          outputStream.write(bytes);
          outputStream.flush();
          fileSize += DEFAULT_FILE_SIZE;
        }
      }
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

  /**
   * Append specified length of bytes to a given file
   */
  private static void appendFile(Path p, int length) throws IOException {
    byte[] toAppend = new byte[length];
    Random random = new Random();
    random.nextBytes(toAppend);
    FSDataOutputStream out = cluster.getFileSystem().append(p);
    try {
      out.write(toAppend);
    } finally {
      IOUtils.closeStream(out);
    }
  }

  @Test
  public void testCopyWithDifferentChecksumType() throws Exception {
    testCopy(true);
  }

  @Test(timeout=40000)
  public void testRun() throws Exception {
    testCopy(false);
  }

  @Test
  public void testCopyWithAppend() throws Exception {
    final FileSystem fs = cluster.getFileSystem();
    // do the first distcp
    testCopy(false);
    // start appending data to source
    appendSourceData();

    // do the distcp again with -update and -append option
    CopyMapper copyMapper = new CopyMapper();
    Configuration conf = getConfiguration();
    // set the buffer size to 1/10th the size of the file.
    conf.setInt(DistCpOptionSwitch.COPY_BUFFER_SIZE.getConfigLabel(),
        DEFAULT_FILE_SIZE/10);
    StubContext stubContext = new StubContext(conf, null, 0);
    Mapper<Text, CopyListingFileStatus, Text, Text>.Context context =
        stubContext.getContext();
    // Enable append 
    context.getConfiguration().setBoolean(
        DistCpOptionSwitch.APPEND.getConfigLabel(), true);
    copyMapper.setup(context);

    int numFiles = 0;
    MetricsRecordBuilder rb =
        getMetrics(cluster.getDataNodes().get(0).getMetrics().name());
    String readCounter = "ReadsFromLocalClient";
    long readsFromClient = getLongCounter(readCounter, rb);
    for (Path path: pathList) {
      if (fs.getFileStatus(path).isFile()) {
        numFiles++;
      }

      copyMapper.map(new Text(DistCpUtils.getRelativePath(new Path(SOURCE_PATH), path)),
              new CopyListingFileStatus(cluster.getFileSystem().getFileStatus(
                  path)), context);
    }

    verifyCopy(fs, false);
    // verify that we only copied new appended data
    Assert.assertEquals(nFiles * DEFAULT_FILE_SIZE * 2, stubContext
        .getReporter().getCounter(CopyMapper.Counter.BYTESCOPIED)
        .getValue());
    Assert.assertEquals(numFiles, stubContext.getReporter().
        getCounter(CopyMapper.Counter.COPY).getValue());
    rb = getMetrics(cluster.getDataNodes().get(0).getMetrics().name());
    /*
     * added as part of HADOOP-15292 to ensure that multiple readBlock()
     * operations are not performed to read a block from a single Datanode.
     * assert assumes that there is only one block per file, and that the number
     * of files appended to in appendSourceData() above is captured by the
     * variable numFiles.
     */
    assertCounter(readCounter, readsFromClient + numFiles, rb);
  }

  private void testCopy(boolean preserveChecksum) throws Exception {
    deleteState();
    if (preserveChecksum) {
      createSourceDataWithDifferentChecksumType();
    } else {
      createSourceData();
    }

    FileSystem fs = cluster.getFileSystem();
    CopyMapper copyMapper = new CopyMapper();
    StubContext stubContext = new StubContext(getConfiguration(), null, 0);
    Mapper<Text, CopyListingFileStatus, Text, Text>.Context context
            = stubContext.getContext();

    Configuration configuration = context.getConfiguration();
    EnumSet<DistCpOptions.FileAttribute> fileAttributes
            = EnumSet.of(DistCpOptions.FileAttribute.REPLICATION);
    if (preserveChecksum) {
      fileAttributes.add(DistCpOptions.FileAttribute.CHECKSUMTYPE);
    }
    configuration.set(DistCpOptionSwitch.PRESERVE_STATUS.getConfigLabel(),
            DistCpUtils.packAttributes(fileAttributes));

    copyMapper.setup(context);

    int numFiles = 0;
    int numDirs = 0;
    for (Path path : pathList) {
      if (fs.getFileStatus(path).isDirectory()) {
        numDirs++;
      } else {
        numFiles++;
      }

      copyMapper.map(
          new Text(DistCpUtils.getRelativePath(new Path(SOURCE_PATH), path)),
          new CopyListingFileStatus(fs.getFileStatus(path)), context);
    }

    // Check that the maps worked.
    verifyCopy(fs, preserveChecksum);
    Assert.assertEquals(numFiles, stubContext.getReporter()
        .getCounter(CopyMapper.Counter.COPY).getValue());
    Assert.assertEquals(numDirs, stubContext.getReporter()
        .getCounter(CopyMapper.Counter.DIR_COPY).getValue());
    if (!preserveChecksum) {
      Assert.assertEquals(nFiles * DEFAULT_FILE_SIZE, stubContext
          .getReporter().getCounter(CopyMapper.Counter.BYTESCOPIED)
          .getValue());
    } else {
      Assert.assertEquals(nFiles * NON_DEFAULT_BLOCK_SIZE * 2, stubContext
          .getReporter().getCounter(CopyMapper.Counter.BYTESCOPIED)
          .getValue());
    }

    testCopyingExistingFiles(fs, copyMapper, context);
    for (Text value : stubContext.getWriter().values()) {
      Assert.assertTrue(value.toString() + " is not skipped", value
          .toString().startsWith("SKIP:"));
    }
  }

  private void verifyCopy(FileSystem fs, boolean preserveChecksum)
      throws Exception {
    for (Path path : pathList) {
      final Path targetPath = new Path(path.toString().replaceAll(SOURCE_PATH,
          TARGET_PATH));
      Assert.assertTrue(fs.exists(targetPath));
      Assert.assertTrue(fs.isFile(targetPath) == fs.isFile(path));
      FileStatus sourceStatus = fs.getFileStatus(path);
      FileStatus targetStatus = fs.getFileStatus(targetPath);
      Assert.assertEquals(sourceStatus.getReplication(),
          targetStatus.getReplication());
      if (preserveChecksum) {
        Assert.assertEquals(sourceStatus.getBlockSize(),
            targetStatus.getBlockSize());
      }
      Assert.assertTrue(!fs.isFile(targetPath)
          || fs.getFileChecksum(targetPath).equals(fs.getFileChecksum(path)));
    }
  }

  private void testCopyingExistingFiles(FileSystem fs, CopyMapper copyMapper,
      Mapper<Text, CopyListingFileStatus, Text, Text>.Context context) {
    try {
      for (Path path : pathList) {
        copyMapper.map(new Text(DistCpUtils.getRelativePath(new Path(SOURCE_PATH), path)),
                new CopyListingFileStatus(fs.getFileStatus(path)), context);
      }

      Assert.assertEquals(nFiles,
              context.getCounter(CopyMapper.Counter.SKIP).getValue());
    }
    catch (Exception exception) {
      Assert.assertTrue("Caught unexpected exception:" + exception.getMessage(),
              false);
    }
  }

  @Test(timeout=40000)
  public void testMakeDirFailure() {
    try {
      deleteState();
      createSourceData();

      FileSystem fs = cluster.getFileSystem();
      CopyMapper copyMapper = new CopyMapper();
      StubContext stubContext = new StubContext(getConfiguration(), null, 0);
      Mapper<Text, CopyListingFileStatus, Text, Text>.Context context
              = stubContext.getContext();

      Configuration configuration = context.getConfiguration();
      String workPath = new Path("webhdfs://localhost:1234/*/*/*/?/")
              .makeQualified(fs.getUri(), fs.getWorkingDirectory()).toString();
      configuration.set(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH,
              workPath);
      copyMapper.setup(context);

      copyMapper.map(new Text(DistCpUtils.getRelativePath(new Path(SOURCE_PATH),
          pathList.get(0))),
          new CopyListingFileStatus(fs.getFileStatus(pathList.get(0))), context);

      Assert.assertTrue("There should have been an exception.", false);
    }
    catch (Exception ignore) {
    }
  }

  @Test(timeout=40000)
  public void testIgnoreFailures() {
    doTestIgnoreFailures(true);
    doTestIgnoreFailures(false);
    doTestIgnoreFailuresDoubleWrapped(true);
    doTestIgnoreFailuresDoubleWrapped(false);
  }

  @Test(timeout=40000)
  public void testDirToFile() {
    try {
      deleteState();
      createSourceData();

      FileSystem fs = cluster.getFileSystem();
      CopyMapper copyMapper = new CopyMapper();
      StubContext stubContext = new StubContext(getConfiguration(), null, 0);
      Mapper<Text, CopyListingFileStatus, Text, Text>.Context context
              = stubContext.getContext();

      mkdirs(SOURCE_PATH + "/src/file");
      touchFile(TARGET_PATH + "/src/file");
      try {
        copyMapper.setup(context);
        copyMapper.map(new Text("/src/file"),
            new CopyListingFileStatus(fs.getFileStatus(
              new Path(SOURCE_PATH + "/src/file"))),
            context);
      } catch (IOException e) {
        Assert.assertTrue(e.getMessage().startsWith("Can't replace"));
      }
    } catch (Exception e) {
      LOG.error("Exception encountered ", e);
      Assert.fail("Test failed: " + e.getMessage());
    }
  }

  @Test(timeout=40000)
  public void testPreserve() {
    try {
      deleteState();
      createSourceData();

      UserGroupInformation tmpUser = UserGroupInformation.createRemoteUser("guest");

      final CopyMapper copyMapper = new CopyMapper();

      final Mapper<Text, CopyListingFileStatus, Text, Text>.Context context =
        tmpUser.doAs(
          new PrivilegedAction<Mapper<Text, CopyListingFileStatus, Text, Text>.Context>() {
            @Override
            public Mapper<Text, CopyListingFileStatus, Text, Text>.Context run() {
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
      preserveStatus.remove(DistCpOptions.FileAttribute.ACL);
      preserveStatus.remove(DistCpOptions.FileAttribute.XATTR);

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
                new CopyListingFileStatus(tmpFS.getFileStatus(
                  new Path(SOURCE_PATH + "/src/file"))),
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

  @Test(timeout=40000)
  public void testCopyReadableFiles() {
    try {
      deleteState();
      createSourceData();

      UserGroupInformation tmpUser = UserGroupInformation.createRemoteUser("guest");

      final CopyMapper copyMapper = new CopyMapper();

      final Mapper<Text, CopyListingFileStatus, Text, Text>.Context context =
        tmpUser.doAs(
          new PrivilegedAction<Mapper<Text, CopyListingFileStatus, Text, Text>.Context>() {
            @Override
            public Mapper<Text, CopyListingFileStatus, Text, Text>.Context run() {
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
      cluster.getFileSystem().setPermission(new Path(TARGET_PATH),
          new FsPermission((short)511));

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
                new CopyListingFileStatus(tmpFS.getFileStatus(
                  new Path(SOURCE_PATH + "/src/file"))),
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

  @Test(timeout=40000)
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

      final Mapper<Text, CopyListingFileStatus, Text, Text>.Context context =
        stubContext.getContext();
      EnumSet<DistCpOptions.FileAttribute> preserveStatus =
          EnumSet.allOf(DistCpOptions.FileAttribute.class);
      preserveStatus.remove(DistCpOptions.FileAttribute.ACL);
      preserveStatus.remove(DistCpOptions.FileAttribute.XATTR);
      preserveStatus.remove(DistCpOptions.FileAttribute.TIMES);

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
                new CopyListingFileStatus(tmpFS.getFileStatus(
                  new Path(SOURCE_PATH + "/src/file"))),
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

  @Test(timeout=40000)
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
      preserveStatus.remove(DistCpOptions.FileAttribute.ACL);
      preserveStatus.remove(DistCpOptions.FileAttribute.XATTR);

      final Mapper<Text, CopyListingFileStatus, Text, Text>.Context context
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
                new CopyListingFileStatus(tmpFS.getFileStatus(
                  new Path(SOURCE_PATH + "/src/file"))),
                context);
            Assert.fail("Didn't expect the file to be copied");
          } catch (AccessControlException ignore) {
          } catch (Exception e) {
            // We want to make sure the underlying cause of the exception is
            // due to permissions error. The exception we're interested in is
            // wrapped twice - once in RetriableCommand and again in CopyMapper
            // itself.
            if (e.getCause() == null || e.getCause().getCause() == null ||
                !(e.getCause().getCause() instanceof AccessControlException)) {
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

  @Test(timeout=40000)
  public void testFileToDir() {
    try {
      deleteState();
      createSourceData();

      FileSystem fs = cluster.getFileSystem();
      CopyMapper copyMapper = new CopyMapper();
      StubContext stubContext = new StubContext(getConfiguration(), null, 0);
      Mapper<Text, CopyListingFileStatus, Text, Text>.Context context
              = stubContext.getContext();

      touchFile(SOURCE_PATH + "/src/file");
      mkdirs(TARGET_PATH + "/src/file");
      try {
        copyMapper.setup(context);
        copyMapper.map(new Text("/src/file"),
            new CopyListingFileStatus(fs.getFileStatus(
              new Path(SOURCE_PATH + "/src/file"))),
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
      Mapper<Text, CopyListingFileStatus, Text, Text>.Context context
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
        if (!fileStatus.isDirectory()) {
          fs.delete(path, true);
          copyMapper.map(new Text(DistCpUtils.getRelativePath(new Path(SOURCE_PATH), path)),
                  new CopyListingFileStatus(fileStatus), context);
        }
      }
      if (ignoreFailures) {
        for (Text value : stubContext.getWriter().values()) {
          Assert.assertTrue(value.toString() + " is not skipped",
              value.toString().startsWith("FAIL:"));
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

  /**
   * This test covers the case where the CopyReadException is double-wrapped and
   * the mapper should be able to ignore this nested read exception.
   * @see #doTestIgnoreFailures
   */
  private void doTestIgnoreFailuresDoubleWrapped(final boolean ignoreFailures) {
    try {
      deleteState();
      createSourceData();

      final UserGroupInformation tmpUser = UserGroupInformation
          .createRemoteUser("guest");

      final CopyMapper copyMapper = new CopyMapper();

      final Mapper<Text, CopyListingFileStatus, Text, Text>.Context context =
          tmpUser.doAs(new PrivilegedAction<
              Mapper<Text, CopyListingFileStatus, Text, Text>.Context>() {
            @Override
            public Mapper<Text, CopyListingFileStatus, Text, Text>.Context
            run() {
              try {
                StubContext stubContext = new StubContext(
                    getConfiguration(), null, 0);
                return stubContext.getContext();
              } catch (Exception e) {
                LOG.error("Exception encountered when get stub context", e);
                throw new RuntimeException(e);
              }
            }
          });

      touchFile(SOURCE_PATH + "/src/file");
      mkdirs(TARGET_PATH);
      cluster.getFileSystem().setPermission(new Path(SOURCE_PATH + "/src/file"),
          new FsPermission(FsAction.NONE, FsAction.NONE, FsAction.NONE));
      cluster.getFileSystem().setPermission(new Path(TARGET_PATH),
          new FsPermission((short)511));

      context.getConfiguration().setBoolean(
          DistCpOptionSwitch.IGNORE_FAILURES.getConfigLabel(), ignoreFailures);

      final FileSystem tmpFS = tmpUser.doAs(new PrivilegedAction<FileSystem>() {
        @Override
        public FileSystem run() {
          try {
            return FileSystem.get(configuration);
          } catch (IOException e) {
            LOG.error("Exception encountered when get FileSystem.", e);
            throw new RuntimeException(e);
          }
        }
      });

      tmpUser.doAs(new PrivilegedAction<Integer>() {
        @Override
        public Integer run() {
          try {
            copyMapper.setup(context);
            copyMapper.map(new Text("/src/file"),
                new CopyListingFileStatus(tmpFS.getFileStatus(
                    new Path(SOURCE_PATH + "/src/file"))),
                context);
            Assert.assertTrue("Should have thrown an IOException if not " +
                "ignoring failures", ignoreFailures);
          } catch (IOException e) {
            LOG.error("Unexpected exception encountered. ", e);
            Assert.assertFalse("Should not have thrown an IOException if " +
                "ignoring failures", ignoreFailures);
            // the IOException is not thrown again as it's expected
          } catch (Exception e) {
            LOG.error("Exception encountered when the mapper copies file.", e);
            throw new RuntimeException(e);
          }
          return null;
        }
      });
    } catch (Exception e) {
      LOG.error("Unexpected exception encountered. ", e);
      Assert.fail("Test failed: " + e.getMessage());
    }
  }

  private static void deleteState() throws IOException {
    pathList.clear();
    nFiles = 0;
    cluster.getFileSystem().delete(new Path(SOURCE_PATH), true);
    cluster.getFileSystem().delete(new Path(TARGET_PATH), true);
  }

  @Test(timeout=40000)
  public void testPreserveBlockSizeAndReplication() {
    testPreserveBlockSizeAndReplicationImpl(true);
    testPreserveBlockSizeAndReplicationImpl(false);
  }

  @Test(timeout=40000)
  public void testCopyFailOnBlockSizeDifference() throws Exception {
    try {
      deleteState();
      createSourceDataWithDifferentBlockSize();

      FileSystem fs = cluster.getFileSystem();
      CopyMapper copyMapper = new CopyMapper();
      StubContext stubContext = new StubContext(getConfiguration(), null, 0);
      Mapper<Text, CopyListingFileStatus, Text, Text>.Context context
          = stubContext.getContext();

      Configuration configuration = context.getConfiguration();
      EnumSet<DistCpOptions.FileAttribute> fileAttributes
          = EnumSet.noneOf(DistCpOptions.FileAttribute.class);
      configuration.set(DistCpOptionSwitch.PRESERVE_STATUS.getConfigLabel(),
          DistCpUtils.packAttributes(fileAttributes));

      copyMapper.setup(context);

      for (Path path : pathList) {
        final FileStatus fileStatus = fs.getFileStatus(path);
        copyMapper.map(new Text(DistCpUtils.getRelativePath(new Path(SOURCE_PATH),
            path)), new CopyListingFileStatus(fileStatus), context);
      }

      Assert.fail("Copy should have failed because of block-size difference.");
    }
    catch (IOException exception) {
      // Check that the exception suggests the use of -pb/-skipcrccheck.
      Throwable cause = exception.getCause().getCause();
      GenericTestUtils.assertExceptionContains("-pb", cause);
      GenericTestUtils.assertExceptionContains("-skipcrccheck", cause);
    }
  }

  private void testPreserveBlockSizeAndReplicationImpl(boolean preserve){
    try {

      deleteState();
      createSourceData();

      FileSystem fs = cluster.getFileSystem();
      CopyMapper copyMapper = new CopyMapper();
      StubContext stubContext = new StubContext(getConfiguration(), null, 0);
      Mapper<Text, CopyListingFileStatus, Text, Text>.Context context
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
                new CopyListingFileStatus(fileStatus), context);
      }

      // Check that the block-size/replication aren't preserved.
      for (Path path : pathList) {
        final Path targetPath = new Path(path.toString()
                .replaceAll(SOURCE_PATH, TARGET_PATH));
        final FileStatus source = fs.getFileStatus(path);
        final FileStatus target = fs.getFileStatus(targetPath);
        if (!source.isDirectory() ) {
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
  @Test(timeout=40000)
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
      Mapper<Text, CopyListingFileStatus, Text, Text>.Context context
              = stubContext.getContext();

      context.getConfiguration().set(
              DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH,
              targetFilePath.getParent().toString()); // Parent directory.
      copyMapper.setup(context);

      final CopyListingFileStatus sourceFileStatus = new CopyListingFileStatus(
        fs.getFileStatus(sourceFilePath));

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

  @Test(timeout=40000)
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
      Mapper<Text, CopyListingFileStatus, Text, Text>.Context context
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
                new CopyListingFileStatus(fileStatus), context);
      }

      // Check that the user/group attributes are preserved
      // (only) as necessary.
      for (Path path : pathList) {
        final Path targetPath = new Path(path.toString()
                .replaceAll(SOURCE_PATH, TARGET_PATH));
        final FileStatus source = fs.getFileStatus(path);
        final FileStatus target = fs.getFileStatus(targetPath);
        if (!source.isDirectory()) {
          Assert.assertTrue(!preserve || source.getOwner().equals(target.getOwner()));
          Assert.assertTrue(!preserve || source.getGroup().equals(target.getGroup()));
          Assert.assertTrue(!preserve || source.getPermission().equals(target.getPermission()));
          Assert.assertTrue( preserve || !source.getOwner().equals(target.getOwner()));
          Assert.assertTrue( preserve || !source.getGroup().equals(target.getGroup()));
          Assert.assertTrue( preserve || !source.getPermission().equals(target.getPermission()));
          Assert.assertTrue(source.isDirectory() ||
                  source.getReplication() != target.getReplication());
        }
      }
    }
    catch (Exception e) {
      Assert.assertTrue("Unexpected exception: " + e.getMessage(), false);
      e.printStackTrace();
    }
  }

  @Test
  public void testVerboseLogging() throws Exception {
    deleteState();
    createSourceData();

    FileSystem fs = cluster.getFileSystem();
    CopyMapper copyMapper = new CopyMapper();
    StubContext stubContext = new StubContext(getConfiguration(), null, 0);
    Mapper<Text, CopyListingFileStatus, Text, Text>.Context context
            = stubContext.getContext();
    copyMapper.setup(context);

    int numFiles = 0;
    for (Path path : pathList) {
      if (fs.getFileStatus(path).isFile()) {
        numFiles++;
      }

      copyMapper.map(
          new Text(DistCpUtils.getRelativePath(new Path(SOURCE_PATH), path)),
          new CopyListingFileStatus(fs.getFileStatus(path)), context);
    }

    // Check that the maps worked.
    Assert.assertEquals(numFiles, stubContext.getReporter()
        .getCounter(CopyMapper.Counter.COPY).getValue());

    testCopyingExistingFiles(fs, copyMapper, context);
    // verify the verbose log
    // we shouldn't print verbose log since this option is disabled
    for (Text value : stubContext.getWriter().values()) {
      Assert.assertTrue(!value.toString().startsWith("FILE_COPIED:"));
      Assert.assertTrue(!value.toString().startsWith("FILE_SKIPPED:"));
    }

    // test with verbose logging
    deleteState();
    createSourceData();

    stubContext = new StubContext(getConfiguration(), null, 0);
    context = stubContext.getContext();
    copyMapper.setup(context);

    // enables verbose logging
    context.getConfiguration().setBoolean(
        DistCpOptionSwitch.VERBOSE_LOG.getConfigLabel(), true);
    copyMapper.setup(context);

    for (Path path : pathList) {
      copyMapper.map(
          new Text(DistCpUtils.getRelativePath(new Path(SOURCE_PATH), path)),
          new CopyListingFileStatus(fs.getFileStatus(path)), context);
    }

    Assert.assertEquals(numFiles, stubContext.getReporter()
        .getCounter(CopyMapper.Counter.COPY).getValue());

    // verify the verbose log of COPY log
    int numFileCopied = 0;
    for (Text value : stubContext.getWriter().values()) {
      if (value.toString().startsWith("FILE_COPIED:")) {
        numFileCopied++;
      }
    }
    Assert.assertEquals(numFiles, numFileCopied);

    // verify the verbose log of SKIP log
    int numFileSkipped = 0;
    testCopyingExistingFiles(fs, copyMapper, context);
    for (Text value : stubContext.getWriter().values()) {
      if (value.toString().startsWith("FILE_SKIPPED:")) {
        numFileSkipped++;
      }
    }
    Assert.assertEquals(numFiles, numFileSkipped);
  }
}
