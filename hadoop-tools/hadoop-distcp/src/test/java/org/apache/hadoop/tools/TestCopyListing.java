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

package org.apache.hadoop.tools;

import static org.mockito.Mockito.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.tools.util.TestDistCpUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.Test;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.AfterClass;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@RunWith(value = Parameterized.class)
public class TestCopyListing extends SimpleCopyListing {
  private static final Log LOG = LogFactory.getLog(TestCopyListing.class);

  private static final Credentials CREDENTIALS = new Credentials();

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

  @Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] { { 1 }, { 2 }, { 10 }, { 20} };
    return Arrays.asList(data);
  }

  public TestCopyListing(int numListstatusThreads) {
    super(config, CREDENTIALS, numListstatusThreads);
  }

  protected TestCopyListing(Configuration configuration) {
    super(configuration, CREDENTIALS);
  }

  @Override
  protected long getBytesToCopy() {
    return 0;
  }

  @Override
  protected long getNumberOfPaths() {
    return 0;
  }

  @Test(timeout=10000)
  public void testMultipleSrcToFile() {
    FileSystem fs = null;
    try {
      fs = FileSystem.get(getConf());
      List<Path> srcPaths = new ArrayList<Path>();
      srcPaths.add(new Path("/tmp/in/1"));
      srcPaths.add(new Path("/tmp/in/2"));
      Path target = new Path("/tmp/out/1");
      TestDistCpUtils.createFile(fs, "/tmp/in/1");
      TestDistCpUtils.createFile(fs, "/tmp/in/2");
      fs.mkdirs(target);
      DistCpOptions options = new DistCpOptions(srcPaths, target);
      validatePaths(options);
      TestDistCpUtils.delete(fs, "/tmp");
      //No errors

      target = new Path("/tmp/out/1");
      fs.create(target).close();
      options = new DistCpOptions(srcPaths, target);
      try {
        validatePaths(options);
        Assert.fail("Invalid inputs accepted");
      } catch (InvalidInputException ignore) { }
      TestDistCpUtils.delete(fs, "/tmp");

      srcPaths.clear();
      srcPaths.add(new Path("/tmp/in/1"));
      fs.mkdirs(new Path("/tmp/in/1"));
      target = new Path("/tmp/out/1");
      fs.create(target).close();
      options = new DistCpOptions(srcPaths, target);
      try {
        validatePaths(options);
        Assert.fail("Invalid inputs accepted");
      } catch (InvalidInputException ignore) { }
      TestDistCpUtils.delete(fs, "/tmp");
    } catch (IOException e) {
      LOG.error("Exception encountered ", e);
      Assert.fail("Test input validation failed");
    } finally {
      TestDistCpUtils.delete(fs, "/tmp");
    }
  }

  @Test(timeout=10000)
  public void testDuplicates() {
    FileSystem fs = null;
    try {
      fs = FileSystem.get(getConf());
      List<Path> srcPaths = new ArrayList<Path>();
      srcPaths.add(new Path("/tmp/in/*/*"));
      TestDistCpUtils.createFile(fs, "/tmp/in/src1/1.txt");
      TestDistCpUtils.createFile(fs, "/tmp/in/src2/1.txt");
      Path target = new Path("/tmp/out");
      Path listingFile = new Path("/tmp/list");
      DistCpOptions options = new DistCpOptions(srcPaths, target);
      CopyListing listing = CopyListing.getCopyListing(getConf(), CREDENTIALS, options);
      try {
        listing.buildListing(listingFile, options);
        Assert.fail("Duplicates not detected");
      } catch (DuplicateFileException ignore) {
      }
    } catch (IOException e) {
      LOG.error("Exception encountered in test", e);
      Assert.fail("Test failed " + e.getMessage());
    } finally {
      TestDistCpUtils.delete(fs, "/tmp");
    }
  }

  @Test(timeout=10000)
  public void testBuildListing() {
    FileSystem fs = null;
    try {
      fs = FileSystem.get(getConf());
      List<Path> srcPaths = new ArrayList<Path>();
      Path p1 = new Path("/tmp/in/1");
      Path p2 = new Path("/tmp/in/2");
      Path p3 = new Path("/tmp/in2/2");
      Path target = new Path("/tmp/out/1");
      srcPaths.add(p1.getParent());
      srcPaths.add(p3.getParent());
      TestDistCpUtils.createFile(fs, "/tmp/in/1");
      TestDistCpUtils.createFile(fs, "/tmp/in/2");
      TestDistCpUtils.createFile(fs, "/tmp/in2/2");
      fs.mkdirs(target);
      OutputStream out = fs.create(p1);
      out.write("ABC".getBytes());
      out.close();

      out = fs.create(p2);
      out.write("DEF".getBytes());
      out.close();

      out = fs.create(p3);
      out.write("GHIJ".getBytes());
      out.close();

      Path listingFile = new Path("/tmp/file");

      DistCpOptions options = new DistCpOptions(srcPaths, target);
      options.setSyncFolder(true);
      CopyListing listing = new SimpleCopyListing(getConf(), CREDENTIALS);
      try {
        listing.buildListing(listingFile, options);
        Assert.fail("Duplicates not detected");
      } catch (DuplicateFileException ignore) {
      }
      Assert.assertEquals(listing.getBytesToCopy(), 10);
      Assert.assertEquals(listing.getNumberOfPaths(), 3);
      TestDistCpUtils.delete(fs, "/tmp");

      try {
        listing.buildListing(listingFile, options);
        Assert.fail("Invalid input not detected");
      } catch (InvalidInputException ignore) {
      }
      TestDistCpUtils.delete(fs, "/tmp");
    } catch (IOException e) {
      LOG.error("Exception encountered ", e);
      Assert.fail("Test build listing failed");
    } finally {
      TestDistCpUtils.delete(fs, "/tmp");
    }
  }

  @Test(timeout=10000)
  public void testBuildListingForSingleFile() {
    FileSystem fs = null;
    String testRootString = "/singleFileListing";
    Path testRoot = new Path(testRootString);
    SequenceFile.Reader reader = null;
    try {
      fs = FileSystem.get(getConf());
      if (fs.exists(testRoot))
        TestDistCpUtils.delete(fs, testRootString);

      Path sourceFile = new Path(testRoot, "/source/foo/bar/source.txt");
      Path decoyFile  = new Path(testRoot, "/target/moo/source.txt");
      Path targetFile = new Path(testRoot, "/target/moo/target.txt");

      TestDistCpUtils.createFile(fs, sourceFile.toString());
      TestDistCpUtils.createFile(fs, decoyFile.toString());
      TestDistCpUtils.createFile(fs, targetFile.toString());

      List<Path> srcPaths = new ArrayList<Path>();
      srcPaths.add(sourceFile);

      DistCpOptions options = new DistCpOptions(srcPaths, targetFile);
      CopyListing listing = new SimpleCopyListing(getConf(), CREDENTIALS);

      final Path listFile = new Path(testRoot, "/tmp/fileList.seq");
      listing.buildListing(listFile, options);

      reader = new SequenceFile.Reader(getConf(), SequenceFile.Reader.file(listFile));

      CopyListingFileStatus fileStatus = new CopyListingFileStatus();
      Text relativePath = new Text();
      Assert.assertTrue(reader.next(relativePath, fileStatus));
      Assert.assertTrue(relativePath.toString().equals(""));
    }
    catch (Exception e) {
      Assert.fail("Unexpected exception encountered.");
      LOG.error("Unexpected exception: ", e);
    }
    finally {
      TestDistCpUtils.delete(fs, testRootString);
      IOUtils.closeStream(reader);
    }
  }
  
  @Test
  public void testFailOnCloseError() throws IOException {
    File inFile = File.createTempFile("TestCopyListingIn", null);
    inFile.deleteOnExit();
    File outFile = File.createTempFile("TestCopyListingOut", null);
    outFile.deleteOnExit();
    List<Path> srcs = new ArrayList<Path>();
    srcs.add(new Path(inFile.toURI()));
    
    Exception expectedEx = new IOException("boom");
    SequenceFile.Writer writer = mock(SequenceFile.Writer.class);
    doThrow(expectedEx).when(writer).close();
    
    SimpleCopyListing listing = new SimpleCopyListing(getConf(), CREDENTIALS);
    DistCpOptions options = new DistCpOptions(srcs, new Path(outFile.toURI()));
    Exception actualEx = null;
    try {
      listing.doBuildListing(writer, options);
    } catch (Exception e) {
      actualEx = e;
    }
    Assert.assertNotNull("close writer didn't fail", actualEx);
    Assert.assertEquals(expectedEx, actualEx);
  }
}
