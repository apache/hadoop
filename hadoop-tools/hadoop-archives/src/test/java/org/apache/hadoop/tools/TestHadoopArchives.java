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

import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 * test {@link HadoopArchives}
 */
public class TestHadoopArchives {

  public static final String HADOOP_ARCHIVES_JAR = JarFinder
      .getJar(HadoopArchives.class);

  {
    ((Log4JLogger) LogFactory.getLog(org.apache.hadoop.security.Groups.class))
        .getLogger().setLevel(Level.ERROR);

  }

  private static final String inputDir = "input";

  private Path inputPath;
  private Path archivePath;
  private final List<String> fileList = new ArrayList<String>();
  private MiniDFSCluster dfscluster;

  private Configuration conf;
  private FileSystem fs;

  private static String createFile(Path root, FileSystem fs, String... dirsAndFile
      ) throws IOException {
    String fileBaseName = dirsAndFile[dirsAndFile.length - 1]; 
    return createFile(root, fs, fileBaseName.getBytes("UTF-8"), dirsAndFile);
  }
  
  private static String createFile(Path root, FileSystem fs, byte[] fileContent, String... dirsAndFile
    ) throws IOException {
    StringBuilder sb = new StringBuilder();
    for (String segment: dirsAndFile) {
      if (sb.length() > 0) {
        sb.append(Path.SEPARATOR);  
      }
      sb.append(segment);
    }
    final Path f = new Path(root, sb.toString());
    final FSDataOutputStream out = fs.create(f);
    try {
         out.write(fileContent);
    } finally {
      out.close();
    }
    return sb.toString();
  }

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.set(CapacitySchedulerConfiguration.PREFIX
        + CapacitySchedulerConfiguration.ROOT + "."
        + CapacitySchedulerConfiguration.QUEUES, "default");
    conf.set(CapacitySchedulerConfiguration.PREFIX
        + CapacitySchedulerConfiguration.ROOT + ".default."
        + CapacitySchedulerConfiguration.CAPACITY, "100");
    dfscluster = new MiniDFSCluster
      .Builder(conf)
      .checkExitOnShutdown(true)
      .numDataNodes(2)
      .format(true)
      .racks(null)
      .build();

    fs = dfscluster.getFileSystem();
    
    // prepare archive path:
    archivePath = new Path(fs.getHomeDirectory(), "archive");
    fs.delete(archivePath, true);
    
    // prepare input path:
    inputPath = new Path(fs.getHomeDirectory(), inputDir);
    fs.delete(inputPath, true);
    fs.mkdirs(inputPath);
    // create basic input files:
    fileList.add(createFile(inputPath, fs, "a"));
    fileList.add(createFile(inputPath, fs, "b"));
    fileList.add(createFile(inputPath, fs, "c"));
  }

  @After
  public void tearDown() throws Exception {
    if (dfscluster != null) {
      dfscluster.shutdown();
    }
  }

  @Test
  public void testRelativePath() throws Exception {
    final Path sub1 = new Path(inputPath, "dir1");
    fs.mkdirs(sub1);
    createFile(inputPath, fs, sub1.getName(), "a");
    final FsShell shell = new FsShell(conf);

    final List<String> originalPaths = lsr(shell, "input");
    System.out.println("originalPaths: " + originalPaths);

    // make the archive:
    final String fullHarPathStr = makeArchive();

    // compare results:
    final List<String> harPaths = lsr(shell, fullHarPathStr);
    Assert.assertEquals(originalPaths, harPaths);
  }

  @Test
  public void testRelativePathWitRepl() throws Exception {
    final Path sub1 = new Path(inputPath, "dir1");
    fs.mkdirs(sub1);
    createFile(inputPath, fs, sub1.getName(), "a");
    final FsShell shell = new FsShell(conf);

    final List<String> originalPaths = lsr(shell, "input");
    System.out.println("originalPaths: " + originalPaths);

    // make the archive:
    final String fullHarPathStr = makeArchiveWithRepl();

    // compare results:
    final List<String> harPaths = lsr(shell, fullHarPathStr);
    Assert.assertEquals(originalPaths, harPaths);
  }
  
@Test
  public void testPathWithSpaces() throws Exception {
    // create files/directories with spaces
    createFile(inputPath, fs, "c c");
    final Path sub1 = new Path(inputPath, "sub 1");
    fs.mkdirs(sub1);
    createFile(sub1, fs, "file x y z");
    createFile(sub1, fs, "file");
    createFile(sub1, fs, "x");
    createFile(sub1, fs, "y");
    createFile(sub1, fs, "z");
    final Path sub2 = new Path(inputPath, "sub 1 with suffix");
    fs.mkdirs(sub2);
    createFile(sub2, fs, "z");

    final FsShell shell = new FsShell(conf);
    final String inputPathStr = inputPath.toUri().getPath();
    final List<String> originalPaths = lsr(shell, inputPathStr);

    // make the archive:
    final String fullHarPathStr = makeArchive();

    // compare results
    final List<String> harPaths = lsr(shell, fullHarPathStr);
    Assert.assertEquals(originalPaths, harPaths);
  }

  private static List<String> lsr(final FsShell shell, String dir)
      throws Exception {
    System.out.println("lsr root=" + dir);
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final PrintStream out = new PrintStream(bytes);
    final PrintStream oldOut = System.out;
    final PrintStream oldErr = System.err;
    System.setOut(out);
    System.setErr(out);
    final String results;
    try {
      Assert.assertEquals(0, shell.run(new String[] { "-lsr", dir }));
      results = bytes.toString();
    } finally {
      IOUtils.closeStream(out);
      System.setOut(oldOut);
      System.setErr(oldErr);
    }
    System.out.println("lsr results:\n" + results);
    String dirname = dir;
    if (dir.lastIndexOf(Path.SEPARATOR) != -1) {
      dirname = dir.substring(dir.lastIndexOf(Path.SEPARATOR));
    }

    final List<String> paths = new ArrayList<String>();
    for (StringTokenizer t = new StringTokenizer(results, "\n"); t
        .hasMoreTokens();) {
      final String s = t.nextToken();
      final int i = s.indexOf(dirname);
      if (i >= 0) {
        paths.add(s.substring(i + dirname.length()));
      }
    }
    Collections.sort(paths);
    System.out
        .println("lsr paths = " + paths.toString().replace(", ", ",\n  "));
    return paths;
  }
  
  @Test
  public void testReadFileContent() throws Exception {
    fileList.add(createFile(inputPath, fs, "c c"));
    final Path sub1 = new Path(inputPath, "sub 1");
    fs.mkdirs(sub1);
    fileList.add(createFile(inputPath, fs, sub1.getName(), "file x y z"));
    fileList.add(createFile(inputPath, fs, sub1.getName(), "file"));
    fileList.add(createFile(inputPath, fs, sub1.getName(), "x"));
    fileList.add(createFile(inputPath, fs, sub1.getName(), "y"));
    fileList.add(createFile(inputPath, fs, sub1.getName(), "z"));
    final Path sub2 = new Path(inputPath, "sub 1 with suffix");
    fs.mkdirs(sub2);
    fileList.add(createFile(inputPath, fs, sub2.getName(), "z"));
    // Generate a big binary file content:
    final byte[] binContent = prepareBin();
    fileList.add(createFile(inputPath, fs, binContent, sub2.getName(), "bin"));
    fileList.add(createFile(inputPath, fs, new byte[0], sub2.getName(), "zero-length"));

    final String fullHarPathStr = makeArchive();

    // Create fresh HarFs:
    final HarFileSystem harFileSystem = new HarFileSystem(fs);
    try {
      final URI harUri = new URI(fullHarPathStr);
      harFileSystem.initialize(harUri, fs.getConf());
      // now read the file content and compare it against the expected:
      int readFileCount = 0;
      for (final String pathStr0 : fileList) {
        final Path path = new Path(fullHarPathStr + Path.SEPARATOR + pathStr0);
        final String baseName = path.getName();
        final FileStatus status = harFileSystem.getFileStatus(path);
        if (status.isFile()) {
          // read the file:
          final byte[] actualContentSimple = readAllSimple(
              harFileSystem.open(path), true);
          
          final byte[] actualContentBuffer = readAllWithBuffer(
              harFileSystem.open(path), true);
          assertArrayEquals(actualContentSimple, actualContentBuffer);
          
          final byte[] actualContentFully = readAllWithReadFully(
              actualContentSimple.length,
              harFileSystem.open(path), true);
          assertArrayEquals(actualContentSimple, actualContentFully);
          
          final byte[] actualContentSeek = readAllWithSeek(
              actualContentSimple.length,
              harFileSystem.open(path), true);
          assertArrayEquals(actualContentSimple, actualContentSeek);
          
          final byte[] actualContentRead4
          = readAllWithRead4(harFileSystem.open(path), true);
          assertArrayEquals(actualContentSimple, actualContentRead4);
          
          final byte[] actualContentSkip = readAllWithSkip(
              actualContentSimple.length, 
              harFileSystem.open(path), 
              harFileSystem.open(path), 
              true);
          assertArrayEquals(actualContentSimple, actualContentSkip);
          
          if ("bin".equals(baseName)) {
            assertArrayEquals(binContent, actualContentSimple);
          } else if ("zero-length".equals(baseName)) {
            assertEquals(0, actualContentSimple.length);
          } else {
            String actual = new String(actualContentSimple, "UTF-8");
            assertEquals(baseName, actual);
          }
          readFileCount++;
        }
      }
      assertEquals(fileList.size(), readFileCount);
    } finally {
      harFileSystem.close();
    }
  }
  
  private static byte[] readAllSimple(FSDataInputStream fsdis, boolean close) throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      int b;
      while (true) {
        b = fsdis.read();
        if (b < 0) {
          break;
        } else {
          baos.write(b);
        }
      }
      baos.close();
      return baos.toByteArray();
    } finally {
      if (close) {
        fsdis.close();
      }
    }
  }

  private static byte[] readAllWithBuffer(FSDataInputStream fsdis, boolean close)
      throws IOException {
    try {
      final int available = fsdis.available();
      final byte[] buffer;
      final ByteArrayOutputStream baos;
      if (available < 0) {
        buffer = new byte[1024];
        baos = new ByteArrayOutputStream(buffer.length * 2);
      } else {
        buffer = new byte[available];
        baos = new ByteArrayOutputStream(available);
      }
      int readIntoBuffer = 0;
      int read; 
      while (true) {
        read = fsdis.read(buffer, readIntoBuffer, buffer.length - readIntoBuffer);
        if (read < 0) {
          // end of stream:
          if (readIntoBuffer > 0) {
            baos.write(buffer, 0, readIntoBuffer);
          }
          return baos.toByteArray();
        } else {
          readIntoBuffer += read;
          if (readIntoBuffer == buffer.length) {
            // buffer is full, need to clean the buffer.
            // drop the buffered data to baos:
            baos.write(buffer);
            // reset the counter to start reading to the buffer beginning:
            readIntoBuffer = 0;
          } else if (readIntoBuffer > buffer.length) {
            throw new IOException("Read more than the buffer length: "
                + readIntoBuffer + ", buffer length = " + buffer.length);
          }
        }
      }
    } finally {
      if (close) {
        fsdis.close();
      }
    }
  }

  private static byte[] readAllWithReadFully(int totalLength, FSDataInputStream fsdis, boolean close)
      throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    // Simulate reading of some data structures of known length:
    final byte[] buffer = new byte[17];
    final int times = totalLength / buffer.length;
    final int remainder = totalLength % buffer.length;
    // it would be simpler to leave the position tracking to the 
    // InputStream, but we need to check the methods #readFully(2) 
    // and #readFully(4) that receive the position as a parameter:
    int position = 0;
    try {
      // read "data structures":
      for (int i=0; i<times; i++) {
        fsdis.readFully(position, buffer);
        position += buffer.length;
        baos.write(buffer);
      }
      if (remainder > 0) {
        // read the remainder:
        fsdis.readFully(position, buffer, 0, remainder);
        position += remainder;
        baos.write(buffer, 0, remainder);
      }
      try {
        fsdis.readFully(position, buffer, 0, 1);
        assertTrue(false);
      } catch (IOException ioe) {
        // okay
      }
      assertEquals(totalLength, position);
      final byte[] result = baos.toByteArray();
      assertEquals(totalLength, result.length);
      return result;
    } finally {
      if (close) {
        fsdis.close();
      }
    }
  }

  private static byte[] readAllWithRead4(FSDataInputStream fsdis, boolean close)
      throws IOException {
    try {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final byte[] buffer = new byte[17];
      int totalRead = 0;
      int read;
      while (true) {
        read = fsdis.read(totalRead, buffer, 0, buffer.length);
        if (read > 0) {
          totalRead += read;
          baos.write(buffer, 0, read);
        } else if (read < 0) {
          break; // EOF
        } else {
          // read == 0:
          // zero result may be returned *only* in case if the 4th 
          // parameter is 0. Since in our case this is 'buffer.length',
          // zero return value clearly indicates a bug: 
          throw new AssertionError("FSDataInputStream#read(4) returned 0, while " +
          		" the 4th method parameter is " + buffer.length + ".");
        }
      }
      final byte[] result = baos.toByteArray();
      return result;
    } finally {
      if (close) {
        fsdis.close();
      }
    }
  }
  
  private static byte[] readAllWithSeek(final int totalLength, 
      final FSDataInputStream fsdis, final boolean close)
      throws IOException {
    final byte[] result = new byte[totalLength];
    long pos;
    try {
      // read the data in the reverse order, from 
      // the tail to the head by pieces of 'buffer' length:
      final byte[] buffer = new byte[17];
      final int times = totalLength / buffer.length;
      int read;
      int expectedRead;
      for (int i=times; i>=0; i--) {
        pos = i * buffer.length;
        fsdis.seek(pos);
        // check that seek is successful:
        assertEquals(pos, fsdis.getPos());
        read = fsdis.read(buffer);
        // check we read right number of bytes:
        if (i == times) {
          expectedRead = totalLength % buffer.length; // remainder
          if (expectedRead == 0) {
            // zero remainder corresponds to the EOS, so
            // by the contract of DataInpitStream#read(byte[]) -1 should be 
            // returned:
            expectedRead = -1;
          }
        } else {
          expectedRead = buffer.length;
        }
        assertEquals(expectedRead, read);
        if (read > 0) {
          System.arraycopy(buffer, 0, result, (int)pos, read);
        }
      }
      
      // finally, check that #seek() to not existing position leads to IOE:
      expectSeekIOE(fsdis, Long.MAX_VALUE, "Seek to Long.MAX_VALUE should lead to IOE.");
      expectSeekIOE(fsdis, Long.MIN_VALUE, "Seek to Long.MIN_VALUE should lead to IOE.");
      long pp = -1L;
      expectSeekIOE(fsdis, pp, "Seek to "+pp+" should lead to IOE.");
      
      // NB: is is *possible* to #seek(length), but *impossible* to #seek(length + 1):
      fsdis.seek(totalLength);
      assertEquals(totalLength, fsdis.getPos());
      pp = totalLength + 1;
      expectSeekIOE(fsdis, pp, "Seek to the length position + 1 ("+pp+") should lead to IOE.");
      
      return result;
    } finally {
      if (close) {
        fsdis.close();
      }
    }
  }  

  private static void expectSeekIOE(FSDataInputStream fsdis, long seekPos, String message) {
    try {
      fsdis.seek(seekPos);
      assertTrue(message + " (Position = " + fsdis.getPos() + ")", false);
    } catch (IOException ioe) {
      // okay
    }
  }
  
  /*
   * Reads data by chunks from 2 input streams:
   * reads chunk from stream 1, and skips this chunk in the stream 2;
   * Then reads next chunk from stream 2, and skips this chunk in stream 1. 
   */
  private static byte[] readAllWithSkip(
      final int totalLength, 
      final FSDataInputStream fsdis1, 
      final FSDataInputStream fsdis2, 
      final boolean close)
      throws IOException {
    // test negative skip arg: 
    assertEquals(0, fsdis1.skip(-1));
    // test zero skip arg: 
    assertEquals(0, fsdis1.skip(0));
    
    final ByteArrayOutputStream baos = new ByteArrayOutputStream(totalLength);
    try {
      // read the data in the reverse order, from 
      // the tail to the head by pieces of 'buffer' length:
      final byte[] buffer = new byte[17];
      final int times = totalLength / buffer.length;
      final int remainder = totalLength % buffer.length;
      long skipped;
      long expectedPosition;
      int toGo;
      for (int i=0; i<=times; i++) {
        toGo = (i < times) ? buffer.length : remainder;
        if (i % 2 == 0) {
          fsdis1.readFully(buffer, 0, toGo);
          skipped = skipUntilZero(fsdis2, toGo);
        } else {
          fsdis2.readFully(buffer, 0, toGo);
          skipped = skipUntilZero(fsdis1, toGo);
        }
        if (i < times) {
          assertEquals(buffer.length, skipped);
          expectedPosition = (i + 1) * buffer.length;
        } else { 
          // remainder:
          if (remainder > 0) {
            assertEquals(remainder, skipped);
          } else {
            assertEquals(0, skipped);
          }
          expectedPosition = totalLength;
        }
        // check if the 2 streams have equal and correct positions:
        assertEquals(expectedPosition, fsdis1.getPos());
        assertEquals(expectedPosition, fsdis2.getPos());
        // save the read data:
        if (toGo > 0) {
          baos.write(buffer, 0, toGo);
        }
      }

      // finally, check up if ended stream cannot skip:
      assertEquals(0, fsdis1.skip(-1));
      assertEquals(0, fsdis1.skip(0));
      assertEquals(0, fsdis1.skip(1));
      assertEquals(0, fsdis1.skip(Long.MAX_VALUE));
      
      return baos.toByteArray();
    } finally {
      if (close) {
        fsdis1.close();
        fsdis2.close();
      }
    }
  }  
  
  private static long skipUntilZero(final FilterInputStream fis, 
      final long toSkip) throws IOException {
    long skipped = 0;
    long remainsToSkip = toSkip;
    long s;
    while (skipped < toSkip) {
      s = fis.skip(remainsToSkip); // actually skippped
      if (s == 0) {
        return skipped; // EOF or impossible to skip.
      }
      skipped += s; 
      remainsToSkip -= s;
    }
    return skipped;
  }
  
  private static byte[] prepareBin() {
    byte[] bb = new byte[77777];
    for (int i=0; i<bb.length; i++) {
      // Generate unique values, as possible:
      double d = Math.log(i + 2);
      long bits = Double.doubleToLongBits(d);
      bb[i] = (byte)bits;
    }
    return bb;
  }

  /*
   * Run the HadoopArchives tool to create an archive on the 
   * given file system.
   */
  private String makeArchive() throws Exception {
    final String inputPathStr = inputPath.toUri().getPath();
    System.out.println("inputPathStr = " + inputPathStr);

    final URI uri = fs.getUri();
    final String prefix = "har://hdfs-" + uri.getHost() + ":" + uri.getPort()
        + archivePath.toUri().getPath() + Path.SEPARATOR;

    final String harName = "foo.har";
    final String fullHarPathStr = prefix + harName;
    final String[] args = { "-archiveName", harName, "-p", inputPathStr, "*",
        archivePath.toString() };
    System.setProperty(HadoopArchives.TEST_HADOOP_ARCHIVES_JAR_PATH,
        HADOOP_ARCHIVES_JAR);
    final HadoopArchives har = new HadoopArchives(conf);
    assertEquals(0, ToolRunner.run(har, args));
    return fullHarPathStr;
  }

  /*
 * Run the HadoopArchives tool to create an archive on the
 * given file system with a specified replication degree.
 */
  private String makeArchiveWithRepl() throws Exception {
    final String inputPathStr = inputPath.toUri().getPath();
    System.out.println("inputPathStr = " + inputPathStr);

    final URI uri = fs.getUri();
    final String prefix = "har://hdfs-" + uri.getHost() + ":" + uri.getPort()
        + archivePath.toUri().getPath() + Path.SEPARATOR;

    final String harName = "foo.har";
    final String fullHarPathStr = prefix + harName;
    final String[] args = { "-archiveName", harName, "-p", inputPathStr,
        "-r 3", "*", archivePath.toString() };
    System.setProperty(HadoopArchives.TEST_HADOOP_ARCHIVES_JAR_PATH,
        HADOOP_ARCHIVES_JAR);
    final HadoopArchives har = new HadoopArchives(conf);
    assertEquals(0, ToolRunner.run(har, args));
    return fullHarPathStr;
  }
  
  @Test
  /*
   * Tests copying from archive file system to a local file system
   */
  public void testCopyToLocal() throws Exception {
    final String fullHarPathStr = makeArchive();

    // make path to copy the file to:
    final String tmpDir
      = System.getProperty("test.build.data","build/test/data") + "/work-dir/har-fs-tmp";
    final Path tmpPath = new Path(tmpDir);
    final LocalFileSystem localFs = FileSystem.getLocal(new Configuration());
    localFs.delete(tmpPath, true);
    localFs.mkdirs(tmpPath);
    assertTrue(localFs.exists(tmpPath));
    
    // Create fresh HarFs:
    final HarFileSystem harFileSystem = new HarFileSystem(fs);
    try {
      final URI harUri = new URI(fullHarPathStr);
      harFileSystem.initialize(harUri, fs.getConf());
      
      final Path sourcePath = new Path(fullHarPathStr + Path.SEPARATOR + "a");
      final Path targetPath = new Path(tmpPath, "straus");
      // copy the Har file to a local file system:
      harFileSystem.copyToLocalFile(false, sourcePath, targetPath);
      FileStatus straus = localFs.getFileStatus(targetPath);
      // the file should contain just 1 character:
      assertEquals(1, straus.getLen());
    } finally {
      harFileSystem.close();
      localFs.delete(tmpPath, true);      
    }
  }
  
}
