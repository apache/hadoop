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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
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
  private MiniDFSCluster dfscluster;

  private Configuration conf;
  private FileSystem fs;
  private Path archivePath;

  static private Path createFile(Path dir, String filename, FileSystem fs)
      throws IOException {
    final Path f = new Path(dir, filename);
    final FSDataOutputStream out = fs.create(f);
    out.write(filename.getBytes());
    out.close();
    return f;
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
    dfscluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).format(true)
        .build();

    fs = dfscluster.getFileSystem();
    inputPath = new Path(fs.getHomeDirectory(), inputDir);
    archivePath = new Path(fs.getHomeDirectory(), "archive");
    fs.mkdirs(inputPath);
    createFile(inputPath, "a", fs);
    createFile(inputPath, "b", fs);
    createFile(inputPath, "c", fs);
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (dfscluster != null) {
        dfscluster.shutdown();
      }
      if (dfscluster != null) {
        dfscluster.shutdown();
      }
    } catch (Exception e) {
      System.err.println(e);
    }
  }

  @Test
  public void testRelativePath() throws Exception {
    fs.delete(archivePath, true);

    final Path sub1 = new Path(inputPath, "dir1");
    fs.mkdirs(sub1);
    createFile(sub1, "a", fs);
    final FsShell shell = new FsShell(conf);

    final List<String> originalPaths = lsr(shell, "input");
    System.out.println("originalPath: " + originalPaths);
    final URI uri = fs.getUri();
    final String prefix = "har://hdfs-" + uri.getHost() + ":" + uri.getPort()
        + archivePath.toUri().getPath() + Path.SEPARATOR;

    {
      final String harName = "foo.har";
      final String[] args = { "-archiveName", harName, "-p", "input", "*",
          "archive" };
      System.setProperty(HadoopArchives.TEST_HADOOP_ARCHIVES_JAR_PATH,
          HADOOP_ARCHIVES_JAR);
      final HadoopArchives har = new HadoopArchives(conf);
      Assert.assertEquals(0, ToolRunner.run(har, args));

      // compare results
      final List<String> harPaths = lsr(shell, prefix + harName);
      Assert.assertEquals(originalPaths, harPaths);
    }
  }
  
@Test
  public void testPathWithSpaces() throws Exception {
    fs.delete(archivePath, true);

    // create files/directories with spaces
    createFile(inputPath, "c c", fs);
    final Path sub1 = new Path(inputPath, "sub 1");
    fs.mkdirs(sub1);
    createFile(sub1, "file x y z", fs);
    createFile(sub1, "file", fs);
    createFile(sub1, "x", fs);
    createFile(sub1, "y", fs);
    createFile(sub1, "z", fs);
    final Path sub2 = new Path(inputPath, "sub 1 with suffix");
    fs.mkdirs(sub2);
    createFile(sub2, "z", fs);

    final FsShell shell = new FsShell(conf);

    final String inputPathStr = inputPath.toUri().getPath();

    final List<String> originalPaths = lsr(shell, inputPathStr);
    final URI uri = fs.getUri();
    final String prefix = "har://hdfs-" + uri.getHost() + ":" + uri.getPort()
        + archivePath.toUri().getPath() + Path.SEPARATOR;

    {// Enable space replacement
      final String harName = "foo.har";
      final String[] args = { "-archiveName", harName, "-p", inputPathStr, "*",
          archivePath.toString() };
      System.setProperty(HadoopArchives.TEST_HADOOP_ARCHIVES_JAR_PATH,
          HADOOP_ARCHIVES_JAR);
      final HadoopArchives har = new HadoopArchives(conf);
      Assert.assertEquals(0, ToolRunner.run(har, args));

      // compare results
      final List<String> harPaths = lsr(shell, prefix + harName);
      Assert.assertEquals(originalPaths, harPaths);
    }

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
}
