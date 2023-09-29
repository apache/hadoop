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

package org.apache.hadoop.fs;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.net.InetSocketAddress;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;


public class TestFileSystem {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestFileSystem.class);

  private static Configuration conf = new Configuration();
  private static int BUFFER_SIZE = conf.getInt("io.file.buffer.size", 4096);

  private static final long MEGA = 1024 * 1024;
  private static final int SEEKS_PER_FILE = 4;

  private static String ROOT = System.getProperty("test.build.data","fs_test");
  private static Path CONTROL_DIR = new Path(ROOT, "fs_control");
  private static Path WRITE_DIR = new Path(ROOT, "fs_write");
  private static Path READ_DIR = new Path(ROOT, "fs_read");
  private static Path DATA_DIR = new Path(ROOT, "fs_data");

  @Test
  public void testFs() throws Exception {
    testFs(10 * MEGA, 100, 0);
  }

  public static void testFs(long megaBytes, int numFiles, long seed)
    throws Exception {

    FileSystem fs = FileSystem.get(conf);

    if (seed == 0)
      seed = new Random().nextLong();

    LOG.info("seed = "+seed);

    createControlFile(fs, megaBytes, numFiles, seed);
    writeTest(fs, false);
    readTest(fs, false);
    seekTest(fs, false);
    fs.delete(CONTROL_DIR, true);
    fs.delete(DATA_DIR, true);
    fs.delete(WRITE_DIR, true);
    fs.delete(READ_DIR, true);
  }

  @Test
  public void testCommandFormat() throws Exception {
    // This should go to TestFsShell.java when it is added.
    CommandFormat cf;
    cf= new CommandFormat("copyToLocal", 2,2,"crc","ignoreCrc");
    assertThat(cf.parse(new String[] {"-get", "file", "-"}, 1).get(1))
        .isEqualTo("-");
    try {
      cf.parse(new String[] {"-get","file","-ignoreCrc","/foo"}, 1);
      fail("Expected parsing to fail as it should stop at first non-option");
    }
    catch (Exception e) {
      // Expected
    }  
    cf = new CommandFormat("tail", 1, 1, "f");
    assertThat(cf.parse(new String[] {"-tail", "fileName"}, 1).get(0))
        .isEqualTo("fileName");
    assertThat(cf.parse(new String[] {"-tail", "-f", "fileName"}, 1).get(0))
        .isEqualTo("fileName");
    cf = new CommandFormat("setrep", 2, 2, "R", "w");
    assertThat(cf.parse(new String[] {"-setrep", "-R", "2", "/foo/bar"}, 1)
        .get(1)).isEqualTo("/foo/bar");
    cf = new CommandFormat("put", 2, 10000);
    assertThat(cf.parse(new String[] {"-put", "-", "dest"}, 1).get(1))
        .isEqualTo("dest");
  }

  public static void createControlFile(FileSystem fs,
                                       long megaBytes, int numFiles,
                                       long seed) throws Exception {

    LOG.info("creating control file: "+megaBytes+" bytes, "+numFiles+" files");

    Path controlFile = new Path(CONTROL_DIR, "files");
    fs.delete(controlFile, true);
    Random random = new Random(seed);

    SequenceFile.Writer writer =
      SequenceFile.createWriter(fs, conf, controlFile, 
                                Text.class, LongWritable.class, CompressionType.NONE);

    long totalSize = 0;
    long maxSize = ((megaBytes / numFiles) * 2) + 1;
    try {
      while (totalSize < megaBytes) {
        Text name = new Text(Long.toString(random.nextLong()));

        long size = random.nextLong();
        if (size < 0)
          size = -size;
        size = size % maxSize;

        //LOG.info(" adding: name="+name+" size="+size);

        writer.append(name, new LongWritable(size));

        totalSize += size;
      }
    } finally {
      writer.close();
    }
    LOG.info("created control file for: "+totalSize+" bytes");
  }

  public static class WriteMapper extends Configured
      implements Mapper<Text, LongWritable, Text, LongWritable> {
    
    private Random random = new Random();
    private byte[] buffer = new byte[BUFFER_SIZE];
    private FileSystem fs;
    private boolean fastCheck;

    // a random suffix per task
    private String suffix = "-"+random.nextLong();
    
    {
      try {
        fs = FileSystem.get(conf);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public WriteMapper() { super(null); }
    
    public WriteMapper(Configuration conf) { super(conf); }

    public void configure(JobConf job) {
      setConf(job);
      fastCheck = job.getBoolean("fs.test.fastCheck", false);
    }

    public void map(Text key, LongWritable value,
                    OutputCollector<Text, LongWritable> collector,
                    Reporter reporter)
      throws IOException {
      
      String name = key.toString();
      long size = value.get();
      long seed = Long.parseLong(name);

      random.setSeed(seed);
      reporter.setStatus("creating " + name);

      // write to temp file initially to permit parallel execution
      Path tempFile = new Path(DATA_DIR, name+suffix);
      OutputStream out = fs.create(tempFile);

      long written = 0;
      try {
        while (written < size) {
          if (fastCheck) {
            Arrays.fill(buffer, (byte)random.nextInt(Byte.MAX_VALUE));
          } else {
            random.nextBytes(buffer);
          }
          long remains = size - written;
          int length = (remains<=buffer.length) ? (int)remains : buffer.length;
          out.write(buffer, 0, length);
          written += length;
          reporter.setStatus("writing "+name+"@"+written+"/"+size);
        }
      } finally {
        out.close();
      }
      // rename to final location
      fs.rename(tempFile, new Path(DATA_DIR, name));

      collector.collect(new Text("bytes"), new LongWritable(written));

      reporter.setStatus("wrote " + name);
    }
    
    public void close() {
    }
    
  }

  public static void writeTest(FileSystem fs, boolean fastCheck)
    throws Exception {

    fs.delete(DATA_DIR, true);
    fs.delete(WRITE_DIR, true);
    
    JobConf job = new JobConf(conf, TestFileSystem.class);
    job.setBoolean("fs.test.fastCheck", fastCheck);

    FileInputFormat.setInputPaths(job, CONTROL_DIR);
    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(WriteMapper.class);
    job.setReducerClass(LongSumReducer.class);

    FileOutputFormat.setOutputPath(job, WRITE_DIR);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    job.setNumReduceTasks(1);
    JobClient.runJob(job);
  }

  public static class ReadMapper extends Configured
      implements Mapper<Text, LongWritable, Text, LongWritable> {
    
    private Random random = new Random();
    private byte[] buffer = new byte[BUFFER_SIZE];
    private byte[] check  = new byte[BUFFER_SIZE];
    private FileSystem fs;
    private boolean fastCheck;

    {
      try {
        fs = FileSystem.get(conf);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public ReadMapper() { super(null); }
    
    public ReadMapper(Configuration conf) { super(conf); }

    public void configure(JobConf job) {
      setConf(job);
      fastCheck = job.getBoolean("fs.test.fastCheck", false);
    }

    public void map(Text key, LongWritable value,
                    OutputCollector<Text, LongWritable> collector,
                    Reporter reporter)
      throws IOException {
      
      String name = key.toString();
      long size = value.get();
      long seed = Long.parseLong(name);

      random.setSeed(seed);
      reporter.setStatus("opening " + name);

      DataInputStream in =
        new DataInputStream(fs.open(new Path(DATA_DIR, name)));

      long read = 0;
      try {
        while (read < size) {
          long remains = size - read;
          int n = (remains<=buffer.length) ? (int)remains : buffer.length;
          in.readFully(buffer, 0, n);
          read += n;
          if (fastCheck) {
            Arrays.fill(check, (byte)random.nextInt(Byte.MAX_VALUE));
          } else {
            random.nextBytes(check);
          }
          if (n != buffer.length) {
            Arrays.fill(buffer, n, buffer.length, (byte)0);
            Arrays.fill(check, n, check.length, (byte)0);
          }
          assertTrue(Arrays.equals(buffer, check));

          reporter.setStatus("reading "+name+"@"+read+"/"+size);

        }
      } finally {
        in.close();
      }

      collector.collect(new Text("bytes"), new LongWritable(read));

      reporter.setStatus("read " + name);
    }
    
    public void close() {
    }
    
  }

  public static void readTest(FileSystem fs, boolean fastCheck)
    throws Exception {

    fs.delete(READ_DIR, true);

    JobConf job = new JobConf(conf, TestFileSystem.class);
    job.setBoolean("fs.test.fastCheck", fastCheck);


    FileInputFormat.setInputPaths(job, CONTROL_DIR);
    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(ReadMapper.class);
    job.setReducerClass(LongSumReducer.class);

    FileOutputFormat.setOutputPath(job, READ_DIR);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    job.setNumReduceTasks(1);
    JobClient.runJob(job);
  }


  public static class SeekMapper<K> extends Configured
    implements Mapper<Text, LongWritable, K, LongWritable> {
    
    private Random random = new Random();
    private byte[] check  = new byte[BUFFER_SIZE];
    private FileSystem fs;
    private boolean fastCheck;

    {
      try {
        fs = FileSystem.get(conf);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public SeekMapper() { super(null); }
    
    public SeekMapper(Configuration conf) { super(conf); }

    public void configure(JobConf job) {
      setConf(job);
      fastCheck = job.getBoolean("fs.test.fastCheck", false);
    }

    public void map(Text key, LongWritable value,
                    OutputCollector<K, LongWritable> collector,
                    Reporter reporter)
      throws IOException {
      String name = key.toString();
      long size = value.get();
      long seed = Long.parseLong(name);

      if (size == 0) return;

      reporter.setStatus("opening " + name);

      FSDataInputStream in = fs.open(new Path(DATA_DIR, name));
        
      try {
        for (int i = 0; i < SEEKS_PER_FILE; i++) {
          // generate a random position
          long position = Math.abs(random.nextLong()) % size;
          
          // seek file to that position
          reporter.setStatus("seeking " + name);
          in.seek(position);
          byte b = in.readByte();
          
          // check that byte matches
          byte checkByte = 0;
          // advance random state to that position
          random.setSeed(seed);
          for (int p = 0; p <= position; p+= check.length) {
            reporter.setStatus("generating data for " + name);
            if (fastCheck) {
              checkByte = (byte)random.nextInt(Byte.MAX_VALUE);
            } else {
              random.nextBytes(check);
              checkByte = check[(int)(position % check.length)];
            }
          }
          assertEquals(b, checkByte);
        }
      } finally {
        in.close();
      }
    }
    
    public void close() {
    }
    
  }

  public static void seekTest(FileSystem fs, boolean fastCheck)
    throws Exception {

    fs.delete(READ_DIR, true);

    JobConf job = new JobConf(conf, TestFileSystem.class);
    job.setBoolean("fs.test.fastCheck", fastCheck);

    FileInputFormat.setInputPaths(job,CONTROL_DIR);
    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(SeekMapper.class);
    job.setReducerClass(LongSumReducer.class);

    FileOutputFormat.setOutputPath(job, READ_DIR);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    job.setNumReduceTasks(1);
    JobClient.runJob(job);
  }


  public static void main(String[] args) throws Exception {
    int megaBytes = 10;
    int files = 100;
    boolean noRead = false;
    boolean noWrite = false;
    boolean noSeek = false;
    boolean fastCheck = false;
    long seed = new Random().nextLong();

    String usage = "Usage: TestFileSystem -files N -megaBytes M [-noread] [-nowrite] [-noseek] [-fastcheck]";
    
    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }
    for (int i = 0; i < args.length; i++) {       // parse command line
      if (args[i].equals("-files")) {
        files = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-megaBytes")) {
        megaBytes = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-noread")) {
        noRead = true;
      } else if (args[i].equals("-nowrite")) {
        noWrite = true;
      } else if (args[i].equals("-noseek")) {
        noSeek = true;
      } else if (args[i].equals("-fastcheck")) {
        fastCheck = true;
      }
    }

    LOG.info("seed = "+seed);
    LOG.info("files = " + files);
    LOG.info("megaBytes = " + megaBytes);
  
    FileSystem fs = FileSystem.get(conf);

    if (!noWrite) {
      createControlFile(fs, megaBytes*MEGA, files, seed);
      writeTest(fs, fastCheck);
    }
    if (!noRead) {
      readTest(fs, fastCheck);
    }
    if (!noSeek) {
      seekTest(fs, fastCheck);
    }
  }

  @Test
  public void testFsCache() throws Exception {
    {
      long now = System.currentTimeMillis();
      String[] users = new String[]{"foo","bar"};
      final Configuration conf = new Configuration();
      FileSystem[] fs = new FileSystem[users.length];
  
      for(int i = 0; i < users.length; i++) {
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(users[i]);
        fs[i] = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
          public FileSystem run() throws IOException {
            return FileSystem.get(conf);
        }});
        for(int j = 0; j < i; j++) {
          assertFalse(fs[j] == fs[i]);
        }
      }
      FileSystem.closeAll();
    }
    
    {
      try {
        runTestCache(HdfsClientConfigKeys.DFS_NAMENODE_RPC_PORT_DEFAULT);
      } catch(java.net.BindException be) {
        LOG.warn("Cannot test HdfsClientConfigKeys.DFS_NAMENODE_RPC_PORT_DEFAULT (="
            + HdfsClientConfigKeys.DFS_NAMENODE_RPC_PORT_DEFAULT + ")", be);
      }

      runTestCache(0);
    }
  }
  
  static void runTestCache(int port) throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).nameNodePort(port)
          .numDataNodes(2).build();
      URI uri = cluster.getFileSystem().getUri();
      LOG.info("uri=" + uri);

      {
        FileSystem fs = FileSystem.get(uri, new Configuration());
        checkPath(cluster, fs);
        for(int i = 0; i < 100; i++) {
          assertTrue(fs == FileSystem.get(uri, new Configuration()));
        }
      }
      
      if (port == HdfsClientConfigKeys.DFS_NAMENODE_RPC_PORT_DEFAULT) {
        //test explicit default port
        URI uri2 = new URI(uri.getScheme(), uri.getUserInfo(),
            uri.getHost(), HdfsClientConfigKeys.DFS_NAMENODE_RPC_PORT_DEFAULT,
            uri.getPath(), uri.getQuery(), uri.getFragment());
        LOG.info("uri2=" + uri2);
        FileSystem fs = FileSystem.get(uri2, conf);
        checkPath(cluster, fs);
        for(int i = 0; i < 100; i++) {
          assertTrue(fs == FileSystem.get(uri2, new Configuration()));
        }
      }
    } finally {
      if (cluster != null) cluster.shutdown(); 
    }
  }
  
  static void checkPath(MiniDFSCluster cluster, FileSystem fileSys) throws IOException {
    InetSocketAddress add = cluster.getNameNode().getNameNodeAddress();
    // Test upper/lower case
    fileSys.checkPath(new Path("hdfs://"
        + StringUtils.toUpperCase(add.getHostName()) + ":" + add.getPort()));
  }

  @Test
  public void testFsClose() throws Exception {
    {
      Configuration conf = new Configuration();
      new Path("file:///").getFileSystem(conf);
      FileSystem.closeAll();
    }
  }

  @Test
  public void testFsShutdownHook() throws Exception {
    final Set<FileSystem> closed = Collections.synchronizedSet(new HashSet<FileSystem>());
    Configuration conf = new Configuration();
    Configuration confNoAuto = new Configuration();

    conf.setClass("fs.test.impl", TestShutdownFileSystem.class, FileSystem.class);
    confNoAuto.setClass("fs.test.impl", TestShutdownFileSystem.class, FileSystem.class);
    confNoAuto.setBoolean("fs.automatic.close", false);

    TestShutdownFileSystem fsWithAuto =
      (TestShutdownFileSystem)(new Path("test://a/").getFileSystem(conf));
    TestShutdownFileSystem fsWithoutAuto =
      (TestShutdownFileSystem)(new Path("test://b/").getFileSystem(confNoAuto));

    fsWithAuto.setClosedSet(closed);
    fsWithoutAuto.setClosedSet(closed);

    // Different URIs should result in different FS instances
    assertNotSame(fsWithAuto, fsWithoutAuto);

    FileSystem.CACHE.closeAll(true);
    assertEquals(1, closed.size());
    assertTrue(closed.contains(fsWithAuto));

    closed.clear();

    FileSystem.closeAll();
    assertEquals(1, closed.size());
    assertTrue(closed.contains(fsWithoutAuto));
  }

  @Test
  public void testCacheKeysAreCaseInsensitive()
    throws Exception
  {
    Configuration conf = new Configuration();
    
    // check basic equality
    FileSystem.Cache.Key lowercaseCachekey1 = new FileSystem.Cache.Key(new URI("hdfs://localhost:12345/"), conf);
    FileSystem.Cache.Key lowercaseCachekey2 = new FileSystem.Cache.Key(new URI("hdfs://localhost:12345/"), conf);
    assertEquals( lowercaseCachekey1, lowercaseCachekey2 );

    // check insensitive equality    
    FileSystem.Cache.Key uppercaseCachekey = new FileSystem.Cache.Key(new URI("HDFS://Localhost:12345/"), conf);
    assertEquals( lowercaseCachekey2, uppercaseCachekey );

    // check behaviour with collections
    List<FileSystem.Cache.Key> list = new ArrayList<FileSystem.Cache.Key>();
    list.add(uppercaseCachekey);
    assertTrue(list.contains(uppercaseCachekey));
    assertTrue(list.contains(lowercaseCachekey2));

    Set<FileSystem.Cache.Key> set = new HashSet<FileSystem.Cache.Key>();
    set.add(uppercaseCachekey);
    assertTrue(set.contains(uppercaseCachekey));
    assertTrue(set.contains(lowercaseCachekey2));

    Map<FileSystem.Cache.Key, String> map = new HashMap<FileSystem.Cache.Key, String>();
    map.put(uppercaseCachekey, "");
    assertTrue(map.containsKey(uppercaseCachekey));
    assertTrue(map.containsKey(lowercaseCachekey2));    

  }

  public static void testFsUniqueness(long megaBytes, int numFiles, long seed)
    throws Exception {

    // multiple invocations of FileSystem.get return the same object.
    FileSystem fs1 = FileSystem.get(conf);
    FileSystem fs2 = FileSystem.get(conf);
    assertTrue(fs1 == fs2);

    // multiple invocations of FileSystem.newInstance return different objects
    fs1 = FileSystem.newInstance(conf);
    fs2 = FileSystem.newInstance(conf);
    assertTrue(fs1 != fs2 && !fs1.equals(fs2));
    fs1.close();
    fs2.close();
  }

  public static class TestShutdownFileSystem extends RawLocalFileSystem {
    private Set<FileSystem> closedSet;

    public void setClosedSet(Set<FileSystem> closedSet) {
      this.closedSet = closedSet;
    }
    public void close() throws IOException {
      if (closedSet != null) {
        closedSet.add(this);
      }
      super.close();
    }
  }
}
