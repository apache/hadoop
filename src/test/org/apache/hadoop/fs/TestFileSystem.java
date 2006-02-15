/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.io.*;
import java.util.*;
import junit.framework.TestCase;
import java.util.logging.*;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;

public class TestFileSystem extends TestCase {
  private static final Logger LOG = InputFormatBase.LOG;

  private static Configuration conf = new Configuration();
  private static int BUFFER_SIZE = conf.getInt("io.file.buffer.size", 4096);

  private static final long MEGA = 1024 * 1024;
  private static final int SEEKS_PER_FILE = 4;

  private static String ROOT = System.getProperty("test.build.data","fs_test");
  private static File CONTROL_DIR = new File(ROOT, "fs_control");
  private static File WRITE_DIR = new File(ROOT, "fs_write");
  private static File READ_DIR = new File(ROOT, "fs_read");
  private static File DATA_DIR = new File(ROOT, "fs_data");

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
  }

  public static void createControlFile(FileSystem fs,
                                       long megaBytes, int numFiles,
                                       long seed) throws Exception {

    LOG.info("creating control file: "+megaBytes+" bytes, "+numFiles+" files");

    File controlFile = new File(CONTROL_DIR, "files");
    fs.delete(controlFile);
    Random random = new Random(seed);

    SequenceFile.Writer writer =
      new SequenceFile.Writer(fs, controlFile.toString(),
                              UTF8.class, LongWritable.class);

    long totalSize = 0;
    long maxSize = ((megaBytes / numFiles) * 2) + 1;
    try {
      while (totalSize < megaBytes) {
        UTF8 name = new UTF8(Long.toString(random.nextLong()));

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

  public static class WriteMapper extends Configured implements Mapper {
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

    public void map(WritableComparable key, Writable value,
                    OutputCollector collector, Reporter reporter)
      throws IOException {
      String name = ((UTF8)key).toString();
      long size = ((LongWritable)value).get();
      long seed = Long.parseLong(name);

      random.setSeed(seed);
      reporter.setStatus("creating " + name);

      // write to temp file initially to permit parallel execution
      File tempFile = new File(DATA_DIR, name+suffix);
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
      fs.rename(tempFile, new File(DATA_DIR, name));

      collector.collect(new UTF8("bytes"), new LongWritable(written));

      reporter.setStatus("wrote " + name);
    }
    
    public void close() {
    }
    
  }

  public static void writeTest(FileSystem fs, boolean fastCheck)
    throws Exception {

    fs.delete(DATA_DIR);
    fs.delete(WRITE_DIR);
    
    JobConf job = new JobConf(conf);
    job.setBoolean("fs.test.fastCheck", fastCheck);

    job.setInputDir(CONTROL_DIR);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setInputKeyClass(UTF8.class);
    job.setInputValueClass(LongWritable.class);

    job.setMapperClass(WriteMapper.class);
    job.setReducerClass(LongSumReducer.class);

    job.setOutputDir(WRITE_DIR);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(LongWritable.class);
    job.setNumReduceTasks(1);
    JobClient.runJob(job);
  }

  public static class ReadMapper extends Configured implements Mapper {
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

    public void map(WritableComparable key, Writable value,
                    OutputCollector collector, Reporter reporter)
      throws IOException {
      String name = ((UTF8)key).toString();
      long size = ((LongWritable)value).get();
      long seed = Long.parseLong(name);

      random.setSeed(seed);
      reporter.setStatus("opening " + name);

      DataInputStream in =
        new DataInputStream(fs.open(new File(DATA_DIR, name)));

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

      collector.collect(new UTF8("bytes"), new LongWritable(read));

      reporter.setStatus("read " + name);
    }
    
    public void close() {
    }
    
  }

  public static void readTest(FileSystem fs, boolean fastCheck)
    throws Exception {

    fs.delete(READ_DIR);

    JobConf job = new JobConf(conf);
    job.setBoolean("fs.test.fastCheck", fastCheck);


    job.setInputDir(CONTROL_DIR);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setInputKeyClass(UTF8.class);
    job.setInputValueClass(LongWritable.class);

    job.setMapperClass(ReadMapper.class);
    job.setReducerClass(LongSumReducer.class);

    job.setOutputDir(READ_DIR);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(LongWritable.class);
    job.setNumReduceTasks(1);
    JobClient.runJob(job);
  }


  public static class SeekMapper extends Configured implements Mapper {
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

    public void map(WritableComparable key, Writable value,
                    OutputCollector collector, Reporter reporter)
      throws IOException {
      String name = ((UTF8)key).toString();
      long size = ((LongWritable)value).get();
      long seed = Long.parseLong(name);

      if (size == 0) return;

      reporter.setStatus("opening " + name);

      FSDataInputStream in = fs.open(new File(DATA_DIR, name));
        
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

    fs.delete(READ_DIR);

    JobConf job = new JobConf(conf);
    job.setBoolean("fs.test.fastCheck", fastCheck);

    job.setInputDir(CONTROL_DIR);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setInputKeyClass(UTF8.class);
    job.setInputValueClass(LongWritable.class);

    job.setMapperClass(SeekMapper.class);
    job.setReducerClass(LongSumReducer.class);

    job.setOutputDir(READ_DIR);
    job.setOutputKeyClass(UTF8.class);
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

}
