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

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

/**
 * Distributed i/o benchmark.
 * <p>
 * This test writes into or reads from a specified number of files.
 * File size is specified as a parameter to the test. 
 * Each file is accessed in a separate map task.
 * <p>
 * The reducer collects the following statistics:
 * <ul>
 * <li>number of tasks completed</li>
 * <li>number of bytes written/read</li>
 * <li>execution time</li>
 * <li>io rate</li>
 * <li>io rate squared</li>
 * </ul>
 *    
 * Finally, the following information is appended to a local file
 * <ul>
 * <li>read or write test</li>
 * <li>date and time the test finished</li>   
 * <li>number of files</li>
 * <li>total number of bytes processed</li>
 * <li>throughput in mb/sec (total number of bytes / sum of processing times)</li>
 * <li>average i/o rate in mb/sec per file</li>
 * <li>standard deviation of i/o rate </li>
 * </ul>
 */
public class TestDFSIO extends Configured implements Tool {
  // Constants
  private static final Log LOG = LogFactory.getLog(TestDFSIO.class);
  private static final int TEST_TYPE_READ = 0;
  private static final int TEST_TYPE_WRITE = 1;
  private static final int TEST_TYPE_CLEANUP = 2;
  private static final int DEFAULT_BUFFER_SIZE = 1000000;
  private static final String BASE_FILE_NAME = "test_io_";
  private static final String DEFAULT_RES_FILE_NAME = "TestDFSIO_results.log";
  
  private static final long MEGA = 0x100000;
  private static String TEST_ROOT_DIR = System.getProperty("test.build.data","/benchmarks/TestDFSIO");
  private static Path CONTROL_DIR = new Path(TEST_ROOT_DIR, "io_control");
  private static Path WRITE_DIR = new Path(TEST_ROOT_DIR, "io_write");
  private static Path READ_DIR = new Path(TEST_ROOT_DIR, "io_read");
  private static Path DATA_DIR = new Path(TEST_ROOT_DIR, "io_data");

  static{
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
  }

  /**
   * Run the test with default parameters.
   * 
   * @throws Exception
   */
  @Test
  public void testIOs() throws Exception {
    testIOs(10, 10, new Configuration());
  }

  /**
   * Run the test with the specified parameters.
   * 
   * @param fileSize file size
   * @param nrFiles number of files
   * @throws IOException
   */
  public static void testIOs(int fileSize, int nrFiles, Configuration fsConfig)
    throws IOException {

    FileSystem fs = FileSystem.get(fsConfig);

    createControlFile(fs, fileSize, nrFiles, fsConfig);
    writeTest(fs, fsConfig);
    readTest(fs, fsConfig);
    cleanup(fs);
  }

  private static void createControlFile(FileSystem fs,
                                        int fileSize, // in MB 
                                        int nrFiles,
                                        Configuration fsConfig
                                        ) throws IOException {
    LOG.info("creating control file: "+fileSize+" mega bytes, "+nrFiles+" files");

    fs.delete(CONTROL_DIR, true);

    for(int i=0; i < nrFiles; i++) {
      String name = getFileName(i);
      Path controlFile = new Path(CONTROL_DIR, "in_file_" + name);
      SequenceFile.Writer writer = null;
      try {
        writer = SequenceFile.createWriter(fs, fsConfig, controlFile,
                                           Text.class, LongWritable.class,
                                           CompressionType.NONE);
        writer.append(new Text(name), new LongWritable(fileSize));
      } catch(Exception e) {
        throw new IOException(e.getLocalizedMessage());
      } finally {
    	if (writer != null)
          writer.close();
    	writer = null;
      }
    }
    LOG.info("created control files for: "+nrFiles+" files");
  }

  private static String getFileName(int fIdx) {
    return BASE_FILE_NAME + Integer.toString(fIdx);
  }
  
  /**
   * Write/Read mapper base class.
   * <p>
   * Collects the following statistics per task:
   * <ul>
   * <li>number of tasks completed</li>
   * <li>number of bytes written/read</li>
   * <li>execution time</li>
   * <li>i/o rate</li>
   * <li>i/o rate squared</li>
   * </ul>
   */
  private abstract static class IOStatMapper<T> extends IOMapperBase<T> {
    IOStatMapper() { 
    }
    
    void collectStats(OutputCollector<Text, Text> output, 
                      String name,
                      long execTime, 
                      Long objSize) throws IOException {
      long totalSize = objSize.longValue();
      float ioRateMbSec = (float)totalSize * 1000 / (execTime * MEGA);
      LOG.info("Number of bytes processed = " + totalSize);
      LOG.info("Exec time = " + execTime);
      LOG.info("IO rate = " + ioRateMbSec);
      
      output.collect(new Text(AccumulatingReducer.VALUE_TYPE_LONG + "tasks"),
          new Text(String.valueOf(1)));
      output.collect(new Text(AccumulatingReducer.VALUE_TYPE_LONG + "size"),
          new Text(String.valueOf(totalSize)));
      output.collect(new Text(AccumulatingReducer.VALUE_TYPE_LONG + "time"),
          new Text(String.valueOf(execTime)));
      output.collect(new Text(AccumulatingReducer.VALUE_TYPE_FLOAT + "rate"),
          new Text(String.valueOf(ioRateMbSec*1000)));
      output.collect(new Text(AccumulatingReducer.VALUE_TYPE_FLOAT + "sqrate"),
          new Text(String.valueOf(ioRateMbSec*ioRateMbSec*1000)));
    }
  }

  /**
   * Write mapper class.
   */
  public static class WriteMapper extends IOStatMapper<Long> {

    public WriteMapper() { 
      for(int i=0; i < bufferSize; i++)
        buffer[i] = (byte)('0' + i % 50);
    }

    public Long doIO(Reporter reporter, 
                       String name, 
                       long totalSize 
                       ) throws IOException {
      // create file
      totalSize *= MEGA;
      OutputStream out;
      out = fs.create(new Path(DATA_DIR, name), true, bufferSize);
      
      try {
        // write to the file
        long nrRemaining;
        for (nrRemaining = totalSize; nrRemaining > 0; nrRemaining -= bufferSize) {
          int curSize = (bufferSize < nrRemaining) ? bufferSize : (int)nrRemaining; 
          out.write(buffer, 0, curSize);
          reporter.setStatus("writing " + name + "@" + 
                             (totalSize - nrRemaining) + "/" + totalSize 
                             + " ::host = " + hostName);
        }
      } finally {
        out.close();
      }
      return Long.valueOf(totalSize);
    }
  }

  private static void writeTest(FileSystem fs, Configuration fsConfig)
  throws IOException {

    fs.delete(DATA_DIR, true);
    fs.delete(WRITE_DIR, true);
    
    runIOTest(WriteMapper.class, WRITE_DIR, fsConfig);
  }
  
  @SuppressWarnings("deprecation")
  private static void runIOTest(
          Class<? extends Mapper<Text, LongWritable, Text, Text>> mapperClass, 
          Path outputDir,
          Configuration fsConfig) throws IOException {
    JobConf job = new JobConf(fsConfig, TestDFSIO.class);

    FileInputFormat.setInputPaths(job, CONTROL_DIR);
    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(mapperClass);
    job.setReducerClass(AccumulatingReducer.class);

    FileOutputFormat.setOutputPath(job, outputDir);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(1);
    JobClient.runJob(job);
  }

  /**
   * Read mapper class.
   */
  public static class ReadMapper extends IOStatMapper<Long> {

    public ReadMapper() { 
    }

    public Long doIO(Reporter reporter, 
                       String name, 
                       long totalSize 
                       ) throws IOException {
      totalSize *= MEGA;
      // open file
      DataInputStream in = fs.open(new Path(DATA_DIR, name));
      long actualSize = 0;
      try {
        while (actualSize < totalSize) {
          int curSize = in.read(buffer, 0, bufferSize);
          if (curSize < 0) break;
          actualSize += curSize;
          reporter.setStatus("reading " + name + "@" + 
                             actualSize + "/" + totalSize 
                             + " ::host = " + hostName);
        }
      } finally {
        in.close();
      }
      return Long.valueOf(actualSize);
    }
  }

  private static void readTest(FileSystem fs, Configuration fsConfig)
  throws IOException {
    fs.delete(READ_DIR, true);
    runIOTest(ReadMapper.class, READ_DIR, fsConfig);
  }

  private static void sequentialTest(FileSystem fs, 
                                     int testType, 
                                     int fileSize, 
                                     int nrFiles
                                     ) throws Exception {
    IOStatMapper<Long> ioer = null;
    if (testType == TEST_TYPE_READ)
      ioer = new ReadMapper();
    else if (testType == TEST_TYPE_WRITE)
      ioer = new WriteMapper();
    else
      return;
    for(int i=0; i < nrFiles; i++)
      ioer.doIO(Reporter.NULL,
                BASE_FILE_NAME+Integer.toString(i), 
                MEGA*fileSize);
  }

  public static void main(String[] args) throws Exception{
    int res = ToolRunner.run(new TestDFSIO(), args);
    System.exit(res);
  }
  
  private static void analyzeResult( FileSystem fs, 
                                     int testType,
                                     long execTime,
                                     String resFileName
                                     ) throws IOException {
    Path reduceFile;
    if (testType == TEST_TYPE_WRITE)
      reduceFile = new Path(WRITE_DIR, "part-00000");
    else
      reduceFile = new Path(READ_DIR, "part-00000");
    long tasks = 0;
    long size = 0;
    long time = 0;
    float rate = 0;
    float sqrate = 0;
    DataInputStream in = null;
    BufferedReader lines = null;
    try {
      in = new DataInputStream(fs.open(reduceFile));
      lines = new BufferedReader(new InputStreamReader(in));
      String line;
      while((line = lines.readLine()) != null) {
        StringTokenizer tokens = new StringTokenizer(line, " \t\n\r\f%");
        String attr = tokens.nextToken(); 
        if (attr.endsWith(":tasks"))
          tasks = Long.parseLong(tokens.nextToken());
        else if (attr.endsWith(":size"))
          size = Long.parseLong(tokens.nextToken());
        else if (attr.endsWith(":time"))
          time = Long.parseLong(tokens.nextToken());
        else if (attr.endsWith(":rate"))
          rate = Float.parseFloat(tokens.nextToken());
        else if (attr.endsWith(":sqrate"))
          sqrate = Float.parseFloat(tokens.nextToken());
      }
    } finally {
      if(in != null) in.close();
      if(lines != null) lines.close();
    }
    
    double med = rate / 1000 / tasks;
    double stdDev = Math.sqrt(Math.abs(sqrate / 1000 / tasks - med*med));
    String resultLines[] = {
      "----- TestDFSIO ----- : " + ((testType == TEST_TYPE_WRITE) ? "write" :
                                    (testType == TEST_TYPE_READ) ? "read" : 
                                    "unknown"),
      "           Date & time: " + new Date(System.currentTimeMillis()),
      "       Number of files: " + tasks,
      "Total MBytes processed: " + size/MEGA,
      "     Throughput mb/sec: " + size * 1000.0 / (time * MEGA),
      "Average IO rate mb/sec: " + med,
      " IO rate std deviation: " + stdDev,
      "    Test exec time sec: " + (float)execTime / 1000,
      "" };

    PrintStream res = null;
    try {
      res = new PrintStream(new FileOutputStream(new File(resFileName), true)); 
      for(int i = 0; i < resultLines.length; i++) {
        LOG.info(resultLines[i]);
        res.println(resultLines[i]);
      }
    } finally {
      if(res != null) res.close();
    }
  }

  private static void cleanup(FileSystem fs) throws IOException {
    LOG.info("Cleaning up test files");
    fs.delete(new Path(TEST_ROOT_DIR), true);
  }

  @Override
  public int run(String[] args) throws Exception {
    int testType = TEST_TYPE_READ;
    int bufferSize = DEFAULT_BUFFER_SIZE;
    int fileSize = 1;
    int nrFiles = 1;
    String resFileName = DEFAULT_RES_FILE_NAME;
    boolean isSequential = false;
    
    String className = TestDFSIO.class.getSimpleName();
    String version = className + ".0.0.4";
    String usage = "Usage: " + className + " -read | -write | -clean " +
    		"[-nrFiles N] [-fileSize MB] [-resFile resultFileName] " +
    		"[-bufferSize Bytes] ";
    
    System.out.println(version);
    if (args.length == 0) {
      System.err.println(usage);
      return -1;
    }
    for (int i = 0; i < args.length; i++) {       // parse command line
      if (args[i].startsWith("-read")) {
        testType = TEST_TYPE_READ;
      } else if (args[i].equals("-write")) {
        testType = TEST_TYPE_WRITE;
      } else if (args[i].equals("-clean")) {
        testType = TEST_TYPE_CLEANUP;
      } else if (args[i].startsWith("-seq")) {
        isSequential = true;
      } else if (args[i].equals("-nrFiles")) {
        nrFiles = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-fileSize")) {
        fileSize = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-bufferSize")) {
        bufferSize = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-resFile")) {
        resFileName = args[++i];
      }
    }

    LOG.info("nrFiles = " + nrFiles);
    LOG.info("fileSize (MB) = " + fileSize);
    LOG.info("bufferSize = " + bufferSize);
  
    try {
      Configuration fsConfig = new Configuration(getConf());
      fsConfig.setInt("test.io.file.buffer.size", bufferSize);
      FileSystem fs = FileSystem.get(fsConfig);

      if (isSequential) {
        long tStart = System.currentTimeMillis();
        sequentialTest(fs, testType, fileSize, nrFiles);
        long execTime = System.currentTimeMillis() - tStart;
        String resultLine = "Seq Test exec time sec: " + (float)execTime / 1000;
        LOG.info(resultLine);
        return 0;
      }
      if (testType == TEST_TYPE_CLEANUP) {
        cleanup(fs);
        return 0;
      }
      createControlFile(fs, fileSize, nrFiles, fsConfig);
      long tStart = System.currentTimeMillis();
      if (testType == TEST_TYPE_WRITE)
        writeTest(fs, fsConfig);
      if (testType == TEST_TYPE_READ)
        readTest(fs, fsConfig);
      long execTime = System.currentTimeMillis() - tStart;
    
      analyzeResult(fs, testType, execTime, resFileName);
    } catch(Exception e) {
      System.err.print(StringUtils.stringifyException(e));
      return -1;
    }
    return 0;
  }
}
