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

import java.io.*;

import junit.framework.TestCase;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.commons.logging.*;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.conf.*;

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
public class TestDFSIO extends TestCase {
  // Constants
  private static final int TEST_TYPE_READ = 0;
  private static final int TEST_TYPE_WRITE = 1;
  private static final int TEST_TYPE_CLEANUP = 2;
  private static final int DEFAULT_BUFFER_SIZE = 1000000;
  private static final String BASE_FILE_NAME = "test_io_";
  private static final String DEFAULT_RES_FILE_NAME = "TestDFSIO_results.log";
  
  private static final Log LOG = FileInputFormat.LOG;
  private static Configuration fsConfig = new Configuration();
  private static final long MEGA = 0x100000;
  private static String TEST_ROOT_DIR = System.getProperty("test.build.data","/benchmarks/TestDFSIO");
  private static Path CONTROL_DIR = new Path(TEST_ROOT_DIR, "io_control");
  private static Path WRITE_DIR = new Path(TEST_ROOT_DIR, "io_write");
  private static Path READ_DIR = new Path(TEST_ROOT_DIR, "io_read");
  private static Path DATA_DIR = new Path(TEST_ROOT_DIR, "io_data");

  /**
   * Run the test with default parameters.
   * 
   * @throws Exception
   */
  public void testIOs() throws Exception {
    testIOs(10, 10);
  }

  /**
   * Run the test with the specified parameters.
   * 
   * @param fileSize file size
   * @param nrFiles number of files
   * @throws IOException
   */
  public static void testIOs(int fileSize, int nrFiles)
    throws IOException {

    FileSystem fs = FileSystem.get(fsConfig);

    createControlFile(fs, fileSize, nrFiles);
    writeTest(fs);
    readTest(fs);
    cleanup(fs);
  }

  private static void createControlFile(
                                        FileSystem fs,
                                        int fileSize, // in MB 
                                        int nrFiles
                                        ) throws IOException {
    LOG.info("creating control file: "+fileSize+" mega bytes, "+nrFiles+" files");

    fs.delete(CONTROL_DIR, true);

    for(int i=0; i < nrFiles; i++) {
      String name = getFileName(i);
      Path controlFile = new Path(CONTROL_DIR, "in_file_" + name);
      SequenceFile.Writer writer = null;
      try {
        writer = SequenceFile.createWriter(fs, fsConfig, controlFile,
                                           UTF8.class, LongWritable.class,
                                           CompressionType.NONE);
        writer.append(new UTF8(name), new LongWritable(fileSize));
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
  private abstract static class IOStatMapper extends IOMapperBase {
    IOStatMapper() { 
      super(fsConfig);
    }
    
    void collectStats(OutputCollector<UTF8, UTF8> output, 
                      String name,
                      long execTime, 
                      Object objSize) throws IOException {
      long totalSize = ((Long)objSize).longValue();
      float ioRateMbSec = (float)totalSize * 1000 / (execTime * MEGA);
      LOG.info("Number of bytes processed = " + totalSize);
      LOG.info("Exec time = " + execTime);
      LOG.info("IO rate = " + ioRateMbSec);
      
      output.collect(new UTF8("l:tasks"), new UTF8(String.valueOf(1)));
      output.collect(new UTF8("l:size"), new UTF8(String.valueOf(totalSize)));
      output.collect(new UTF8("l:time"), new UTF8(String.valueOf(execTime)));
      output.collect(new UTF8("f:rate"), new UTF8(String.valueOf(ioRateMbSec*1000)));
      output.collect(new UTF8("f:sqrate"), new UTF8(String.valueOf(ioRateMbSec*ioRateMbSec*1000)));
    }
  }

  /**
   * Write mapper class.
   */
  public static class WriteMapper extends IOStatMapper {

    public WriteMapper() { 
      super(); 
      for(int i=0; i < bufferSize; i++)
        buffer[i] = (byte)('0' + i % 50);
    }

    public Object doIO(Reporter reporter, 
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
      return new Long(totalSize);
    }
  }

  private static void writeTest(FileSystem fs)
    throws IOException {

    fs.delete(DATA_DIR, true);
    fs.delete(WRITE_DIR, true);
    
    runIOTest(WriteMapper.class, WRITE_DIR);
  }
  
  private static void runIOTest( Class<? extends Mapper> mapperClass, 
                                 Path outputDir
                                 ) throws IOException {
    JobConf job = new JobConf(fsConfig, TestDFSIO.class);

    FileInputFormat.setInputPaths(job, CONTROL_DIR);
    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(mapperClass);
    job.setReducerClass(AccumulatingReducer.class);

    FileOutputFormat.setOutputPath(job, outputDir);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(UTF8.class);
    job.setNumReduceTasks(1);
    JobClient.runJob(job);
  }

  /**
   * Read mapper class.
   */
  public static class ReadMapper extends IOStatMapper {

    public ReadMapper() { 
      super(); 
    }

    public Object doIO(Reporter reporter, 
                       String name, 
                       long totalSize 
                       ) throws IOException {
      totalSize *= MEGA;
      // open file
      DataInputStream in = fs.open(new Path(DATA_DIR, name));
      try {
        long actualSize = 0;
        for(int curSize = bufferSize; curSize == bufferSize;) {
          curSize = in.read(buffer, 0, bufferSize);
          actualSize += curSize;
          reporter.setStatus("reading " + name + "@" + 
                             actualSize + "/" + totalSize 
                             + " ::host = " + hostName);
        }
      } finally {
        in.close();
      }
      return new Long(totalSize);
    }
  }

  private static void readTest(FileSystem fs) throws IOException {
    fs.delete(READ_DIR, true);
    runIOTest(ReadMapper.class, READ_DIR);
  }

  private static void sequentialTest(
                                     FileSystem fs, 
                                     int testType, 
                                     int fileSize, 
                                     int nrFiles
                                     ) throws Exception {
    IOStatMapper ioer = null;
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

  public static void main(String[] args) {
    int testType = TEST_TYPE_READ;
    int bufferSize = DEFAULT_BUFFER_SIZE;
    int fileSize = 1;
    int nrFiles = 1;
    String resFileName = DEFAULT_RES_FILE_NAME;
    boolean isSequential = false;

    String version="TestFDSIO.0.0.4";
    String usage = "Usage: TestFDSIO -read | -write | -clean [-nrFiles N] [-fileSize MB] [-resFile resultFileName] [-bufferSize Bytes] ";
    
    System.out.println(version);
    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
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
      fsConfig.setInt("test.io.file.buffer.size", bufferSize);
      FileSystem fs = FileSystem.get(fsConfig);

      if (isSequential) {
        long tStart = System.currentTimeMillis();
        sequentialTest(fs, testType, fileSize, nrFiles);
        long execTime = System.currentTimeMillis() - tStart;
        String resultLine = "Seq Test exec time sec: " + (float)execTime / 1000;
        LOG.info(resultLine);
        return;
      }
      if (testType == TEST_TYPE_CLEANUP) {
        cleanup(fs);
        return;
      }
      createControlFile(fs, fileSize, nrFiles);
      long tStart = System.currentTimeMillis();
      if (testType == TEST_TYPE_WRITE)
        writeTest(fs);
      if (testType == TEST_TYPE_READ)
        readTest(fs);
      long execTime = System.currentTimeMillis() - tStart;
    
      analyzeResult(fs, testType, execTime, resFileName);
    } catch(Exception e) {
      System.err.print(StringUtils.stringifyException(e));
      System.exit(-1);
    }
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
    DataInputStream in;
    in = new DataInputStream(fs.open(reduceFile));
  
    BufferedReader lines;
    lines = new BufferedReader(new InputStreamReader(in));
    long tasks = 0;
    long size = 0;
    long time = 0;
    float rate = 0;
    float sqrate = 0;
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

    PrintStream res = new PrintStream(
                                      new FileOutputStream(
                                                           new File(resFileName), true)); 
    for(int i = 0; i < resultLines.length; i++) {
      LOG.info(resultLines[i]);
      res.println(resultLines[i]);
    }
  }

  private static void cleanup(FileSystem fs) throws IOException {
    LOG.info("Cleaning up test files");
    fs.delete(new Path(TEST_ROOT_DIR), true);
  }
}
