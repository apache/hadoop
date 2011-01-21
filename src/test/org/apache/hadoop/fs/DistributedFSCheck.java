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
import java.util.TreeSet;
import java.util.Vector;

import org.apache.commons.logging.*;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.conf.*;

/**
 * Distributed checkup of the file system consistency.
 * <p>
 * Test file system consistency by reading each block of each file
 * of the specified file tree. 
 * Report corrupted blocks and general file statistics.
 * <p>
 * Optionally displays statistics on read performance.
 * 
 */
public class DistributedFSCheck extends TestCase {
  // Constants
  private static final int TEST_TYPE_READ = 0;
  private static final int TEST_TYPE_CLEANUP = 2;
  private static final int DEFAULT_BUFFER_SIZE = 1000000;
  private static final String DEFAULT_RES_FILE_NAME = "DistributedFSCheck_results.log";
  private static final long MEGA = 0x100000;
  
  private static Configuration fsConfig = new Configuration();
  private static final Log LOG = FileInputFormat.LOG;
  private static Path TEST_ROOT_DIR = new Path(System.getProperty("test.build.data","/benchmarks/DistributedFSCheck"));
  private static Path MAP_INPUT_DIR = new Path(TEST_ROOT_DIR, "map_input");
  private static Path READ_DIR = new Path(TEST_ROOT_DIR, "io_read");

  private FileSystem fs;
  private long nrFiles;
  
  DistributedFSCheck(Configuration conf) throws Exception {
    fsConfig = conf;
    this.fs = FileSystem.get(conf);
  }

  /**
   * Run distributed checkup for the entire files system.
   * 
   * @throws Exception
   */
  public void testFSBlocks() throws Exception {
    testFSBlocks("/");
  }

  /**
   * Run distributed checkup for the specified directory.
   * 
   * @param rootName root directory name
   * @throws Exception
   */
  public void testFSBlocks(String rootName) throws Exception {
    createInputFile(rootName);
    runDistributedFSCheck();
    cleanup();  // clean up after all to restore the system state
  }

  private void createInputFile(String rootName) throws IOException {
    cleanup();  // clean up if previous run failed

    Path inputFile = new Path(MAP_INPUT_DIR, "in_file");
    SequenceFile.Writer writer =
      SequenceFile.createWriter(fs, fsConfig, inputFile, 
                                UTF8.class, LongWritable.class, CompressionType.NONE);
    
    try {
      nrFiles = 0;
      listSubtree(new Path(rootName), writer);
    } finally {
      writer.close();
    }
    LOG.info("Created map input files.");
  }
  
  private void listSubtree(Path rootFile,
                           SequenceFile.Writer writer
                           ) throws IOException {
    if (!fs.isDirectory(rootFile)) {
      nrFiles++;
      // For a regular file generate <fName,offset> pairs
      long blockSize = fs.getDefaultBlockSize();
      long fileLength = fs.getLength(rootFile);
      for(long offset = 0; offset < fileLength; offset += blockSize)
        writer.append(new UTF8(rootFile.toString()), new LongWritable(offset));
      return;
    }
    
    FileStatus children[] = fs.listStatus(rootFile);
    if (children == null)
      throw new IOException("Could not get listing for " + rootFile);
    for (int i = 0; i < children.length; i++)
      listSubtree(children[i].getPath(), writer);
  }

  /**
   * DistributedFSCheck mapper class.
   */
  public static class DistributedFSCheckMapper extends IOMapperBase {

    public DistributedFSCheckMapper() { 
      super(fsConfig); 
    }

    public Object doIO(Reporter reporter, 
                       String name, 
                       long offset 
                       ) throws IOException {
      // open file
      FSDataInputStream in = null;
      try {
        in = fs.open(new Path(name));
      } catch(IOException e) {
        return name + "@(missing)";
      }
      in.seek(offset);
      long actualSize = 0;
      try {
        long blockSize = fs.getDefaultBlockSize();
        reporter.setStatus("reading " + name + "@" + 
                           offset + "/" + blockSize);
        for( int curSize = bufferSize; 
             curSize == bufferSize && actualSize < blockSize;
             actualSize += curSize) {
          curSize = in.read(buffer, 0, bufferSize);
        }
      } catch(IOException e) {
        LOG.info("Corrupted block detected in \"" + name + "\" at " + offset);
        return name + "@" + offset;
      } finally {
        in.close();
      }
      return new Long(actualSize);
    }
    
    void collectStats(OutputCollector<UTF8, UTF8> output, 
                      String name, 
                      long execTime, 
                      Object corruptedBlock) throws IOException {
      output.collect(new UTF8("l:blocks"), new UTF8(String.valueOf(1)));

      if (corruptedBlock.getClass().getName().endsWith("String")) {
        output.collect(new UTF8("s:badBlocks"), new UTF8((String)corruptedBlock));
        return;
      }
      long totalSize = ((Long)corruptedBlock).longValue();
      float ioRateMbSec = (float)totalSize * 1000 / (execTime * 0x100000);
      LOG.info("Number of bytes processed = " + totalSize);
      LOG.info("Exec time = " + execTime);
      LOG.info("IO rate = " + ioRateMbSec);
      
      output.collect(new UTF8("l:size"), new UTF8(String.valueOf(totalSize)));
      output.collect(new UTF8("l:time"), new UTF8(String.valueOf(execTime)));
      output.collect(new UTF8("f:rate"), new UTF8(String.valueOf(ioRateMbSec*1000)));
    }
  }
  
  private void runDistributedFSCheck() throws Exception {
    JobConf job = new JobConf(fs.getConf(), DistributedFSCheck.class);

    FileInputFormat.setInputPaths(job, MAP_INPUT_DIR);
    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(DistributedFSCheckMapper.class);
    job.setReducerClass(AccumulatingReducer.class);

    FileOutputFormat.setOutputPath(job, READ_DIR);
    job.setOutputKeyClass(UTF8.class);
    job.setOutputValueClass(UTF8.class);
    job.setNumReduceTasks(1);
    JobClient.runJob(job);
  }

  public static void main(String[] args) throws Exception {
    int testType = TEST_TYPE_READ;
    int bufferSize = DEFAULT_BUFFER_SIZE;
    String resFileName = DEFAULT_RES_FILE_NAME;
    String rootName = "/";
    boolean viewStats = false;

    String usage = "Usage: DistributedFSCheck [-root name] [-clean] [-resFile resultFileName] [-bufferSize Bytes] [-stats] ";
    
    if (args.length == 1 && args[0].startsWith("-h")) {
      System.err.println(usage);
      System.exit(-1);
    }
    for(int i = 0; i < args.length; i++) {       // parse command line
      if (args[i].equals("-root")) {
        rootName = args[++i];
      } else if (args[i].startsWith("-clean")) {
        testType = TEST_TYPE_CLEANUP;
      } else if (args[i].equals("-bufferSize")) {
        bufferSize = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-resFile")) {
        resFileName = args[++i];
      } else if (args[i].startsWith("-stat")) {
        viewStats = true;
      }
    }

    LOG.info("root = " + rootName);
    LOG.info("bufferSize = " + bufferSize);
  
    Configuration conf = new Configuration();  
    conf.setInt("test.io.file.buffer.size", bufferSize);
    DistributedFSCheck test = new DistributedFSCheck(conf);

    if (testType == TEST_TYPE_CLEANUP) {
      test.cleanup();
      return;
    }
    test.createInputFile(rootName);
    long tStart = System.currentTimeMillis();
    test.runDistributedFSCheck();
    long execTime = System.currentTimeMillis() - tStart;
    
    test.analyzeResult(execTime, resFileName, viewStats);
    // test.cleanup();  // clean up after all to restore the system state
  }
  
  private void analyzeResult(long execTime,
                             String resFileName,
                             boolean viewStats
                             ) throws IOException {
    Path reduceFile= new Path(READ_DIR, "part-00000");
    DataInputStream in;
    in = new DataInputStream(fs.open(reduceFile));
  
    BufferedReader lines;
    lines = new BufferedReader(new InputStreamReader(in));
    long blocks = 0;
    long size = 0;
    long time = 0;
    float rate = 0;
    StringTokenizer  badBlocks = null;
    long nrBadBlocks = 0;
    String line;
    while((line = lines.readLine()) != null) {
      StringTokenizer tokens = new StringTokenizer(line, " \t\n\r\f%");
      String attr = tokens.nextToken(); 
      if (attr.endsWith("blocks"))
        blocks = Long.parseLong(tokens.nextToken());
      else if (attr.endsWith("size"))
        size = Long.parseLong(tokens.nextToken());
      else if (attr.endsWith("time"))
        time = Long.parseLong(tokens.nextToken());
      else if (attr.endsWith("rate"))
        rate = Float.parseFloat(tokens.nextToken());
      else if (attr.endsWith("badBlocks")) {
        badBlocks = new StringTokenizer(tokens.nextToken(), ";");
        nrBadBlocks = badBlocks.countTokens();
      }
    }
    
    Vector<String> resultLines = new Vector<String>();
    resultLines.add( "----- DistributedFSCheck ----- : ");
    resultLines.add( "               Date & time: " + new Date(System.currentTimeMillis()));
    resultLines.add( "    Total number of blocks: " + blocks);
    resultLines.add( "    Total number of  files: " + nrFiles);
    resultLines.add( "Number of corrupted blocks: " + nrBadBlocks);
    
    int nrBadFilesPos = resultLines.size();
    TreeSet<String> badFiles = new TreeSet<String>();
    long nrBadFiles = 0;
    if (nrBadBlocks > 0) {
      resultLines.add("");
      resultLines.add("----- Corrupted Blocks (file@offset) ----- : ");
      while(badBlocks.hasMoreTokens()) {
        String curBlock = badBlocks.nextToken();
        resultLines.add(curBlock);
        badFiles.add(curBlock.substring(0, curBlock.indexOf('@')));
      }
      nrBadFiles = badFiles.size();
    }
    
    resultLines.insertElementAt(" Number of corrupted files: " + nrBadFiles, nrBadFilesPos);
    
    if (viewStats) {
      resultLines.add("");
      resultLines.add("-----   Performance  ----- : ");
      resultLines.add("         Total MBytes read: " + size/MEGA);
      resultLines.add("         Throughput mb/sec: " + (float)size * 1000.0 / (time * MEGA));
      resultLines.add("    Average IO rate mb/sec: " + rate / 1000 / blocks);
      resultLines.add("        Test exec time sec: " + (float)execTime / 1000);
    }

    PrintStream res = new PrintStream(
                                      new FileOutputStream(
                                                           new File(resFileName), true)); 
    for(int i = 0; i < resultLines.size(); i++) {
      String cur = resultLines.get(i);
      LOG.info(cur);
      res.println(cur);
    }
  }

  private void cleanup() throws IOException {
    LOG.info("Cleaning up test files");
    fs.delete(TEST_ROOT_DIR, true);
  }
}
