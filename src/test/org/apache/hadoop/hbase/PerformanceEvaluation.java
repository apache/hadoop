/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.filter.PageRowFilter;
import org.apache.hadoop.hbase.filter.WhileMatchRowFilter;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


/**
 * Script used evaluating HBase performance and scalability.  Runs a HBase
 * client that steps through one of a set of hardcoded tests or 'experiments'
 * (e.g. a random reads test, a random writes test, etc.). Pass on the
 * command-line which test to run and how many clients are participating in
 * this experiment. Run <code>java PerformanceEvaluation --help</code> to
 * obtain usage.
 * 
 * <p>This class sets up and runs the evaluation programs described in
 * Section 7, <i>Performance Evaluation</i>, of the <a
 * href="http://labs.google.com/papers/bigtable.html">Bigtable</a>
 * paper, pages 8-10.
 * 
 * <p>If number of clients > 1, we start up a MapReduce job. Each map task
 * runs an individual client. Each client does about 1GB of data.
 */
public class PerformanceEvaluation implements HConstants {
  protected static final Log LOG = LogFactory.getLog(PerformanceEvaluation.class.getName());
  
  private static final int ROW_LENGTH = 1000;
  private static final int ONE_GB = 1024 * 1024 * 1000;
  private static final int ROWS_PER_GB = ONE_GB / ROW_LENGTH;
  
  static final byte [] COLUMN_NAME = Bytes.toBytes(COLUMN_FAMILY_STR + "data");
  
  protected static final HTableDescriptor TABLE_DESCRIPTOR;
  static {
    TABLE_DESCRIPTOR = new HTableDescriptor("TestTable");
    TABLE_DESCRIPTOR.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
  }
  
  private static final String RANDOM_READ = "randomRead";
  private static final String RANDOM_SEEK_SCAN = "randomSeekScan";
  private static final String RANDOM_READ_MEM = "randomReadMem";
  private static final String RANDOM_WRITE = "randomWrite";
  private static final String SEQUENTIAL_READ = "sequentialRead";
  private static final String SEQUENTIAL_WRITE = "sequentialWrite";
  private static final String SCAN = "scan";
  
  private static final List<String> COMMANDS =
    Arrays.asList(new String [] {RANDOM_READ,
      RANDOM_SEEK_SCAN,
      RANDOM_READ_MEM,
      RANDOM_WRITE,
      SEQUENTIAL_READ,
      SEQUENTIAL_WRITE,
      SCAN});
  
  volatile HBaseConfiguration conf;
  private boolean miniCluster = false;
  private boolean nomapred = false;
  private int N = 1;
  private int R = ROWS_PER_GB;
  private static final Path PERF_EVAL_DIR = new Path("performance_evaluation");
  
  /**
   * Regex to parse lines in input file passed to mapreduce task.
   */
  public static final Pattern LINE_PATTERN =
    Pattern.compile("startRow=(\\d+),\\s+" +
    "perClientRunRows=(\\d+),\\s+totalRows=(\\d+),\\s+clients=(\\d+)");
  
  /**
   * Enum for map metrics.  Keep it out here rather than inside in the Map
   * inner-class so we can find associated properties.
   */
  protected static enum Counter {
    /** elapsed time */
    ELAPSED_TIME,
    /** number of rows */
    ROWS}
  
  
  /**
   * Constructor
   * @param c Configuration object
   */
  public PerformanceEvaluation(final HBaseConfiguration c) {
    this.conf = c;
  }
  
  /**
   * Implementations can have their status set.
   */
  static interface Status {
    /**
     * Sets status
     * @param msg status message
     * @throws IOException
     */
    void setStatus(final String msg) throws IOException;
  }
  
  /**
   * MapReduce job that runs a performance evaluation client in each map task.
   */
  @SuppressWarnings("unchecked")
  public static class EvaluationMapTask extends MapReduceBase
  implements Mapper {
    /** configuration parameter name that contains the command */
    public final static String CMD_KEY = "EvaluationMapTask.command";
    private String cmd;
    private PerformanceEvaluation pe;
    
    @Override
    public void configure(JobConf j) {
      this.cmd = j.get(CMD_KEY);

      this.pe = new PerformanceEvaluation(new HBaseConfiguration(j));
    }
    
    public void map(final Object key,
      final Object value, final OutputCollector output,
      final Reporter reporter)
    throws IOException {
      Matcher m = LINE_PATTERN.matcher(((Text)value).toString());
      if (m != null && m.matches()) {
        int startRow = Integer.parseInt(m.group(1));
        int perClientRunRows = Integer.parseInt(m.group(2));
        int totalRows = Integer.parseInt(m.group(3));
        Status status = new Status() {
          public void setStatus(String msg) {
            reporter.setStatus(msg);
          }
        };
        long elapsedTime =  this.pe.runOneClient(this.cmd, startRow,
          perClientRunRows, totalRows, status);
        // Collect how much time the thing took.  Report as map output and
        // to the ELAPSED_TIME counter.
        reporter.incrCounter(Counter.ELAPSED_TIME, elapsedTime);
        reporter.incrCounter(Counter.ROWS, perClientRunRows);
        output.collect(new LongWritable(startRow),
          new Text(Long.toString(elapsedTime)));
      }
    }
  }
  
  /*
   * If table does not already exist, create.
   * @param c Client to use checking.
   * @return True if we created the table.
   * @throws IOException
   */
  private boolean checkTable(HBaseAdmin admin) throws IOException {
    boolean tableExists = admin.tableExists(TABLE_DESCRIPTOR.getName());
    if (!tableExists) {
      admin.createTable(TABLE_DESCRIPTOR);
      LOG.info("Table " + TABLE_DESCRIPTOR + " created");
    }
    return !tableExists;
  }
 
  /*
   * We're to run multiple clients concurrently.  Setup a mapreduce job.  Run
   * one map per client.  Then run a single reduce to sum the elapsed times.
   * @param cmd Command to run.
   * @throws IOException
   */
  private void runNIsMoreThanOne(final String cmd)
  throws IOException {
    checkTable(new HBaseAdmin(conf));
    if (this.nomapred) {
      doMultipleClients(cmd);
    } else {
      doMapReduce(cmd);
    }
  }
  
  /*
   * Run all clients in this vm each to its own thread.
   * @param cmd Command to run.
   * @throws IOException
   */
  @SuppressWarnings("unused")
  private void doMultipleClients(final String cmd) throws IOException {
    final List<Thread> threads = new ArrayList<Thread>(this.N);
    final int perClientRows = R/N;
    for (int i = 0; i < this.N; i++) {
      Thread t = new Thread (Integer.toString(i)) {
        @Override
        public void run() {
          super.run();
          PerformanceEvaluation pe = new PerformanceEvaluation(conf);
          int index = Integer.parseInt(getName());
          try {
            long elapsedTime = pe.runOneClient(cmd, index * perClientRows,
               perClientRows, perClientRows,
               new Status() {
                  public void setStatus(final String msg) throws IOException {
                    LOG.info("client-" + getName() + " " + msg);
                  }
                });
            LOG.info("Finished " + getName() + " in " + elapsedTime +
              "ms writing " + perClientRows + " rows");
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      };
      threads.add(t);
    }
    for (Thread t: threads) {
      t.start();
    }
    for (Thread t: threads) {
      while(t.isAlive()) {
        try {
          t.join();
        } catch (InterruptedException e) {
          LOG.debug("Interrupted, continuing" + e.toString());
        }
      }
    }
  }
  
  /*
   * Run a mapreduce job.  Run as many maps as asked-for clients.
   * Before we start up the job, write out an input file with instruction
   * per client regards which row they are to start on.
   * @param cmd Command to run.
   * @throws IOException
   */
  private void doMapReduce(final String cmd) throws IOException {
    Path inputDir = writeInputFile(this.conf);
    this.conf.set(EvaluationMapTask.CMD_KEY, cmd);
    JobConf job = new JobConf(this.conf, this.getClass());
    FileInputFormat.setInputPaths(job, inputDir);
    job.setInputFormat(TextInputFormat.class);
    job.setJobName("HBase Performance Evaluation");
    job.setMapperClass(EvaluationMapTask.class);
    job.setMaxMapAttempts(1);
    job.setMaxReduceAttempts(1);
    job.setNumMapTasks(this.N * 10); // Ten maps per client.
    job.setNumReduceTasks(1);
    job.setOutputFormat(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(inputDir, "outputs"));
    JobClient.runJob(job);
  }
  
  /*
   * Write input file of offsets-per-client for the mapreduce job.
   * @param c Configuration
   * @return Directory that contains file written.
   * @throws IOException
   */
  private Path writeInputFile(final Configuration c) throws IOException {
    FileSystem fs = FileSystem.get(c);
    if (!fs.exists(PERF_EVAL_DIR)) {
      fs.mkdirs(PERF_EVAL_DIR);
    }
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
    Path subdir = new Path(PERF_EVAL_DIR, formatter.format(new Date()));
    fs.mkdirs(subdir);
    Path inputFile = new Path(subdir, "input.txt");
    PrintStream out = new PrintStream(fs.create(inputFile));
    int perClientRows = (this.R / this.N);
    try {
      for (int i = 0; i < 10; i++) {
        for (int j = 0; j < N; j++) {
          out.println("startRow=" + ((j * perClientRows) + (i * perClientRows)) +
          ", perClientRunRows=" + (perClientRows / 10) +
          ", totalRows=" + this.R +
          ", clients=" + this.N);
        }
      }
    } finally {
      out.close();
    }
    return subdir;
  }

  /*
   * A test.
   * Subclass to particularize what happens per row.
   */
  static abstract class Test {
    protected final Random rand = new Random(System.currentTimeMillis());
    protected final int startRow;
    protected final int perClientRunRows;
    protected final int totalRows;
    private final Status status;
    protected HBaseAdmin admin;
    protected HTable table;
    protected volatile HBaseConfiguration conf;
    
    Test(final HBaseConfiguration conf, final int startRow,
        final int perClientRunRows, final int totalRows, final Status status) {
      super();
      this.startRow = startRow;
      this.perClientRunRows = perClientRunRows;
      this.totalRows = totalRows;
      this.status = status;
      this.table = null;
      this.conf = conf;
    }
    
    private String generateStatus(final int sr, final int i, final int lr) {
      return sr + "/" + i + "/" + lr;
    }
    
    protected int getReportingPeriod() {
      return this.perClientRunRows / 10;
    }
    
    void testSetup() throws IOException {
      this.admin = new HBaseAdmin(conf);
      this.table = new HTable(conf, TABLE_DESCRIPTOR.getName());
      this.table.setAutoFlush(false);
      this.table.setWriteBufferSize(1024*1024*12);
    }

    void testTakedown()  throws IOException {
      this.table.flushCommits();
    }
    
    /*
     * Run test
     * @return Elapsed time.
     * @throws IOException
     */
    long test() throws IOException {
      long elapsedTime;
      testSetup();
      long startTime = System.currentTimeMillis();
      try {
        int lastRow = this.startRow + this.perClientRunRows;
        // Report on completion of 1/10th of total.
        for (int i = this.startRow; i < lastRow; i++) {
          testRow(i);
          if (status != null && i > 0 && (i % getReportingPeriod()) == 0) {
            status.setStatus(generateStatus(this.startRow, i, lastRow));
          }
        }
        elapsedTime = System.currentTimeMillis() - startTime;
      } finally {
        testTakedown();
      }
      return elapsedTime;
    }
    
    /*
     * Test for individual row.
     * @param i Row index.
     */
    abstract void testRow(final int i) throws IOException;
    
    /*
     * @return Test name.
     */
    abstract String getTestName();
  }

  class RandomSeekScanTest extends Test {
    RandomSeekScanTest(final HBaseConfiguration conf, final int startRow,
        final int perClientRunRows, final int totalRows, final Status status) {
      super(conf, startRow, perClientRunRows, totalRows, status);
    }
    
    @Override
    void testRow(final int i) throws IOException {
      Scanner s = this.table.getScanner(new byte [][] {COLUMN_NAME},
        getRandomRow(this.rand, this.totalRows),
        new WhileMatchRowFilter(new PageRowFilter(120)));
      //int count = 0;
      for (RowResult rr = null; (rr = s.next()) != null;) {
        // LOG.info("" + count++ + " " + rr.toString());
      }
      s.close();
    }
 
    @Override
    protected int getReportingPeriod() {
      // 
      return this.perClientRunRows / 100;
    }

    @Override
    String getTestName() {
      return "randomSeekScanTest";
    }
  }

  class RandomReadTest extends Test {
    RandomReadTest(final HBaseConfiguration conf, final int startRow,
        final int perClientRunRows, final int totalRows, final Status status) {
      super(conf, startRow, perClientRunRows, totalRows, status);
    }
    
    @Override
    void testRow(final int i) throws IOException {
      this.table.get(getRandomRow(this.rand, this.totalRows), COLUMN_NAME);
    }

    @Override
    protected int getReportingPeriod() {
      // 
      return this.perClientRunRows / 100;
    }

    @Override
    String getTestName() {
      return "randomRead";
    }
  }
  
  class RandomWriteTest extends Test {
    RandomWriteTest(final HBaseConfiguration conf, final int startRow,
        final int perClientRunRows, final int totalRows, final Status status) {
      super(conf, startRow, perClientRunRows, totalRows, status);
    }
    
    @Override
    void testRow(final int i) throws IOException {
      byte [] row = getRandomRow(this.rand, this.totalRows);
      BatchUpdate b = new BatchUpdate(row);
      b.put(COLUMN_NAME, generateValue(this.rand));
      table.commit(b);
    }

    @Override
    String getTestName() {
      return "randomWrite";
    }
  }
  
  class ScanTest extends Test {
    private Scanner testScanner;
    
    ScanTest(final HBaseConfiguration conf, final int startRow,
        final int perClientRunRows, final int totalRows, final Status status) {
      super(conf, startRow, perClientRunRows, totalRows, status);
    }
    
    @Override
    void testSetup() throws IOException {
      super.testSetup();
      this.testScanner = table.getScanner(new byte [][] {COLUMN_NAME},
        format(this.startRow));
    }
    
    @Override
    void testTakedown() throws IOException {
      if (this.testScanner != null) {
        this.testScanner.close();
      }
      super.testTakedown();
    }
    
    
    @Override
    void testRow(final int i) throws IOException {
      testScanner.next();
    }

    @Override
    String getTestName() {
      return "scan";
    }
  }
  
  class SequentialReadTest extends Test {
    SequentialReadTest(final HBaseConfiguration conf, final int startRow,
        final int perClientRunRows, final int totalRows, final Status status) {
      super(conf, startRow, perClientRunRows, totalRows, status);
    }
    
    @Override
    void testRow(final int i) throws IOException {
      table.get(format(i), COLUMN_NAME);
    }

    @Override
    String getTestName() {
      return "sequentialRead";
    }
  }
  
  class SequentialWriteTest extends Test {
    SequentialWriteTest(final HBaseConfiguration conf, final int startRow,
        final int perClientRunRows, final int totalRows, final Status status) {
      super(conf, startRow, perClientRunRows, totalRows, status);
    }
    
    @Override
    void testRow(final int i) throws IOException {
      BatchUpdate b = new BatchUpdate(format(i));
      b.put(COLUMN_NAME, generateValue(this.rand));
      table.commit(b);
    }

    @Override
    String getTestName() {
      return "sequentialWrite";
    }
  }
  
  /*
   * Format passed integer.
   * @param number
   * @return Returns zero-prefixed 10-byte wide decimal version of passed
   * number (Does absolute in case number is negative).
   */
  public static byte [] format(final int number) {
    byte [] b = new byte[10];
    int d = Math.abs(number);
    for (int i = b.length - 1; i >= 0; i--) {
      b[i] = (byte)((d % 10) + '0');
      d /= 10;
    }
    return b;
  }
  
  /*
   * This method takes some time and is done inline uploading data.  For
   * example, doing the mapfile test, generation of the key and value
   * consumes about 30% of CPU time.
   * @return Generated random value to insert into a table cell.
   */
  static byte[] generateValue(final Random r) {
    byte [] b = new byte [ROW_LENGTH];
    r.nextBytes(b);
    return b;
  }
  
  static byte [] getRandomRow(final Random random, final int totalRows) {
    return format(random.nextInt(Integer.MAX_VALUE) % totalRows);
  }
  
  long runOneClient(final String cmd, final int startRow,
    final int perClientRunRows, final int totalRows, final Status status)
  throws IOException {
    status.setStatus("Start " + cmd + " at offset " + startRow + " for " +
      perClientRunRows + " rows");
    long totalElapsedTime = 0;
    if (cmd.equals(RANDOM_READ)) {
      Test t = new RandomReadTest(this.conf, startRow, perClientRunRows,
        totalRows, status);
      totalElapsedTime = t.test();
    } else if (cmd.equals(RANDOM_READ_MEM)) {
      throw new UnsupportedOperationException("Not yet implemented");
    } else if (cmd.equals(RANDOM_WRITE)) {
      Test t = new RandomWriteTest(this.conf, startRow, perClientRunRows,
        totalRows, status);
      totalElapsedTime = t.test();
    } else if (cmd.equals(SCAN)) {
      Test t = new ScanTest(this.conf, startRow, perClientRunRows,
        totalRows, status);
      totalElapsedTime = t.test();
    } else if (cmd.equals(SEQUENTIAL_READ)) {
      Test t = new SequentialReadTest(this.conf, startRow, perClientRunRows,
        totalRows, status);
      totalElapsedTime = t.test();
    } else if (cmd.equals(SEQUENTIAL_WRITE)) {
      Test t = new SequentialWriteTest(this.conf, startRow, perClientRunRows,
        totalRows, status);
      totalElapsedTime = t.test();
    } else if (cmd.equals(RANDOM_SEEK_SCAN)) {
      Test t = new RandomSeekScanTest(this.conf, startRow, perClientRunRows,
          totalRows, status);
        totalElapsedTime = t.test();
    } else {
      throw new IllegalArgumentException("Invalid command value: " + cmd);
    }
    status.setStatus("Finished " + cmd + " in " + totalElapsedTime +
      "ms at offset " + startRow + " for " + perClientRunRows + " rows");
    return totalElapsedTime;
  }
  
  private void runNIsOne(final String cmd) {
    Status status = new Status() {
      public void setStatus(String msg) throws IOException {
        LOG.info(msg);
      }
    };

    HBaseAdmin admin = null;
    try {
      admin = new HBaseAdmin(this.conf);
      checkTable(admin);
      runOneClient(cmd, 0, this.R, this.R, status);
    } catch (Exception e) {
      LOG.error("Failed", e);
    } 
  }

  private void runTest(final String cmd) throws IOException {
    if (cmd.equals(RANDOM_READ_MEM)) {
      // For this one test, so all fits in memory, make R smaller (See
      // pg. 9 of BigTable paper).
      R = (this.R / 10) * N;
    }
    
    MiniHBaseCluster hbaseMiniCluster = null;
    MiniDFSCluster dfsCluster = null;
    if (this.miniCluster) {
      dfsCluster = new MiniDFSCluster(conf, 2, true, (String[])null);
      // mangle the conf so that the fs parameter points to the minidfs we
      // just started up
      FileSystem fs = dfsCluster.getFileSystem();
      conf.set("fs.default.name", fs.getUri().toString());      
      Path parentdir = fs.getHomeDirectory();
      conf.set(HConstants.HBASE_DIR, parentdir.toString());
      fs.mkdirs(parentdir);
      FSUtils.setVersion(fs, parentdir);
      hbaseMiniCluster = new MiniHBaseCluster(this.conf, N);
    }
    
    try {
      if (N == 1) {
        // If there is only one client and one HRegionServer, we assume nothing
        // has been set up at all.
        runNIsOne(cmd);
      } else {
        // Else, run 
        runNIsMoreThanOne(cmd);
      }
    } finally {
      if(this.miniCluster && hbaseMiniCluster != null) {
        hbaseMiniCluster.shutdown();
        HBaseTestCase.shutdownDfs(dfsCluster);
      }
    }
  }
  
  private void printUsage() {
    printUsage(null);
  }
  
  private void printUsage(final String message) {
    if (message != null && message.length() > 0) {
      System.err.println(message);
    }
    System.err.println("Usage: java " + this.getClass().getName() +
        " [--master=HOST:PORT] \\");
    System.err.println("  [--miniCluster] [--nomapred] [--rows=ROWS] <command> <nclients>");
    System.err.println();
    System.err.println("Options:");
    System.err.println(" master          Specify host and port of HBase " +
        "cluster master. If not present,");
    System.err.println("                 address is read from configuration");
    System.err.println(" miniCluster     Run the test on an HBaseMiniCluster");
    System.err.println(" nomapred        Run multiple clients using threads " +
      "(rather than use mapreduce)");
    System.err.println(" rows            Rows each client runs. Default: One million");
    System.err.println();
    System.err.println("Command:");
    System.err.println(" randomRead      Run random read test");
    System.err.println(" randomReadMem   Run random read test where table " +
      "is in memory");
    System.err.println(" randomSeekScan  Run random seek and scan 100 test");
    System.err.println(" randomWrite     Run random write test");
    System.err.println(" sequentialRead  Run sequential read test");
    System.err.println(" sequentialWrite Run sequential write test");
    System.err.println(" scan            Run scan test");
    System.err.println();
    System.err.println("Args:");
    System.err.println(" nclients        Integer. Required. Total number of " +
      "clients (and HRegionServers)");
    System.err.println("                 running: 1 <= value <= 500");
    System.err.println("Examples:");
    System.err.println(" To run a single evaluation client:");
    System.err.println(" $ bin/hbase " +
      "org.apache.hadoop.hbase.PerformanceEvaluation sequentialWrite 1");
  }

  private void getArgs(final int start, final String[] args) {
    if(start + 1 > args.length) {
      throw new IllegalArgumentException("must supply the number of clients");
    }
    
    N = Integer.parseInt(args[start]);
    if (N < 1) {
      throw new IllegalArgumentException("Number of clients must be > 1");
    }
   
    // Set total number of rows to write.
    this.R = this.R * N;
  }
  
  private int doCommandLine(final String[] args) {
    // Process command-line args. TODO: Better cmd-line processing
    // (but hopefully something not as painful as cli options).    
    int errCode = -1;
    if (args.length < 1) {
      printUsage();
      return errCode;
    }
    
    try {
      for (int i = 0; i < args.length; i++) {
        String cmd = args[i];
        if (cmd.equals("-h") || cmd.startsWith("--h")) {
          printUsage();
          errCode = 0;
          break;
        }
        
        final String masterArgKey = "--master=";
        if (cmd.startsWith(masterArgKey)) {
          this.conf.set(MASTER_ADDRESS, cmd.substring(masterArgKey.length()));
          continue;
        }
       
        final String miniClusterArgKey = "--miniCluster";
        if (cmd.startsWith(miniClusterArgKey)) {
          this.miniCluster = true;
          continue;
        }
        
        final String nmr = "--nomapred";
        if (cmd.startsWith(nmr)) {
          this.nomapred = true;
          continue;
        }
        
        final String rows = "--rows=";
        if (cmd.startsWith(rows)) {
          this.R = Integer.parseInt(cmd.substring(rows.length()));
          continue;
        }
       
        if (COMMANDS.contains(cmd)) {
          getArgs(i + 1, args);
          runTest(cmd);
          errCode = 0;
          break;
        }
    
        printUsage();
        break;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    
    return errCode;
  }
  
  /**
   * @param args
   */
  public static void main(final String[] args) {
    HBaseConfiguration c = new HBaseConfiguration();
    System.exit(new PerformanceEvaluation(c).doCommandLine(args));
  }
}
