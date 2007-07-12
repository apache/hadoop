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
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.log4j.Logger;

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
 * 
 * <p>If client == 1, the test table is created and deleted at end of each run
 * and the <code>sequentialWrite</code> test is run first if a test requires
 * a populated test table: e.g. if you are running the
 * <code>sequentialRead</code> test, the test table must hold data for it to
 * read.  If client > 1, and we are running clients in a map task, the table
 * is not deleted at the end-of-run.  Also, if running the
 * <code>sequentialRead</code> or </code>randomRead</code> tests, the
 * <code>sequentialWrite</code> test is not automatically run first.
 */
public class PerformanceEvaluation implements HConstants {
  static final Logger LOG =
    Logger.getLogger(PerformanceEvaluation.class.getName());
  
  private static final int ROW_LENGTH = 1000;
  private static final int ONE_GB = 1024 * 1024 * 1000;
  private static final int ROWS_PER_GB = ONE_GB / ROW_LENGTH;
  static final Text COLUMN_NAME = new Text(COLUMN_FAMILY + "data");
  
  protected static HTableDescriptor tableDescriptor;
  static {
    tableDescriptor = new HTableDescriptor("TestTable");
    tableDescriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY.toString()));
  }
  
  private static final String RANDOM_READ = "randomRead";
  private static final String RANDOM_READ_MEM = "randomReadMem";
  private static final String RANDOM_WRITE = "randomWrite";
  private static final String SEQUENTIAL_READ = "sequentialRead";
  private static final String SEQUENTIAL_WRITE = "sequentialWrite";
  private static final String SCAN = "scan";
  
  private static final List<String> COMMANDS =
    Arrays.asList(new String [] {RANDOM_READ,
      RANDOM_READ_MEM,
      RANDOM_WRITE,
      SEQUENTIAL_READ,
      SEQUENTIAL_WRITE,
      SCAN});
  
  private final Configuration conf;
  private final HClient client;
  private boolean miniCluster = false;
  private int N = 1;
  private int R = ROWS_PER_GB;
  private static final Path PERF_EVAL_DIR = new Path("performance_evaluation");
  
  /*
   * Regex to parse lines in input file passed to mapreduce task.
   */
  public static final Pattern LINE_PATTERN =
    Pattern.compile("startRow=(\\d+),\\s+" +
    "perClientRunRows=(\\d+),\\s+totalRows=(\\d+),\\s+clients=(\\d+)");
  
  /*
   * Enum for map metrics.  Keep it out here rather than inside in the Map
   * inner-class so we can find associated properties.
   */
  protected static enum Counter {ELAPSED_TIME, ROWS}
  
  
  public PerformanceEvaluation(final HBaseConfiguration c) {
    this.conf = c;
    this.client = new HClient(conf);
  }
  
  /*
   * Implementations can have their status set.
   */
  static interface Status {
    void setStatus(final String msg) throws IOException;
  }
  
  /*
   * MapReduce job that runs a performance evaluation client in each map task.
   */
  public static class EvaluationMapTask extends MapReduceBase
  implements Mapper {
    public final static String CMD_KEY = "EvaluationMapTask.command";
    private String cmd;
    private PerformanceEvaluation pe;
    
    @Override
    public void configure(JobConf j) {
      this.cmd = j.get(CMD_KEY);
      this.pe = new PerformanceEvaluation(new HBaseConfiguration());
    }
    
    public void map(@SuppressWarnings("unused") final WritableComparable key,
      final Writable value, final OutputCollector output,
      final Reporter reporter)
    throws IOException {
      Matcher m = LINE_PATTERN.matcher(((Text)value).toString());
      if (m != null && m.matches()) {
        int startRow = Integer.parseInt(m.group(1));
        int perClientRunRows = Integer.parseInt(m.group(2));
        int totalRows = Integer.parseInt(m.group(3));
        Status status = new Status() {
          public void setStatus(String msg) throws IOException {
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
  private boolean checkTable(final HClient c) throws IOException {
    HTableDescriptor [] extantTables = c.listTables();
    boolean tableExists = false;
    if (extantTables.length > 0) {
      // Check to see if our table already exists.  Print warning if it does.
      for (int i = 0; i < extantTables.length; i++) {
        if (extantTables[0].equals(tableDescriptor)) {
          LOG.warn("Table " + tableDescriptor + " already exists");
          tableExists = true;
          break;
        }
      }
    }
    if (!tableExists) {
      c.createTable(tableDescriptor);
      LOG.info("Table " + tableDescriptor + " created");
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
    checkTable(this.client);
    
    // Run a mapreduce job.  Run as many maps as asked-for clients.
    // Before we start up the job, write out an input file with instruction
    // per client regards which row they are to start on.
    Path inputDir = writeInputFile(this.conf);
    this.conf.set(EvaluationMapTask.CMD_KEY, cmd);
    JobConf job = new JobConf(this.conf, this.getClass());
    job.setInputPath(inputDir);
    job.setInputFormat(TextInputFormat.class);
    job.setJobName("HBase Performance Evaluation");
    job.setMapperClass(EvaluationMapTask.class);
    job.setMaxMapAttempts(1);
    job.setMaxReduceAttempts(1);
    job.setNumMapTasks(this.N * 10); // Ten maps per client.
    job.setNumReduceTasks(1);
    job.setOutputFormat(TextOutputFormat.class);
    job.setOutputPath(new Path(inputDir, "outputs"));
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
    try {
      for (int i = 0; i < (this.N * 10); i++) {
        // Write out start row, total number of rows per client run: 1/10th of
        // (R/N).
        int perClientRows = (this.R / this.N);
        out.println("startRow=" + i * perClientRows +
          ", perClientRunRows=" + (perClientRows / 10) +
          ", totalRows=" + this.R +
          ", clients=" + this.N);
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
    protected final HClient client;
    protected final int startRow;
    protected final int perClientRunRows;
    protected final int totalRows;
    private final Status status;
    
    Test(final HClient c, final int startRow, final int perClientRunRows,
        final int totalRows, final Status status) {
      super();
      this.client = c;
      this.startRow = startRow;
      this.perClientRunRows = perClientRunRows;
      this.totalRows = totalRows;
      this.status = status;
    }
    
    /*
     * @return Generated random value to insert into a table cell.
     */
    byte[] generateValue() {
      StringBuilder val = new StringBuilder();
      while(val.length() < ROW_LENGTH) {
        val.append(Long.toString(this.rand.nextLong()));
      }
      return val.toString().getBytes();
    }
    
    private String generateStatus(final int sr, final int i, final int lr) {
      return sr + "/" + i + "/" + lr;
    }
    
    protected int getReportingPeriod() {
      return this.perClientRunRows / 10;
    }
    
    void testSetup() throws IOException {
      this.client.openTable(tableDescriptor.getName());
    }
    
    void testTakedown()  throws IOException {
      // Empty
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
    
    Text getRandomRow() {
      return new Text(Integer.toString(this.rand.nextInt(Integer.MAX_VALUE) %
        this.totalRows));
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
  
  class RandomReadTest extends Test {
    RandomReadTest(final HClient c, final int startRow,
      final int perClientRunRows, final int totalRows, final Status status) {
      super(c, startRow, perClientRunRows, totalRows, status);
    }
    
    @Override
    void testRow(@SuppressWarnings("unused") final int i) throws IOException {
      this.client.get(getRandomRow(), COLUMN_NAME);
    }
    
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
    RandomWriteTest(final HClient c, final int startRow,
      final int perClientRunRows, final int totalRows, final Status status) {
      super(c, startRow, perClientRunRows, totalRows, status);
    }
    
    @Override
    void testRow(@SuppressWarnings("unused") final int i) throws IOException {
      Text row = getRandomRow();
      long lockid = client.startUpdate(row);
      client.put(lockid, COLUMN_NAME, generateValue());
      client.commit(lockid);
    }

    @Override
    String getTestName() {
      return "randomWrite";
    }
  }
  
  class ScanTest extends Test {
    private HScannerInterface testScanner;
    private HStoreKey key = new HStoreKey();
    private TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
    
    ScanTest(final HClient c, final int startRow, final int perClientRunRows,
        final int totalRows, final Status status) {
      super(c, startRow, perClientRunRows, totalRows, status);
    }
    
    @Override
    void testSetup() throws IOException {
      super.testSetup();
      this.testScanner = client.obtainScanner(new Text[] {COLUMN_NAME},
          new Text(Integer.toString(this.startRow)));
    }
    
    @Override
    void testTakedown() throws IOException {
      if (this.testScanner != null) {
        this.testScanner.close();
      }
      super.testTakedown();
    }
    
    
    @Override
    void testRow(@SuppressWarnings("unused") final int i) throws IOException {
      this.testScanner.next(this.key, this.results);
      this.results.clear();
    }

    @Override
    String getTestName() {
      return "scan";
    }
  }
  
  class SequentialReadTest extends Test {
    SequentialReadTest(final HClient c, final int startRow,
        final int perClientRunRows, final int totalRows, final Status status) {
      super(c, startRow, perClientRunRows, totalRows, status);
    }
    
    @Override
    void testRow(final int i) throws IOException {
      client.get(new Text(Integer.toString(i)), COLUMN_NAME);
    }

    @Override
    String getTestName() {
      return "sequentialRead";
    }
  }
  
  class SequentialWriteTest extends Test {
    SequentialWriteTest(final HClient c, final int startRow,
        final int perClientRunRows, final int totalRows, final Status status) {
      super(c, startRow, perClientRunRows, totalRows, status);
    }
    
    @Override
    void testRow(final int i) throws IOException {
      long lockid = client.startUpdate(new Text(Integer.toString(i)));
      client.put(lockid, COLUMN_NAME, generateValue());
      client.commit(lockid);
    }

    @Override
    String getTestName() {
      return "sequentialWrite";
    }
  }
  
  long runOneClient(final String cmd, final int startRow,
    final int perClientRunRows, final int totalRows, final Status status)
  throws IOException {
    status.setStatus("Start " + cmd + " at offset " + startRow + " for " +
      perClientRunRows + " rows");
    long totalElapsedTime = 0;
    if (cmd.equals(RANDOM_READ)) {
      Test t = new RandomReadTest(this.client, startRow, perClientRunRows,
        totalRows, status);
      totalElapsedTime = t.test();
    } else if (cmd.equals(RANDOM_READ_MEM)) {
      throw new UnsupportedOperationException("Not yet implemented");
    } else if (cmd.equals(RANDOM_WRITE)) {
      Test t = new RandomWriteTest(this.client, startRow, perClientRunRows,
        totalRows, status);
      totalElapsedTime = t.test();
    } else if (cmd.equals(SCAN)) {
      Test t = new ScanTest(this.client, startRow, perClientRunRows,
        totalRows, status);
      totalElapsedTime = t.test();
    } else if (cmd.equals(SEQUENTIAL_READ)) {
      Test t = new SequentialReadTest(this.client, startRow, perClientRunRows,
        totalRows, status);
      totalElapsedTime = t.test();
    } else if (cmd.equals(SEQUENTIAL_WRITE)) {
      Test t = new SequentialWriteTest(this.client, startRow, perClientRunRows,
        totalRows, status);
      totalElapsedTime = t.test();
    } else {
      new IllegalArgumentException("Invalid command value: " + cmd);
    }
    status.setStatus("Finished " + cmd + " in " + totalElapsedTime +
      "ms at offset " + startRow + " for " + perClientRunRows + " rows");
    return totalElapsedTime;
  }
  
  private void runNIsOne(final String cmd) throws IOException {
    Status status = new Status() {
      @SuppressWarnings("unused")
      public void setStatus(String msg) throws IOException {
        LOG.info(msg);
      }
    };
    
    try {
      checkTable(this.client);

      if (cmd.equals(RANDOM_READ) || cmd.equals(RANDOM_READ_MEM) ||
          cmd.equals(SCAN) || cmd.equals(SEQUENTIAL_READ)) {
        status.setStatus("Running " + SEQUENTIAL_WRITE + " first so " +
            cmd + " has data to work against");
        runOneClient(SEQUENTIAL_WRITE, 0, this.R, this.R, status);
      }
      
      runOneClient(cmd, 0, this.R, this.R, status);
    } catch (Exception e) {
      LOG.error("Failed", e);
    } finally {
      LOG.info("Deleting table " + tableDescriptor.getName());
      this.client.deleteTable(tableDescriptor.getName());
    }
  }
  
  private void runTest(final String cmd) throws IOException {
    if (cmd.equals(RANDOM_READ_MEM)) {
      // For this one test, so all fits in memory, make R smaller (See
      // pg. 9 of BigTable paper).
      R = (ONE_GB / 10) * N;
    }
    
    MiniHBaseCluster hbaseMiniCluster = null;
    if (this.miniCluster) {
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
        "[--master=host:port] [--miniCluster] <command> <nclients>");
    System.err.println();
    System.err.println("Options:");
    System.err.println(" master          Specify host and port of HBase " +
        "cluster master. If not present,");
    System.err.println("                 address is read from configuration");
    System.err.println(" miniCluster     Run the test on an HBaseMiniCluster");
    System.err.println();
    System.err.println("Command:");
    System.err.println(" randomRead      Run random read test");
    System.err.println(" randomReadMem   Run random read test where table " +
      "is in memory");
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
    if (N > 500 || N < 1) {
      throw new IllegalArgumentException("Number of clients must be between " +
        "1 and 500.");
    }
   
    // Set total number of rows to write.
    R = ROWS_PER_GB * N;
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
    System.exit(new PerformanceEvaluation(new HBaseConfiguration()).
      doCommandLine(args));
  }
}
