/**
 * Copyright 2006 The Apache Software Foundation
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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.Random;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * Client used evaluating HBase performance and scalability.  Steps through
 * one of a set of hardcoded tests or 'experiments' (e.g. a random reads test,
 * a random writes test, etc.). Pass on the command-line which test to run,
 * how many clients are participating in this experiment, and the row range
 * this client instance is to operate on. Run
 * <code>java EvaluationClient --help</code> to obtain usage.
 * 
 * <p>This class implements the client used in the
 * <i>Performance Evaluation</i> benchmarks described in Section 7 of the <a
 * href="http://labs.google.com/papers/bigtable.html">Bigtable</a>
 * paper on pages 8-10.
 */
public class EvaluationClient implements HConstants {
  private final Logger LOG = Logger.getLogger(this.getClass().getName());
  
  private static final int ROW_LENGTH = 1024;
  

  private static final int ONE_HUNDRED_MB = 1024 * 1024 * 1 /*100 RESTORE*/;
  private static final int ROWS_PER_100_MB = ONE_HUNDRED_MB / ROW_LENGTH;
  
  private static final int ONE_GB = ONE_HUNDRED_MB * 10;
  private static final int ROWS_PER_GB = ONE_GB / ROW_LENGTH;
  
  private static final Text COLUMN_NAME = new Text(COLUMN_FAMILY + "data");
  
  private static HTableDescriptor tableDescriptor;
  
  static {
    tableDescriptor = new HTableDescriptor("TestTable");
    tableDescriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY.toString()));
  }
  
  private static enum Test {RANDOM_READ,
    RANDOM_READ_MEM,
    RANDOM_WRITE,
    SEQUENTIAL_READ,
    SEQUENTIAL_WRITE,
    SCAN};
  
  private Random rand;
  private Configuration conf;
  private HClient client;
  private boolean miniCluster;
  private int N;                        // Number of clients and HRegionServers
  private int range;                    // Row range for this client
  private int R;                        // Total number of rows
  
  private EvaluationClient() {
    this.rand = new Random(System.currentTimeMillis());
    this.conf = new HBaseConfiguration();
    this.miniCluster = false;
    this.client = new HClient(conf);
    this.N = 1;                                 // Default is one client
    this.range = 0;                             // Range for this client
    this.R = ROWS_PER_GB;                  // Default for one client
  }
  
  private byte[] generateValue() {
    StringBuilder val = new StringBuilder();
    while(val.length() < ROW_LENGTH) {
      val.append(Long.toString(rand.nextLong()));
    }
    return val.toString().getBytes();
  }
  
  private long randomRead(int startRow, int nRows) throws Exception {
    LOG.info("startRow: " + startRow + ", nRows: " + nRows);
    client.openTable(tableDescriptor.getName());
    
    long startTime = System.currentTimeMillis();
    int lastRow = startRow + nRows;
    for(int i = startRow; i < lastRow; i++) {
      client.get(new Text(Integer.toString(rand.nextInt() % R)), COLUMN_NAME);
    }
    return System.currentTimeMillis() - startTime;
  }

  private long randomWrite(int startRow, int nRows) throws Exception {
    LOG.info("startRow: " + startRow + ", nRows: " + nRows);
    client.openTable(tableDescriptor.getName());
    
    long startTime = System.currentTimeMillis();
    int lastRow = startRow + nRows;
    for(int i = startRow; i < lastRow; i++) {
      long lockid = client.startUpdate(new Text(Integer.toString(rand.nextInt() % R)));
      client.put(lockid, COLUMN_NAME, generateValue());
      client.commit(lockid);
    }
    return System.currentTimeMillis() - startTime;
  }
  
  private long scan(int startRow, int nRows) throws Exception {
    LOG.info("startRow: " + startRow + ", nRows: " + nRows);
    client.openTable(tableDescriptor.getName());
    
    HScannerInterface scanner = client.obtainScanner(new Text[] { COLUMN_NAME },
        new Text(Integer.toString(startRow)));
    
    long startTime = System.currentTimeMillis();
    
    try {
      int lastRow = startRow + nRows;
      HStoreKey key = new HStoreKey();
      TreeMap<Text, byte[]> results = new TreeMap<Text, byte[]>();
      for(int i = startRow; i < lastRow; i++) {
        scanner.next(key, results);
        results.clear();
      }
      
    } finally {
      scanner.close();
    }
    
    return System.currentTimeMillis() - startTime;
  }

  private long sequentialRead(int startRow, int nRows) throws Exception {
    LOG.info("startRow: " + startRow + ", nRows: " + nRows);
    client.openTable(tableDescriptor.getName());
    
    long startTime = System.currentTimeMillis();
    int lastRow = startRow + nRows;
    for(int i = startRow; i < lastRow; i++) {
      client.get(new Text(Integer.toString(i)), COLUMN_NAME);
    }
    return System.currentTimeMillis() - startTime;
  }

  private long sequentialWrite(int startRow, int nRows) throws Exception {
    LOG.info("startRow: " + startRow + ", nRows: " + nRows);
    client.openTable(tableDescriptor.getName());
    long startTime = System.currentTimeMillis();
    int lastRow = startRow + nRows;
    for(int i = startRow; i < lastRow; i++) {
      long lockid = client.startUpdate(new Text(Integer.toString(i)));
      client.put(lockid, COLUMN_NAME, generateValue());
      client.commit(lockid);
    }
    return System.currentTimeMillis() - startTime;
  }
  
  private void runNIsOne(Test test) throws IOException {
    try {
      client.createTable(tableDescriptor);

      long totalElapsedTime = 0;
      int nRows = R / (10 * N);

      if (test == Test.RANDOM_READ || test == Test.RANDOM_READ_MEM ||
          test == Test.SCAN || test == Test.SEQUENTIAL_READ ||
          test == Test.SEQUENTIAL_WRITE) {

        for(int range = 0; range < 10; range++) {
          long elapsedTime = sequentialWrite(range * nRows, nRows);
          if (test == Test.SEQUENTIAL_WRITE) {
            totalElapsedTime += elapsedTime;
          }
        }
      }

      switch(test) {
      
      case RANDOM_READ:
        for(int range = 0 ; range < 10; range++) {
          long elapsedTime = randomRead(range * nRows, nRows);
          totalElapsedTime += elapsedTime;
        }
        System.out.print("Random read of " + R + " rows completed in: ");
        break;

      case RANDOM_READ_MEM:
        throw new UnsupportedOperationException("Not yet implemented");

      case RANDOM_WRITE:
        for(int range = 0 ; range < 10; range++) {
          long elapsedTime = randomWrite(range * nRows, nRows);
          totalElapsedTime += elapsedTime;
        }
        System.out.print("Random write of " + R + " rows completed in: ");
        break;

      case SCAN:
        for(int range = 0 ; range < 10; range++) {
          long elapsedTime = scan(range * nRows, nRows);
          totalElapsedTime += elapsedTime;
        }
        System.out.print("Scan of " + R + " rows completed in: ");
        break;

      case SEQUENTIAL_READ:
        for(int range = 0 ; range < 10; range++) {
          long elapsedTime = sequentialRead(range * nRows, nRows);
          totalElapsedTime += elapsedTime;
        }
        System.out.print("Sequential read of " + R + " rows completed in: ");
        break;

      case SEQUENTIAL_WRITE:                      // We already did it!
        System.out.print("Sequential write of " + R + " rows completed in: ");
        break;

      default:
        throw new IllegalArgumentException("Invalid command value: " + test);
      }
      System.out.println((totalElapsedTime / 1000.0));

    } catch(Exception e) {
      e.printStackTrace();
      
    } finally {
      this.client.deleteTable(tableDescriptor.getName());
    }
  }
  
  private void runOneTest(Test cmd) {
  }
  
  private void runTest(Test test) throws IOException {
    if (test  == Test.RANDOM_READ_MEM) {
      // For this one test, so all fits in memory, make R smaller (See
      // pg. 9 of BigTable paper).
      R = ROWS_PER_100_MB * N;
    }
    
    MiniHBaseCluster hbaseMiniCluster = null;
    if (this.miniCluster) {
      hbaseMiniCluster = new MiniHBaseCluster(this.conf, N);
    }
    
    try {
      if (N == 1) {
        // If there is only one client and one HRegionServer, we assume nothing
        // has been set up at all.
        runNIsOne(test);
      } else {
        // Else, run 
        runOneTest(test);
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
        "[--master=host:port] [--miniCluster] <command> <args>");
    System.err.println();
    System.err.println("Options:");
    System.err.println(" master          Specify host and port of HBase " +
        "cluster master. If not present,");
    System.err.println("                 address is read from configuration");
    System.err.println(" miniCluster     Run the test on an HBaseMiniCluster");
    System.err.println();
    System.err.println("Commands:");
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
    System.err.println(" range           Integer. Required. 0 <= value <= " +
      "(nclients * 10) - 1");
  }

  private void getArgs(final int start, final String[] args) {
    if(start + 1 > args.length) {
      throw new IllegalArgumentException("must supply the number of clients " +
        "and the range for this client.");
    }
    
    N = Integer.parseInt(args[start]);
    if (N > 500 || N < 1) {
      throw new IllegalArgumentException("Number of clients must be between " +
        "1 and 500.");
    }
   
    R = ROWS_PER_GB * N;
    
    range = Integer.parseInt(args[start + 1]);
    if (range < 0 || range > (N * 10) - 1) {
      throw new IllegalArgumentException("Range must be between 0 and "
          + ((N * 10) - 1));
    }
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
       
        if (cmd.equals("randomRead")) {
          getArgs(i + 1, args);
          runTest(Test.RANDOM_READ);
          errCode = 0;
          break;
        }
        
        if (cmd.equals("randomReadMem")) {
          getArgs(i + 1, args);
          runTest(Test.RANDOM_READ_MEM);
          errCode = 0;
          break;
        }
        
        if (cmd.equals("randomWrite")) {
          getArgs(i + 1, args);
          runTest(Test.RANDOM_WRITE);
          errCode = 0;
          break;
        }
        
        if (cmd.equals("sequentialRead")) {
          getArgs(i + 1, args);
          runTest(Test.SEQUENTIAL_READ);
          errCode = 0;
          break;
        }
        
        if (cmd.equals("sequentialWrite")) {
          getArgs(i + 1, args);
          runTest(Test.SEQUENTIAL_WRITE);
          errCode = 0;
          break;
        }
        
        if (cmd.equals("scan")) {
          getArgs(i + 1, args);
          runTest(Test.SCAN);
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
  
  public static void main(final String[] args) {
    StaticTestEnvironment.initialize();
    System.exit(new EvaluationClient().doCommandLine(args));
  }
}