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

package org.apache.hadoop.fs.loadGenerator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Preconditions;

/** The load generator is a tool for testing NameNode behavior under
 * different client loads.
 * It allows the user to generate different mixes of read, write,
 * and list requests by specifying the probabilities of read and
 * write. The user controls the intensity of the load by
 * adjusting parameters for the number of worker threads and the delay
 * between operations. While load generators are running, the user
 * can profile and monitor the running of the NameNode. When a load
 * generator exits, it print some NameNode statistics like the average
 * execution time of each kind of operations and the NameNode
 * throughput.
 * 
 * The user may either specify constant duration, read and write 
 * probabilities via the command line, or may specify a text file
 * that acts as a script of which read and write probabilities to
 * use for specified durations.
 * 
 * The script takes the form of lines of duration in seconds, read
 * probability and write probability, each separated by white space.
 * Blank lines and lines starting with # (comments) are ignored.
 * 
 * After command line argument parsing and data initialization,
 * the load generator spawns the number of worker threads 
 * as specified by the user.
 * Each thread sends a stream of requests to the NameNode.
 * For each iteration, it first decides if it is going to read a file,
 * create a file, or listing a directory following the read and write 
 * probabilities specified by the user.
 * When reading, it randomly picks a file in the test space and reads
 * the entire file. When writing, it randomly picks a directory in the
 * test space and creates a file whose name consists of the current 
 * machine's host name and the thread id. The length of the file
 * follows Gaussian distribution with an average size of 2 blocks and
 * the standard deviation of 1 block. The new file is filled with 'a'.
 * Immediately after the file creation completes, the file is deleted
 * from the test space.
 * While listing, it randomly picks a directory in the test space and
 * list the directory content.
 * Between two consecutive operations, the thread pauses for a random
 * amount of time in the range of [0, maxDelayBetweenOps] 
 * if the specified max delay is not zero.
 * All threads are stopped when the specified elapsed time has passed 
 * in command-line execution, or all the lines of script have been 
 * executed, if using a script.
 * Before exiting, the program prints the average execution for 
 * each kind of NameNode operations, and the number of requests
 * served by the NameNode.
 *
 * The synopsis of the command is
 * java LoadGenerator
 *   -readProbability <read probability>: read probability [0, 1]
 *                                        with a default value of 0.3333. 
 *   -writeProbability <write probability>: write probability [0, 1]
 *                                         with a default value of 0.3333.
 *   -root <root>: test space with a default value of /testLoadSpace
 *   -maxDelayBetweenOps <maxDelayBetweenOpsInMillis>: 
 *      Max delay in the unit of milliseconds between two operations with a 
 *      default value of 0 indicating no delay.
 *   -numOfThreads <numOfThreads>: 
 *      number of threads to spawn with a default value of 200.
 *   -elapsedTime <elapsedTimeInSecs>: 
 *      the elapsed time of program with a default value of 0 
 *      indicating running forever
 *   -startTime <startTimeInMillis> : when the threads start to run.
 *   -scriptFile <file name>: text file to parse for scripted operation
 */
public class LoadGenerator extends Configured implements Tool {
  public static final Log LOG = LogFactory.getLog(LoadGenerator.class);
  
  private volatile boolean shouldRun = true;
  private Path root = DataGenerator.DEFAULT_ROOT;
  private FileContext fc;
  private int maxDelayBetweenOps = 0;
  private int numOfThreads = 200;
  private long [] durations = {0};
  private double [] readProbs = {0.3333};
  private double [] writeProbs = {0.3333};
  private volatile int currentIndex = 0;
  long totalTime = 0;
  private long startTime = Time.now()+10000;
  final static private int BLOCK_SIZE = 10;
  private ArrayList<String> files = new ArrayList<String>();  // a table of file names
  private ArrayList<String> dirs = new ArrayList<String>(); // a table of directory names
  private Random r = null;
  final private static String USAGE = "java LoadGenerator\n" +
  	"-readProbability <read probability>\n" +
    "-writeProbability <write probability>\n" +
    "-root <root>\n" +
    "-maxDelayBetweenOps <maxDelayBetweenOpsInMillis>\n" +
    "-numOfThreads <numOfThreads>\n" +
    "-elapsedTime <elapsedTimeInSecs>\n" +
    "-startTime <startTimeInMillis>\n" +
    "-scriptFile <filename>";
  final private String hostname;
  private final byte[] WRITE_CONTENTS = new byte[4096];

  private static final int ERR_TEST_FAILED = 2;

  /** Constructor */
  public LoadGenerator() throws IOException, UnknownHostException {
    InetAddress addr = InetAddress.getLocalHost();
    hostname = addr.getHostName();
    Arrays.fill(WRITE_CONTENTS, (byte) 'a');
  }

  private final static int OPEN = 0;
  private final static int LIST = 1;
  private final static int CREATE = 2;
  private final static int WRITE_CLOSE = 3;
  private final static int DELETE = 4;
  private final static int TOTAL_OP_TYPES =5;
  private long [] executionTime = new long[TOTAL_OP_TYPES];
  private long [] totalNumOfOps = new long[TOTAL_OP_TYPES];
  
  /** A thread sends a stream of requests to the NameNode.
   * At each iteration, it first decides if it is going to read a file,
   * create a file, or listing a directory following the read
   * and write probabilities.
   * When reading, it randomly picks a file in the test space and reads
   * the entire file. When writing, it randomly picks a directory in the
   * test space and creates a file whose name consists of the current 
   * machine's host name and the thread id. The length of the file
   * follows Gaussian distribution with an average size of 2 blocks and
   * the standard deviation of 1 block. The new file is filled with 'a'.
   * Immediately after the file creation completes, the file is deleted
   * from the test space.
   * While listing, it randomly picks a directory in the test space and
   * list the directory content.
   * Between two consecutive operations, the thread pauses for a random
   * amount of time in the range of [0, maxDelayBetweenOps] 
   * if the specified max delay is not zero.
   * A thread runs for the specified elapsed time if the time isn't zero.
   * Otherwise, it runs forever.
   */
  private class DFSClientThread extends Thread {
    private int id;
    private long [] executionTime = new long[TOTAL_OP_TYPES];
    private long [] totalNumOfOps = new long[TOTAL_OP_TYPES];
    private byte[] buffer = new byte[1024];
    private boolean failed;

    private DFSClientThread(int id) {
      this.id = id;
    }
    
    /** Main loop
     * Each iteration decides what's the next operation and then pauses.
     */
    @Override
    public void run() {
      try {
        while (shouldRun) {
          nextOp();
          delay();
        }
      } catch (Exception ioe) {
        System.err.println(ioe.getLocalizedMessage());
        ioe.printStackTrace();
        failed = true;
      }
    }
    
    /** Let the thread pause for a random amount of time in the range of
     * [0, maxDelayBetweenOps] if the delay is not zero. Otherwise, no pause.
     */
    private void delay() throws InterruptedException {
      if (maxDelayBetweenOps>0) {
        int delay = r.nextInt(maxDelayBetweenOps);
        Thread.sleep(delay);
      }
    }
    
    /** Perform the next operation. 
     * 
     * Depending on the read and write probabilities, the next
     * operation could be either read, write, or list.
     */
    private void nextOp() throws IOException {
      double rn = r.nextDouble();
      int i = currentIndex;
      
      if(LOG.isDebugEnabled())
        LOG.debug("Thread " + this.id + " moving to index " + i);
      
      if (rn < readProbs[i]) {
        read();
      } else if (rn < readProbs[i] + writeProbs[i]) {
        write();
      } else {
        list();
      }
    }
    
    /** Read operation randomly picks a file in the test space and reads
     * the entire file */
    private void read() throws IOException {
      String fileName = files.get(r.nextInt(files.size()));
      long startTime = Time.now();
      InputStream in = fc.open(new Path(fileName));
      executionTime[OPEN] += (Time.now()-startTime);
      totalNumOfOps[OPEN]++;
      while (in.read(buffer) != -1) {}
      in.close();
    }
    
    /** The write operation randomly picks a directory in the
     * test space and creates a file whose name consists of the current 
     * machine's host name and the thread id. The length of the file
     * follows Gaussian distribution with an average size of 2 blocks and
     * the standard deviation of 1 block. The new file is filled with 'a'.
     * Immediately after the file creation completes, the file is deleted
     * from the test space.
     */
    private void write() throws IOException {
      String dirName = dirs.get(r.nextInt(dirs.size()));
      Path file = new Path(dirName, hostname+id);
      double fileSize = 0;
      while ((fileSize = r.nextGaussian()+2)<=0) {}
      genFile(file, (long)(fileSize*BLOCK_SIZE));
      long startTime = Time.now();
      fc.delete(file, true);
      executionTime[DELETE] += (Time.now()-startTime);
      totalNumOfOps[DELETE]++;
    }
    
    /** The list operation randomly picks a directory in the test space and
     * list the directory content.
     */
    private void list() throws IOException {
      String dirName = dirs.get(r.nextInt(dirs.size()));
      long startTime = Time.now();
      fc.listStatus(new Path(dirName));
      executionTime[LIST] += (Time.now()-startTime);
      totalNumOfOps[LIST]++;
    }

    /** Create a file with a length of <code>fileSize</code>.
     * The file is filled with 'a'.
     */
    private void genFile(Path file, long fileSize) throws IOException {
      long startTime = Time.now();
      FSDataOutputStream out = null;
      try {
        out = fc.create(file,
            EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE),
            CreateOpts.createParent(), CreateOpts.bufferSize(4096),
            CreateOpts.repFac((short) 3));
        executionTime[CREATE] += (Time.now() - startTime);
        totalNumOfOps[CREATE]++;

        long i = fileSize;
        while (i > 0) {
          long s = Math.min(fileSize, WRITE_CONTENTS.length);
          out.write(WRITE_CONTENTS, 0, (int) s);
          i -= s;
        }

        startTime = Time.now();
        executionTime[WRITE_CLOSE] += (Time.now() - startTime);
        totalNumOfOps[WRITE_CLOSE]++;
      } finally {
        IOUtils.cleanup(LOG, out);
      }
    }
  }
  
  /** Main function:
   * It first initializes data by parsing the command line arguments.
   * It then starts the number of DFSClient threads as specified by
   * the user.
   * It stops all the threads when the specified elapsed time is passed.
   * Before exiting, it prints the average execution for 
   * each operation and operation throughput.
   */
  @Override
  public int run(String[] args) throws Exception {
    int exitCode = init(args);
    if (exitCode != 0) {
      return exitCode;
    }
    
    barrier();
    
    DFSClientThread[] threads = new DFSClientThread[numOfThreads];
    for (int i=0; i<numOfThreads; i++) {
      threads[i] = new DFSClientThread(i); 
      threads[i].start();
    }
    
    if (durations[0] > 0) {
      while(shouldRun) {
        Thread.sleep(durations[currentIndex] * 1000);
        totalTime += durations[currentIndex];
        
        // Are we on the final line of the script?
        if( (currentIndex + 1) == durations.length) {
          shouldRun = false;
        } else {
          if(LOG.isDebugEnabled()) {
            LOG.debug("Moving to index " + currentIndex + ": r = "
                + readProbs[currentIndex] + ", w = " + writeProbs
                + " for duration " + durations[currentIndex]);
          }
          currentIndex++;
        }
      }
    } 
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("Done with testing.  Waiting for threads to finish.");
    }

    boolean failed = false;
    for (DFSClientThread thread : threads) {
      thread.join();
      for (int i=0; i<TOTAL_OP_TYPES; i++) {
        executionTime[i] += thread.executionTime[i];
        totalNumOfOps[i] += thread.totalNumOfOps[i];
      }
      failed = failed || thread.failed;
    }

    if (failed) {
      exitCode = -ERR_TEST_FAILED;
    }

    long totalOps = 0;
    for (int i=0; i<TOTAL_OP_TYPES; i++) {
      totalOps += totalNumOfOps[i];
    }
    
    if (totalNumOfOps[OPEN] != 0) {
      System.out.println("Average open execution time: " + 
          (double)executionTime[OPEN]/totalNumOfOps[OPEN] + "ms");
    }
    if (totalNumOfOps[LIST] != 0) {
      System.out.println("Average list execution time: " + 
          (double)executionTime[LIST]/totalNumOfOps[LIST] + "ms");
    }
    if (totalNumOfOps[DELETE] != 0) {
      System.out.println("Average deletion execution time: " + 
          (double)executionTime[DELETE]/totalNumOfOps[DELETE] + "ms");
      System.out.println("Average create execution time: " + 
          (double)executionTime[CREATE]/totalNumOfOps[CREATE] + "ms");
      System.out.println("Average write_close execution time: " + 
          (double)executionTime[WRITE_CLOSE]/totalNumOfOps[WRITE_CLOSE] + "ms");
    }
    if (durations[0] != 0) { 
      System.out.println("Average operations per second: " + 
          (double)totalOps/totalTime +"ops/s");
    }
    System.out.println();
    return exitCode;
  }

  /** Parse the command line arguments and initialize the data */
  private int init(String[] args) throws IOException {
    try {
      fc = FileContext.getFileContext(getConf());
    } catch (IOException ioe) {
      System.err.println("Can not initialize the file system: " + 
          ioe.getLocalizedMessage());
      return -1;
    }
    int hostHashCode = hostname.hashCode();
    boolean scriptSpecified = false;
    
    try {
      for (int i = 0; i < args.length; i++) { // parse command line
        if (args[i].equals("-scriptFile")) {
          if(loadScriptFile(args[++i]) == -1)
            return -1;
          scriptSpecified = true;
        } else if (args[i].equals("-readProbability")) {
          if(scriptSpecified) {
            System.err.println("Can't specify probabilities and use script.");
            return -1;
          }
          readProbs[0] = Double.parseDouble(args[++i]);
          if (readProbs[0] < 0 || readProbs[0] > 1) {
            System.err.println( 
                "The read probability must be [0, 1]: " + readProbs[0]);
            return -1;
          }
        } else if (args[i].equals("-writeProbability")) {
          if(scriptSpecified) {
            System.err.println("Can't specify probabilities and use script.");
            return -1;
          }
          writeProbs[0] = Double.parseDouble(args[++i]);
          if (writeProbs[0] < 0 || writeProbs[0] > 1) {
            System.err.println( 
                "The write probability must be [0, 1]: " + writeProbs[0]);
            return -1;
          }
        } else if (args[i].equals("-root")) {
          root = new Path(args[++i]);
        } else if (args[i].equals("-maxDelayBetweenOps")) {
          maxDelayBetweenOps = Integer.parseInt(args[++i]); // in milliseconds
        } else if (args[i].equals("-numOfThreads")) {
          numOfThreads = Integer.parseInt(args[++i]);
          if (numOfThreads <= 0) {
            System.err.println(
                "Number of threads must be positive: " + numOfThreads);
            return -1;
          }
        } else if (args[i].equals("-startTime")) {
          startTime = Long.parseLong(args[++i]);
        } else if (args[i].equals("-elapsedTime")) {
          if(scriptSpecified) {
            System.err.println("Can't specify elapsedTime and use script.");
            return -1;
          }
          durations[0] = Long.parseLong(args[++i]);
        } else if (args[i].equals("-seed")) {
          r = new Random(Long.parseLong(args[++i])+hostHashCode);
        } else {
          System.err.println(USAGE);
          ToolRunner.printGenericCommandUsage(System.err);
          return -1;
        }
      }
    } catch (NumberFormatException e) {
      System.err.println("Illegal parameter: " + e.getLocalizedMessage());
      System.err.println(USAGE);
      return -1;
    }
    
    for(int i = 0; i < readProbs.length; i++) {
      if (readProbs[i] + writeProbs[i] <0 || readProbs[i]+ writeProbs[i] > 1) {
        System.err.println(
            "The sum of read probability and write probability must be [0, 1]: "
            + readProbs[i] + " " + writeProbs[i]);
        return -1;
      }
    }
    
    if (r==null) {
      r = new Random(Time.now()+hostHashCode);
    }
    
    return initFileDirTables();
  }

  private static void parseScriptLine(String line, ArrayList<Long> duration,
      ArrayList<Double> readProb, ArrayList<Double> writeProb) {
    String[] a = line.split("\\s");

    if (a.length != 3) {
      throw new IllegalArgumentException("Incorrect number of parameters: "
          + line);
    }

    try {
      long d = Long.parseLong(a[0]);
      double r = Double.parseDouble(a[1]);
      double w = Double.parseDouble(a[2]);

      Preconditions.checkArgument(d >= 0, "Invalid duration: " + d);
      Preconditions.checkArgument(0 <= r && r <= 1.0,
          "The read probability must be [0, 1]: " + r);
      Preconditions.checkArgument(0 <= w && w <= 1.0,
          "The read probability must be [0, 1]: " + w);

      readProb.add(r);
      duration.add(d);
      writeProb.add(w);
    } catch (NumberFormatException nfe) {
      throw new IllegalArgumentException("Cannot parse: " + line);
    }
  }

  /**
   * Read a script file of the form: lines of text with duration in seconds,
   * read probability and write probability, separated by white space.
   * 
   * @param filename Script file
   * @return 0 if successful, -1 if not
   * @throws IOException if errors with file IO
   */
  private int loadScriptFile(String filename) throws IOException  {
    FileReader fr = new FileReader(new File(filename));
    BufferedReader br = new BufferedReader(fr);
    ArrayList<Long> duration  = new ArrayList<Long>();
    ArrayList<Double> readProb  = new ArrayList<Double>();
    ArrayList<Double> writeProb = new ArrayList<Double>();
    int lineNum = 0;
    
    String line;
    // Read script, parse values, build array of duration, read and write probs

    try {
      while ((line = br.readLine()) != null) {
        lineNum++;
        if (line.startsWith("#") || line.isEmpty()) // skip comments and blanks
          continue;

        parseScriptLine(line, duration, readProb, writeProb);
      }
    } catch (IllegalArgumentException e) {
      System.err.println("Line: " + lineNum + ", " + e.getMessage());
      return -1;
    } finally {
      IOUtils.cleanup(LOG, br);
    }
    
    // Copy vectors to arrays of values, to avoid autoboxing overhead later
    durations = new long[duration.size()];
    readProbs = new double[readProb.size()];
    writeProbs = new double[writeProb.size()];
    
    for(int i = 0; i < durations.length; i++) {
      durations[i] = duration.get(i);
      readProbs[i] = readProb.get(i);
      writeProbs[i] = writeProb.get(i);
    }
    
    if(durations[0] == 0)
      System.err.println("Initial duration set to 0.  " +
          		                             "Will loop until stopped manually.");
    
    return 0;
  }
  
  /** Create a table that contains all directories under root and
   * another table that contains all files under root.
   */
  private int initFileDirTables() {
    try {
      initFileDirTables(root);
    } catch (IOException e) {
      System.err.println(e.getLocalizedMessage());
      e.printStackTrace();
      return -1;
    }
    if (dirs.isEmpty()) {
      System.err.println("The test space " + root + " is empty");
      return -1;
    }
    if (files.isEmpty()) {
      System.err.println("The test space " + root + 
          " does not have any file");
      return -1;
    }
    return 0;
  }
  
  /** Create a table that contains all directories under the specified path and
   * another table that contains all files under the specified path and
   * whose name starts with "_file_".
   */
  private void initFileDirTables(Path path) throws IOException {
    FileStatus[] stats = fc.util().listStatus(path);

    for (FileStatus stat : stats) {
      if (stat.isDirectory()) {
        dirs.add(stat.getPath().toString());
        initFileDirTables(stat.getPath());
      } else {
        Path filePath = stat.getPath();
        if (filePath.getName().startsWith(StructureGenerator.FILE_NAME_PREFIX)) {
          files.add(filePath.toString());
        }
      }
    }
  }
  
  /** Returns when the current number of seconds from the epoch equals
   * the command line argument given by <code>-startTime</code>.
   * This allows multiple instances of this program, running on clock
   * synchronized nodes, to start at roughly the same time.
   */
  private void barrier() {
    long sleepTime;
    while ((sleepTime = startTime - Time.now()) > 0) {
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException ex) {
      }
    }
  }
  
  /** Main program
   * 
   * @param args command line arguments
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(),
        new LoadGenerator(), args);
    System.exit(res);
  }

}
