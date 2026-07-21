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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.net.UnknownHostException;
import java.util.EnumSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The load generator is a tool for testing NameNode behavior under
 * different client loads.
 * The main code is in HadoopCommon, @LoadGenerator. This class, LoadGeneratorMR
 * lets you run that LoadGenerator as a MapReduce job.
 * 
 * The synopsis of the command is
 * java LoadGeneratorMR
 *   -mr <numMapJobs> <outputDir> : results in outputDir/Results
 *   the rest of the args are the same as the original LoadGenerator.
 *
 */
public class LoadGeneratorMR extends LoadGenerator {
  public static final Logger LOG = LoggerFactory.getLogger(LoadGenerator.class);
  private static int numMapTasks = 1;
  private String mrOutDir;
  
  final private static String USAGE_CMD = "java LoadGeneratorMR\n";
  final private static String USAGE = USAGE_CMD
		  + "-mr <numMapJobs> <outputDir> [MUST be first 3 args] \n" + USAGE_ARGS ;
  
  // Constant "keys" used to communicate between map and reduce
  final private static Text OPEN_EXECTIME = new Text("OpenExecutionTime");
  final private static Text NUMOPS_OPEN = new Text("NumOpsOpen");
  final private static Text LIST_EXECTIME = new Text("ListExecutionTime");
  final private static Text NUMOPS_LIST = new Text("NumOpsList");
  final private static Text DELETE_EXECTIME = new Text("DeletionExecutionTime");
  final private static Text NUMOPS_DELETE = new Text("NumOpsDelete");
  final private static Text CREATE_EXECTIME = new Text("CreateExecutionTime");
  final private static Text NUMOPS_CREATE = new Text("NumOpsCreate");
  final private static Text WRITE_CLOSE_EXECTIME = new Text("WriteCloseExecutionTime");
  final private static Text NUMOPS_WRITE_CLOSE = new Text("NumOpsWriteClose");
  final private static Text ELAPSED_TIME = new Text("ElapsedTime");
  final private static Text TOTALOPS = new Text("TotalOps");
  
  // Config keys to pass args from Main to the Job
  final private static String LG_ROOT = "LG.root";
  final private static String LG_SCRIPTFILE = "LG.scriptFile";
  final private static String LG_MAXDELAYBETWEENOPS = "LG.maxDelayBetweenOps";
  final private static String LG_NUMOFTHREADS = "LG.numOfThreads";
  final private static String LG_READPR = "LG.readPr";
  final private static String LG_WRITEPR = "LG.writePr";
  final private static String LG_SEED = "LG.r";
  final private static String LG_NUMMAPTASKS = "LG.numMapTasks";
  final private static String LG_ELAPSEDTIME = "LG.elapsedTime";
  final private static String LG_STARTTIME = "LG.startTime";
  final private static String LG_FLAGFILE = "LG.flagFile";


  /** Constructor */
  public LoadGeneratorMR() throws IOException, UnknownHostException {
	super();
  }
  
  public LoadGeneratorMR(Configuration conf) throws IOException, UnknownHostException {
    this();
    setConf(conf);
  } 
  
  /** Main function called by tool runner.
   * It first initializes data by parsing the command line arguments.
   * It then calls the loadGenerator
   */
  @Override
  public int run(String[] args) throws Exception {
    int exitCode = parseArgsMR(args);
    if (exitCode != 0) {
      return exitCode;
    }
    System.out.println("Running LoadGeneratorMR against fileSystem: " + 
    FileContext.getFileContext().getDefaultFileSystem().getUri());

    return submitAsMapReduce(); // reducer will print the results
  }


  /** 
   * Parse the command line arguments and initialize the data.
   * Only parse the first arg: -mr <numMapTasks> <mrOutDir> (MUST be first three Args)
   * The rest are parsed by the Parent LoadGenerator
   **/
 
	private int parseArgsMR(String[] args) throws IOException {
	  try {
		if (args.length >= 3 && args[0].equals("-mr")) {
		  numMapTasks = Integer.parseInt(args[1]);
		  mrOutDir = args[2];
		  if (mrOutDir.startsWith("-")) {
			System.err.println("Missing output file parameter, instead got: "
							+ mrOutDir);
			System.err.println(USAGE);
			return -1;
		  }
		} else {
		  System.err.println(USAGE);
		  ToolRunner.printGenericCommandUsage(System.err);
		  return -1;
		}
		String[] strippedArgs = new String[args.length - 3];
		for (int i = 0; i < strippedArgs.length; i++) {
		  strippedArgs[i] = args[i + 3];
		}
		super.parseArgs(true, strippedArgs); // Parse normal LoadGenerator args
	  } catch (NumberFormatException e) {
		System.err.println("Illegal parameter: " + e.getLocalizedMessage());
		System.err.println(USAGE);
		return -1;
	  }
	  return 0;
	}

  /** Main program
   * 
   * @param args command line arguments
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new LoadGeneratorMR(), args);
    System.exit(res);
  }

  
  // The following methods are only used when LoadGenerator is run a MR job
  /**
   * Based on args we submit the LoadGenerator as MR job.
   * Number of MapTasks is numMapTasks
   * @return exitCode for job submission
   */
  private int submitAsMapReduce() {
    
    System.out.println("Running as a MapReduce job with " + 
        numMapTasks + " mapTasks;  Output to file " + mrOutDir);


    Configuration conf = new Configuration(getConf());
    
    // First set all the args of LoadGenerator as Conf vars to pass to MR tasks

    conf.set(LG_ROOT , root.toString());
    conf.setInt(LG_MAXDELAYBETWEENOPS, maxDelayBetweenOps);
    conf.setInt(LG_NUMOFTHREADS, numOfThreads);
    conf.set(LG_READPR, readProbs[0]+""); //Pass Double as string
    conf.set(LG_WRITEPR, writeProbs[0]+""); //Pass Double as string
    conf.setLong(LG_SEED, seed); //No idea what this is
    conf.setInt(LG_NUMMAPTASKS, numMapTasks);
    if (scriptFile == null && durations[0] <=0) {
      System.err.println("When run as a MapReduce job, elapsed Time or ScriptFile must be specified");
      System.exit(-1);
    }
    conf.setLong(LG_ELAPSEDTIME, durations[0]);
    conf.setLong(LG_STARTTIME, startTime); 
    if (scriptFile != null) {
      conf.set(LG_SCRIPTFILE , scriptFile);
    }
    conf.set(LG_FLAGFILE, flagFile.toString());
    
    // Now set the necessary conf variables that apply to run MR itself.
    JobConf jobConf = new JobConf(conf, LoadGenerator.class);
    jobConf.setJobName("NNLoadGeneratorViaMR");
    jobConf.setNumMapTasks(numMapTasks);
    jobConf.setNumReduceTasks(1); // 1 reducer to collect the results

    jobConf.setOutputKeyClass(Text.class);
    jobConf.setOutputValueClass(IntWritable.class);

    jobConf.setMapperClass(MapperThatRunsNNLoadGenerator.class);
    jobConf.setReducerClass(ReducerThatCollectsLGdata.class);

    jobConf.setInputFormat(DummyInputFormat.class);
    jobConf.setOutputFormat(TextOutputFormat.class);
    
    // Explicitly set number of max map attempts to 1.
    jobConf.setMaxMapAttempts(1);
    // Explicitly turn off speculative execution
    jobConf.setSpeculativeExecution(false);

    // This mapReduce job has no input but has output
    FileOutputFormat.setOutputPath(jobConf, new Path(mrOutDir));

    try {
      JobClient.runJob(jobConf);
    } catch (IOException e) {
      System.err.println("Failed to run job: " + e.getMessage());
      return -1;
    }
    return 0;
    
  }

  
  // Each split is empty
  public static class EmptySplit implements InputSplit {
    public void write(DataOutput out) throws IOException {}
    public void readFields(DataInput in) throws IOException {}
    public long getLength() {return 0L;}
    public String[] getLocations() {return new String[0];}
  }

  // Dummy Input format to send 1 record - number of spits is numMapTasks
  public static class DummyInputFormat extends Configured implements
      InputFormat<LongWritable, Text> {

    public InputSplit[] getSplits(JobConf conf, int numSplits) {
      numSplits = conf.getInt("LG.numMapTasks", 1);
      InputSplit[] ret = new InputSplit[numSplits];
      for (int i = 0; i < numSplits; ++i) {
        ret[i] = new EmptySplit();
      }
      return ret;
    }

    public RecordReader<LongWritable, Text> getRecordReader(
        InputSplit ignored, JobConf conf, Reporter reporter) throws IOException {

      return new RecordReader<LongWritable, Text>() {

        boolean sentOneRecord = false;

        public boolean next(LongWritable key, Text value)
            throws IOException {
          key.set(1);
          value.set("dummy");
          if (sentOneRecord == false) { // first call
            sentOneRecord = true;
            return true;
          }
          return false; // we have sent one record - we are done
        }

        public LongWritable createKey() {
          return new LongWritable();
        }
        public Text createValue() {
          return new Text();
        }
        public long getPos() throws IOException {
          return 1;
        }
        public void close() throws IOException {
        }
        public float getProgress() throws IOException {
          return 1;
        }
      };
    }
  }

  public static class MapperThatRunsNNLoadGenerator extends MapReduceBase
      implements Mapper<LongWritable, Text, Text, IntWritable> {

    private JobConf jobConf;

    @Override
    public void configure(JobConf job) {
      this.jobConf = job;
      getArgsFromConfiguration(jobConf);
    }

    private class ProgressThread extends Thread {

      boolean keepGoing; // while this is true, thread runs.
      private Reporter reporter;

      public ProgressThread(final Reporter r) {
        this.reporter = r;
        this.keepGoing = true;
      }

      public void run() {
        while (keepGoing) {
          if (!ProgressThread.interrupted()) {
            try {
              sleep(30 * 1000);
            } catch (InterruptedException e) {
            }
          }
          reporter.progress();
        }
      }
    }

    public void map(LongWritable key, Text value,
        OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      ProgressThread progressThread = new ProgressThread(reporter);
      progressThread.start();
      try {
        new LoadGenerator(jobConf).generateLoadOnNN();
        System.out
            .println("Finished generating load on NN, sending results to the reducer");
        printResults(System.out);
        progressThread.keepGoing = false;
        progressThread.join();

        // Send results to Reducer
        output.collect(OPEN_EXECTIME,
            new IntWritable((int) executionTime[OPEN]));
        output.collect(NUMOPS_OPEN, new IntWritable((int) numOfOps[OPEN]));

        output.collect(LIST_EXECTIME,
            new IntWritable((int) executionTime[LIST]));
        output.collect(NUMOPS_LIST, new IntWritable((int) numOfOps[LIST]));

        output.collect(DELETE_EXECTIME, new IntWritable(
            (int) executionTime[DELETE]));
        output.collect(NUMOPS_DELETE, new IntWritable((int) numOfOps[DELETE]));

        output.collect(CREATE_EXECTIME, new IntWritable(
            (int) executionTime[CREATE]));
        output.collect(NUMOPS_CREATE, new IntWritable((int) numOfOps[CREATE]));

        output.collect(WRITE_CLOSE_EXECTIME, new IntWritable(
            (int) executionTime[WRITE_CLOSE]));
        output.collect(NUMOPS_WRITE_CLOSE, new IntWritable(
            (int) numOfOps[WRITE_CLOSE]));

        output.collect(TOTALOPS, new IntWritable((int) totalOps));
        output.collect(ELAPSED_TIME, new IntWritable((int) totalTime));

      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    public void getArgsFromConfiguration(Configuration conf) {

      maxDelayBetweenOps = conf.getInt(LG_MAXDELAYBETWEENOPS,
          maxDelayBetweenOps);
      numOfThreads = conf.getInt(LG_NUMOFTHREADS, numOfThreads);
      readProbs[0] = Double.parseDouble(conf.get(LG_READPR, readProbs[0] + ""));
      writeProbs[0] = Double.parseDouble(conf.get(LG_WRITEPR, writeProbs[0]
          + ""));
      seed = conf.getLong(LG_SEED, seed);
      numMapTasks = conf.getInt(LG_NUMMAPTASKS, numMapTasks);
      root = new Path(conf.get(LG_ROOT, root.toString()));
      durations[0] = conf.getLong(LG_ELAPSEDTIME, 0);
      startTime = conf.getLong(LG_STARTTIME, 0);
      scriptFile = conf.get(LG_SCRIPTFILE, null);
      flagFile = new Path(conf.get(LG_FLAGFILE, FLAGFILE_DEFAULT));
      if (durations[0] > 0 && scriptFile != null) {
        System.err.println("Cannot specify both ElapsedTime and ScriptFile, exiting");
        System.exit(-1);
      }
     
      try {
        if (scriptFile != null && loadScriptFile(scriptFile, false) < 0) {
          System.err.println("Error in scriptFile, exiting");
          System.exit(-1);
        }
      } catch (IOException e) {
       System.err.println("Error loading script file " + scriptFile);
        e.printStackTrace();
      }
      if (durations[0] <= 0) {
        System.err.println("A duration of zero or less is not allowed when running via MapReduce.");
        System.exit(-1);
      }
    }
  }

  public static class ReducerThatCollectsLGdata extends MapReduceBase implements
      Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();
    private JobConf jobConf;
    
    @Override
    public void configure(JobConf job) {
      this.jobConf = job;
    }
    
    @Override
    public void reduce(Text key, Iterator<IntWritable> values,
        OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      if (key.equals(OPEN_EXECTIME)){
        executionTime[OPEN] = sum;
      } else if (key.equals(NUMOPS_OPEN)){
        numOfOps[OPEN] = sum;
      } else if (key.equals(LIST_EXECTIME)){
        executionTime[LIST] = sum;
      } else if (key.equals(NUMOPS_LIST)){
        numOfOps[LIST] = sum;
      } else if (key.equals(DELETE_EXECTIME)){
        executionTime[DELETE] = sum;
      } else if (key.equals(NUMOPS_DELETE)){
        numOfOps[DELETE] = sum;
      } else if (key.equals(CREATE_EXECTIME)){
        executionTime[CREATE] = sum;
      } else if (key.equals(NUMOPS_CREATE)){
        numOfOps[CREATE] = sum;
      } else if (key.equals(WRITE_CLOSE_EXECTIME)){
        System.out.println(WRITE_CLOSE_EXECTIME + " = " + sum);
        executionTime[WRITE_CLOSE]= sum;
      } else if (key.equals(NUMOPS_WRITE_CLOSE)){
        numOfOps[WRITE_CLOSE] = sum;
      } else if (key.equals(TOTALOPS)){
        totalOps = sum;
      } else if (key.equals(ELAPSED_TIME)){
        totalTime = sum;
      }
      result.set(sum);
      output.collect(key, result);
      // System.out.println("Key = " + key + " Sum is =" + sum);
      // printResults(System.out);
    }

    @Override
    public void close() throws IOException {
      // Output the result to a file Results in the output dir
      FileContext fc;
      try {
        fc = FileContext.getFileContext(jobConf);
      } catch (IOException ioe) {
        System.err.println("Can not initialize the file system: " + 
            ioe.getLocalizedMessage());
        return;
      }
      FSDataOutputStream o = fc.create(FileOutputFormat.getTaskOutputPath(jobConf, "Results"),
          EnumSet.of(CreateFlag.CREATE));
         
      PrintStream out = new PrintStream(o);
      printResults(out);
      out.close();
      o.close();
    }
  }
}
