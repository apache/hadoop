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
package org.apache.hadoop.examples.pi;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.examples.pi.math.Summation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The main class for computing sums using map/reduce jobs.
 * A sum is partitioned into jobs.
 * A job may be executed on the map-side or on the reduce-side.
 * A map-side job has multiple maps and zero reducer.
 * A reduce-side job has one map and multiple reducers.
 * Depending on the clusters status in runtime,
 * a mix-type job may be executed on either side.
 */
public final class DistSum extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(DistSum.class);

  private static final String NAME = DistSum.class.getSimpleName();
  private static final String N_PARTS = "mapreduce.pi." + NAME + ".nParts";
  /////////////////////////////////////////////////////////////////////////////
  /** DistSum job parameters */
  static class Parameters {
    static final int COUNT = 6;
    static final String LIST = "<nThreads> <nJobs> <type> <nPart> <remoteDir> <localDir>";
    static final String DESCRIPTION =
        "\n  <nThreads> The number of working threads."
      + "\n  <nJobs> The number of jobs per sum."
      + "\n  <type> 'm' for map side job, 'r' for reduce side job, 'x' for mix type."
      + "\n  <nPart> The number of parts per job."
      + "\n  <remoteDir> Remote directory for submitting jobs."
      + "\n  <localDir> Local directory for storing output files.";

    /** Number of worker threads */
    final int nThreads;
    /** Number of jobs */
    final int nJobs;
    /** Number of parts per job */
    final int nParts;
    /** The machine used in the computation */
    final Machine machine;
    /** The remote job directory */
    final String remoteDir;
    /** The local output directory */
    final File localDir;
  
    private Parameters(Machine machine, int nThreads, int nJobs, int nParts,
        String remoteDir, File localDir) {
      this.machine = machine;
      this.nThreads = nThreads;
      this.nJobs = nJobs;
      this.nParts = nParts;
      this.remoteDir = remoteDir;
      this.localDir = localDir;
    }

    /** {@inheritDoc} */
    public String toString() {
      return "\nnThreads  = " + nThreads
           + "\nnJobs     = " + nJobs
           + "\nnParts    = " + nParts + " (" + machine + ")"
           + "\nremoteDir = " + remoteDir
           + "\nlocalDir  = " + localDir;
    }

    /** Parse parameters */
    static Parameters parse(String[] args, int i) {
      if (args.length - i < COUNT)
        throw new IllegalArgumentException("args.length - i < COUNT = "
            + COUNT + ", args.length="
            + args.length + ", i=" + i + ", args=" + Arrays.asList(args));
      
      final int nThreads = Integer.parseInt(args[i++]);
      final int nJobs = Integer.parseInt(args[i++]);
      final String type = args[i++];
      final int nParts = Integer.parseInt(args[i++]);
      final String remoteDir = args[i++];
      final File localDir = new File(args[i++]);

      if (!"m".equals(type) && !"r".equals(type) && !"x".equals(type)) { 
        throw new IllegalArgumentException("type=" + type + " is not equal to m, r or x");
      } else if (nParts <= 0) {
        throw new IllegalArgumentException("nParts = " + nParts + " <= 0");
      } else if (nJobs <= 0) {
        throw new IllegalArgumentException("nJobs = " + nJobs + " <= 0");
      } else if (nThreads <= 0) {
        throw new IllegalArgumentException("nThreads = " + nThreads + " <= 0");
      }
      Util.checkDirectory(localDir);

      return new Parameters("m".equals(type)? MapSide.INSTANCE
          : "r".equals(type)? ReduceSide.INSTANCE: MixMachine.INSTANCE,
          nThreads, nJobs, nParts, remoteDir, localDir);
    }
  }
  /////////////////////////////////////////////////////////////////////////////
  /** Abstract machine for job execution. */
  public static abstract class Machine {
    /** Initialize a job */
    abstract void init(Job job) throws IOException;
    
    /** {@inheritDoc} */
    public String toString() {return getClass().getSimpleName();}

    /** Compute sigma */
    static void compute(Summation sigma,
        TaskInputOutputContext<?, ?, NullWritable, TaskResult> context
        ) throws IOException, InterruptedException {
      String s;
      LOG.info(s = "sigma=" + sigma);
      context.setStatus(s);

      final long start = System.currentTimeMillis();
      sigma.compute();
      final long duration = System.currentTimeMillis() - start;
      final TaskResult result = new TaskResult(sigma, duration);

      LOG.info(s = "result=" + result);
      context.setStatus(s);
      context.write(NullWritable.get(), result);
    }

    /** Split for the summations */
    public static final class SummationSplit extends InputSplit implements Writable, Container<Summation> {
      private final static String[] EMPTY = {};
  
      private Summation sigma;
  
      public SummationSplit() {}
      private SummationSplit(Summation sigma) {this.sigma = sigma;}
      /** {@inheritDoc} */
      @Override
      public Summation getElement() {return sigma;}
      /** {@inheritDoc} */
      @Override
      public long getLength() {return 1;}
      /** {@inheritDoc} */
      @Override
      public String[] getLocations() {return EMPTY;}
  
      /** {@inheritDoc} */
      @Override
      public void readFields(DataInput in) throws IOException {
        sigma = SummationWritable.read(in);
      }
      /** {@inheritDoc} */
      @Override
      public void write(DataOutput out) throws IOException {
        new SummationWritable(sigma).write(out);
      }
    }

    /** An abstract InputFormat for the jobs */
    public static abstract class AbstractInputFormat extends InputFormat<NullWritable, SummationWritable> {
      /** Specify how to read the records */
      @Override
      public final RecordReader<NullWritable, SummationWritable> createRecordReader(
          InputSplit generic, TaskAttemptContext context) {
        final SummationSplit split = (SummationSplit)generic;
  
        //return a record reader
        return new RecordReader<NullWritable, SummationWritable>() {
          boolean done = false;
  
          /** {@inheritDoc} */
          @Override
          public void initialize(InputSplit split, TaskAttemptContext context) {}
          /** {@inheritDoc} */
          @Override
          public boolean nextKeyValue() {return !done ? done = true : false;}
          /** {@inheritDoc} */
          @Override
          public NullWritable getCurrentKey() {return NullWritable.get();}
          /** {@inheritDoc} */
          @Override
          public SummationWritable getCurrentValue() {return new SummationWritable(split.getElement());}
          /** {@inheritDoc} */
          @Override
          public float getProgress() {return done? 1f: 0f;}
          /** {@inheritDoc} */
          @Override
          public void close() {}
        };
      }
    }
  }
  /////////////////////////////////////////////////////////////////////////////
  /**
   * A machine which does computation on the map side.
   */
  public static class MapSide extends Machine {
    private static final MapSide INSTANCE = new MapSide();

    /** {@inheritDoc} */
    @Override
    public void init(Job job) {
      // setup mapper
      job.setMapperClass(SummingMapper.class);
      job.setMapOutputKeyClass(NullWritable.class);
      job.setMapOutputValueClass(TaskResult.class);

      // zero reducer
      job.setNumReduceTasks(0);

      // setup input
      job.setInputFormatClass(PartitionInputFormat.class);
    }

    /** An InputFormat which partitions a summation */
    public static class PartitionInputFormat extends AbstractInputFormat {
      /** Partitions the summation into parts and then return them as splits */
      @Override
      public List<InputSplit> getSplits(JobContext context) {
        //read sigma from conf
        final Configuration conf = context.getConfiguration();
        final Summation sigma = SummationWritable.read(DistSum.class, conf); 
        final int nParts = conf.getInt(N_PARTS, 0);
  
        //create splits
        final List<InputSplit> splits = new ArrayList<InputSplit>(nParts);
        final Summation[] parts = sigma.partition(nParts);
        for(int i = 0; i < parts.length; ++i) {
          splits.add(new SummationSplit(parts[i]));
          //LOG.info("parts[" + i + "] = " + parts[i]);
        }
        return splits;
      }
    }
  
    /** A mapper which computes sums */
    public static class SummingMapper extends
        Mapper<NullWritable, SummationWritable, NullWritable, TaskResult> {
      @Override
      protected void map(NullWritable nw, SummationWritable sigma, final Context context
          ) throws IOException, InterruptedException {
        compute(sigma.getElement(), context);
      }
    }
  }
  /////////////////////////////////////////////////////////////////////////////
  /**
   * A machine which does computation on the reduce side.
   */
  public static class ReduceSide extends Machine {
    private static final ReduceSide INSTANCE = new ReduceSide();

    /** {@inheritDoc} */
    @Override
    public void init(Job job) {
      // setup mapper
      job.setMapperClass(PartitionMapper.class);
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(SummationWritable.class);

      // setup partitioner
      job.setPartitionerClass(IndexPartitioner.class);

      // setup reducer
      job.setReducerClass(SummingReducer.class);
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(TaskResult.class);
      final Configuration conf = job.getConfiguration();
      final int nParts = conf.getInt(N_PARTS, 1);
      job.setNumReduceTasks(nParts);

      // setup input
      job.setInputFormatClass(SummationInputFormat.class);
    }

    /** An InputFormat which returns a single summation. */
    public static class SummationInputFormat extends AbstractInputFormat {
      /** @return a list containing a single split of summation */
      @Override
      public List<InputSplit> getSplits(JobContext context) {
        //read sigma from conf
        final Configuration conf = context.getConfiguration();
        final Summation sigma = SummationWritable.read(DistSum.class, conf); 
  
        //create splits
        final List<InputSplit> splits = new ArrayList<InputSplit>(1);
        splits.add(new SummationSplit(sigma));
        return splits;
      }
    }

    /** A Mapper which partitions a summation */
    public static class PartitionMapper extends
        Mapper<NullWritable, SummationWritable, IntWritable, SummationWritable> {
      /** Partitions sigma into parts */
      @Override
      protected void map(NullWritable nw, SummationWritable sigma, final Context context
          ) throws IOException, InterruptedException {
        final Configuration conf = context.getConfiguration();
        final int nParts = conf.getInt(N_PARTS, 0);
        final Summation[] parts = sigma.getElement().partition(nParts);
        for(int i = 0; i < parts.length; ++i) {
          context.write(new IntWritable(i), new SummationWritable(parts[i]));
          LOG.info("parts[" + i + "] = " + parts[i]);
        }
      }
    }

    /** Use the index for partitioning. */
    public static class IndexPartitioner extends Partitioner<IntWritable, SummationWritable> {
      /** Return the index as the partition. */
      @Override
      public int getPartition(IntWritable index, SummationWritable value, int numPartitions) {
        return index.get();
      }
    }    

    /** A Reducer which computes sums */
    public static class SummingReducer extends
        Reducer<IntWritable, SummationWritable, NullWritable, TaskResult> {
      @Override
      protected void reduce(IntWritable index, Iterable<SummationWritable> sums,
          Context context) throws IOException, InterruptedException {
        LOG.info("index=" + index);
        for(SummationWritable sigma : sums)
          compute(sigma.getElement(), context);
      }
    }
  }
  /////////////////////////////////////////////////////////////////////////////
  /**
   * A machine which chooses Machine in runtime according to the cluster status
   */
  public static class MixMachine extends Machine {
    private static final MixMachine INSTANCE = new MixMachine();
    
    private Cluster cluster;

    /** {@inheritDoc} */
    @Override
    public synchronized void init(Job job) throws IOException {
      final Configuration conf = job.getConfiguration();
      if (cluster == null) {
        String jobTrackerStr = conf.get("mapreduce.jobtracker.address", "localhost:8012");
        cluster = new Cluster(NetUtils.createSocketAddr(jobTrackerStr), conf);

      }
      chooseMachine(conf).init(job);
    }

    /**
     * Choose a Machine in runtime according to the cluster status.
     */
    private Machine chooseMachine(Configuration conf) throws IOException {
      final int parts = conf.getInt(N_PARTS, Integer.MAX_VALUE);
      try {
        for(;; Thread.sleep(2000)) {
          //get cluster status
          final ClusterMetrics status = cluster.getClusterStatus();
          final int m = 
            status.getMapSlotCapacity() - status.getOccupiedMapSlots();
          final int r = 
            status.getReduceSlotCapacity() - status.getOccupiedReduceSlots();
          if (m >= parts || r >= parts) {
            //favor ReduceSide machine
            final Machine value = r >= parts?
                ReduceSide.INSTANCE: MapSide.INSTANCE;
            Util.out.println("  " + this + " is " + value + " (m=" + m + ", r=" + r + ")");
            return value;
          }
        }
      } catch (InterruptedException e) {
        throw new IOException(e);
      }    
    }

  }
  /////////////////////////////////////////////////////////////////////////////
  private final Util.Timer timer = new Util.Timer(true);  
  private Parameters parameters;

  /** Get Parameters */
  Parameters getParameters() {return parameters;}
  /** Set Parameters */
  void setParameters(Parameters p) {parameters = p;}

  /** Create a job */
  private Job createJob(String name, Summation sigma) throws IOException {
    final Job job = new Job(getConf(), parameters.remoteDir + "/" + name);
    final Configuration jobconf = job.getConfiguration();
    job.setJarByClass(DistSum.class);
    jobconf.setInt(N_PARTS, parameters.nParts);
    SummationWritable.write(sigma, DistSum.class, jobconf);

    // disable task timeout
    jobconf.setLong(MRJobConfig.TASK_TIMEOUT, 0);
    // do not use speculative execution
    jobconf.setBoolean(MRJobConfig.MAP_SPECULATIVE, false);
    jobconf.setBoolean(MRJobConfig.REDUCE_SPECULATIVE, false);

    return job; 
  }

  /** Start a job to compute sigma */
  private void compute(final String name, Summation sigma) throws IOException {
    if (sigma.getValue() != null)
      throw new IOException("sigma.getValue() != null, sigma=" + sigma);

    //setup remote directory
    final FileSystem fs = FileSystem.get(getConf());
    final Path dir = fs.makeQualified(new Path(parameters.remoteDir, name));
    if (!Util.createNonexistingDirectory(fs, dir))
      return;

    //setup a job
    final Job job = createJob(name, sigma);
    final Path outdir = new Path(dir, "out");
    FileOutputFormat.setOutputPath(job, outdir);

    //start a map/reduce job
    final String startmessage = "steps/parts = "
      + sigma.E.getSteps() + "/" + parameters.nParts
      + " = " + Util.long2string(sigma.E.getSteps()/parameters.nParts);
    Util.runJob(name, job, parameters.machine, startmessage, timer);
    final List<TaskResult> results = Util.readJobOutputs(fs, outdir);
    Util.writeResults(name, results, fs, parameters.remoteDir);
    fs.delete(dir, true);

    //combine results
    final List<TaskResult> combined = Util.combine(results);
    final PrintWriter out = Util.createWriter(parameters.localDir, name);
    try {
      for(TaskResult r : combined) {
        final String s = taskResult2string(name, r);
        out.println(s);
        out.flush();
        Util.out.println(s);
      }
    } finally {
      out.close();
    }
    if (combined.size() == 1) {
      final Summation s = combined.get(0).getElement();
      if (sigma.contains(s) && s.contains(sigma))
        sigma.setValue(s.getValue());
    }
  }

  /** Convert a TaskResult to a String */
  public static String taskResult2string(String name, TaskResult result) {
    return NAME + " " + name + "> " + result;
  }

  /** Convert a String to a (String, TaskResult) pair */
  public static Map.Entry<String, TaskResult> string2TaskResult(final String s) {
    //  LOG.info("line = " + line);
    int j = s.indexOf(NAME);
    if (j == 0) {
      int i = j + NAME.length() + 1;
      j = s.indexOf("> ", i);
      final String key = s.substring(i, j);
      final TaskResult value = TaskResult.valueOf(s.substring(j + 2));
      return new Map.Entry<String, TaskResult>(){
        @Override
        public String getKey() {return key;}
        @Override
        public TaskResult getValue() {return value;}
        @Override
        public TaskResult setValue(TaskResult value) {
          throw new UnsupportedOperationException();
        }
      };
    }
    return null;
  }

  /** Callable computation */
  class Computation implements Callable<Computation> {
    private final int index;
    private final String name;
    private final Summation sigma;

    Computation(int index, String name, Summation sigma) {
      this.index = index;
      this.name = name;
      this.sigma = sigma;
    }

    /** @return The job name */
    String getJobName() {return String.format("%s.job%03d", name, index);}

    /** {@inheritDoc} */
    @Override
    public String toString() {return getJobName() + sigma;}

    /** Start the computation */
    @Override
    public Computation call() {
      if (sigma.getValue() == null)
        try {
          compute(getJobName(), sigma);
        } catch(Exception e) {
          Util.out.println("ERROR: Got an exception from " + getJobName());
          e.printStackTrace(Util.out);
        }
      return this;
    }
  }

  /** Partition sigma and execute the computations. */
  private Summation execute(String name, Summation sigma) {
    final Summation[] summations = sigma.partition(parameters.nJobs);
    final List<Computation> computations = new ArrayList<Computation>(); 
    for(int i = 0; i < summations.length; i++)
      computations.add(new Computation(i, name, summations[i]));
    try {
      Util.execute(parameters.nThreads, computations);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    final List<Summation> combined = Util.combine(Arrays.asList(summations));
    return combined.size() == 1? combined.get(0): null;
  }

  /** {@inheritDoc} */
  @Override
  public int run(String[] args) throws Exception {
    //parse arguments
    if (args.length != Parameters.COUNT + 2)
      return Util.printUsage(args, getClass().getName()
          + " <name> <sigma> " + Parameters.LIST
          + "\n  <name> The name."
          + "\n  <sigma> The summation."
          + Parameters.DESCRIPTION);

    int i = 0;
    final String name = args[i++];
    final Summation sigma = Summation.valueOf(args[i++]);
    setParameters(DistSum.Parameters.parse(args, i));

    Util.out.println();
    Util.out.println("name  = " + name);
    Util.out.println("sigma = " + sigma);
    Util.out.println(parameters);
    Util.out.println();

    //run jobs
    final Summation result = execute(name, sigma);
    if (result.equals(sigma)) {
      sigma.setValue(result.getValue());
      timer.tick("\n\nDONE\n\nsigma=" + sigma);
      return 0;
    } else {
      timer.tick("\n\nDONE WITH ERROR\n\nresult=" + result);
      return 1;
    }
  }

  /** main */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(null, new DistSum(), args));
  }
}
