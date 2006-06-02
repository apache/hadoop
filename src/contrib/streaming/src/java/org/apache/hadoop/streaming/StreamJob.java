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

package org.apache.hadoop.streaming;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;

import org.apache.hadoop.util.LogFormatter;

/** All the client-side work happens here. 
 * (Jar packaging, MapRed job submission and monitoring)
 * @author Michel Tourn
 */
public class StreamJob
{
  protected static final Logger LOG = LogFormatter.getLogger(StreamJob.class.getName());
    
  public StreamJob(String[] argv, boolean mayExit)
  {
    argv_ = argv;
    mayExit_ = mayExit;    
  }
  
  public void go() throws IOException
  {
    init();
    
    preProcessArgs();
    parseArgv();
    postProcessArgs();
    
    setJobConf();
    submitAndMonitorJob();
  }

  protected void init()
  {
     try {
        env_ = new Environment();
     } catch(IOException io) {
        throw new RuntimeException(io);
     }
  }
  
  void preProcessArgs()
  {
    verbose_ = false;
  }
  
  void postProcessArgs()
  {
    if(cluster_ == null) {
        // hadoop-default.xml is standard, hadoop-local.xml is not.
        cluster_ = "default";
    }
    hadoopAliasConf_ = "hadoop-" + getClusterNick() + ".xml";
    if(inputGlobs_.size() == 0) {
        fail("Required argument: -input <name>");
    }
    if(output_ == null) {
        fail("Required argument: -output ");
    }
    // careful with class names..
    mapCmd_ = packageOrTrimNoShip(mapCmd_);
    redCmd_ = packageOrTrimNoShip(redCmd_);
    
    // TBD -D format or sthg on cmdline. 
    // Plus maybe a standard list originating on client or server    
    addTaskEnvironment_ = ""; 
  }
  
  String packageOrTrimNoShip(String cmd)
  {
    if(cmd == null) {
      //    
    } else if(cmd.startsWith(NOSHIP)) {
      // don't package the file, but keep the abolute path
      cmd = cmd.substring(NOSHIP.length());
    } else {
      String prog = cmd;
      String args = "";
      int s = cmd.indexOf(" ");
      if(s != -1) {
        prog = cmd.substring(0, s);
        args = cmd.substring(s+1);
      }
      packageFiles_.add(new File(prog).getAbsolutePath());
      // Change path to simple filename. 
      // That way when PipeMapRed calls Runtime.exec(), 
      // it will look for the excutable in Task's working dir.
      // And this is where TaskRunner unjars our job jar.
      prog = new File(prog).getName();
      if(args.length() > 0) {
        cmd = prog + " " + args;
      } else {
        cmd = prog;
      }
    }
    return cmd;
  }
  
  String getHadoopAliasConfFile()
  {
    return new File(getHadoopClientHome() + "/conf", hadoopAliasConf_).getAbsolutePath();
  }
  
  void parseArgv()
  {
    if(argv_.length==0) {
      exitUsage();
    }
    int i=0; 
    while(i < argv_.length) {
      String s;
      if(argv_[i].equals("-verbose")) {
        verbose_ = true;      
      } else if(argv_[i].equals("-debug")) {
        debug_++;
      } else if((s = optionArg(argv_, i, "-input", false)) != null) {
        i++;
        inputGlobs_.add(s);
      } else if((s = optionArg(argv_, i, "-output", output_ != null)) != null) {
        i++;
        output_ = s;
      } else if((s = optionArg(argv_, i, "-mapper", mapCmd_ != null)) != null) {
        i++;
        mapCmd_ = s;
      } else if((s = optionArg(argv_, i, "-reducer", redCmd_ != null)) != null) {
        i++;
        redCmd_ = s;
      } else if((s = optionArg(argv_, i, "-files", false)) != null) {
        i++;
        packageFiles_.add(s);
      } else if((s = optionArg(argv_, i, "-cluster", cluster_ != null)) != null) {
        i++;
        cluster_ = s;
      } else if((s = optionArg(argv_, i, "-config", false)) != null) {
        i++;
        configPath_.add(s);
      } else if((s = optionArg(argv_, i, "-inputreader", inReaderSpec_ != null)) != null) {
        i++;
        inReaderSpec_ = s;
      } else {
        System.err.println("Unexpected argument: " + argv_[i]);
        exitUsage();
      }
      i++;
    }
  }
  
  String optionArg(String[] args, int index, String arg, boolean argSet)
  {
    if(index >= args.length || ! args[index].equals(arg)) {
      return null;
    }
    if(argSet) {
      throw new IllegalArgumentException("Can only have one " + arg + " option");
    }
    if(index >= args.length-1) {
      throw new IllegalArgumentException("Expected argument after option " + args[index]);
    }    
    return args[index+1];
  }
  
  protected void msg(String msg)
  {
    if(verbose_) {
      System.out.println("STREAM: " + msg);
    }
  }

  public void exitUsage()
  {
                      //         1         2         3         4         5         6         7         
                      //1234567890123456789012345678901234567890123456789012345678901234567890123456789
    System.out.println("Usage: bin/hadoop jar build/hadoop-streaming.jar [options]");
    System.out.println("Options:");
    System.out.println("  -input   <path>     DFS input file(s) for the Map step");
    System.out.println("  -output  <path>     DFS output directory for the Reduce step");
    System.out.println("  -mapper  <cmd>      The streaming command to run");
    System.out.println("  -reducer <cmd>      The streaming command to run");
    System.out.println("  -files   <file>     Additional files to be shipped in the Job jar file");
    System.out.println("  -cluster <name>     Default uses hadoop-default.xml and hadoop-site.xml");
    System.out.println("  -config  <file>     Optional. One or more paths to xml config files");
    System.out.println("  -inputreader <spec> Optional. See below");
    System.out.println("  -verbose");
    System.out.println();
    System.out.println("In -input: globbing on <path> is supported and can have multiple -input");
    System.out.println("Default Map input format: a line is a record in UTF-8");
    System.out.println("  the key part ends at first TAB, the rest of the line is the value");
    System.out.println("Custom Map input format: -inputreader package.MyRecordReader,n=v,n=v ");
    System.out.println("  comma-separated name-values can be specified to configure the InputFormat");
    System.out.println("  Ex: -inputreader 'StreamXmlRecordReader,begin=<doc>,end=</doc>'");
    System.out.println("Map output format, reduce input/output format:");
    System.out.println("  Format defined by what mapper command outputs. Line-oriented");
    System.out.println("Mapper and Reducer <cmd> syntax: ");
    System.out.println("  If the mapper or reducer programs are prefixed with " + NOSHIP + " then ");
    System.out.println("  the paths are assumed to be valid absolute paths on the task tracker machines");
    System.out.println("  and are NOT packaged with the Job jar file.");
    System.out.println("Use -cluster <name> to switch between \"local\" Hadoop and one or more remote ");
    System.out.println("  Hadoop clusters. ");
    System.out.println("  The default is to use the normal hadoop-default.xml and hadoop-site.xml");
    System.out.println("  Else configuration will use $HADOOP_HOME/conf/hadoop-<name>.xml");
    System.out.println();
    System.out.println("Example: hadoopStreaming -mapper \"noship:/usr/local/bin/perl5 filter.pl\"");
    System.out.println("           -files /local/filter.pl -input \"/logs/0604*/*\" [...]");
    System.out.println("  Ships a script, invokes the non-shipped perl interpreter");
    System.out.println("  Shipped files go to the working directory so filter.pl is found by perl");
    System.out.println("  Input files are all the daily logs for days in month 2006-04");
    fail("");    
  }
  
  public void fail(String message)
  {
    if(mayExit_) {
        System.err.println(message);
        System.exit(1);
    } else {
       throw new IllegalArgumentException(message);
    }
  }

  // --------------------------------------------
  
  
  protected String getHadoopClientHome()
  {
    String h = env_.getProperty("HADOOP_HOME"); // standard Hadoop
    if(h == null) {
      //fail("Missing required environment variable: HADOOP_HOME");
      h = "UNDEF";
    }
    return h;
  }


  protected boolean isLocalHadoop()
  {
    boolean local;
    if(jobConf_ == null) {
        local = getClusterNick().equals("local");
    } else {
        local = jobConf_.get("mapred.job.tracker", "").equals("local");
    }
    return local;
  }
  protected String getClusterNick() 
  { 
    return cluster_;
  }
  
  /** @return path to the created Jar file or null if no files are necessary.
  */
  protected String packageJobJar() throws IOException
  {
    ArrayList unjarFiles = new ArrayList();

    // Runtime code: ship same version of code as self (job submitter code)
    // usually found in: build/contrib or build/hadoop-<version>-dev-streaming.jar
    String runtimeClasses = StreamUtil.findInClasspath(StreamJob.class.getName());
    if(runtimeClasses == null) {
        throw new IOException("runtime classes not found: " + getClass().getPackage());
    } else {
        msg("Found runtime classes in: " + runtimeClasses);
    }
    if(isLocalHadoop()) {
      // don't package class files (they might get unpackaged in . and then 
      //  hide the intended CLASSPATH entry)
      // we still package everything else (so that scripts and executable are found in 
      //  Task workdir like distributed Hadoop)
    } else {
      if(new File(runtimeClasses).isDirectory()) {    
          packageFiles_.add(runtimeClasses);
      } else {
          unjarFiles.add(runtimeClasses);
      }
    }
    if(packageFiles_.size() + unjarFiles.size()==0) {
      return null;
    }
    File jobJar = File.createTempFile("streamjob", ".jar");
    System.out.println("packageJobJar: " + packageFiles_ + " " + unjarFiles + " " + jobJar);    
    if(debug_ == 0) {
      jobJar.deleteOnExit();
    }
    JarBuilder builder = new JarBuilder();
    if(verbose_) {
      builder.setVerbose(true);
    }
    String jobJarName = jobJar.getAbsolutePath();
    builder.merge(packageFiles_, unjarFiles, jobJarName);
    return jobJarName;
  }
  
  protected void setJobConf() throws IOException
  {
    msg("hadoopAliasConf_ = " + hadoopAliasConf_);
    config_ = new Configuration();
    if(!cluster_.equals("default")) {
        config_.addFinalResource(new Path(getHadoopAliasConfFile()));
    } else {
      // use only defaults: hadoop-default.xml and hadoop-site.xml
    }
    Iterator it = configPath_.iterator();
    while(it.hasNext()) {
        String pathName = (String)it.next();
        config_.addFinalResource(new Path(pathName));
    }   
    // general MapRed job properties
    jobConf_ = new JobConf(config_);
    for(int i=0; i<inputGlobs_.size(); i++) {
      jobConf_.addInputDir(new File((String)inputGlobs_.get(i)));
    }
    
    jobConf_.setInputFormat(StreamInputFormat.class);
    jobConf_.setInputKeyClass(UTF8.class);
    jobConf_.setInputValueClass(UTF8.class);
    jobConf_.setOutputKeyClass(UTF8.class);
    jobConf_.setOutputValueClass(UTF8.class);
    //jobConf_.setCombinerClass();

    jobConf_.setOutputDir(new File(output_));
    jobConf_.setOutputFormat(StreamOutputFormat.class);
    
    jobConf_.set("stream.addenvironment", addTaskEnvironment_);
    
    String defaultPackage = this.getClass().getPackage().getName();
    
    Class c = StreamUtil.goodClassOrNull(mapCmd_, defaultPackage);
    if(c != null) {
      jobConf_.setMapperClass(c);
    } else {
      jobConf_.setMapperClass(PipeMapper.class);
      jobConf_.set("stream.map.streamprocessor", mapCmd_);
    }

    if(redCmd_ != null) {
      c = StreamUtil.goodClassOrNull(redCmd_, defaultPackage);
      if(c != null) {
        jobConf_.setReducerClass(c);
      } else {
        jobConf_.setReducerClass(PipeReducer.class);
        jobConf_.set("stream.reduce.streamprocessor", redCmd_);
      }
    }
    
    if(inReaderSpec_ != null) {
        String[] args = inReaderSpec_.split(",");
        String readerClass = args[0];
        // this argument can only be a Java class
        c = StreamUtil.goodClassOrNull(readerClass, defaultPackage);
        if(c != null) {            
            jobConf_.set("stream.recordreader.class", c.getName());
        } else {
            fail("-inputreader: class not found: " + readerClass);
        }
        for(int i=1; i<args.length; i++) {
            String[] nv = args[i].split("=", 2);
            String k = "stream.recordreader." + nv[0];
            String v = (nv.length>1) ? nv[1] : "";
            jobConf_.set(k, v);
        }
    }
    
    jar_ = packageJobJar();
    if(jar_ != null) {
        jobConf_.setJar(jar_);
    }
    //jobConf_.mtdump();System.exit(1);
  }
  
  protected String getJobTrackerHostPort()
  {
    return jobConf_.get("mapred.job.tracker");
  }
  
  protected void jobInfo()
  {    
    if(isLocalHadoop()) {
      LOG.info("Job running in-process (local Hadoop)"); 
    } else {
      String hp = getJobTrackerHostPort();
      LOG.info("To kill this job, run:"); 
      LOG.info(getHadoopClientHome() + "/bin/hadoop job  -Dmapred.job.tracker=" + hp + " -kill " + jobId_);
      //LOG.info("Job file: " + running_.getJobFile() );
      LOG.info("Tracking URL: "  + StreamUtil.qualifyHost(running_.getTrackingURL()));
    }
  }
  
  // Based on JobClient
  public void submitAndMonitorJob() throws IOException {
    
    if(jar_ != null && isLocalHadoop()) {
        // getAbs became required when shell and subvm have different working dirs...
        File wd = new File(".").getAbsoluteFile();
        StreamUtil.unJar(new File(jar_), wd);
    }
    
    // if jobConf_ changes must recreate a JobClient 
    jc_ = new JobClient(jobConf_); 
    boolean error = true;
    running_ = null;
    String lastReport = null;
    try {
      running_ = jc_.submitJob(jobConf_);
      jobId_ = running_.getJobID();

      LOG.info("getLocalDirs(): " + Arrays.toString(jobConf_.getLocalDirs()));     
      LOG.info("Running job: " + jobId_);      
      jobInfo();

      while (!running_.isComplete()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {}
        running_ = jc_.getJob(jobId_);
        String report = null;
        report = " map "+Math.round(running_.mapProgress()*100)
        +"%  reduce " + Math.round(running_.reduceProgress()*100)+"%";

        if (!report.equals(lastReport)) {
          LOG.info(report);
          lastReport = report;
        }
      }
      if (!running_.isSuccessful()) {
        jobInfo();
        throw new IOException("Job not Successful!");
      }
      LOG.info("Job complete: " + jobId_);
      LOG.info("Output: " + output_);
      error = false;
    } finally {    
      if (error && (running_ != null)) {
        LOG.info("killJob...");
        running_.killJob();
      }
      jc_.close();
    }
  }
  

  public final static String NOSHIP = "noship:";
  
  protected boolean mayExit_;
  protected String[] argv_;
  protected boolean verbose_;
  protected int debug_;

  protected Environment env_;
  
  protected String jar_;
  protected boolean localHadoop_;
  protected Configuration config_;
  protected JobConf jobConf_;
  protected JobClient jc_;

  // command-line arguments
  protected ArrayList inputGlobs_   = new ArrayList(); // <String>
  protected ArrayList packageFiles_ = new ArrayList(); // <String>
  protected String output_;
  protected String mapCmd_;
  protected String redCmd_;
  protected String cluster_;
  protected ArrayList configPath_ = new ArrayList(); // <String>
  protected String hadoopAliasConf_;
  protected String inReaderSpec_;
  

  // Use to communicate config to the external processes (ex env.var.HADOOP_USER)
  // encoding "a=b c=d"
  protected String addTaskEnvironment_;
  
  protected boolean outputSingleNode_;
  protected long minRecWrittenToEnableSkip_;
  
  protected RunningJob running_;
  protected String jobId_;
  
  
}

