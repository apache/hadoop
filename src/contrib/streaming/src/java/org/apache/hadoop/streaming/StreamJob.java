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

package org.apache.hadoop.streaming;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.cli2.Argument;
import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.OptionException;
import org.apache.commons.cli2.WriteableCommandLine;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.commons.cli2.option.PropertyOption;
import org.apache.commons.cli2.resource.ResourceConstants;
import org.apache.commons.cli2.util.HelpFormatter;
import org.apache.commons.cli2.validation.InvalidArgumentException;
import org.apache.commons.cli2.validation.Validator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.aggregate.ValueAggregatorCombiner;
import org.apache.hadoop.mapred.lib.aggregate.ValueAggregatorReducer;
import org.apache.hadoop.util.StringUtils;

/** All the client-side work happens here.
 * (Jar packaging, MapRed job submission and monitoring)
 */
public class StreamJob {

  protected static final Log LOG = LogFactory.getLog(StreamJob.class.getName());
  final static String REDUCE_NONE = "NONE";
    
  /** -----------Streaming CLI Implementation  **/
  private DefaultOptionBuilder builder = 
    new DefaultOptionBuilder("-","-", false);
  private ArgumentBuilder argBuilder = new ArgumentBuilder(); 
  private Parser parser = new Parser(); 
  private Group allOptions; 
  HelpFormatter helpFormatter = new HelpFormatter("  ", "  ", "  ", 900);
  // need these two at class level to extract values later from 
  // commons-cli command line
  private MultiPropertyOption jobconf = new MultiPropertyOption(
                                                                "-jobconf", "(n=v) Optional. Add or override a JobConf property.", 'D'); 
  private MultiPropertyOption cmdenv = new MultiPropertyOption(
                                                               "-cmdenv", "(n=v) Pass env.var to streaming commands.", 'E');  
  
  public StreamJob(String[] argv, boolean mayExit) {
    setupOptions();
    argv_ = argv;
    mayExit_ = mayExit;
  }
  
  /**
   * This is the method that actually 
   * intializes the job conf and submits the job
   * to the jobtracker
   * @throws IOException
   */
  public int go() throws IOException {
    init();

    preProcessArgs();
    parseArgv();
    postProcessArgs();

    setJobConf();
    return submitAndMonitorJob();
  }
  
  protected void init() {
    try {
      env_ = new Environment();
    } catch (IOException io) {
      throw new RuntimeException(io);
    }
  }

  void preProcessArgs() {
    verbose_ = false;
    addTaskEnvironment_ = "";
  }

  void postProcessArgs() throws IOException {
    if (cluster_ == null) {
      // hadoop-default.xml is standard, hadoop-local.xml is not.
      cluster_ = "default";
    }
    hadoopAliasConf_ = "hadoop-" + getClusterNick() + ".xml";
    if (inputSpecs_.size() == 0) {
      fail("Required argument: -input <name>");
    }
    if (output_ == null) {
      fail("Required argument: -output ");
    }
    msg("addTaskEnvironment=" + addTaskEnvironment_);

    Iterator it = packageFiles_.iterator();
    while (it.hasNext()) {
      File f = new File((String) it.next());
      if (f.isFile()) {
        shippedCanonFiles_.add(f.getCanonicalPath());
      }
    }
    msg("shippedCanonFiles_=" + shippedCanonFiles_);

    // careful with class names..
    mapCmd_ = unqualifyIfLocalPath(mapCmd_);
    comCmd_ = unqualifyIfLocalPath(comCmd_);
    redCmd_ = unqualifyIfLocalPath(redCmd_);
  }

  String unqualifyIfLocalPath(String cmd) throws IOException {
    if (cmd == null) {
      //
    } else {
      String prog = cmd;
      String args = "";
      int s = cmd.indexOf(" ");
      if (s != -1) {
        prog = cmd.substring(0, s);
        args = cmd.substring(s + 1);
      }
      String progCanon;
      try {
        progCanon = new File(prog).getCanonicalPath();
      } catch (IOException io) {
        progCanon = prog;
      }
      boolean shipped = shippedCanonFiles_.contains(progCanon);
      msg("shipped: " + shipped + " " + progCanon);
      if (shipped) {
        // Change path to simple filename.
        // That way when PipeMapRed calls Runtime.exec(),
        // it will look for the excutable in Task's working dir.
        // And this is where TaskRunner unjars our job jar.
        prog = new File(prog).getName();
        if (args.length() > 0) {
          cmd = prog + " " + args;
        } else {
          cmd = prog;
        }
      }
    }
    msg("cmd=" + cmd);
    return cmd;
  }

  String getHadoopAliasConfFile() {
    return new File(getHadoopClientHome() + "/conf", hadoopAliasConf_).getAbsolutePath();
  }

  void parseArgv(){
    CommandLine cmdLine = null; 
    try{
      cmdLine = parser.parse(argv_);
    }catch(Exception oe){
      LOG.error(oe.getMessage());
      if (detailedUsage_) {
        exitUsage(true);
      } else {
        exitUsage(false);
      }
    }
    
    if (cmdLine != null){
      verbose_ =  cmdLine.hasOption("-verbose");
      detailedUsage_ = cmdLine.hasOption("-info");
      debug_ = cmdLine.hasOption("-debug")? debug_ + 1 : debug_;
      
      inputSpecs_.addAll(cmdLine.getValues("-input"));
      output_ = (String) cmdLine.getValue("-output"); 
      
      mapCmd_ = (String)cmdLine.getValue("-mapper"); 
      comCmd_ = (String)cmdLine.getValue("-combiner"); 
      redCmd_ = (String)cmdLine.getValue("-reducer"); 
      
      packageFiles_.addAll(cmdLine.getValues("-file"));
      
      cluster_ = (String)cmdLine.getValue("-cluster");
      
      configPath_.addAll(cmdLine.getValues("-config"));
      
      String fsName = (String)cmdLine.getValue("-dfs");
      if (null != fsName){
        userJobConfProps_.put("fs.default.name", fsName);        
      }
      
      String jt = (String)cmdLine.getValue("mapred.job.tracker");
      if (null != jt){
        userJobConfProps_.put("fs.default.name", jt);        
      }
      
      additionalConfSpec_ = (String)cmdLine.getValue("-additionalconfspec"); 
      inputFormatSpec_ = (String)cmdLine.getValue("-inputformat"); 
      outputFormatSpec_ = (String)cmdLine.getValue("-outputformat");
      numReduceTasksSpec_ = (String)cmdLine.getValue("-numReduceTasks"); 
      partitionerSpec_ = (String)cmdLine.getValue("-partitioner");
      inReaderSpec_ = (String)cmdLine.getValue("-inputreader"); 
      mapDebugSpec_ = (String)cmdLine.getValue("-mapdebug");    
      reduceDebugSpec_ = (String)cmdLine.getValue("-reducedebug");
      
      List<String> car = cmdLine.getValues("-cacheArchive"); 
      if (null != car){
        for(String s : car){
          cacheArchives = (cacheArchives == null)?s :cacheArchives + "," + s;  
        }
      }

      List<String> caf = cmdLine.getValues("-cacheFile"); 
      if (null != caf){
        for(String s : caf){
          cacheFiles = (cacheFiles == null)?s :cacheFiles + "," + s;  
        }
      }
      
      List<String> jobConfArgs = (List<String>)cmdLine.getValue(jobconf); 
      List<String> envArgs = (List<String>)cmdLine.getValue(cmdenv); 
      
      if (null != jobConfArgs){
        for(String s : jobConfArgs){
          String []parts = s.split("=", 2); 
          userJobConfProps_.put(parts[0], parts[1]);
        }
      }
      if (null != envArgs){
        for(String s : envArgs){
          if (addTaskEnvironment_.length() > 0) {
            addTaskEnvironment_ += " ";
          }
          addTaskEnvironment_ += s;
        }
      }
    }else if (detailedUsage_) {
      exitUsage(true);
    }
  }

  protected void msg(String msg) {
    if (verbose_) {
      System.out.println("STREAM: " + msg);
    }
  }
  
  private Option createOption(String name, String desc, 
                              String argName, int max, boolean required){
    Argument argument = argBuilder.
      withName(argName).
      withMinimum(1).
      withMaximum(max).
      create();
    return builder.
      withLongName(name).
      withArgument(argument).
      withDescription(desc).
      withRequired(required).
      create();
  }
  
  private Option createOption(String name, String desc, 
                              String argName, int max, boolean required, Validator validator){
    
    Argument argument = argBuilder.
      withName(argName).
      withMinimum(1).
      withMaximum(max).
      withValidator(validator).
      create();
   
    return builder.
      withLongName(name).
      withArgument(argument).
      withDescription(desc).
      withRequired(required).
      create();
  }  
  
  private Option createBoolOption(String name, String desc){
    return builder.withLongName(name).withDescription(desc).create();
  }
  
  private void setupOptions(){

    final Validator fileValidator = new Validator(){
        public void validate(final List values) throws InvalidArgumentException {
          // Note : This code doesnt belong here, it should be changed to 
          // an can exec check in java 6
          for (String file : (List<String>)values) {
            File f = new File(file);  
            if (!f.exists()) {
              throw new InvalidArgumentException("Argument : " + 
                                                 f.getAbsolutePath() + " doesn't exist."); 
            }
            if (!f.isFile()) {
              throw new InvalidArgumentException("Argument : " + 
                                                 f.getAbsolutePath() + " is not a file."); 
            }
            if (!f.canRead()) {
              throw new InvalidArgumentException("Argument : " + 
                                                 f.getAbsolutePath() + " is not accessible"); 
            }
          }
        }      
      }; 

    // Note: not extending CLI2's FileValidator, that overwrites 
    // the String arg into File and causes ClassCastException 
    // in inheritance tree. 
    final Validator execValidator = new Validator(){
        public void validate(final List values) throws InvalidArgumentException {
          // Note : This code doesnt belong here, it should be changed to 
          // an can exec check in java 6
          for (String file : (List<String>)values) {
            try{
              Runtime.getRuntime().exec("chmod 0777 " + (new File(file)).getAbsolutePath());
            }catch(IOException ioe){
              // ignore 
            }
          }
          fileValidator.validate(values);
        }      
      }; 

    Option input   = createOption("input", 
                                  "DFS input file(s) for the Map step", 
                                  "path", 
                                  Integer.MAX_VALUE, 
                                  true);  
    
    Option output  = createOption("output", 
                                  "DFS output directory for the Reduce step", 
                                  "path", 1, true); 
    Option mapper  = createOption("mapper", 
                                  "The streaming command to run", "cmd", 1, false);
    Option combiner = createOption("combiner", 
                                   "The streaming command to run", "cmd", 1, false);
    // reducer could be NONE 
    Option reducer = createOption("reducer", 
                                  "The streaming command to run", "cmd", 1, false); 
    Option file = createOption("file", 
                               "File/dir to be shipped in the Job jar file", 
                               "file", Integer.MAX_VALUE, false, execValidator); 
    Option dfs = createOption("dfs", 
                              "Optional. Override DFS configuration", "<h:p>|local", 1, false); 
    Option jt = createOption("jt", 
                             "Optional. Override JobTracker configuration", "<h:p>|local", 1, false);
    Option additionalconfspec = createOption("additionalconfspec", 
                                             "Optional.", "spec", 1, false);
    Option inputformat = createOption("inputformat", 
                                      "Optional.", "spec", 1, false);
    Option outputformat = createOption("outputformat", 
                                       "Optional.", "spec", 1, false);
    Option partitioner = createOption("partitioner", 
                                      "Optional.", "spec", 1, false);
    Option numReduceTasks = createOption("numReduceTasks", 
        "Optional.", "spec",1, false );
    Option inputreader = createOption("inputreader", 
                                      "Optional.", "spec", 1, false);
    Option mapDebug = createOption("mapdebug",
                                   "Optional.", "spec", 1, false);
    Option reduceDebug = createOption("reducedebug",
                                      "Optional", "spec",1, false);
    Option cacheFile = createOption("cacheFile", 
                                    "File name URI", "fileNameURI", Integer.MAX_VALUE, false);
    Option cacheArchive = createOption("cacheArchive", 
                                       "File name URI", "fileNameURI", Integer.MAX_VALUE, false);
    
    // boolean properties
    
    Option verbose = createBoolOption("verbose", "print verbose output"); 
    Option info = createBoolOption("info", "print verbose output"); 
    Option help = createBoolOption("help", "print this help message"); 
    Option debug = createBoolOption("debug", "print debug output"); 
    Option inputtagged = createBoolOption("inputtagged", "inputtagged"); 
    
    allOptions = new GroupBuilder().
      withOption(input).
      withOption(output).
      withOption(mapper).
      withOption(combiner).
      withOption(reducer).
      withOption(file).
      withOption(dfs).
      withOption(jt).
      withOption(additionalconfspec).
      withOption(inputformat).
      withOption(outputformat).
      withOption(partitioner).
      withOption(numReduceTasks).
      withOption(inputreader).
      withOption(mapDebug).
      withOption(reduceDebug).
      withOption(jobconf).
      withOption(cmdenv).
      withOption(cacheFile).
      withOption(cacheArchive).
      withOption(verbose).
      withOption(info).
      withOption(debug).
      withOption(inputtagged).
      withOption(help).
      create();
    parser.setGroup(allOptions);
    
  }

  public void exitUsage(boolean detailed) {
    //         1         2         3         4         5         6         7
    //1234567890123456789012345678901234567890123456789012345678901234567890123456789
    if (!detailed) {
      System.out.println("Usage: $HADOOP_HOME/bin/hadoop [--config dir] jar \\");
      System.out.println("          $HADOOP_HOME/hadoop-streaming.jar [options]");
      System.out.println("Options:");
      System.out.println("  -input    <path>     DFS input file(s) for the Map step");
      System.out.println("  -output   <path>     DFS output directory for the Reduce step");
      System.out.println("  -mapper   <cmd|JavaClassName>      The streaming command to run");
      System.out.println("  -combiner <JavaClassName> Combiner has to be a Java class");
      System.out.println("  -reducer  <cmd|JavaClassName>      The streaming command to run");
      System.out.println("  -file     <file>     File/dir to be shipped in the Job jar file");
      System.out.println("  -dfs    <h:p>|local  Optional. Override DFS configuration");
      System.out.println("  -jt     <h:p>|local  Optional. Override JobTracker configuration");
      System.out.println("  -additionalconfspec specfile  Optional.");
      System.out.println("  -inputformat TextInputFormat(default)|SequenceFileAsTextInputFormat|JavaClassName Optional.");
      System.out.println("  -outputformat TextOutputFormat(default)|JavaClassName  Optional.");
      System.out.println("  -partitioner JavaClassName  Optional.");
      System.out.println("  -numReduceTasks <num>  Optional.");
      System.out.println("  -inputreader <spec>  Optional.");
      System.out.println("  -jobconf  <n>=<v>    Optional. Add or override a JobConf property");
      System.out.println("  -cmdenv   <n>=<v>    Optional. Pass env.var to streaming commands");
      System.out.println("  -mapdebug <path>  Optional. " +
                                "To run this script when a map task fails ");
      System.out.println("  -reducedebug <path>  Optional." +
                             " To run this script when a reduce task fails ");
      System.out.println("  -cacheFile fileNameURI");
      System.out.println("  -cacheArchive fileNameURI");
      System.out.println("  -verbose");
      System.out.println();      
      System.out.println("For more details about these options:");
      System.out.println("Use $HADOOP_HOME/bin/hadoop jar build/hadoop-streaming.jar -info");
      fail("");
    }
    System.out.println("In -input: globbing on <path> is supported and can have multiple -input");
    System.out.println("Default Map input format: a line is a record in UTF-8");
    System.out.println("  the key part ends at first TAB, the rest of the line is the value");
    System.out.println("Custom input format: -inputformat package.MyInputFormat ");
    System.out.println("Map output format, reduce input/output format:");
    System.out.println("  Format defined by what the mapper command outputs. Line-oriented");
    System.out.println();
    System.out.println("The files or directories named in the -file argument[s] end up in the");
    System.out.println("  working directory when the mapper and reducer are run.");
    System.out.println("  The location of this working directory is unspecified.");
    System.out.println();
    System.out.println("To set the number of reduce tasks (num. of output files):");
    System.out.println("  -jobconf mapred.reduce.tasks=10");
    System.out.println("To skip the sort/combine/shuffle/sort/reduce step:");
    System.out.println("  Use -numReduceTasks 0");
    System.out
      .println("  A Task's Map output then becomes a 'side-effect output' rather than a reduce input");
    System.out
      .println("  This speeds up processing, This also feels more like \"in-place\" processing");
    System.out.println("  because the input filename and the map input order are preserved");
    System.out.println("  This equivalent -reducer NONE");
    System.out.println();
    System.out.println("To speed up the last maps:");
    System.out.println("  -jobconf mapred.map.tasks.speculative.execution=true");
    System.out.println("To speed up the last reduces:");
    System.out.println("  -jobconf mapred.reduce.tasks.speculative.execution=true");
    System.out.println("To name the job (appears in the JobTracker Web UI):");
    System.out.println("  -jobconf mapred.job.name='My Job' ");
    System.out.println("To change the local temp directory:");
    System.out.println("  -jobconf dfs.data.dir=/tmp/dfs");
    System.out.println("  -jobconf stream.tmpdir=/tmp/streaming");
    System.out.println("Additional local temp directories with -cluster local:");
    System.out.println("  -jobconf mapred.local.dir=/tmp/local");
    System.out.println("  -jobconf mapred.system.dir=/tmp/system");
    System.out.println("  -jobconf mapred.temp.dir=/tmp/temp");
    System.out.println("To treat tasks with non-zero exit status as SUCCEDED:");    
    System.out.println("  -jobconf stream.non.zero.exit.is.failure=false");
    System.out.println("Use a custom hadoopStreaming build along a standard hadoop install:");
    System.out.println("  $HADOOP_HOME/bin/hadoop jar /path/my-hadoop-streaming.jar [...]\\");
    System.out
      .println("    [...] -jobconf stream.shipped.hadoopstreaming=/path/my-hadoop-streaming.jar");
    System.out.println("For more details about jobconf parameters see:");
    System.out.println("  http://wiki.apache.org/hadoop/JobConfFile");
    System.out.println("To set an environement variable in a streaming command:");
    System.out.println("   -cmdenv EXAMPLE_DIR=/home/example/dictionaries/");
    System.out.println();
    System.out.println("Shortcut:");
    System.out
      .println("   setenv HSTREAMING \"$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/hadoop-streaming.jar\"");
    System.out.println();
    System.out.println("Example: $HSTREAMING -mapper \"/usr/local/bin/perl5 filter.pl\"");
    System.out.println("           -file /local/filter.pl -input \"/logs/0604*/*\" [...]");
    System.out.println("  Ships a script, invokes the non-shipped perl interpreter");
    System.out.println("  Shipped files go to the working directory so filter.pl is found by perl");
    System.out.println("  Input files are all the daily logs for days in month 2006-04");
    fail("");
  }

  public void fail(String message) {
    if (mayExit_) {
      System.err.println(message);
      throw new RuntimeException(message);
    } else {
      throw new IllegalArgumentException(message);
    }
  }

  // --------------------------------------------

  protected String getHadoopClientHome() {
    String h = env_.getProperty("HADOOP_HOME"); // standard Hadoop
    if (h == null) {
      //fail("Missing required environment variable: HADOOP_HOME");
      h = "UNDEF";
    }
    return h;
  }

  protected boolean isLocalHadoop() {
    boolean local;
    if (jobConf_ == null) {
      local = getClusterNick().equals("local");
    } else {
      local = StreamUtil.isLocalJobTracker(jobConf_);
    }
    return local;
  }

  protected String getClusterNick() {
    return cluster_;
  }

  /** @return path to the created Jar file or null if no files are necessary.
   */
  protected String packageJobJar() throws IOException {
    ArrayList unjarFiles = new ArrayList();

    // Runtime code: ship same version of code as self (job submitter code)
    // usually found in: build/contrib or build/hadoop-<version>-dev-streaming.jar

    // First try an explicit spec: it's too hard to find our own location in this case:
    // $HADOOP_HOME/bin/hadoop jar /not/first/on/classpath/custom-hadoop-streaming.jar
    // where findInClasspath() would find the version of hadoop-streaming.jar in $HADOOP_HOME
    String runtimeClasses = userJobConfProps_.get("stream.shipped.hadoopstreaming"); // jar or class dir
    System.out.println(runtimeClasses + "=@@@userJobConfProps_.get(stream.shipped.hadoopstreaming");
    
    if (runtimeClasses == null) {
      runtimeClasses = StreamUtil.findInClasspath(StreamJob.class.getName());
    }
    if (runtimeClasses == null) {
      throw new IOException("runtime classes not found: " + getClass().getPackage());
    } else {
      msg("Found runtime classes in: " + runtimeClasses);
    }
    if (isLocalHadoop()) {
      // don't package class files (they might get unpackaged in "." and then
      //  hide the intended CLASSPATH entry)
      // we still package everything else (so that scripts and executable are found in
      //  Task workdir like distributed Hadoop)
    } else {
      if (new File(runtimeClasses).isDirectory()) {
        packageFiles_.add(runtimeClasses);
      } else {
        unjarFiles.add(runtimeClasses);
      }
    }
    if (packageFiles_.size() + unjarFiles.size() == 0) {
      return null;
    }
    String tmp = jobConf_.get("stream.tmpdir"); //, "/tmp/${user.name}/"
    File tmpDir = (tmp == null) ? null : new File(tmp);
    // tmpDir=null means OS default tmp dir
    File jobJar = File.createTempFile("streamjob", ".jar", tmpDir);
    System.out.println("packageJobJar: " + packageFiles_ + " " + unjarFiles + " " + jobJar
                       + " tmpDir=" + tmpDir);
    if (debug_ == 0) {
      jobJar.deleteOnExit();
    }
    JarBuilder builder = new JarBuilder();
    if (verbose_) {
      builder.setVerbose(true);
    }
    String jobJarName = jobJar.getAbsolutePath();
    builder.merge(packageFiles_, unjarFiles, jobJarName);
    return jobJarName;
  }

  /**
   * This method sets the user jobconf variable specified
   * by user using -jobconf key=value
   * @param doEarlyProps
   */
  protected void setUserJobConfProps(boolean doEarlyProps) {
    Iterator it = userJobConfProps_.keySet().iterator();
    while (it.hasNext()) {
      String key = (String) it.next();
      String val = userJobConfProps_.get(key);
      boolean earlyName = key.equals("fs.default.name");
      earlyName |= key.equals("stream.shipped.hadoopstreaming");
      if (doEarlyProps == earlyName) {
        msg("xxxJobConf: set(" + key + ", " + val + ") early=" + doEarlyProps);
        jobConf_.set(key, val);
      }
    }
  }
  
  /**
   * get the uris of all the files/caches
   */
  protected void getURIs(String lcacheArchives, String lcacheFiles) {
    String archives[] = StringUtils.getStrings(lcacheArchives);
    String files[] = StringUtils.getStrings(lcacheFiles);
    fileURIs = StringUtils.stringToURI(files);
    archiveURIs = StringUtils.stringToURI(archives);
  }
  
  protected void setJobConf() throws IOException {
    msg("hadoopAliasConf_ = " + hadoopAliasConf_);
    config_ = new Configuration();
    if (!cluster_.equals("default")) {
      config_.addResource(new Path(getHadoopAliasConfFile()));
    } else {
      // use only defaults: hadoop-default.xml and hadoop-site.xml
    }
    System.out.println("additionalConfSpec_:" + additionalConfSpec_);
    if (additionalConfSpec_ != null) {
      config_.addResource(new Path(additionalConfSpec_));
    }
    Iterator it = configPath_.iterator();
    while (it.hasNext()) {
      String pathName = (String) it.next();
      config_.addResource(new Path(pathName));
    }

    // general MapRed job properties
    jobConf_ = new JobConf(config_);
    
    // All streaming jobs get the task timeout value
    // from the configuration settings.

    setUserJobConfProps(true);

    // The correct FS must be set before this is called!
    // (to resolve local vs. dfs drive letter differences) 
    // (mapred.working.dir will be lazily initialized ONCE and depends on FS)
    for (int i = 0; i < inputSpecs_.size(); i++) {
      FileInputFormat.addInputPaths(jobConf_, 
                        (String) inputSpecs_.get(i));
    }
    jobConf_.set("stream.numinputspecs", "" + inputSpecs_.size());

    String defaultPackage = this.getClass().getPackage().getName();
    Class c;
    Class fmt = null;
    if (inReaderSpec_ == null && inputFormatSpec_ == null) {
      fmt = TextInputFormat.class;
    } else if (inputFormatSpec_ != null) {
      if (inputFormatSpec_.equals(TextInputFormat.class.getName())
          || inputFormatSpec_.equals(TextInputFormat.class.getCanonicalName())
          || inputFormatSpec_.equals(TextInputFormat.class.getSimpleName())) {
        fmt = TextInputFormat.class;
      } else if (inputFormatSpec_.equals(KeyValueTextInputFormat.class
          .getName())
          || inputFormatSpec_.equals(KeyValueTextInputFormat.class
              .getCanonicalName())
          || inputFormatSpec_.equals(KeyValueTextInputFormat.class.getSimpleName())) {
      } else if (inputFormatSpec_.equals(SequenceFileInputFormat.class
          .getName())
          || inputFormatSpec_
              .equals(org.apache.hadoop.mapred.SequenceFileInputFormat.class
                  .getCanonicalName())
          || inputFormatSpec_
              .equals(org.apache.hadoop.mapred.SequenceFileInputFormat.class.getSimpleName())) {
      } else if (inputFormatSpec_.equals(SequenceFileAsTextInputFormat.class
          .getName())
          || inputFormatSpec_.equals(SequenceFileAsTextInputFormat.class
              .getCanonicalName())
          || inputFormatSpec_.equals(SequenceFileAsTextInputFormat.class.getSimpleName())) {
        fmt = SequenceFileAsTextInputFormat.class;
      } else {
        c = StreamUtil.goodClassOrNull(inputFormatSpec_, defaultPackage);
        if (c != null) {
          fmt = c;
        } else {
          fail("-inputformat : class not found : " + inputFormatSpec_);
        }
      }
    } 
    if (fmt == null) {
      fmt = StreamInputFormat.class;
    }

    jobConf_.setInputFormat(fmt);

    jobConf_.setOutputKeyClass(Text.class);
    jobConf_.setOutputValueClass(Text.class);

    jobConf_.set("stream.addenvironment", addTaskEnvironment_);

    if (mapCmd_ != null) {
      c = StreamUtil.goodClassOrNull(mapCmd_, defaultPackage);
      if (c != null) {
        jobConf_.setMapperClass(c);
      } else {
        jobConf_.setMapperClass(PipeMapper.class);
        jobConf_.setMapRunnerClass(PipeMapRunner.class);
        jobConf_.set("stream.map.streamprocessor", 
                     URLEncoder.encode(mapCmd_, "UTF-8"));
      }
    }

    if (comCmd_ != null) {
      c = StreamUtil.goodClassOrNull(comCmd_, defaultPackage);
      if (c != null) {
        jobConf_.setCombinerClass(c);
      } else {
        fail("-combiner : class not found : " + comCmd_);
      }
    }

    boolean reducerNone_ = false;
    if (redCmd_ != null) {
      reducerNone_ = redCmd_.equals(REDUCE_NONE);
      if (redCmd_.compareToIgnoreCase("aggregate") == 0) {
        jobConf_.setReducerClass(ValueAggregatorReducer.class);
        jobConf_.setCombinerClass(ValueAggregatorCombiner.class);
      } else {

        c = StreamUtil.goodClassOrNull(redCmd_, defaultPackage);
        if (c != null) {
          jobConf_.setReducerClass(c);
        } else {
          jobConf_.setReducerClass(PipeReducer.class);
          jobConf_.set("stream.reduce.streamprocessor", URLEncoder.encode(
              redCmd_, "UTF-8"));
        }
      }
    }

    if (inReaderSpec_ != null) {
      String[] args = inReaderSpec_.split(",");
      String readerClass = args[0];
      // this argument can only be a Java class
      c = StreamUtil.goodClassOrNull(readerClass, defaultPackage);
      if (c != null) {
        jobConf_.set("stream.recordreader.class", c.getName());
      } else {
        fail("-inputreader: class not found: " + readerClass);
      }
      for (int i = 1; i < args.length; i++) {
        String[] nv = args[i].split("=", 2);
        String k = "stream.recordreader." + nv[0];
        String v = (nv.length > 1) ? nv[1] : "";
        jobConf_.set(k, v);
      }
    }
    
    setUserJobConfProps(false);
    FileOutputFormat.setOutputPath(jobConf_, new Path(output_));
    fmt = null;
    if (outputFormatSpec_!= null) {
      c = StreamUtil.goodClassOrNull(outputFormatSpec_, defaultPackage);
      if (c != null) {
        fmt = c;
      } else {
        fail("-outputformat : class not found : " + outputFormatSpec_);
      }
    }
    if (fmt == null) {
      fmt = TextOutputFormat.class;
    }
    jobConf_.setOutputFormat(fmt);

    if (partitionerSpec_!= null) {
      c = StreamUtil.goodClassOrNull(partitionerSpec_, defaultPackage);
      if (c != null) {
        jobConf_.setPartitionerClass(c);
      } else {
        fail("-partitioner : class not found : " + partitionerSpec_);
      }
    }
    
    if (numReduceTasksSpec_!= null) {
      int numReduceTasks = Integer.parseInt(numReduceTasksSpec_);
      jobConf_.setNumReduceTasks(numReduceTasks);
    }
    if (reducerNone_) {
      jobConf_.setNumReduceTasks(0);
    }
    
    if(mapDebugSpec_ != null){
    	jobConf_.setMapDebugScript(mapDebugSpec_);
    }
    if(reduceDebugSpec_ != null){
    	jobConf_.setReduceDebugScript(reduceDebugSpec_);
    }
    // last, allow user to override anything
    // (although typically used with properties we didn't touch)

    jar_ = packageJobJar();
    if (jar_ != null) {
      jobConf_.setJar(jar_);
    }
    
    if ((cacheArchives != null) || (cacheFiles != null)){
      getURIs(cacheArchives, cacheFiles);
      boolean b = DistributedCache.checkURIs(fileURIs, archiveURIs);
      if (!b)
        fail(LINK_URI);
    }
    DistributedCache.createSymlink(jobConf_);
    // set the jobconf for the caching parameters
    if (cacheArchives != null)
      DistributedCache.setCacheArchives(archiveURIs, jobConf_);
    if (cacheFiles != null)
      DistributedCache.setCacheFiles(fileURIs, jobConf_);
    
    if (verbose_) {
      listJobConfProperties();
    }
   
    msg("submitting to jobconf: " + getJobTrackerHostPort());
  }

  /**
   * Prints out the jobconf properties on stdout
   * when verbose is specified.
   */
  protected void listJobConfProperties()
  {
    msg("==== JobConf properties:");
    Iterator it = jobConf_.iterator();
    TreeMap sorted = new TreeMap();
    while(it.hasNext()) {
      Map.Entry en = (Map.Entry)it.next();
      sorted.put(en.getKey(), en.getValue());
    }
    it = sorted.entrySet().iterator();
    while(it.hasNext()) {
      Map.Entry en = (Map.Entry)it.next();
      msg(en.getKey() + "=" + en.getValue());
    }
    msg("====");
  }

  protected String getJobTrackerHostPort() {
    return jobConf_.get("mapred.job.tracker");
  }

  protected void jobInfo() {
    if (isLocalHadoop()) {
      LOG.info("Job running in-process (local Hadoop)");
    } else {
      String hp = getJobTrackerHostPort();
      LOG.info("To kill this job, run:");
      LOG.info(getHadoopClientHome() + "/bin/hadoop job  -Dmapred.job.tracker=" + hp + " -kill "
               + jobId_);
      //LOG.info("Job file: " + running_.getJobFile());
      LOG.info("Tracking URL: " + StreamUtil.qualifyHost(running_.getTrackingURL()));
    }
  }

  // Based on JobClient
  public int submitAndMonitorJob() throws IOException {

    if (jar_ != null && isLocalHadoop()) {
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
      jobId_ = running_.getID();

      LOG.info("getLocalDirs(): " + Arrays.asList(jobConf_.getLocalDirs()));
      LOG.info("Running job: " + jobId_);
      jobInfo();

      while (!running_.isComplete()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        running_ = jc_.getJob(jobId_);
        String report = null;
        report = " map " + Math.round(running_.mapProgress() * 100) + "%  reduce "
          + Math.round(running_.reduceProgress() * 100) + "%";

        if (!report.equals(lastReport)) {
          LOG.info(report);
          lastReport = report;
        }
      }
      if (!running_.isSuccessful()) {
        jobInfo();
	LOG.error("Job not Successful!");
	return 1;
      }
      LOG.info("Job complete: " + jobId_);
      LOG.info("Output: " + output_);
      error = false;
    } catch(FileNotFoundException fe) {
      LOG.error("Error launching job , bad input path : " + fe.getMessage());
      return 2;
    } catch(InvalidJobConfException je) {
      LOG.error("Error launching job , Invalid job conf : " + je.getMessage());
      return 3;
    } catch(FileAlreadyExistsException fae) {
      LOG.error("Error launching job , Output path already exists : " 
                + fae.getMessage());
      return 4;
    } catch(IOException ioe) {
      LOG.error("Error Launching job : " + ioe.getMessage());
      return 5;
    } finally {
      if (error && (running_ != null)) {
        LOG.info("killJob...");
        running_.killJob();
      }
      jc_.close();
    }
    return 0;
  }
  /** Support -jobconf x=y x1=y1 type options **/
  class MultiPropertyOption extends PropertyOption{
    private String optionString; 
    MultiPropertyOption(){
      super(); 
    }
    
    MultiPropertyOption(final String optionString,
                        final String description,
                        final int id){
      super(optionString, description, id); 
      this.optionString = optionString;
    }

    @Override
    public boolean canProcess(final WriteableCommandLine commandLine,
                              final String argument) {
      boolean ret = (argument != null) && argument.startsWith(optionString);
        
      return ret;
    }    
    @Override
    public void process(final WriteableCommandLine commandLine,
                        final ListIterator arguments) throws OptionException {
      final String arg = (String) arguments.next();

      if (!canProcess(commandLine, arg)) {
        throw new OptionException(this, 
                                  ResourceConstants.UNEXPECTED_TOKEN, arg);
      }
      
      ArrayList properties = new ArrayList(); 
      String next = ""; 
      while(arguments.hasNext()){
        next = (String) arguments.next();
        if (!next.startsWith("-")){
          properties.add(next);
        }else{
          arguments.previous();
          break; 
        }
      } 

      // add to any existing values (support specifying args multiple times)
      List<String> oldVal = (List<String>)commandLine.getValue(this); 
      if (oldVal == null){
        commandLine.addValue(this, properties);
      }else{
        oldVal.addAll(properties); 
      }
    }
  }

  protected boolean mayExit_;
  protected String[] argv_;
  protected boolean verbose_;
  protected boolean detailedUsage_;
  protected int debug_;

  protected Environment env_;

  protected String jar_;
  protected boolean localHadoop_;
  protected Configuration config_;
  protected JobConf jobConf_;
  protected JobClient jc_;

  // command-line arguments
  protected ArrayList inputSpecs_ = new ArrayList(); // <String>
  protected TreeSet seenPrimary_ = new TreeSet(); // <String>
  protected boolean hasSimpleInputSpecs_;
  protected ArrayList packageFiles_ = new ArrayList(); // <String>
  protected ArrayList shippedCanonFiles_ = new ArrayList(); // <String>
  protected TreeMap<String, String> userJobConfProps_ = new TreeMap<String, String>(); 
  protected String output_;
  protected String mapCmd_;
  protected String comCmd_;
  protected String redCmd_;
  protected String cluster_;
  protected String cacheFiles;
  protected String cacheArchives;
  protected URI[] fileURIs;
  protected URI[] archiveURIs;
  protected ArrayList configPath_ = new ArrayList(); // <String>
  protected String hadoopAliasConf_;
  protected String inReaderSpec_;
  protected String inputFormatSpec_;
  protected String outputFormatSpec_;
  protected String partitionerSpec_;
  protected String numReduceTasksSpec_;
  protected String additionalConfSpec_;
  protected String mapDebugSpec_;
  protected String reduceDebugSpec_;

  // Use to communicate config to the external processes (ex env.var.HADOOP_USER)
  // encoding "a=b c=d"
  protected String addTaskEnvironment_;

  protected boolean outputSingleNode_;
  protected long minRecWrittenToEnableSkip_;

  protected RunningJob running_;
  protected JobID jobId_;
  protected static String LINK_URI = "You need to specify the uris as hdfs://host:port/#linkname," +
    "Please specify a different link name for all of your caching URIs";
}
