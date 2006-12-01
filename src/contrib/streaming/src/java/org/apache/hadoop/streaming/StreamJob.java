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
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.*;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.filecache.*;
import org.apache.hadoop.util.*;
/** All the client-side work happens here.
 * (Jar packaging, MapRed job submission and monitoring)
 * @author Michel Tourn
 */
public class StreamJob {

  protected static final Log LOG = LogFactory.getLog(StreamJob.class.getName());
  final static String REDUCE_NONE = "NONE";
  private boolean reducerNone_;

  public StreamJob(String[] argv, boolean mayExit) {
    argv_ = argv;
    mayExit_ = mayExit;
  }
  
  /**
   * This is the method that actually 
   * intializes the job conf and submits the job
   * to the jobtracker
   * @throws IOException
   */
  public void go() throws IOException {
    init();

    preProcessArgs();
    parseArgv();
    postProcessArgs();

    setJobConf();
    submitAndMonitorJob();
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

  String[] parseNameEqValue(String neqv) {
    String[] nv = neqv.split("=", 2);
    if (nv.length < 2) {
      fail("Invalid name=value spec: " + neqv);
    }
    msg("Recording name=value: name=" + nv[0] + " value=" + nv[1]);
    return nv;
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

  /**
   * This method parses the command line args
   * to a hadoop streaming job
   */
  void parseArgv() {
    if (argv_.length == 0) {
      exitUsage(false);
    }
    int i = 0;
    while (i < argv_.length) {
      String s;
      if (argv_[i].equals("-verbose")) {
        verbose_ = true;
      } else if (argv_[i].equals("-info")) {
        detailedUsage_ = true;
      } else if (argv_[i].equals("-debug")) {
        debug_++;
      } else if ((s = optionArg(argv_, i, "-input", false)) != null) {
        i++;
        inputSpecs_.add(s);
      } else if (argv_[i].equals("-inputtagged")) {
        inputTagged_ = true;
      } else if ((s = optionArg(argv_, i, "-output", output_ != null)) != null) {
        i++;
        output_ = s;
      } else if ((s = optionArg(argv_, i, "-mapsideoutput", mapsideoutURI_ != null)) != null) {
        i++;
        mapsideoutURI_ = s;
      } else if ((s = optionArg(argv_, i, "-mapper", mapCmd_ != null)) != null) {
        i++;
        mapCmd_ = s;
      } else if ((s = optionArg(argv_, i, "-combiner", comCmd_ != null)) != null) {
        i++;
        comCmd_ = s;
      } else if ((s = optionArg(argv_, i, "-reducer", redCmd_ != null)) != null) {
        i++;
        redCmd_ = s;
      } else if ((s = optionArg(argv_, i, "-file", false)) != null) {
        i++;
        packageFiles_.add(s);
      } else if ((s = optionArg(argv_, i, "-cluster", cluster_ != null)) != null) {
        i++;
        cluster_ = s;
      } else if ((s = optionArg(argv_, i, "-config", false)) != null) {
        i++;
        configPath_.add(s);
      } else if ((s = optionArg(argv_, i, "-dfs", false)) != null) {
        i++;
        userJobConfProps_.put("fs.default.name", s);
      } else if ((s = optionArg(argv_, i, "-jt", false)) != null) {
        i++;
        userJobConfProps_.put("mapred.job.tracker", s);
      } else if ((s = optionArg(argv_, i, "-jobconf", false)) != null) {
        i++;
        String[] nv = parseNameEqValue(s);
        userJobConfProps_.put(nv[0], nv[1]);
      } else if ((s = optionArg(argv_, i, "-cmdenv", false)) != null) {
        i++;
        parseNameEqValue(s);
        if (addTaskEnvironment_.length() > 0) {
          addTaskEnvironment_ += " ";
        }
        addTaskEnvironment_ += s;
      } else if ((s = optionArg(argv_, i, "-inputreader", inReaderSpec_ != null)) != null) {
        i++;
        inReaderSpec_ = s;
      } else if((s = optionArg(argv_, i, "-cacheArchive", false)) != null) {
    	  i++;
    	  if (cacheArchives == null)
    		  cacheArchives = s;
    	  else
    		  cacheArchives = cacheArchives + "," + s;    	  
      } else if((s = optionArg(argv_, i, "-cacheFile", false)) != null) {
        i++;
        System.out.println(" the val of s is " + s);
        if (cacheFiles == null)
          cacheFiles = s;
        else
          cacheFiles = cacheFiles + "," + s;
        System.out.println(" the val of cachefiles is " + cacheFiles);
      }
      else {
        System.err.println("Unexpected argument: " + argv_[i]);
        exitUsage(false);
      }
      i++;
    }
    if (detailedUsage_) {
      exitUsage(true);
    }
  }

  String optionArg(String[] args, int index, String arg, boolean argSet) {
    if (index >= args.length || !args[index].equals(arg)) {
      return null;
    }
    if (argSet) {
      throw new IllegalArgumentException("Can only have one " + arg + " option");
    }
    if (index >= args.length - 1) {
      throw new IllegalArgumentException("Expected argument after option " + args[index]);
    }
    return args[index + 1];
  }

  protected void msg(String msg) {
    if (verbose_) {
      System.out.println("STREAM: " + msg);
    }
  }

  public void exitUsage(boolean detailed) {
    //         1         2         3         4         5         6         7
    //1234567890123456789012345678901234567890123456789012345678901234567890123456789
    System.out.println("Usage: $HADOOP_HOME/bin/hadoop [--config dir] jar \\");
    System.out.println("          $HADOOP_HOME/hadoop-streaming.jar [options]");
    System.out.println("Options:");
    System.out.println("  -input    <path>     DFS input file(s) for the Map step");
    System.out.println("  -output   <path>     DFS output directory for the Reduce step");
    System.out.println("  -mapper   <cmd>      The streaming command to run");
    System.out.println("  -combiner <cmd>      The streaming command to run");
    System.out.println("  -reducer  <cmd>      The streaming command to run");
    System.out.println("  -file     <file>     File/dir to be shipped in the Job jar file");
    //Only advertise the standard way: [--config dir] in our launcher 
    //System.out.println("  -cluster  <name>     Default uses hadoop-default.xml and hadoop-site.xml");
    //System.out.println("  -config   <file>     Optional. One or more paths to xml config files");
    System.out.println("  -dfs    <h:p>|local  Optional. Override DFS configuration");
    System.out.println("  -jt     <h:p>|local  Optional. Override JobTracker configuration");
    System.out.println("  -inputreader <spec>  Optional.");
    System.out.println("  -jobconf  <n>=<v>    Optional. Add or override a JobConf property");
    System.out.println("  -cmdenv   <n>=<v>    Optional. Pass env.var to streaming commands");
    System.out.println("  -cacheFile fileNameURI");
    System.out.println("  -cacheArchive fileNameURI");
    System.out.println("  -verbose");
    System.out.println();
    if (!detailed) {
      System.out.println("For more details about these options:");
      System.out.println("Use $HADOOP_HOME/bin/hadoop jar build/hadoop-streaming.jar -info");
      fail("");
    }
    System.out.println("In -input: globbing on <path> is supported and can have multiple -input");
    System.out.println("Default Map input format: a line is a record in UTF-8");
    System.out.println("  the key part ends at first TAB, the rest of the line is the value");
    System.out.println("Custom Map input format: -inputreader package.MyRecordReader,n=v,n=v ");
    System.out
        .println("  comma-separated name-values can be specified to configure the InputFormat");
    System.out.println("  Ex: -inputreader 'StreamXmlRecordReader,begin=<doc>,end=</doc>'");
    System.out.println("Map output format, reduce input/output format:");
    System.out.println("  Format defined by what the mapper command outputs. Line-oriented");
    System.out.println();
    System.out.println("The files or directories named in the -file argument[s] end up in the");
    System.out.println("  working directory when the mapper and reducer are run.");
    System.out.println("  The location of this working directory is unspecified.");
    System.out.println();
    //System.out.println("Use -cluster <name> to switch between \"local\" Hadoop and one or more remote ");
    //System.out.println("  Hadoop clusters. ");
    //System.out.println("  The default is to use the normal hadoop-default.xml and hadoop-site.xml");
    //System.out.println("  Else configuration will use $HADOOP_HOME/conf/hadoop-<name>.xml");
    //System.out.println();
    System.out.println("To skip the sort/combine/shuffle/sort/reduce step:");
    System.out.println("  Use -reducer " + REDUCE_NONE);
    System.out
        .println("  A Task's Map output then becomes a 'side-effect output' rather than a reduce input");
    System.out
        .println("  This speeds up processing, This also feels more like \"in-place\" processing");
    System.out.println("  because the input filename and the map input order are preserved");
    System.out.println("To specify a single side-effect output file");
    System.out.println("    -mapsideoutput [file:/C:/win|file:/unix/|socket://host:port]");//-output for side-effects will be soon deprecated
    System.out.println("  If the jobtracker is local this is a local file");
    System.out.println("  This currently requires -reducer NONE");
    System.out.println();
    System.out.println("To set the number of reduce tasks (num. of output files):");
    System.out.println("  -jobconf mapred.reduce.tasks=10");
    System.out.println("To speed up the last reduces:");
    System.out.println("  -jobconf mapred.speculative.execution=true");
    System.out.println("  Do not use this along -reducer " + REDUCE_NONE);
    System.out.println("To name the job (appears in the JobTracker Web UI):");
    System.out.println("  -jobconf mapred.job.name='My Job' ");
    System.out.println("To specify that line-oriented input is in gzip format:");
    System.out
        .println("(at this time ALL input files must be gzipped and this is not recognized based on file extension)");
    System.out.println("   -jobconf stream.recordreader.compression=gzip ");
    System.out.println("To change the local temp directory:");
    System.out.println("  -jobconf dfs.data.dir=/tmp/dfs");
    System.out.println("  -jobconf stream.tmpdir=/tmp/streaming");
    System.out.println("Additional local temp directories with -cluster local:");
    System.out.println("  -jobconf mapred.local.dir=/tmp/local");
    System.out.println("  -jobconf mapred.system.dir=/tmp/system");
    System.out.println("  -jobconf mapred.temp.dir=/tmp/temp");
    System.out.println("Use a custom hadoopStreaming build along a standard hadoop install:");
    System.out.println("  $HADOOP_HOME/bin/hadoop jar /path/my-hadoop-streaming.jar [...]\\");
    System.out
        .println("    [...] -jobconf stream.shipped.hadoopstreaming=/path/my-hadoop-streaming.jar");
    System.out.println("For more details about jobconf parameters see:");
    System.out.println("  http://wiki.apache.org/lucene-hadoop/JobConfFile");
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
      System.exit(1);
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
      String val = (String)userJobConfProps_.get(key);
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
      config_.addFinalResource(new Path(getHadoopAliasConfFile()));
    } else {
      // use only defaults: hadoop-default.xml and hadoop-site.xml
    }
    Iterator it = configPath_.iterator();
    while (it.hasNext()) {
      String pathName = (String) it.next();
      config_.addFinalResource(new Path(pathName));
    }

    testMerge_ = (-1 != userJobConfProps_.toString().indexOf("stream.testmerge"));

    // general MapRed job properties
    jobConf_ = new JobConf(config_);

    setUserJobConfProps(true);

    // The correct FS must be set before this is called!
    // (to resolve local vs. dfs drive letter differences) 
    // (mapred.working.dir will be lazily initialized ONCE and depends on FS)
    for (int i = 0; i < inputSpecs_.size(); i++) {
      addInputSpec((String) inputSpecs_.get(i), i);
    }
    jobConf_.setBoolean("stream.inputtagged", inputTagged_);
    jobConf_.set("stream.numinputspecs", "" + inputSpecs_.size());

    Class fmt;
    if (testMerge_ && false == hasSimpleInputSpecs_) {
      // this ignores -inputreader
      fmt = MergerInputFormat.class;
    } else {
      // need to keep this case to support custom -inputreader 
      // and their parameters ,n=v,n=v
      fmt = StreamInputFormat.class;
    }
    jobConf_.setInputFormat(fmt);

    // for SequenceFile, input classes may be overriden in getRecordReader
    jobConf_.setInputKeyClass(Text.class);
    jobConf_.setInputValueClass(Text.class);

    jobConf_.setOutputKeyClass(Text.class);
    jobConf_.setOutputValueClass(Text.class);

    jobConf_.set("stream.addenvironment", addTaskEnvironment_);

    String defaultPackage = this.getClass().getPackage().getName();

    Class c = StreamUtil.goodClassOrNull(mapCmd_, defaultPackage);
    if (c != null) {
      jobConf_.setMapperClass(c);
    } else {
      jobConf_.setMapperClass(PipeMapper.class);
      jobConf_.set("stream.map.streamprocessor", URLEncoder.encode(mapCmd_, "UTF-8"));
    }

    if (comCmd_ != null) {
      c = StreamUtil.goodClassOrNull(comCmd_, defaultPackage);
      if (c != null) {
        jobConf_.setCombinerClass(c);
      } else {
        jobConf_.setCombinerClass(PipeCombiner.class);
        jobConf_.set("stream.combine.streamprocessor", URLEncoder.encode(comCmd_, "UTF-8"));
      }
    }

    reducerNone_ = false;
    if (redCmd_ != null) {
      reducerNone_ = redCmd_.equals(REDUCE_NONE);
      c = StreamUtil.goodClassOrNull(redCmd_, defaultPackage);
      if (c != null) {
        jobConf_.setReducerClass(c);
      } else {
        jobConf_.setReducerClass(PipeReducer.class);
        jobConf_.set("stream.reduce.streamprocessor", URLEncoder.encode(redCmd_, "UTF-8"));
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
    // output setup is done late so we can customize for reducerNone_
    //jobConf_.setOutputDir(new File(output_));
    setOutputSpec();
    if (testMerge_) {
      fmt = MuxOutputFormat.class;
    } else {
      fmt = StreamOutputFormat.class;
    }
    jobConf_.setOutputFormat(fmt);

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
      DistributedCache.createSymlink(jobConf_);
    }
    // set the jobconf for the caching parameters
    if (cacheArchives != null)
      DistributedCache.setCacheArchives(archiveURIs, jobConf_);
    if (cacheFiles != null)
      DistributedCache.setCacheFiles(fileURIs, jobConf_);
    
    if(verbose_) {
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
    Iterator it = jobConf_.entries();
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
  
  /** InputSpec-s encode: a glob pattern x additional column files x additional joins */
  protected void addInputSpec(String inSpec, int index) {
    if (!testMerge_) {
      jobConf_.addInputPath(new Path(inSpec));
    } else {
      CompoundDirSpec spec = new CompoundDirSpec(inSpec, true);
      msg("Parsed -input:\n" + spec.toTableString());
      if (index == 0) {
        hasSimpleInputSpecs_ = (spec.paths_.length == 0);
        msg("hasSimpleInputSpecs_=" + hasSimpleInputSpecs_);
      }
      String primary = spec.primarySpec();
      if (!seenPrimary_.add(primary)) {
        // this won't detect glob overlaps and noncanonical path variations
        fail("Primary used in multiple -input spec: " + primary);
      }
      jobConf_.addInputPath(new Path(primary));
      // during Job execution, will reparse into a CompoundDirSpec 
      jobConf_.set("stream.inputspecs." + index, inSpec);
    }
  }

  /** uses output_ and mapsideoutURI_ */
  protected void setOutputSpec() throws IOException {
    CompoundDirSpec spec = new CompoundDirSpec(output_, false);
    msg("Parsed -output:\n" + spec.toTableString());
    String primary = spec.primarySpec();
    String channel0;
    // TODO simplify cases, encapsulate in a StreamJobConf
    if (!reducerNone_) {
      channel0 = primary;
    } else {
      if (mapsideoutURI_ != null) {
        // user can override in case this is in a difft filesystem..
        try {
          URI uri = new URI(mapsideoutURI_);
          if (uri.getScheme() == null || uri.getScheme().equals("file")) { // || uri.getScheme().equals("hdfs")
            if (!new Path(uri.getSchemeSpecificPart()).isAbsolute()) {
              fail("Must be absolute: " + mapsideoutURI_);
            }
          } else if (uri.getScheme().equals("socket")) {
            // ok
          } else {
            fail("Invalid scheme: " + uri.getScheme() + " for -mapsideoutput " + mapsideoutURI_);
          }
        } catch (URISyntaxException e) {
          throw (IOException) new IOException().initCause(e);
        }
      }
      // an empty reduce output named "part-00002" will go here and not collide.
      channel0 = primary + ".NONE";
      // the side-effect of the first split of an input named "part-00002" 
      // will go in this directory
      jobConf_.set("stream.sideoutput.dir", primary);
      // oops if user overrides low-level this isn't set yet :-(
      boolean localjt = StreamUtil.isLocalJobTracker(jobConf_);
      // just a guess user may prefer remote..
      jobConf_.setBoolean("stream.sideoutput.localfs", localjt);
    }
    // a path in fs.name.default filesystem
    System.out.println(channel0);
    System.out.println(new Path(channel0));
    jobConf_.setOutputPath(new Path(channel0));
    // will reparse remotely
    jobConf_.set("stream.outputspec", output_);
    if (null != mapsideoutURI_) {
      // a path in "jobtracker's filesystem"
      // overrides sideoutput.dir
      jobConf_.set("stream.sideoutput.uri", mapsideoutURI_);
    }
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
      //LOG.info("Job file: " + running_.getJobFile() );
      LOG.info("Tracking URL: " + StreamUtil.qualifyHost(running_.getTrackingURL()));
    }
  }

  // Based on JobClient
  public void submitAndMonitorJob() throws IOException {

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
      jobId_ = running_.getJobID();

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
  protected boolean inputTagged_ = false;
  protected TreeSet seenPrimary_ = new TreeSet(); // <String>
  protected boolean hasSimpleInputSpecs_;
  protected ArrayList packageFiles_ = new ArrayList(); // <String>
  protected ArrayList shippedCanonFiles_ = new ArrayList(); // <String>
  //protected ArrayList userJobConfProps_ = new ArrayList(); // <String> name=value
  protected TreeMap<String, String> userJobConfProps_ = new TreeMap<String, String>(); 
  protected String output_;
  protected String mapsideoutURI_;
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

  protected boolean testMerge_;

  // Use to communicate config to the external processes (ex env.var.HADOOP_USER)
  // encoding "a=b c=d"
  protected String addTaskEnvironment_;

  protected boolean outputSingleNode_;
  protected long minRecWrittenToEnableSkip_;

  protected RunningJob running_;
  protected String jobId_;
  protected static String LINK_URI = "You need to specify the uris as hdfs://host:port/#linkname," +
      "Please specify a different link name for all of your caching URIs";
}
