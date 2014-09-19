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
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.LazyOutputFormat;
import org.apache.hadoop.mapred.lib.aggregate.ValueAggregatorCombiner;
import org.apache.hadoop.mapred.lib.aggregate.ValueAggregatorReducer;
import org.apache.hadoop.streaming.io.IdentifierResolver;
import org.apache.hadoop.streaming.io.InputWriter;
import org.apache.hadoop.streaming.io.OutputReader;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;

/** All the client-side work happens here.
 * (Jar packaging, MapRed job submission and monitoring)
 */
public class StreamJob implements Tool {

  protected static final Log LOG = LogFactory.getLog(StreamJob.class.getName());
  final static String REDUCE_NONE = "NONE";

  /** -----------Streaming CLI Implementation  **/
  private CommandLineParser parser = new BasicParser();
  private Options allOptions;
  /**@deprecated use StreamJob() with ToolRunner or set the
   * Configuration using {@link #setConf(Configuration)} and
   * run with {@link #run(String[])}.
   */
  @Deprecated
  public StreamJob(String[] argv, boolean mayExit) {
    this();
    argv_ = Arrays.copyOf(argv, argv.length);
    this.config_ = new Configuration();
  }

  public StreamJob() {
    setupOptions();
    this.config_ = new Configuration();
  }

  @Override
  public Configuration getConf() {
    return config_;
  }

  @Override
  public void setConf(Configuration conf) {
    this.config_ = conf;
  }

  @Override
  public int run(String[] args) throws Exception {
    try {
      this.argv_ = Arrays.copyOf(args, args.length);
      init();

      preProcessArgs();
      parseArgv();
      if (printUsage) {
        printUsage(detailedUsage_);
        return 0;
      }
      postProcessArgs();

      setJobConf();
    } catch (IllegalArgumentException ex) {
      //ignore, since log will already be printed
      // print the log in debug mode.
      LOG.debug("Error in streaming job", ex);
      return 1;
    }
    return submitAndMonitorJob();
  }

  /**
   * This method creates a streaming job from the given argument list.
   * The created object can be used and/or submitted to a jobtracker for
   * execution by a job agent such as JobControl
   * @param argv the list args for creating a streaming job
   * @return the created JobConf object
   * @throws IOException
   */
  static public JobConf createJob(String[] argv) throws IOException {
    StreamJob job = new StreamJob();
    job.argv_ = argv;
    job.init();
    job.preProcessArgs();
    job.parseArgv();
    job.postProcessArgs();
    job.setJobConf();
    return job.jobConf_;
  }

  /**
   * This is the method that actually
   * intializes the job conf and submits the job
   * to the jobtracker
   * @throws IOException
   * @deprecated use {@link #run(String[])} instead.
   */
  @Deprecated
  public int go() throws IOException {
    try {
      return run(argv_);
    }
    catch (Exception ex) {
      throw new IOException(ex.getMessage());
    }
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
    // Unset HADOOP_ROOT_LOGGER in case streaming job
    // invokes additional hadoop commands.
    addTaskEnvironment_ = "HADOOP_ROOT_LOGGER=";
  }

  void postProcessArgs() throws IOException {

    if (inputSpecs_.size() == 0) {
      fail("Required argument: -input <name>");
    }
    if (output_ == null) {
      fail("Required argument: -output ");
    }
    msg("addTaskEnvironment=" + addTaskEnvironment_);

    for (final String packageFile : packageFiles_) {
      File f = new File(packageFile);
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

  void parseArgv() {
    CommandLine cmdLine = null;
    try {
      cmdLine = parser.parse(allOptions, argv_);
    } catch(Exception oe) {
      LOG.error(oe.getMessage());
      exitUsage(argv_.length > 0 && "-info".equals(argv_[0]));
    }

    if (cmdLine != null) {
      @SuppressWarnings("unchecked")
      List<String> args = cmdLine.getArgList();
      if(args != null && args.size() > 0) {
        fail("Found " + args.size() + " unexpected arguments on the " +
            "command line " + args);
      }
      
      detailedUsage_ = cmdLine.hasOption("info");
      if (cmdLine.hasOption("help") || detailedUsage_) {
        printUsage = true;
        return;
      }
      verbose_ =  cmdLine.hasOption("verbose");
      background_ =  cmdLine.hasOption("background");
      debug_ = cmdLine.hasOption("debug")? debug_ + 1 : debug_;

      String[] values = cmdLine.getOptionValues("input");
      if (values != null && values.length > 0) {
        for (String input : values) {
          inputSpecs_.add(input);
        }
      }
      output_ =  cmdLine.getOptionValue("output");

      mapCmd_ = cmdLine.getOptionValue("mapper");
      comCmd_ = cmdLine.getOptionValue("combiner");
      redCmd_ = cmdLine.getOptionValue("reducer");

      lazyOutput_ = cmdLine.hasOption("lazyOutput");

      values = cmdLine.getOptionValues("file");
      if (values != null && values.length > 0) {
        LOG.warn("-file option is deprecated, please use generic option" +
        		" -files instead.");

        StringBuffer fileList = new StringBuffer();
        for (String file : values) {
          packageFiles_.add(file);
          try {
            Path path = new Path(file);
            FileSystem localFs = FileSystem.getLocal(config_);
            String finalPath = path.makeQualified(localFs).toString();
            if(fileList.length() > 0) {
              fileList.append(',');
            }
            fileList.append(finalPath);
          } catch (Exception e) {
            throw new IllegalArgumentException(e);
          }
        }
        String tmpFiles = config_.get("tmpfiles", "");
        if (tmpFiles.isEmpty()) {
          tmpFiles = fileList.toString();
        } else {
          tmpFiles = tmpFiles + "," + fileList;
        }
        config_.set("tmpfiles", tmpFiles);
        validate(packageFiles_);
      }

      String fsName = cmdLine.getOptionValue("dfs");
      if (null != fsName){
        LOG.warn("-dfs option is deprecated, please use -fs instead.");
        config_.set("fs.default.name", fsName);
      }

      additionalConfSpec_ = cmdLine.getOptionValue("additionalconfspec");
      inputFormatSpec_ = cmdLine.getOptionValue("inputformat");
      outputFormatSpec_ = cmdLine.getOptionValue("outputformat");
      numReduceTasksSpec_ = cmdLine.getOptionValue("numReduceTasks");
      partitionerSpec_ = cmdLine.getOptionValue("partitioner");
      inReaderSpec_ = cmdLine.getOptionValue("inputreader");
      mapDebugSpec_ = cmdLine.getOptionValue("mapdebug");
      reduceDebugSpec_ = cmdLine.getOptionValue("reducedebug");
      ioSpec_ = cmdLine.getOptionValue("io");

      String[] car = cmdLine.getOptionValues("cacheArchive");
      if (null != car && car.length > 0){
        LOG.warn("-cacheArchive option is deprecated, please use -archives instead.");
        for(String s : car){
          cacheArchives = (cacheArchives == null)?s :cacheArchives + "," + s;
        }
      }

      String[] caf = cmdLine.getOptionValues("cacheFile");
      if (null != caf && caf.length > 0){
        LOG.warn("-cacheFile option is deprecated, please use -files instead.");
        for(String s : caf){
          cacheFiles = (cacheFiles == null)?s :cacheFiles + "," + s;
        }
      }

      String[] jobconf = cmdLine.getOptionValues("jobconf");
      if (null != jobconf && jobconf.length > 0){
        LOG.warn("-jobconf option is deprecated, please use -D instead.");
        for(String s : jobconf){
          String[] parts = s.split("=", 2);
          config_.set(parts[0], parts[1]);
        }
      }

      String[] cmd = cmdLine.getOptionValues("cmdenv");
      if (null != cmd && cmd.length > 0){
        for(String s : cmd) {
          if (addTaskEnvironment_.length() > 0) {
            addTaskEnvironment_ += " ";
          }
          addTaskEnvironment_ += s;
        }
      }
    } else {
      exitUsage(argv_.length > 0 && "-info".equals(argv_[0]));
    }
  }

  protected void msg(String msg) {
    if (verbose_) {
      System.out.println("STREAM: " + msg);
    }
  }

  private Option createOption(String name, String desc,
                              String argName, int max, boolean required){
    return OptionBuilder
           .withArgName(argName)
           .hasArgs(max)
           .withDescription(desc)
           .isRequired(required)
           .create(name);
  }

  private Option createBoolOption(String name, String desc){
    return OptionBuilder.withDescription(desc).create(name);
  }

  private void validate(final List<String> values)
  throws IllegalArgumentException {
    for (String file : values) {
      File f = new File(file);
      if (!FileUtil.canRead(f)) {
        fail("File: " + f.getAbsolutePath()
          + " does not exist, or is not readable.");
      }
    }
  }

  private void setupOptions(){

    // input and output are not required for -info and -help options,
    // though they are required for streaming job to be run.
    Option input   = createOption("input",
                                  "DFS input file(s) for the Map step",
                                  "path",
                                  Integer.MAX_VALUE,
                                  false);

    Option output  = createOption("output",
                                  "DFS output directory for the Reduce step",
                                  "path", 1, false);
    Option mapper  = createOption("mapper",
                                  "The streaming command to run", "cmd", 1, false);
    Option combiner = createOption("combiner",
                                   "The streaming command to run", "cmd", 1, false);
    // reducer could be NONE
    Option reducer = createOption("reducer",
                                  "The streaming command to run", "cmd", 1, false);
    Option file = createOption("file",
                               "File to be shipped in the Job jar file",
                               "file", Integer.MAX_VALUE, false);
    Option dfs = createOption("dfs",
                              "Optional. Override DFS configuration", "<h:p>|local", 1, false);
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
    Option jobconf =
      createOption("jobconf",
                   "(n=v) Optional. Add or override a JobConf property.",
                   "spec", 1, false);

    Option cmdenv =
      createOption("cmdenv", "(n=v) Pass env.var to streaming commands.",
                   "spec", 1, false);
    Option cacheFile = createOption("cacheFile",
                                    "File name URI", "fileNameURI", Integer.MAX_VALUE, false);
    Option cacheArchive = createOption("cacheArchive",
                                       "File name URI", "fileNameURI", Integer.MAX_VALUE, false);
    Option io = createOption("io",
                             "Optional.", "spec", 1, false);

    // boolean properties

    Option background = createBoolOption("background", "Submit the job and don't wait till it completes.");
    Option verbose = createBoolOption("verbose", "print verbose output");
    Option info = createBoolOption("info", "print verbose output");
    Option help = createBoolOption("help", "print this help message");
    Option debug = createBoolOption("debug", "print debug output");
    Option lazyOutput = createBoolOption("lazyOutput", "create outputs lazily");

    allOptions = new Options().
      addOption(input).
      addOption(output).
      addOption(mapper).
      addOption(combiner).
      addOption(reducer).
      addOption(file).
      addOption(dfs).
      addOption(additionalconfspec).
      addOption(inputformat).
      addOption(outputformat).
      addOption(partitioner).
      addOption(numReduceTasks).
      addOption(inputreader).
      addOption(mapDebug).
      addOption(reduceDebug).
      addOption(jobconf).
      addOption(cmdenv).
      addOption(cacheFile).
      addOption(cacheArchive).
      addOption(io).
      addOption(background).
      addOption(verbose).
      addOption(info).
      addOption(debug).
      addOption(help).
      addOption(lazyOutput);
  }

  public void exitUsage(boolean detailed) {
    printUsage(detailed);
    fail("");
  }

  private void printUsage(boolean detailed) {
    System.out.println("Usage: $HADOOP_PREFIX/bin/hadoop jar hadoop-streaming.jar"
        + " [options]");
    System.out.println("Options:");
    System.out.println("  -input          <path> DFS input file(s) for the Map"
        + " step.");
    System.out.println("  -output         <path> DFS output directory for the"
        + " Reduce step.");
    System.out.println("  -mapper         <cmd|JavaClassName> Optional. Command"
        + " to be run as mapper.");
    System.out.println("  -combiner       <cmd|JavaClassName> Optional. Command"
        + " to be run as combiner.");
    System.out.println("  -reducer        <cmd|JavaClassName> Optional. Command"
        + " to be run as reducer.");
    System.out.println("  -file           <file> Optional. File/dir to be "
        + "shipped in the Job jar file.\n" +
        "                  Deprecated. Use generic option \"-files\" instead.");
    System.out.println("  -inputformat    <TextInputFormat(default)"
        + "|SequenceFileAsTextInputFormat|JavaClassName>\n"
        + "                  Optional. The input format class.");
    System.out.println("  -outputformat   <TextOutputFormat(default)"
        + "|JavaClassName>\n"
        + "                  Optional. The output format class.");
    System.out.println("  -partitioner    <JavaClassName>  Optional. The"
        + " partitioner class.");
    System.out.println("  -numReduceTasks <num> Optional. Number of reduce "
        + "tasks.");
    System.out.println("  -inputreader    <spec> Optional. Input recordreader"
        + " spec.");
    System.out.println("  -cmdenv         <n>=<v> Optional. Pass env.var to"
        + " streaming commands.");
    System.out.println("  -mapdebug       <cmd> Optional. "
        + "To run this script when a map task fails.");
    System.out.println("  -reducedebug    <cmd> Optional."
        + " To run this script when a reduce task fails.");
    System.out.println("  -io             <identifier> Optional. Format to use"
        + " for input to and output");
    System.out.println("                  from mapper/reducer commands");
    System.out.println("  -lazyOutput     Optional. Lazily create Output.");
    System.out.println("  -background     Optional. Submit the job and don't wait till it completes.");
    System.out.println("  -verbose        Optional. Print verbose output.");
    System.out.println("  -info           Optional. Print detailed usage.");
    System.out.println("  -help           Optional. Print help message.");
    System.out.println();
    GenericOptionsParser.printGenericCommandUsage(System.out);

    if (!detailed) {
      System.out.println();
      System.out.println("For more details about these options:");
      System.out.println("Use " +
          "$HADOOP_PREFIX/bin/hadoop jar hadoop-streaming.jar -info");
      return;
    }
    System.out.println();
    System.out.println("Usage tips:");
    System.out.println("In -input: globbing on <path> is supported and can "
        + "have multiple -input");
    System.out.println();
    System.out.println("Default Map input format: a line is a record in UTF-8 "
        + "the key part ends at first");
    System.out.println("  TAB, the rest of the line is the value");
    System.out.println();
    System.out.println("To pass a Custom input format:");
    System.out.println("  -inputformat package.MyInputFormat");
    System.out.println();
    System.out.println("Similarly, to pass a custom output format:");
    System.out.println("  -outputformat package.MyOutputFormat");
    System.out.println();
    System.out.println("The files with extensions .class and .jar/.zip," +
        " specified for the -file");
    System.out.println("  argument[s], end up in \"classes\" and \"lib\" " +
        "directories respectively inside");
    System.out.println("  the working directory when the mapper and reducer are"
        + " run. All other files");
    System.out.println("  specified for the -file argument[s]" +
        " end up in the working directory when the");
    System.out.println("  mapper and reducer are run. The location of this " +
        "working directory is");
    System.out.println("  unspecified.");
    System.out.println();
    System.out.println("To set the number of reduce tasks (num. of output " +
        "files) as, say 10:");
    System.out.println("  Use -numReduceTasks 10");
    System.out.println("To skip the sort/combine/shuffle/sort/reduce step:");
    System.out.println("  Use -numReduceTasks 0");
    System.out.println("  Map output then becomes a 'side-effect " +
        "output' rather than a reduce input.");
    System.out.println("  This speeds up processing. This also feels " +
        "more like \"in-place\" processing");
    System.out.println("  because the input filename and the map " +
        "input order are preserved.");
    System.out.println("  This is equivalent to -reducer NONE");
    System.out.println();
    System.out.println("To speed up the last maps:");
    System.out.println("  -D " + MRJobConfig.MAP_SPECULATIVE + "=true");
    System.out.println("To speed up the last reduces:");
    System.out.println("  -D " + MRJobConfig.REDUCE_SPECULATIVE + "=true");
    System.out.println("To name the job (appears in the JobTracker Web UI):");
    System.out.println("  -D " + MRJobConfig.JOB_NAME + "='My Job'");
    System.out.println("To change the local temp directory:");
    System.out.println("  -D dfs.data.dir=/tmp/dfs");
    System.out.println("  -D stream.tmpdir=/tmp/streaming");
    System.out.println("Additional local temp directories with -jt local:");
    System.out.println("  -D " + MRConfig.LOCAL_DIR + "=/tmp/local");
    System.out.println("  -D " + JTConfig.JT_SYSTEM_DIR + "=/tmp/system");
    System.out.println("  -D " + MRConfig.TEMP_DIR + "=/tmp/temp");
    System.out.println("To treat tasks with non-zero exit status as SUCCEDED:");
    System.out.println("  -D stream.non.zero.exit.is.failure=false");
    System.out.println("Use a custom hadoop streaming build along with standard"
        + " hadoop install:");
    System.out.println("  $HADOOP_PREFIX/bin/hadoop jar " +
        "/path/my-hadoop-streaming.jar [...]\\");
    System.out.println("    [...] -D stream.shipped.hadoopstreaming=" +
        "/path/my-hadoop-streaming.jar");
    System.out.println("For more details about jobconf parameters see:");
    System.out.println("  http://wiki.apache.org/hadoop/JobConfFile");
    System.out.println("To set an environement variable in a streaming " +
        "command:");
    System.out.println("   -cmdenv EXAMPLE_DIR=/home/example/dictionaries/");
    System.out.println();
    System.out.println("Shortcut:");
    System.out.println("   setenv HSTREAMING \"$HADOOP_PREFIX/bin/hadoop jar " +
        "hadoop-streaming.jar\"");
    System.out.println();
    System.out.println("Example: $HSTREAMING -mapper " +
        "\"/usr/local/bin/perl5 filter.pl\"");
    System.out.println("           -file /local/filter.pl -input " +
        "\"/logs/0604*/*\" [...]");
    System.out.println("  Ships a script, invokes the non-shipped perl " +
        "interpreter. Shipped files go to");
    System.out.println("  the working directory so filter.pl is found by perl. "
        + "Input files are all the");
    System.out.println("  daily logs for days in month 2006-04");
  }

  public void fail(String message) {
    System.err.println(message);
    System.err.println("Try -help for more information");
    throw new IllegalArgumentException(message);
  }

  // --------------------------------------------

  protected String getHadoopClientHome() {
    String h = env_.getProperty("HADOOP_PREFIX"); // standard Hadoop
    if (h == null) {
      //fail("Missing required environment variable: HADOOP_PREFIX");
      h = "UNDEF";
    }
    return h;
  }

  protected boolean isLocalHadoop() {
    return StreamUtil.isLocalJobTracker(jobConf_);
  }

  @Deprecated
  protected String getClusterNick() {
    return "default";
  }

  /** @return path to the created Jar file or null if no files are necessary.
   */
  protected String packageJobJar() throws IOException {
    ArrayList<String> unjarFiles = new ArrayList<String>();

    // Runtime code: ship same version of code as self (job submitter code)
    // usually found in: build/contrib or build/hadoop-<version>-dev-streaming.jar

    // First try an explicit spec: it's too hard to find our own location in this case:
    // $HADOOP_PREFIX/bin/hadoop jar /not/first/on/classpath/custom-hadoop-streaming.jar
    // where findInClasspath() would find the version of hadoop-streaming.jar in $HADOOP_PREFIX
    String runtimeClasses = config_.get("stream.shipped.hadoopstreaming"); // jar or class dir

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
    String tmp = jobConf_.get("stream.tmpdir"); //, "/tmp/${mapreduce.job.user.name}/"
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
   * get the uris of all the files/caches
   */
  protected void getURIs(String lcacheArchives, String lcacheFiles) {
    String archives[] = StringUtils.getStrings(lcacheArchives);
    String files[] = StringUtils.getStrings(lcacheFiles);
    fileURIs = StringUtils.stringToURI(files);
    archiveURIs = StringUtils.stringToURI(archives);
  }

  protected void setJobConf() throws IOException {
    if (additionalConfSpec_ != null) {
      LOG.warn("-additionalconfspec option is deprecated, please use -conf instead.");
      config_.addResource(new Path(additionalConfSpec_));
    }

    // general MapRed job properties
    jobConf_ = new JobConf(config_, StreamJob.class);

    // All streaming jobs get the task timeout value
    // from the configuration settings.

    // The correct FS must be set before this is called!
    // (to resolve local vs. dfs drive letter differences)
    // (mapreduce.job.working.dir will be lazily initialized ONCE and depends on FS)
    for (int i = 0; i < inputSpecs_.size(); i++) {
      FileInputFormat.addInputPaths(jobConf_,
                        (String) inputSpecs_.get(i));
    }

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
        if (inReaderSpec_ == null) {
          fmt = KeyValueTextInputFormat.class;
        }
      } else if (inputFormatSpec_.equals(SequenceFileInputFormat.class
          .getName())
          || inputFormatSpec_
              .equals(org.apache.hadoop.mapred.SequenceFileInputFormat.class
                  .getCanonicalName())
          || inputFormatSpec_
              .equals(org.apache.hadoop.mapred.SequenceFileInputFormat.class.getSimpleName())) {
        if (inReaderSpec_ == null) {
          fmt = SequenceFileInputFormat.class;
        }
      } else if (inputFormatSpec_.equals(SequenceFileAsTextInputFormat.class
          .getName())
          || inputFormatSpec_.equals(SequenceFileAsTextInputFormat.class
              .getCanonicalName())
          || inputFormatSpec_.equals(SequenceFileAsTextInputFormat.class.getSimpleName())) {
        fmt = SequenceFileAsTextInputFormat.class;
      } else {
        c = StreamUtil.goodClassOrNull(jobConf_, inputFormatSpec_, defaultPackage);
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

    if (ioSpec_ != null) {
      jobConf_.set("stream.map.input", ioSpec_);
      jobConf_.set("stream.map.output", ioSpec_);
      jobConf_.set("stream.reduce.input", ioSpec_);
      jobConf_.set("stream.reduce.output", ioSpec_);
    }

    Class<? extends IdentifierResolver> idResolverClass =
      jobConf_.getClass("stream.io.identifier.resolver.class",
        IdentifierResolver.class, IdentifierResolver.class);
    IdentifierResolver idResolver = ReflectionUtils.newInstance(idResolverClass, jobConf_);

    idResolver.resolve(jobConf_.get("stream.map.input", IdentifierResolver.TEXT_ID));
    jobConf_.setClass("stream.map.input.writer.class",
      idResolver.getInputWriterClass(), InputWriter.class);

    idResolver.resolve(jobConf_.get("stream.reduce.input", IdentifierResolver.TEXT_ID));
    jobConf_.setClass("stream.reduce.input.writer.class",
      idResolver.getInputWriterClass(), InputWriter.class);

    jobConf_.set("stream.addenvironment", addTaskEnvironment_);

    boolean isMapperACommand = false;
    if (mapCmd_ != null) {
      c = StreamUtil.goodClassOrNull(jobConf_, mapCmd_, defaultPackage);
      if (c != null) {
        jobConf_.setMapperClass(c);
      } else {
        isMapperACommand = true;
        jobConf_.setMapperClass(PipeMapper.class);
        jobConf_.setMapRunnerClass(PipeMapRunner.class);
        jobConf_.set("stream.map.streamprocessor",
                     URLEncoder.encode(mapCmd_, "UTF-8"));
      }
    }

    if (comCmd_ != null) {
      c = StreamUtil.goodClassOrNull(jobConf_, comCmd_, defaultPackage);
      if (c != null) {
        jobConf_.setCombinerClass(c);
      } else {
        jobConf_.setCombinerClass(PipeCombiner.class);
        jobConf_.set("stream.combine.streamprocessor", URLEncoder.encode(
                comCmd_, "UTF-8"));
      }
    }

    if (numReduceTasksSpec_!= null) {
      int numReduceTasks = Integer.parseInt(numReduceTasksSpec_);
      jobConf_.setNumReduceTasks(numReduceTasks);
    }

    boolean isReducerACommand = false;
    if (redCmd_ != null) {
      if (redCmd_.equals(REDUCE_NONE)) {
        jobConf_.setNumReduceTasks(0);
      }
      if (jobConf_.getNumReduceTasks() != 0) {
        if (redCmd_.compareToIgnoreCase("aggregate") == 0) {
          jobConf_.setReducerClass(ValueAggregatorReducer.class);
          jobConf_.setCombinerClass(ValueAggregatorCombiner.class);
        } else {

          c = StreamUtil.goodClassOrNull(jobConf_, redCmd_, defaultPackage);
          if (c != null) {
            jobConf_.setReducerClass(c);
          } else {
            isReducerACommand = true;
            jobConf_.setReducerClass(PipeReducer.class);
            jobConf_.set("stream.reduce.streamprocessor", URLEncoder.encode(
                redCmd_, "UTF-8"));
          }
        }
      }
    }

    idResolver.resolve(jobConf_.get("stream.map.output",
        IdentifierResolver.TEXT_ID));
    jobConf_.setClass("stream.map.output.reader.class",
      idResolver.getOutputReaderClass(), OutputReader.class);
    if (isMapperACommand || jobConf_.get("stream.map.output") != null) {
      // if mapper is a command, then map output key/value classes come from the
      // idResolver
      jobConf_.setMapOutputKeyClass(idResolver.getOutputKeyClass());
      jobConf_.setMapOutputValueClass(idResolver.getOutputValueClass());

      if (jobConf_.getNumReduceTasks() == 0) {
        jobConf_.setOutputKeyClass(idResolver.getOutputKeyClass());
        jobConf_.setOutputValueClass(idResolver.getOutputValueClass());
      }
    }

    idResolver.resolve(jobConf_.get("stream.reduce.output",
        IdentifierResolver.TEXT_ID));
    jobConf_.setClass("stream.reduce.output.reader.class",
      idResolver.getOutputReaderClass(), OutputReader.class);
    if (isReducerACommand || jobConf_.get("stream.reduce.output") != null) {
      // if reducer is a command, then output key/value classes come from the
      // idResolver
      jobConf_.setOutputKeyClass(idResolver.getOutputKeyClass());
      jobConf_.setOutputValueClass(idResolver.getOutputValueClass());
    }

    if (inReaderSpec_ != null) {
      String[] args = inReaderSpec_.split(",");
      String readerClass = args[0];
      // this argument can only be a Java class
      c = StreamUtil.goodClassOrNull(jobConf_, readerClass, defaultPackage);
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

    FileOutputFormat.setOutputPath(jobConf_, new Path(output_));
    fmt = null;
    if (outputFormatSpec_!= null) {
      c = StreamUtil.goodClassOrNull(jobConf_, outputFormatSpec_, defaultPackage);
      if (c != null) {
        fmt = c;
      } else {
        fail("-outputformat : class not found : " + outputFormatSpec_);
      }
    }
    if (fmt == null) {
      fmt = TextOutputFormat.class;
    }
    if (lazyOutput_) {
      LazyOutputFormat.setOutputFormatClass(jobConf_, fmt);
    } else {
      jobConf_.setOutputFormat(fmt);
    }

    if (partitionerSpec_!= null) {
      c = StreamUtil.goodClassOrNull(jobConf_, partitionerSpec_, defaultPackage);
      if (c != null) {
        jobConf_.setPartitionerClass(c);
      } else {
        fail("-partitioner : class not found : " + partitionerSpec_);
      }
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
    TreeMap<String,String> sorted = new TreeMap<String,String>();
    for (final Map.Entry<String, String> en : jobConf_)  {
      sorted.put(en.getKey(), en.getValue());
    }
    for (final Map.Entry<String,String> en: sorted.entrySet()) {
      msg(en.getKey() + "=" + en.getValue());
    }
    msg("====");
  }

  protected String getJobTrackerHostPort() {
    return jobConf_.get(JTConfig.JT_IPC_ADDRESS);
  }

  // Based on JobClient
  public int submitAndMonitorJob() throws IOException {

    if (jar_ != null && isLocalHadoop()) {
      // getAbs became required when shell and subvm have different working dirs...
      File wd = new File(".").getAbsoluteFile();
      RunJar.unJar(new File(jar_), wd);
    }

    // if jobConf_ changes must recreate a JobClient
    jc_ = new JobClient(jobConf_);
    running_ = null;
    try {
      running_ = jc_.submitJob(jobConf_);
      jobId_ = running_.getID();
      if (background_) {
        LOG.info("Job is running in background.");
      } else if (!jc_.monitorAndPrintJob(jobConf_, running_)) {
        LOG.error("Job not successful!");
        return 1;
      }
      LOG.info("Output directory: " + output_);
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
    } catch (InterruptedException ie) {
      LOG.error("Error monitoring job : " + ie.getMessage());
      return 6;
    } finally {
      jc_.close();
    }
    return 0;
  }

  protected String[] argv_;
  protected boolean background_;
  protected boolean verbose_;
  protected boolean detailedUsage_;
  protected boolean printUsage = false;
  protected int debug_;

  protected Environment env_;

  protected String jar_;
  protected boolean localHadoop_;
  protected Configuration config_;
  protected JobConf jobConf_;
  protected JobClient jc_;

  // command-line arguments
  protected ArrayList<String> inputSpecs_ = new ArrayList<String>();
  protected TreeSet<String> seenPrimary_ = new TreeSet<String>();
  protected boolean hasSimpleInputSpecs_;
  protected ArrayList<String> packageFiles_ = new ArrayList<String>();
  protected ArrayList<String> shippedCanonFiles_ = new ArrayList<String>();
  //protected TreeMap<String, String> userJobConfProps_ = new TreeMap<String, String>();
  protected String output_;
  protected String mapCmd_;
  protected String comCmd_;
  protected String redCmd_;
  protected String cacheFiles;
  protected String cacheArchives;
  protected URI[] fileURIs;
  protected URI[] archiveURIs;
  protected String inReaderSpec_;
  protected String inputFormatSpec_;
  protected String outputFormatSpec_;
  protected String partitionerSpec_;
  protected String numReduceTasksSpec_;
  protected String additionalConfSpec_;
  protected String mapDebugSpec_;
  protected String reduceDebugSpec_;
  protected String ioSpec_;
  protected boolean lazyOutput_;

  // Use to communicate config to the external processes (ex env.var.HADOOP_USER)
  // encoding "a=b c=d"
  protected String addTaskEnvironment_;

  protected boolean outputSingleNode_;
  protected long minRecWrittenToEnableSkip_;

  protected RunningJob running_;
  protected JobID jobId_;
  protected static final String LINK_URI = "You need to specify the uris as scheme://path#linkname," +
    "Please specify a different link name for all of your caching URIs";

}
