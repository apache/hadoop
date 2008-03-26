/* Licensed to the Apache Software Foundation (ASF) under one
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
 
package org.apache.hadoop.mapred;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.*;


/** Provide command line parsing for JobSubmission 
 *  job submission looks like 
 *  hadoop jar -libjars <comma seperated jars> -archives <comma seperated archives> 
 *  -files <comma seperated files> inputjar args
 */
public class JobShell extends Configured implements Tool {
  private FileSystem localFs;
  protected static final Log LOG = LogFactory.getLog(JobShell.class.getName());
  /**
   * cli implementation for 
   * job shell.
   */
  private CommandLineParser parser = new GnuParser();
  private Options opts = new Options();
 
  public JobShell() {this(null);};
  
  public JobShell(Configuration conf) {
    super(conf);
    setUpOptions();
  }
  
  /**
   * a method to create an option
   */
  @SuppressWarnings("static-access")
  private Option createOption(String name, String desc, 
                              String argName, int max) {
    return OptionBuilder.withArgName(argName).
                         hasArg().withDescription(desc).
                         create(name);
  }
  
  
  /**
   * set up options 
   * specific to jobshell
   */
  private void setUpOptions() {
    Option libjar = createOption("libjars",
                                "comma seperated jar files to " +
                                "include in the classpath.",
                                "paths",
                                1);
    Option file = createOption("files",
                               "comma seperated files to be copied to the " +
                               "map reduce cluster.",
                               "paths",
                               1);
    Option archives = createOption("archives",
                                   "comma seperated archives to be unarchives" +
                                   " on the compute machines.",
                                   "paths",
                                   1);
    opts.addOption(libjar);
    opts.addOption(file);
    opts.addOption(archives);
  }
  
  protected void init() throws IOException {
    getConf().setQuietMode(true);
  }
  
  /**
   * takes input as a comma separated list of files
   * and verifies if they exist. It defaults for file:///
   * if the files specified do not have a scheme.
   * it returns the paths uri converted defaulting to file:///.
   * So an input of  /home/user/file1,/home/user/file2 would return
   * file:///home/user/file1,file:///home/user/file2
   * @param files
   * @return
   */
  private String validateFiles(String files) throws IOException {
    if (files == null) 
      return null;
    String[] fileArr = files.split(",");
    String[] finalArr = new String[fileArr.length];
    for (int i =0; i < fileArr.length; i++) {
      String tmp = fileArr[i];
      String finalPath;
      Path path = new Path(tmp);
      URI pathURI =  path.toUri();
      if (pathURI.getScheme() == null) {
        //default to the local file system
        //check if the file exists or not first
        if (!localFs.exists(path)) {
          throw new FileNotFoundException("File " + tmp + " does not exist.");
        }
        finalPath = path.makeQualified(localFs).toString();
      }
      else {
        // check if the file exists in this file system
        // we need to recreate this filesystem object to copy
        // these files to the file system jobtracker is running
        // on.
        FileSystem fs = path.getFileSystem(getConf());
        if (!fs.exists(path)) {
          throw new FileNotFoundException("File " + tmp + " does not exist.");
        }
        finalPath = path.makeQualified(fs).toString();
        try {
          fs.close();
        } catch(IOException e){};
      }
      finalArr[i] = finalPath;
    }
    return StringUtils.arrayToString(finalArr);
  }
  
  /**
   * run method from Tool
   */
  public int run(String argv[]) throws Exception {
    int exitCode = -1;
    Configuration conf = getConf();
    localFs = FileSystem.getLocal(conf);
    CommandLine cmdLine = null;
    try{
      try {
        cmdLine = parser.parse(opts, argv, true);
      } catch(Exception ie) {
        LOG.error(ie.getMessage());
        printUsage();
      }
      // get the options and set it in this 
      // objects conf and then update the jobclient
      // with this config
      if (cmdLine != null) {
        String allFiles = (String) cmdLine.getOptionValue("files");
        String alllibJars = (String) cmdLine.getOptionValue("libjars");
        String allArchives = (String) cmdLine.getOptionValue("archives");
        if (allFiles != null) {
          String allFilesVal = validateFiles(allFiles);
          conf.set("tmpfiles", allFilesVal);
        }
        if (alllibJars != null) {
          String alllibJarsVal = validateFiles(alllibJars);
          conf.set("tmpjars", alllibJarsVal);
        }
        if (allArchives != null) {
          String allArchivesVal = validateFiles(allArchives);
          conf.set("tmparchives", allArchivesVal);
        }
        JobClient.setCommandLineConfig(conf);
        try {
          // pass the rest of arguments to Runjar
          String[] args = cmdLine.getArgs();
          if (args.length == 0) {
           printUsage();
           return -1;
          }
          RunJar.main(cmdLine.getArgs());
          exitCode = 0;
        } catch(Throwable th) {
          System.err.println(StringUtils.stringifyException(th));
        }
      }
      
    } catch(RuntimeException re) {
      exitCode = -1;
      System.err.println(re.getLocalizedMessage());
    }
    return exitCode;
  }
  
  private void printUsage() {
    System.out.println("Usage: $HADOOP_HOME/bin/hadoop \\");
    System.out.println("       [--config dir] jar \\");
    System.out.println("       [-libjars <comma seperated list of jars>] \\");
    System.out.println("       [-archives <comma seperated list of archives>] \\");
    System.out.println("       [-files <comma seperated list of files>] \\");
    System.out.println("       jarFile [mainClass] args");
  }
  
  public static void main(String[] argv) throws Exception {
    JobShell jshell = new JobShell();
    int res;
    res = ToolRunner.run(jshell, argv);
    System.exit(res);
  }
}
