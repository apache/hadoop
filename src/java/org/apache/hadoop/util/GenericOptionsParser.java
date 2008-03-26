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
package org.apache.hadoop.util;

import java.io.PrintStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

/**
 * <code>GenericOptionsParser</code> is a utility to parse command line
 * arguments generic to the Hadoop framework. 
 * 
 * <code>GenericOptionsParser</code> recognizes several standarad command 
 * line arguments, enabling applications to easily specify a namenode, a 
 * jobtracker, additional configuration resources etc.
 * 
 * <h4 id="GenericOptions">Generic Options</h4>
 * 
 * <p>The supported generic options are:</p>
 * <p><blockquote><pre>
 *     -conf &lt;configuration file&gt;     specify a configuration file
 *     -D &lt;property=value&gt;            use value for given property
 *     -fs &lt;local|namenode:port&gt;      specify a namenode
 *     -jt &lt;local|jobtracker:port&gt;    specify a job tracker
 * </pre></blockquote></p>
 * 
 * <p>The general command line syntax is:</p>
 * <p><tt><pre>
 * bin/hadoop command [genericOptions] [commandOptions]
 * </pre></tt></p>
 * 
 * <p>Generic command line arguments <strong>might</strong> modify 
 * <code>Configuration </code> objects, given to constructors.</p>
 * 
 * <p>The functionality is implemented using Commons CLI.</p>
 *
 * <p>Examples:</p>
 * <p><blockquote><pre>
 * $ bin/hadoop dfs -fs darwin:8020 -ls /data
 * list /data directory in dfs with namenode darwin:8020
 * 
 * $ bin/hadoop dfs -D fs.default.name=darwin:8020 -ls /data
 * list /data directory in dfs with namenode darwin:8020
 *     
 * $ bin/hadoop dfs -conf hadoop-site.xml -ls /data
 * list /data directory in dfs with conf specified in hadoop-site.xml
 *     
 * $ bin/hadoop job -D mapred.job.tracker=darwin:50020 -submit job.xml
 * submit a job to job tracker darwin:50020
 *     
 * $ bin/hadoop job -jt darwin:50020 -submit job.xml
 * submit a job to job tracker darwin:50020
 *     
 * $ bin/hadoop job -jt local -submit job.xml
 * submit a job to local runner
 * </pre></blockquote></p>
 *
 * @see Tool
 * @see ToolRunner
 */
public class GenericOptionsParser {

  private static final Log LOG = LogFactory.getLog(GenericOptionsParser.class);

  private CommandLine commandLine;

  /** 
   * Create a <code>GenericOptionsParser<code> to parse only the generic Hadoop  
   * arguments. 
   * 
   * The array of string arguments other than the generic arguments can be 
   * obtained by {@link #getRemainingArgs()}.
   * 
   * @param conf the <code>Configuration</code> to modify.
   * @param args command-line arguments.
   */
  public GenericOptionsParser(Configuration conf, String[] args) {
    this(conf, new Options(), args); 
  }

  /** 
   * Create a <code>GenericOptionsParser</code> to parse given options as well 
   * as generic Hadoop options. 
   * 
   * The resulting <code>CommandLine</code> object can be obtained by 
   * {@link #getCommandLine()}.
   * 
   * @param conf the configuration to modify  
   * @param options options built by the caller 
   * @param args User-specified arguments
   */
  public GenericOptionsParser(Configuration conf, Options options, String[] args) {
    parseGeneralOptions(options, conf, args);
  }

  /**
   * Returns an array of Strings containing only application-specific arguments.
   * 
   * @return array of <code>String</code>s containing the un-parsed arguments.
   */
  public String[] getRemainingArgs() {
    return commandLine.getArgs();
  }

  /**
   * Returns the commons-cli <code>CommandLine</code> object 
   * to process the parsed arguments. 
   * 
   * Note: If the object is created with 
   * {@link #GenericOptionsParser(Configuration, String[])}, then returned 
   * object will only contain parsed generic options.
   * 
   * @return <code>CommandLine</code> representing list of arguments 
   *         parsed against Options descriptor.
   */
  public CommandLine getCommandLine() {
    return commandLine;
  }

  /**
   * Specify properties of each generic option
   */
  @SuppressWarnings("static-access")
  private Options buildGeneralOptions(Options opts) {
    Option fs = OptionBuilder.withArgName("local|namenode:port")
    .hasArg()
    .withDescription("specify a namenode")
    .create("fs");
    Option jt = OptionBuilder.withArgName("local|jobtracker:port")
    .hasArg()
    .withDescription("specify a job tracker")
    .create("jt");
    Option oconf = OptionBuilder.withArgName("configuration file")
    .hasArg()
    .withDescription("specify an application configuration file")
    .create("conf");
    Option property = OptionBuilder.withArgName("property=value")
    .hasArgs()
    .withArgPattern("=", 1)
    .withDescription("use value for given property")
    .create('D');

    opts.addOption(fs);
    opts.addOption(jt);
    opts.addOption(oconf);
    opts.addOption(property);
    
    return opts;
  }

  /**
   * Modify configuration according user-specified generic options
   * @param conf Configuration to be modified
   * @param line User-specified generic options
   */
  private void processGeneralOptions(Configuration conf,
      CommandLine line) {
    if (line.hasOption("fs")) {
      FileSystem.setDefaultUri(conf, line.getOptionValue("fs"));
    }

    if (line.hasOption("jt")) {
      conf.set("mapred.job.tracker", line.getOptionValue("jt"));
    }
    if (line.hasOption("conf")) {
      conf.addResource(new Path(line.getOptionValue("conf")));
    }
    if (line.hasOption('D')) {
      String[] property = line.getOptionValues('D');
      for(int i=0; i<property.length-1; i=i+2) {
        if (property[i]!=null)
          conf.set(property[i], property[i+1]);
      }
    }
  }

  /**
   * Parse the user-specified options, get the generic options, and modify
   * configuration accordingly
   * @param conf Configuration to be modified
   * @param args User-specified arguments
   * @return Command-specific arguments
   */
  private String[] parseGeneralOptions(Options opts, Configuration conf, 
      String[] args) {
    opts = buildGeneralOptions(opts);
    CommandLineParser parser = new GnuParser();
    try {
      commandLine = parser.parse(opts, args, true);
      processGeneralOptions(conf, commandLine);
      return commandLine.getArgs();
    } catch(ParseException e) {
      LOG.warn("options parsing failed: "+e.getMessage());

      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("general options are: ", opts);
    }
    return args;
  }

  /**
   * Print the usage message for generic command-line options supported.
   * 
   * @param out stream to print the usage message to.
   */
  public static void printGenericCommandUsage(PrintStream out) {
    out.println("Generic options supported are");
    out.println("-conf <configuration file>     specify an application configuration file");
    out.println("-D <property=value>            use value for given property");
    out.println("-fs <local|namenode:port>      specify a namenod");
    out.println("-jt <local|jobtracker:port>    specify a job tracker\n");
    out.println("The general command line syntax is");
    out.println("bin/hadoop command [genericOptions] [commandOptions]\n");
  }
  
}
