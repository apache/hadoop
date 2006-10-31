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

import java.io.IOException;

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

/*************************************************************
 * This is a base class to support generic commonad options.
 * Generic command options allow a user to specify a namenode,
 * a job tracker etc. Generic options supported are 
 * <p>-conf <configuration file>     specify an application configuration file
 * <p>-D <property=value>            use value for given property
 * <p>-fs <local|namenode:port>      specify a namenode
 * <p>-jt <local|jobtracker:port>    specify a job tracker
 * <br>
 * <p>The general command line syntax is
 * <p>bin/hadoop command [genericOptions] [commandOptions]
 * 
 * <p>For every tool that inherits from ToolBase, generic options are 
 * handled by ToolBase while command options are passed to the tool.
 * Generic options handling is implemented using Common CLI.
 * 
 * <p>Tools that inherit from ToolBase in Hadoop are
 * DFSShell, DFSck, JobClient, and CopyFiles.
 * <br>
 * <p>Examples using generic options are
 * <p>bin/hadoop dfs -fs darwin:8020 -ls /data
 * <p><blockquote><pre>
 *     list /data directory in dfs with namenode darwin:8020
 * </pre></blockquote>
 * <p>bin/hadoop dfs -D fs.default.name=darwin:8020 -ls /data
 * <p><blockquote><pre>
 *     list /data directory in dfs with namenode darwin:8020
 * </pre></blockquote>
 * <p>bin/hadoop dfs -conf hadoop-site.xml -ls /data
 * <p><blockquote><pre>
 *     list /data directory in dfs with conf specified in hadoop-site.xml
 * </pre></blockquote>
 * <p>bin/hadoop job -D mapred.job.tracker=darwin:50020 -submit job.xml
 * <p><blockquote><pre>
 *     submit a job to job tracker darwin:50020
 * </pre></blockquote>
 * <p>bin/hadoop job -jt darwin:50020 -submit job.xml
 * <p><blockquote><pre>
 *     submit a job to job tracker darwin:50020
 * </pre></blockquote>
 * <p>bin/hadoop job -jt local -submit job.xml
 * <p><blockquote><pre>
 *     submit a job to local runner
 * </pre></blockquote>
 *        
 * @author hairong
 *
 */
public abstract class ToolBase implements Tool {
    private static final Log LOG = LogFactory.getLog(
            "org.apache.hadoop.util.ToolBase");
    public Configuration conf;

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getConf() {
        return conf;
    }
    
    /*
     * Specify properties of each generic option
     */
    static private Options buildGeneralOptions() {
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
                .withDescription("specify an application configuration file" )
                .create("conf");
        Option property = OptionBuilder.withArgName("property=value")
                              .hasArgs()
                              .withArgPattern("=", 1)
                              .withDescription("use value for given property")
                              .create('D');
        Options opts = new Options();
        opts.addOption(fs);
        opts.addOption(jt);
        opts.addOption(oconf);
        opts.addOption(property);
        
        return opts;
    }
    
    /*
     * Modify configuration according user-specified generic options
     * @param conf Configuration to be modified
     * @param line User-specified generic options
     */
    static private void processGeneralOptions( Configuration conf,
                                               CommandLine line ) {
        if(line.hasOption("fs")) {
            conf.set("fs.default.name", line.getOptionValue("fs"));
        }
        
        if(line.hasOption("jt")) {
            conf.set("mapred.job.tracker", line.getOptionValue("jt"));
        }
        if(line.hasOption("conf")) {
            conf.addFinalResource(new Path(line.getOptionValue("conf")));
        }
        if(line.hasOption('D')) {
            String[] property = line.getOptionValues('D');
            for(int i=0; i<property.length-1; i=i+2) {
                if(property[i]!=null)
                    conf.set(property[i], property[i+1]);
            }
         }           
    }
 
    /**
     * Parse the user-specified options, get the generic options, and modify
     * configuration accordingly
     * @param conf Configuration to be modified
     * @param args User-specified arguments
     * @return Commoand-specific arguments
     */
    static private String[] parseGeneralOptions( Configuration conf, 
                 String[] args ) {
        Options opts = buildGeneralOptions();
        CommandLineParser parser = new GnuParser();
        try {
          CommandLine line = parser.parse( opts, args, true );
          processGeneralOptions( conf, line );
          return line.getArgs();
        } catch(ParseException e) {
          LOG.warn("options parsing failed: "+e.getMessage());

          HelpFormatter formatter = new HelpFormatter();
          formatter.printHelp("general options are: ", opts);
        }
        return args;
    }

    /**
     * Work as a main program: execute a command and handle exception if any
     * @param conf Application default configuration
     * @param args User-specified arguments
     * @throws Exception
     * @return exit code to be passed to a caller. General contract is that code
     * equal zero signifies a normal return, negative values signify errors, and
     * positive non-zero values can be used to return application-specific codes.
     */
    public final int doMain(Configuration conf, String[] args) throws Exception {
        String [] commandOptions = parseGeneralOptions(conf, args);
        setConf(conf);
        return this.run(commandOptions);
    }

}
