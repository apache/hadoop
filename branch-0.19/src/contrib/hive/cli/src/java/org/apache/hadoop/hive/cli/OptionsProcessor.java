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

package org.apache.hadoop.hive.cli;

import java.io.*;
import java.util.*;

import org.apache.commons.cli2.*;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.commons.cli2.option.PropertyOption;
import org.apache.commons.cli2.resource.ResourceConstants;
import org.apache.commons.cli2.OptionException;
import org.apache.commons.logging.*;

import org.apache.hadoop.hive.conf.HiveConf;

public class OptionsProcessor {

  protected static final Log l4j = LogFactory.getLog(OptionsProcessor.class.getName());

  private Parser parser = new Parser();
  private Option confOptions, isSilentOption, execOption, fileOption, isHelpOption;

  /** 
   * shameless cloned from hadoop streaming
   * take in multiple -hiveconf x=y parameters
   */
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

    public boolean canProcess(final WriteableCommandLine commandLine,
                              final String argument) {
      boolean ret = (argument != null) && argument.startsWith(optionString);
        
      return ret;
    }    

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
          
          if(next.indexOf("=") == -1) {
            throw new OptionException(this, ResourceConstants.UNEXPECTED_TOKEN,
                                      "argument: '" + next + "' is not of the form x=y");
          }
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
      } else{
        oldVal.addAll(properties); 
      }
    }
  }

  private Option createBoolOption(DefaultOptionBuilder builder, String longName,
                                  String shortName, String desc){
    builder.reset();
    if(longName == null) {
      return builder.withShortName(shortName).withDescription(desc).create();
    } else {
      return builder.withShortName(shortName).withLongName(longName).withDescription(desc).create();
    }
  }


  private Option createOptionWithArg(DefaultOptionBuilder builder, String longName,
                                     String shortName, String desc, Argument arg) {
    builder.reset();

    DefaultOptionBuilder dob =
      builder.withShortName(shortName).
      withArgument(arg).
      withDescription(desc);

    if(longName != null) 
      dob = dob.withLongName(longName);

    return dob.create();
  }


  public OptionsProcessor() {
    DefaultOptionBuilder builder =
      new DefaultOptionBuilder("-","-", false);    

    ArgumentBuilder argBuilder = new ArgumentBuilder();
    
    //-e
    execOption = createOptionWithArg(builder, "exec", "e", "execute the following command",
                                     argBuilder.withMinimum(1).withMaximum(1).create());


    //-f
    fileOption = createOptionWithArg(builder, "file", "f", "execute commands from the following file",
                                     argBuilder.withMinimum(1).withMaximum(1).create());

    // -S
    isSilentOption = createBoolOption(builder, "silent", "S", "silent mode");

    // -help
    isHelpOption = createBoolOption(builder, "help", "h", "help");
    
    // -hiveconf var=val
    confOptions = new MultiPropertyOption("-hiveconf", "(n=v) Optional. Add or override Hive/Hadoop properties.", 'D');

    new PropertyOption();
    Group allOptions = new GroupBuilder().
      withOption(confOptions).
      withOption(isSilentOption).
      withOption(isHelpOption).
      withOption(execOption).
      withOption(fileOption).
      create();
    parser.setGroup(allOptions);
  }

  private CommandLine cmdLine;
  
  public boolean process_stage1(String [] argv) {
    try {
      cmdLine = parser.parse(argv);

      List<String> hiveConfArgs = (List<String>)cmdLine.getValue(confOptions); 
      if (null != hiveConfArgs){
        for(String s : hiveConfArgs){
          String []parts = s.split("=", 2); 
          System.setProperty(parts[0], parts[1]);
        }
      }
    } catch (OptionException oe) {
      System.err.println(oe.getMessage());
      return false;
    }
    return true;
  }


  public boolean process_stage2(CliSessionState ss) {
    HiveConf hconf = ss.getConf();
    //-S
    ss.setIsSilent(cmdLine.hasOption(isSilentOption));
    //-e
    ss.execString = (String) cmdLine.getValue(execOption);
    //-f
    ss.fileName = (String) cmdLine.getValue(fileOption);
    // -h
    if (cmdLine.hasOption(isHelpOption)) {
      printUsage(null);
      return false;
    }
    if(ss.execString != null && ss.fileName != null) {
      printUsage("-e and -f option cannot be specified simultaneously");
      return false;
    }

    List<String> hiveConfArgs = (List<String>)cmdLine.getValue(confOptions); 
    if (null != hiveConfArgs){
      for(String s : hiveConfArgs){
        String []parts = s.split("=", 2); 
        ss.cmdProperties.setProperty(parts[0], parts[1]);
      }
    }

    return true;
  }

  public void printUsage (String error) {
    if (error != null) {
      System.err.println("Invalid arguments: " + error);
    }
    System.err.println("");
    System.err.println("Usage: hive [--config confdir] [-hiveconf x=y]* [<-f filename>|<-e query-string>] [-S]");
    System.err.println("");
    System.err.println("  -e 'quoted query string'  Sql from command line"); 
    System.err.println("  -f <filename>             Sql from files");
    System.err.println("  -S                        Silent mode in interactive shell");
    System.err.println("");
    System.err.println("-e and -f cannot be specified together. In the absence of these");
    System.err.println("options, interactive shell is started");
    System.err.println("");
    
  }
}
