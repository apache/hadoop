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

import org.apache.hadoop.conf.Configuration;

/**
 * ToolRunner can be used to run classes implementing {@link Tool}
 * interface. Static method {@link #run(Tool, String[])} is used.
 * {@link GenericOptionsParser} is used to parse the hadoop generic 
 * arguments to modify the <code>Configuration</code>.
 */
public class ToolRunner {
 
  /**
   * Runs the given Tool by {@link Tool#run(String[])}, with the 
   * given arguments. Uses the given configuration, or builds one if null.
   * Sets the possibly modified version of the conf by Tool#setConf()  
   * 
   * @param conf Configuration object to use
   * @param tool The Tool to run
   * @param args the arguments to the tool(including generic arguments
   * , see {@link GenericOptionsParser})
   * @return exit code of the {@link Tool#run(String[])} method
   */
  public static int run(Configuration conf, Tool tool, String[] args) 
    throws Exception{
    if(conf == null) {
      conf = new Configuration();
    }
    GenericOptionsParser parser = new GenericOptionsParser(conf, args);
    //set the configuration back, so that Tool can configure itself
    tool.setConf(conf);
    
    //get the args w/o generic hadoop args
    String[] toolArgs = parser.getRemainingArgs();
    return tool.run(toolArgs);
  }
  
  /**
   * Runs the tool with the tool's Configuration
   * Equivalent to <code>run(tool.getConf(), tool, args)</code>.
   * @param tool The Tool to run
   * @param args the arguments to the tool(including generic arguments
   * , see {@link GenericOptionsParser})
   * @return exit code of the {@link Tool#run(String[])} method
   */
  public static int run(Tool tool, String[] args) 
    throws Exception{
    return run(tool.getConf(), tool, args);
  }
  
}
