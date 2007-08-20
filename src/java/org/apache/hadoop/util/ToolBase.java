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

/*************************************************************
 * @deprecated This class is depracated. Classes 
 * extending ToolBase should rather implement {@link Tool} 
 * interface, and use {@link ToolRunner} for execution 
 * functionality. Alternativelly, {@link GenericOptionsParser} 
 * can be used to parse generic arguments related to hadoop 
 * framework. 
 */
@Deprecated
public abstract class ToolBase implements Tool {
  
  public Configuration conf;

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return conf;
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
    return ToolRunner.run(this, args);
  }

}
