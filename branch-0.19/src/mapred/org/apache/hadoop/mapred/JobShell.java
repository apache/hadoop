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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** Provide command line parsing for JobSubmission 
 *  job submission looks like 
 *  hadoop jar -libjars <comma seperated jars> -archives <comma seperated archives> 
 *  -files <comma seperated files> inputjar args
 */
public class JobShell extends Configured implements Tool {
  public JobShell() {this(null);};
  
  public JobShell(Configuration conf) {
    super(conf);
  }
  
  protected void init() throws IOException {
    getConf().setQuietMode(false);
  }
  
  /**
   * run method from Tool
   */
  public int run(String argv[]) throws Exception {
    int exitCode = -1;
    Configuration conf = getConf();
    try{
      JobClient.setCommandLineConfig(conf);
      try {
        RunJar.main(argv);
        exitCode = 0;
      } catch(Throwable th) {
        System.err.println(StringUtils.stringifyException(th));
      }
    } catch(RuntimeException re) {
      exitCode = -1;
      System.err.println(re.getLocalizedMessage());
    }
    return exitCode;
  }
  
  public static void main(String[] argv) throws Exception {
    JobShell jshell = new JobShell();
    int status = ToolRunner.run(jshell, argv);
    System.exit(status);
  }
}
