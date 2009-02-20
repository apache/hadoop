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

package org.apache.hadoop.hive.ql.exec;

import java.io.File;
import java.io.FileOutputStream;
import java.io.Serializable;

import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.exec.Utilities.*;
import org.apache.hadoop.hive.conf.HiveConf;

import org.apache.commons.lang.StringUtils;

/**
 * Alternate implementation (to ExecDriver) of spawning a mapreduce task that runs it from
 * a separate jvm. The primary issue with this is the inability to control logging from
 * a separate jvm in a consistent manner
 **/
public class MapRedTask extends Task<mapredWork> implements Serializable {
    
  private static final long serialVersionUID = 1L;

  public int execute() {

    try {
      // enable assertion
      String hadoopExec = conf.getVar(HiveConf.ConfVars.HADOOPBIN);
      String hiveJar = conf.getJar();
      String hiveConfArgs = ExecDriver.generateCmdLine(conf);
      String auxJars = conf.getAuxJars();
      if (!StringUtils.isEmpty(auxJars)) {
        auxJars = " -libjars " + auxJars + " ";
      } else {
        auxJars = " ";
      }

      mapredWork plan = getWork();

      File planFile = File.createTempFile("plan", ".xml");
      LOG.info("Generating plan file " + planFile.toString());
      FileOutputStream out = new FileOutputStream(planFile);
      Utilities.serializeMapRedWork(plan, out);
    
      String cmdLine = hadoopExec + " jar " + auxJars + " " + hiveJar + " org.apache.hadoop.hive.ql.exec.ExecDriver -plan " + planFile.toString() + " " + hiveConfArgs;
      
      LOG.info("Executing: " + cmdLine);
      Process executor = Runtime.getRuntime().exec(cmdLine);


      StreamPrinter outPrinter = new StreamPrinter(executor.getInputStream(), null, System.out);
      StreamPrinter errPrinter = new StreamPrinter(executor.getErrorStream(), null, System.err);
      
      outPrinter.start();
      errPrinter.start();
    
      int exitVal = executor.waitFor();

      if(exitVal != 0) {
        LOG.error("Execution failed with exit status: " + exitVal);
      } else {
        LOG.info("Execution completed successfully");
      }

      return exitVal;
    }
    catch (Exception e) {
      e.printStackTrace();
      LOG.error("Exception: " + e.getMessage());
      return (1);
    }
  }

  @Override
  public boolean isMapRedTask() {
    return true;
  }

  @Override
  public boolean hasReduce() {
    mapredWork w = getWork();
    return w.getReducer() != null;
  }
}
