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

package org.apache.hadoop.mapred;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskLog.LogName;
import org.apache.hadoop.mapreduce.ID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;

@SuppressWarnings("deprecation")
public class MapReduceChildJVM {

  private static String getTaskLogFile(LogName filter) {
    return ApplicationConstants.LOG_DIR_EXPANSION_VAR + Path.SEPARATOR + 
        filter.toString();
  }

  private static String getChildEnv(JobConf jobConf, boolean isMap) {
    if (isMap) {
      return jobConf.get(JobConf.MAPRED_MAP_TASK_ENV,
          jobConf.get(JobConf.MAPRED_TASK_ENV));
    }
    return jobConf.get(JobConf.MAPRED_REDUCE_TASK_ENV,
        jobConf.get(JobConf.MAPRED_TASK_ENV));
  }

  private static String getChildLogLevel(JobConf conf, boolean isMap) {
    if (isMap) {
      return conf.get(
          MRJobConfig.MAP_LOG_LEVEL, 
          JobConf.DEFAULT_LOG_LEVEL.toString()
          );
    } else {
      return conf.get(
          MRJobConfig.REDUCE_LOG_LEVEL, 
          JobConf.DEFAULT_LOG_LEVEL.toString()
          );
    }
  }
  
  public static void setVMEnv(Map<String, String> environment,
      Task task) {

    JobConf conf = task.conf;

    // Add the env variables passed by the user
    String mapredChildEnv = getChildEnv(conf, task.isMapTask());
    Apps.setEnvFromInputString(environment, mapredChildEnv);

    // Set logging level in the environment.
    // This is so that, if the child forks another "bin/hadoop" (common in
    // streaming) it will have the correct loglevel.
    environment.put(
        "HADOOP_ROOT_LOGGER", 
        getChildLogLevel(conf, task.isMapTask()) + ",CLA"); 

    // TODO: The following is useful for instance in streaming tasks. Should be
    // set in ApplicationMaster's env by the RM.
    String hadoopClientOpts = System.getenv("HADOOP_CLIENT_OPTS");
    if (hadoopClientOpts == null) {
      hadoopClientOpts = "";
    } else {
      hadoopClientOpts = hadoopClientOpts + " ";
    }
    // FIXME: don't think this is also needed given we already set java
    // properties.
    long logSize = TaskLog.getTaskLogLength(conf);
    Vector<String> logProps = new Vector<String>(4);
    setupLog4jProperties(task, logProps, logSize);
    Iterator<String> it = logProps.iterator();
    StringBuffer buffer = new StringBuffer();
    while (it.hasNext()) {
      buffer.append(" " + it.next());
    }
    hadoopClientOpts = hadoopClientOpts + buffer.toString();
    environment.put("HADOOP_CLIENT_OPTS", hadoopClientOpts);

    // Add stdout/stderr env
    environment.put(
        MRJobConfig.STDOUT_LOGFILE_ENV,
        getTaskLogFile(TaskLog.LogName.STDOUT)
        );
    environment.put(
        MRJobConfig.STDERR_LOGFILE_ENV,
        getTaskLogFile(TaskLog.LogName.STDERR)
        );
    environment.put(MRJobConfig.APPLICATION_ATTEMPT_ID_ENV, 
        	conf.get(MRJobConfig.APPLICATION_ATTEMPT_ID).toString());
  }

  private static String getChildJavaOpts(JobConf jobConf, boolean isMapTask) {
    String userClasspath = "";
    String adminClasspath = "";
    if (isMapTask) {
      userClasspath = 
          jobConf.get(
              JobConf.MAPRED_MAP_TASK_JAVA_OPTS, 
              jobConf.get(
                  JobConf.MAPRED_TASK_JAVA_OPTS, 
                  JobConf.DEFAULT_MAPRED_TASK_JAVA_OPTS)
          );
      adminClasspath = 
          jobConf.get(
              MRJobConfig.MAPRED_MAP_ADMIN_JAVA_OPTS,
              MRJobConfig.DEFAULT_MAPRED_ADMIN_JAVA_OPTS);
    } else {
      userClasspath =
          jobConf.get(
              JobConf.MAPRED_REDUCE_TASK_JAVA_OPTS, 
              jobConf.get(
                  JobConf.MAPRED_TASK_JAVA_OPTS,
                  JobConf.DEFAULT_MAPRED_TASK_JAVA_OPTS)
              );
      adminClasspath =
          jobConf.get(
              MRJobConfig.MAPRED_REDUCE_ADMIN_JAVA_OPTS,
              MRJobConfig.DEFAULT_MAPRED_ADMIN_JAVA_OPTS);
    }
    
    // Add admin classpath first so it can be overridden by user.
    return adminClasspath + " " + userClasspath;
  }

  private static void setupLog4jProperties(Task task,
      Vector<String> vargs,
      long logSize) {
    String logLevel = getChildLogLevel(task.conf, task.isMapTask()); 
    MRApps.addLog4jSystemProperties(logLevel, logSize, vargs);
  }

  public static List<String> getVMCommand(
      InetSocketAddress taskAttemptListenerAddr, Task task, 
      ID jvmID) {

    TaskAttemptID attemptID = task.getTaskID();
    JobConf conf = task.conf;

    Vector<String> vargs = new Vector<String>(8);

    vargs.add(Environment.JAVA_HOME.$() + "/bin/java");

    // Add child (task) java-vm options.
    //
    // The following symbols if present in mapred.{map|reduce}.child.java.opts 
    // value are replaced:
    // + @taskid@ is interpolated with value of TaskID.
    // Other occurrences of @ will not be altered.
    //
    // Example with multiple arguments and substitutions, showing
    // jvm GC logging, and start of a passwordless JVM JMX agent so can
    // connect with jconsole and the likes to watch child memory, threads
    // and get thread dumps.
    //
    //  <property>
    //    <name>mapred.map.child.java.opts</name>
    //    <value>-Xmx 512M -verbose:gc -Xloggc:/tmp/@taskid@.gc \
    //           -Dcom.sun.management.jmxremote.authenticate=false \
    //           -Dcom.sun.management.jmxremote.ssl=false \
    //    </value>
    //  </property>
    //
    //  <property>
    //    <name>mapred.reduce.child.java.opts</name>
    //    <value>-Xmx 1024M -verbose:gc -Xloggc:/tmp/@taskid@.gc \
    //           -Dcom.sun.management.jmxremote.authenticate=false \
    //           -Dcom.sun.management.jmxremote.ssl=false \
    //    </value>
    //  </property>
    //
    String javaOpts = getChildJavaOpts(conf, task.isMapTask());
    javaOpts = javaOpts.replace("@taskid@", attemptID.toString());
    String [] javaOptsSplit = javaOpts.split(" ");
    for (int i = 0; i < javaOptsSplit.length; i++) {
      vargs.add(javaOptsSplit[i]);
    }

    Path childTmpDir = new Path(Environment.PWD.$(),
        YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR);
    vargs.add("-Djava.io.tmpdir=" + childTmpDir);

    // Setup the log4j prop
    long logSize = TaskLog.getTaskLogLength(conf);
    setupLog4jProperties(task, vargs, logSize);

    if (conf.getProfileEnabled()) {
      if (conf.getProfileTaskRange(task.isMapTask()
                                   ).isIncluded(task.getPartition())) {
        vargs.add(
            String.format(
                conf.getProfileParams(), 
                getTaskLogFile(TaskLog.LogName.PROFILE)
                )
            );
        if (task.isMapTask()) {
          vargs.add(conf.get(MRJobConfig.TASK_MAP_PROFILE_PARAMS, ""));
        }
        else {
          vargs.add(conf.get(MRJobConfig.TASK_REDUCE_PROFILE_PARAMS, ""));
        }
        
      }
    }

    // Add main class and its arguments 
    vargs.add(YarnChild.class.getName());  // main of Child
    // pass TaskAttemptListener's address
    vargs.add(taskAttemptListenerAddr.getAddress().getHostAddress()); 
    vargs.add(Integer.toString(taskAttemptListenerAddr.getPort())); 
    vargs.add(attemptID.toString());                      // pass task identifier

    // Finally add the jvmID
    vargs.add(String.valueOf(jvmID.getId()));
    vargs.add("1>" + getTaskLogFile(TaskLog.LogName.STDOUT));
    vargs.add("2>" + getTaskLogFile(TaskLog.LogName.STDERR));

    // Final commmand
    StringBuilder mergedCommand = new StringBuilder();
    for (CharSequence str : vargs) {
      mergedCommand.append(str).append(" ");
    }
    Vector<String> vargsFinal = new Vector<String>(1);
    vargsFinal.add(mergedCommand.toString());
    return vargsFinal;
  }
}
