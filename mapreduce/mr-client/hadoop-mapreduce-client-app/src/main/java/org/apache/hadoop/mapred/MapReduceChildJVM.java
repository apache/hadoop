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

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.TaskLog.LogName;
import org.apache.hadoop.mapreduce.ID;
import org.apache.hadoop.util.StringUtils;

public class MapReduceChildJVM {
  private static final String SYSTEM_PATH_SEPARATOR = 
    System.getProperty("path.separator");

  private static final Log LOG = LogFactory.getLog(MapReduceChildJVM.class);

  private static File getTaskLogFile(String logDir, LogName filter) {
    return new File(logDir, filter.toString());
  }

  private static String getChildEnv(JobConf jobConf, boolean isMap) {
    if (isMap) {
      return jobConf.get(JobConf.MAPRED_MAP_TASK_ENV,
          jobConf.get(JobConf.MAPRED_TASK_ENV));
    }
    return jobConf.get(JobConf.MAPRED_REDUCE_TASK_ENV,
        jobConf.get(jobConf.MAPRED_TASK_ENV));
  }

  public static void setVMEnv(Map<String, String> env,
      List<String> classPaths, String pwd, String containerLogDir,
      String nmLdLibraryPath, Task task, CharSequence applicationTokensFile) {

    JobConf conf = task.conf;

    // Add classpath.
    CharSequence cp = env.get("CLASSPATH");
    String classpath = StringUtils.join(SYSTEM_PATH_SEPARATOR, classPaths);
    if (null == cp) {
      env.put("CLASSPATH", classpath);
    } else {
      env.put("CLASSPATH", classpath + SYSTEM_PATH_SEPARATOR + cp);
    }

    /////// Environmental variable LD_LIBRARY_PATH
    StringBuilder ldLibraryPath = new StringBuilder();

    ldLibraryPath.append(nmLdLibraryPath);
    ldLibraryPath.append(SYSTEM_PATH_SEPARATOR);
    ldLibraryPath.append(pwd);
    env.put("LD_LIBRARY_PATH", ldLibraryPath.toString());
    /////// Environmental variable LD_LIBRARY_PATH

    // for the child of task jvm, set hadoop.root.logger
    env.put("HADOOP_ROOT_LOGGER", "DEBUG,CLA"); // TODO: Debug

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
    setupLog4jProperties(logProps, logSize, containerLogDir);
    Iterator<String> it = logProps.iterator();
    while (it.hasNext()) {
      hadoopClientOpts += " " + it.next();
    }

    env.put("HADOOP_CLIENT_OPTS", hadoopClientOpts);

    // add the env variables passed by the user
    String mapredChildEnv = getChildEnv(conf, task.isMapTask());
    if (mapredChildEnv != null && mapredChildEnv.length() > 0) {
      String childEnvs[] = mapredChildEnv.split(",");
      for (String cEnv : childEnvs) {
        String[] parts = cEnv.split("="); // split on '='
        String value = (String) env.get(parts[0]);
        if (value != null) {
          // replace $env with the child's env constructed by tt's
          // example LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/tmp
          value = parts[1].replace("$" + parts[0], value);
        } else {
          // this key is not configured by the tt for the child .. get it 
          // from the tt's env
          // example PATH=$PATH:/tmp
          value = System.getenv(parts[0]); // Get from NM?
          if (value != null) {
            // the env key is present in the tt's env
            value = parts[1].replace("$" + parts[0], value);
          } else {
            // the env key is note present anywhere .. simply set it
            // example X=$X:/tmp or X=/tmp
            value = parts[1].replace("$" + parts[0], "");
          }
        }
        env.put(parts[0], value);
      }
    }

    // TODO: Put a random pid in env for now.
    // Long term we will need to get it from the Child
    env.put("JVM_PID", "12344");

    env.put(Constants.STDOUT_LOGFILE_ENV,
        getTaskLogFile(containerLogDir, TaskLog.LogName.STDOUT).toString());
    env.put(Constants.STDERR_LOGFILE_ENV,
        getTaskLogFile(containerLogDir, TaskLog.LogName.STDERR).toString());
  }

  private static String getChildJavaOpts(JobConf jobConf, boolean isMapTask) {
    if (isMapTask) {
      return jobConf.get(JobConf.MAPRED_MAP_TASK_JAVA_OPTS, jobConf.get(
          JobConf.MAPRED_TASK_JAVA_OPTS,
          JobConf.DEFAULT_MAPRED_TASK_JAVA_OPTS));
    }
    return jobConf
        .get(JobConf.MAPRED_REDUCE_TASK_JAVA_OPTS, jobConf.get(
            JobConf.MAPRED_TASK_JAVA_OPTS,
            JobConf.DEFAULT_MAPRED_TASK_JAVA_OPTS));
  }

  private static void setupLog4jProperties(Vector<String> vargs,
      long logSize, String containerLogDir) {
    vargs.add("-Dlog4j.configuration=container-log4j.properties");
    vargs.add("-Dhadoop.yarn.mr.containerLogDir=" + containerLogDir);
    vargs.add("-Dhadoop.yarn.mr.totalLogFileSize=" + logSize);
  }

  public static List<String> getVMCommand(
      InetSocketAddress taskAttemptListenerAddr, Task task, String javaHome,
      String workDir, String logDir, String childTmpDir, ID jvmID) {

    TaskAttemptID attemptID = task.getTaskID();
    JobConf conf = task.conf;

    Vector<String> vargs = new Vector<String>(8);

    vargs.add(javaHome + "/bin/java");

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
    
    // Add java.library.path; necessary for loading native libraries.
    //
    // 1. We add the 'cwd' of the task to it's java.library.path to help 
    //    users distribute native libraries via the DistributedCache.
    // 2. The user can also specify extra paths to be added to the 
    //    java.library.path via mapred.{map|reduce}.child.java.opts.
    //
    String libraryPath = workDir;
    boolean hasUserLDPath = false;
    for(int i=0; i<javaOptsSplit.length ;i++) { 
      if(javaOptsSplit[i].startsWith("-Djava.library.path=")) {
        // TODO: Does the above take care of escaped space chars
        javaOptsSplit[i] += SYSTEM_PATH_SEPARATOR + libraryPath;
        hasUserLDPath = true;
        break;
      }
    }
    if(!hasUserLDPath) {
      vargs.add("-Djava.library.path=" + libraryPath);
    }
    for (int i = 0; i < javaOptsSplit.length; i++) {
      vargs.add(javaOptsSplit[i]);
    }

    if (childTmpDir != null) {
      vargs.add("-Djava.io.tmpdir=" + childTmpDir);
    }

    // Setup the log4j prop
    long logSize = TaskLog.getTaskLogLength(conf);
    setupLog4jProperties(vargs, logSize, logDir);

    if (conf.getProfileEnabled()) {
      if (conf.getProfileTaskRange(task.isMapTask()
                                   ).isIncluded(task.getPartition())) {
        File prof = getTaskLogFile(logDir, TaskLog.LogName.PROFILE);
        vargs.add(String.format(conf.getProfileParams(), prof.toString()));
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
    vargs.add("1>" + getTaskLogFile(logDir, TaskLog.LogName.STDERR));
    vargs.add("2>" + getTaskLogFile(logDir, TaskLog.LogName.STDOUT));

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
