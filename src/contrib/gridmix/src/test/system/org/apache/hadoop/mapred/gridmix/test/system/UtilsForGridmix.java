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
package org.apache.hadoop.mapred.gridmix.test.system;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapred.gridmix.Gridmix;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.JobID;
import java.util.Date;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Arrays;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.io.OutputStream;
import java.util.Set;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.io.File;
import java.io.FileOutputStream;
import org.apache.hadoop.test.system.ProxyUserDefinitions;
import org.apache.hadoop.test.system.ProxyUserDefinitions.GroupsAndHost;

/**
 * Gridmix utilities.
 */
public class UtilsForGridmix {
  private static final Log LOG = LogFactory.getLog(UtilsForGridmix.class);
  private static final Path DEFAULT_TRACES_PATH =
    new Path(System.getProperty("user.dir") + "/src/test/system/resources/");

  /**
   * cleanup the folder or file.
   * @param path - folder or file path.
   * @param conf - cluster configuration 
   * @throws IOException - If an I/O error occurs.
   */
  public static void cleanup(Path path, Configuration conf) 
     throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    fs.delete(path, true);
    fs.close();
  }

  /**
   * Get the login user.
   * @return - login user as string..
   * @throws IOException - if an I/O error occurs.
   */
  public static String getUserName() throws IOException {
    return UserGroupInformation.getLoginUser().getUserName();
  }
  
  /**
   * Get the argument list for gridmix job.
   * @param gridmixDir - gridmix parent directory.
   * @param gridmixRunMode - gridmix modes either 1,2,3.
   * @param values - gridmix runtime values.
   * @param otherArgs - gridmix other generic args.
   * @return - argument list as string array.
   */
  public static String [] getArgsList(Path gridmixDir, int gridmixRunMode, 
                                      String [] values, String [] otherArgs) {
    String [] runtimeArgs = { 
        "-D", GridMixConfig.GRIDMIX_LOG_MODE + "=DEBUG", 
        "-D", GridMixConfig.GRIDMIX_OUTPUT_DIR + "=gridmix", 
        "-D", GridMixConfig.GRIDMIX_JOB_SUBMISSION_QUEUE_IN_TRACE + "=true", 
        "-D", GridMixConfig.GRIDMIX_JOB_TYPE + "=" + values[0], 
        "-D", GridMixConfig.GRIDMIX_USER_RESOLVER + "=" + values[1], 
        "-D", GridMixConfig.GRIDMIX_SUBMISSION_POLICY + "=" + values[2]
    };

    String [] classArgs;
    if ((gridmixRunMode == GridMixRunMode.DATA_GENERATION.getValue() 
       || gridmixRunMode 
       == GridMixRunMode.DATA_GENERATION_AND_RUN_GRIDMIX.getValue()) 
       && values[1].indexOf("RoundRobinUserResolver") > 0) { 
      classArgs = new String[] { 
          "-generate", values[3], 
          "-users", values[4], 
          gridmixDir.toString(), 
          values[5]
      };
    } else if (gridmixRunMode == GridMixRunMode.DATA_GENERATION.getValue() 
              || gridmixRunMode 
              == GridMixRunMode.DATA_GENERATION_AND_RUN_GRIDMIX.getValue()) { 
      classArgs = new String[] { 
          "-generate", values[3], 
          gridmixDir.toString(), 
          values[4]
      };
    } else if (gridmixRunMode == GridMixRunMode.RUN_GRIDMIX.getValue() 
              && values[1].indexOf("RoundRobinUserResolver") > 0) { 
      classArgs = new String[] { 
          "-users", values[3], 
          gridmixDir.toString(), 
          values[4]
      };
    } else { 
      classArgs = new String[] { 
         gridmixDir.toString(),values[3]
      };
    }

    String [] args = new String [runtimeArgs.length + 
       classArgs.length + ((otherArgs != null)?otherArgs.length:0)];
    System.arraycopy(runtimeArgs, 0, args, 0, runtimeArgs.length);

    if (otherArgs != null) {
      System.arraycopy(otherArgs, 0, args, runtimeArgs.length, 
                       otherArgs.length);
      System.arraycopy(classArgs, 0, args, (runtimeArgs.length + 
                       otherArgs.length), classArgs.length);
    } else {
      System.arraycopy(classArgs, 0, args, runtimeArgs.length, 
                       classArgs.length);
    }
    return args;
  }
  
  /**
   * Create a file with specified size in mb.
   * @param sizeInMB - file size in mb.
   * @param inputDir - input directory.
   * @param conf - cluster configuration.
   * @throws Exception - if an exception occurs.
   */
  public static void createFile(int sizeInMB, Path inputDir, 
      Configuration conf) throws Exception {
    Date d = new Date();
    SimpleDateFormat sdf = new SimpleDateFormat("ddMMyy_HHmmssS");
    String formatDate = sdf.format(d);
    FileSystem fs = inputDir.getFileSystem(conf);
    OutputStream out = fs.create(new Path(inputDir,"datafile_" + formatDate));
    final byte[] b = new byte[1024 * 1024];
    for (int index = 0; index < sizeInMB; index++) { 
      out.write(b);
    }    
    out.close();
    fs.close();
  }
  
  /**
   * Create directories for a path.
   * @param path - directories path.
   * @param conf  - cluster configuration.
   * @throws IOException  - if an I/O error occurs.
   */
  public static void createDirs(Path path,Configuration conf) 
     throws IOException { 
    FileSystem fs = path.getFileSystem(conf);
    if (!fs.exists(path)) { 
       fs.mkdirs(path);
       fs.setPermission(path,new FsPermission(FsAction.ALL,
           FsAction.ALL,FsAction.ALL));
    }
  }
  
  /**
   * Run the Gridmix job with given runtime arguments.
   * @param gridmixDir - Gridmix parent directory.
   * @param conf - cluster configuration.
   * @param gridmixRunMode - gridmix run mode either 1,2,3
   * @param runtimeValues -gridmix runtime values.
   * @return - gridmix status either 0 or 1.
   * @throws Exception
   */
  public static int runGridmixJob(Path gridmixDir, Configuration conf, 
     int gridmixRunMode, String [] runtimeValues) throws Exception {
    return runGridmixJob(gridmixDir, conf, gridmixRunMode, runtimeValues, null);
  }
  /**
   * Run the Gridmix job with given runtime arguments.
   * @param gridmixDir - Gridmix parent directory
   * @param conf - cluster configuration.
   * @param gridmixRunMode - gridmix run mode.
   * @param runtimeValues - gridmix runtime values.
   * @param otherArgs - gridmix other generic args.
   * @return - gridmix status either 0 or 1.
   * @throws Exception
   */
  
  public static int runGridmixJob(Path gridmixDir, Configuration conf, 
                                  int gridmixRunMode, String [] runtimeValues, 
                                  String [] otherArgs) throws Exception {
    Path  outputDir = new Path(gridmixDir, "gridmix");
    Path inputDir = new Path(gridmixDir, "input");
    LOG.info("Cleanup the data if data already exists.");
    String modeName = new String();
    switch (gridmixRunMode) { 
      case 1 : 
        cleanup(inputDir, conf);
        cleanup(outputDir, conf);
        modeName = GridMixRunMode.DATA_GENERATION.name();
        break;
      case 2 : 
        cleanup(outputDir, conf);
        modeName = GridMixRunMode.RUN_GRIDMIX.name();
        break;
      case 3 : 
        cleanup(inputDir, conf);
        cleanup(outputDir, conf);
        modeName = GridMixRunMode.DATA_GENERATION_AND_RUN_GRIDMIX.name();
        break;
    }

    final String [] args = 
        UtilsForGridmix.getArgsList(gridmixDir, gridmixRunMode, 
                                    runtimeValues, otherArgs);
    Gridmix gridmix = new Gridmix();
    LOG.info("Submit a Gridmix job in " + runtimeValues[1] 
            + " mode for " + modeName);
    int exitCode = ToolRunner.run(conf, gridmix, args);
    return exitCode;
  }

  /**
   * Get the proxy users file.
   * @param conf - cluster configuration.
   * @return String - proxy users file.
   * @Exception - if no proxy users found in configuration.
   */
  public static String getProxyUsersFile(Configuration conf) 
      throws Exception {
     ProxyUserDefinitions pud = getProxyUsersData(conf);
     String fileName = buildProxyUsersFile(pud.getProxyUsers());
     if (fileName == null) { 
        LOG.error("Proxy users file not found.");
        throw new Exception("Proxy users file not found.");
     } else { 
        return fileName;
     }
  }
  
  /**
  * List the current gridmix jobid's.
  * @param client - job client.
  * @param execJobCount - number of executed jobs.
  * @return - list of gridmix jobid's.
  */
 public static List<JobID> listGridmixJobIDs(JobClient client, 
     int execJobCount) throws Exception { 
   List<JobID> jobids = new ArrayList<JobID>();
   JobStatus [] jobStatus = client.getAllJobs();
   int numJobs = jobStatus.length;
   for (int index = 0; index < 31; index++) {
     Thread.sleep(1000);
     jobStatus = client.getAllJobs();
     numJobs = jobStatus.length;
   }
   for (int index = 1; index <= execJobCount; index++) {
     JobStatus js = jobStatus[numJobs - index];
     JobID jobid = js.getJobID();
     RunningJob runJob = client.getJob(jobid.toString());
     String jobName = runJob.getJobName();
     if (!jobName.equals("GRIDMIX_GENERATE_INPUT_DATA") && 
         !jobName.equals("GRIDMIX_GENERATE_DISTCACHE_DATA")) {
       jobids.add(jobid);
     }
   }
   return (jobids.size() == 0)? null : jobids;
 }

 /**
  * List the proxy users. 
  * @param conf
  * @return
  * @throws Exception
  */
 public static List<String> listProxyUsers(Configuration conf,
     String loginUser) throws Exception {
   List<String> proxyUsers = new ArrayList<String>();
   ProxyUserDefinitions pud = getProxyUsersData(conf);
   Map<String, GroupsAndHost> usersData = pud.getProxyUsers();
   Collection users = usersData.keySet();
   Iterator<String> itr = users.iterator();
   while (itr.hasNext()) { 
     String user = itr.next();
     if (!user.equals(loginUser)){ proxyUsers.add(user); };
   }
   return proxyUsers;
 }

  private static String buildProxyUsersFile(final Map<String, GroupsAndHost> 
      proxyUserData) throws Exception { 
     FileOutputStream fos = null;
     File file = null;
     StringBuffer input = new StringBuffer();
     Set users = proxyUserData.keySet();
     Iterator itr = users.iterator();
     while (itr.hasNext()) { 
       String user = itr.next().toString();
       if (!user.equals(
           UserGroupInformation.getLoginUser().getShortUserName())) {
         input.append(user);
         final GroupsAndHost gah = proxyUserData.get(user);
         final List <String> groups = gah.getGroups();
         for (String group : groups) { 
           input.append(",");
           input.append(group);
         }
         input.append("\n");
       }
     }
     if (input.length() > 0) { 
        try {
           file = File.createTempFile("proxyusers", null);
           fos = new FileOutputStream(file);
           fos.write(input.toString().getBytes());
        } catch(IOException ioexp) { 
           LOG.warn(ioexp.getMessage());
           return null;
        } finally {
           fos.close();
           file.deleteOnExit();
        }
        LOG.info("file.toString():" + file.toString());
        return file.toString();
     } else {
        return null;
     }
  }

  private static ProxyUserDefinitions getProxyUsersData(Configuration conf)
      throws Exception { 
    Iterator itr = conf.iterator();
    List<String> proxyUsersData = new ArrayList<String>();
    while (itr.hasNext()) { 
      String property = itr.next().toString();
      if (property.indexOf("hadoop.proxyuser") >= 0 
         && property.indexOf("groups=") >= 0) { 
        proxyUsersData.add(property.split("\\.")[2]);
      }
    }

    if (proxyUsersData.size() == 0) { 
       LOG.error("No proxy users found in the configuration.");
       throw new Exception("No proxy users found in the configuration.");
    }

    ProxyUserDefinitions pud = new ProxyUserDefinitions() { 
       public boolean writeToFile(URI filePath) throws IOException { 
           throw new UnsupportedOperationException("No such methood exists.");
       };
    };

     for (String userName : proxyUsersData) { 
        List<String> groups = Arrays.asList(conf.get("hadoop.proxyuser." + 
            userName + ".groups").split("//,"));
        List<String> hosts = Arrays.asList(conf.get("hadoop.proxyuser." + 
            userName + ".hosts").split("//,"));
        ProxyUserDefinitions.GroupsAndHost definitions = 
            pud.new GroupsAndHost();
        definitions.setGroups(groups);
        definitions.setHosts(hosts);
        pud.addProxyUser(userName, definitions);
     }
     return pud;
  }

  /**
   *  Gives the list of paths for MR traces against different time 
   *  intervals.It fetches only the paths which followed the below 
   *  file convention.
   *    Syntax : &lt;FileName&gt;_&lt;TimeIntervals&gt;.json.gz
   *  There is a restriction in a  file and user has to  
   *  follow  the below convention for time interval.
   *    Syntax: &lt;numeric&gt;[m|h|d] 
   *    e.g : for 10 minutes trace should specify 10m, 
   *    same way for 1 hour traces should specify 1h, 
   *    for 1 day traces should specify 1d.
   *
   * @param conf - cluster configuration.
   * @return - list of MR paths as key/value pair based on time interval.
   * @throws IOException - if an I/O error occurs.
   */
  public static Map<String, String> getMRTraces(Configuration conf) 
     throws IOException { 
    return getMRTraces(conf, DEFAULT_TRACES_PATH);
  }
  
  /**
   *  It gives the list of paths for MR traces against different time 
   *  intervals. It fetches only the paths which followed the below 
   *  file convention.
   *    Syntax : &lt;FileNames&gt;_&lt;TimeInterval&gt;.json.gz
   *  There is a restriction in a file and user has to follow the 
   *  below convention for time interval. 
   *    Syntax: &lt;numeric&gt;[m|h|d] 
   *    e.g : for 10 minutes trace should specify 10m,
   *    same way for 1 hour traces should specify 1h, 
   *    for 1 day  traces should specify 1d.
   *
   * @param conf - cluster configuration object.
   * @param tracesPath - MR traces path.
   * @return - list of MR paths as key/value pair based on time interval.
   * @throws IOException - If an I/O error occurs.
   */
  public static Map<String,String> getMRTraces(Configuration conf, 
      Path tracesPath) throws IOException { 
     Map <String, String> jobTraces = new HashMap <String, String>();
     final FileSystem fs = FileSystem.getLocal(conf);
     final FileStatus fstat[] = fs.listStatus(tracesPath);
     for (FileStatus fst : fstat) { 
        final String fileName = fst.getPath().getName();
        if (fileName.endsWith("m.json.gz") 
            || fileName.endsWith("h.json.gz") 
            || fileName.endsWith("d.json.gz")) { 
           jobTraces.put(fileName.substring(fileName.indexOf("_") + 1, 
              fileName.indexOf(".json.gz")), fst.getPath().toString());
        }
     }
     if (jobTraces.size() == 0) { 
        LOG.error("No traces found in " + tracesPath.toString() + " path.");
        throw new IOException("No traces found in " 
                             + tracesPath.toString() + " path.");
     }
     return jobTraces;
  }
  
  /**
   * It list the all the MR traces path irrespective of time.
   * @param conf - cluster configuration.
   * @param tracesPath - MR traces path
   * @return - MR paths as a list.
   * @throws IOException - if an I/O error occurs.
   */
  public static List<String> listMRTraces(Configuration conf, 
      Path tracesPath) throws IOException {
     List<String> jobTraces = new ArrayList<String>();
     final FileSystem fs = FileSystem.getLocal(conf);
     final FileStatus fstat[] = fs.listStatus(tracesPath);
     for (FileStatus fst : fstat) {
        jobTraces.add(fst.getPath().toString());
     }
     if (jobTraces.size() == 0) {
        LOG.error("No traces found in " + tracesPath.toString() + " path.");
        throw new IOException("No traces found in " 
                             + tracesPath.toString() + " path.");
     }
     return jobTraces;
  }
  
  /**
   * It list the all the MR traces path irrespective of time.
   * @param conf - cluster configuration.
   * @param tracesPath - MR traces path
   * @return - MR paths as a list.
   * @throws IOException - if an I/O error occurs.
   */
  public static List<String> listMRTraces(Configuration conf) 
      throws IOException { 
     return listMRTraces(conf, DEFAULT_TRACES_PATH);
  }

  /**
   * Gives the list of MR traces for given time interval.
   * The time interval should be following convention.
   *   Syntax : &lt;numeric&gt;[m|h|d]
   *   e.g : 10m or 1h or 2d etc.
   * @param conf - cluster configuration
   * @param timeInterval - trace time interval.
   * @param tracesPath - MR traces Path.
   * @return - MR paths as a list for a given time interval.
   * @throws IOException - If an I/O error occurs.
   */
  public static List<String> listMRTracesByTime(Configuration conf, 
      String timeInterval, Path tracesPath) throws IOException { 
     List<String> jobTraces = new ArrayList<String>();
     final FileSystem fs = FileSystem.getLocal(conf);
     final FileStatus fstat[] = fs.listStatus(tracesPath);
     for (FileStatus fst : fstat) { 
        final String fileName = fst.getPath().getName();
        if (fileName.indexOf(timeInterval) >= 0) { 
           jobTraces.add(fst.getPath().toString());
        }
     }
     return jobTraces;
  }
  
  /**
   * Gives the list of MR traces for given time interval.
   * The time interval should be following convention.
   *   Syntax : &lt;numeric&gt;[m|h|d]
   *   e.g : 10m or 1h or 2d etc.
   * @param conf - cluster configuration
   * @param timeInterval - trace time interval.
   * @return - MR paths as a list for a given time interval.
   * @throws IOException - If an I/O error occurs.
   */
  public static List<String> listMRTracesByTime(Configuration conf, 
      String timeInterval) throws IOException { 
     return listMRTracesByTime(conf, timeInterval, DEFAULT_TRACES_PATH);
  }
}
