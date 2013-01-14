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

package org.apache.hadoop.mapreduce.v2.jobhistory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobHistoryUtils {
  
  /**
   * Permissions for the history staging dir while JobInProgress.
   */
  public static final FsPermission HISTORY_STAGING_DIR_PERMISSIONS =
    
    FsPermission.createImmutable( (short) 0700);
  
  /**
   * Permissions for the user directory under the staging directory.
   */
  public static final FsPermission HISTORY_STAGING_USER_DIR_PERMISSIONS = 
    FsPermission.createImmutable((short) 0700);
  
  
  
  /**
   * Permissions for the history done dir and derivatives.
   */
  public static final FsPermission HISTORY_DONE_DIR_PERMISSION =
    FsPermission.createImmutable((short) 0770); 

  public static final FsPermission HISTORY_DONE_FILE_PERMISSION =
    FsPermission.createImmutable((short) 0770); // rwx------

 /**
   * Umask for the done dir and derivatives.
   */
  public static final FsPermission HISTORY_DONE_DIR_UMASK = FsPermission
      .createImmutable((short) (0770 ^ 0777));

  
  /**
   * Permissions for the intermediate done directory.
   */
  public static final FsPermission HISTORY_INTERMEDIATE_DONE_DIR_PERMISSIONS = 
    FsPermission.createImmutable((short) 01777);
  
  /**
   * Permissions for the user directory under the intermediate done directory.
   */
  public static final FsPermission HISTORY_INTERMEDIATE_USER_DIR_PERMISSIONS = 
    FsPermission.createImmutable((short) 0770);
  
  public static final FsPermission HISTORY_INTERMEDIATE_FILE_PERMISSIONS = 
    FsPermission.createImmutable((short) 0770); // rwx------
  
  /**
   * Suffix for configuration files.
   */
  public static final String CONF_FILE_NAME_SUFFIX = "_conf.xml";
  
  /**
   * Suffix for summary files.
   */
  public static final String SUMMARY_FILE_NAME_SUFFIX = ".summary";
  
  /**
   * Job History File extension.
   */
  public static final String JOB_HISTORY_FILE_EXTENSION = ".jhist";
  
  public static final int VERSION = 4;

  public static final int SERIAL_NUMBER_DIRECTORY_DIGITS = 6;
  
  public static final String TIMESTAMP_DIR_REGEX = "\\d{4}" + "\\" + File.separator +  "\\d{2}" + "\\" + File.separator + "\\d{2}";
  public static final Pattern TIMESTAMP_DIR_PATTERN = Pattern.compile(TIMESTAMP_DIR_REGEX);
  private static final String TIMESTAMP_DIR_FORMAT = "%04d" + File.separator + "%02d" + File.separator + "%02d";

  private static final Splitter ADDR_SPLITTER = Splitter.on(':').trimResults();
  private static final Joiner JOINER = Joiner.on("");

  private static final PathFilter CONF_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      return path.getName().endsWith(CONF_FILE_NAME_SUFFIX);
    }
  };
  
  private static final PathFilter JOB_HISTORY_FILE_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      return path.getName().endsWith(JOB_HISTORY_FILE_EXTENSION);
    }
  };

  /**
   * Checks whether the provided path string is a valid job history file.
   * @param pathString the path to be checked.
   * @return true is the path is a valid job history filename else return false
   */
  public static boolean isValidJobHistoryFileName(String pathString) {
    return pathString.endsWith(JOB_HISTORY_FILE_EXTENSION);
  }

  /**
   * Returns the jobId from a job history file name.
   * @param pathString the path string.
   * @return the JobId
   * @throws IOException if the filename format is invalid.
   */
  public static JobID getJobIDFromHistoryFilePath(String pathString) throws IOException {
    String [] parts = pathString.split(Path.SEPARATOR);
    String fileNamePart = parts[parts.length -1];
    JobIndexInfo jobIndexInfo =  FileNameIndexUtils.getIndexInfo(fileNamePart);
    return TypeConverter.fromYarn(jobIndexInfo.getJobId());
  }

  /**
   * Gets a PathFilter which would match configuration files.
   * @return the patch filter {@link PathFilter} for matching conf files.
   */
  public static PathFilter getConfFileFilter() {
    return CONF_FILTER;
  }
  
  /**
   * Gets a PathFilter which would match job history file names.
   * @return the path filter {@link PathFilter} matching job history files.
   */
  public static PathFilter getHistoryFileFilter() {
    return JOB_HISTORY_FILE_FILTER;
  }

  /**
   * Gets the configured directory prefix for In Progress history files.
   * @param conf the configuration for hte job
   * @param jobId the id of the job the history file is for.
   * @return A string representation of the prefix.
   */
  public static String
      getConfiguredHistoryStagingDirPrefix(Configuration conf, String jobId)
          throws IOException {
    String user = UserGroupInformation.getCurrentUser().getShortUserName();
    Path stagingPath = MRApps.getStagingAreaDir(conf, user);
    Path path = new Path(stagingPath, jobId);
    String logDir = path.toString();
    return logDir;
  }
  
  /**
   * Gets the configured directory prefix for intermediate done history files.
   * @param conf
   * @return A string representation of the prefix.
   */
  public static String getConfiguredHistoryIntermediateDoneDirPrefix(
      Configuration conf) {
    String doneDirPrefix = conf
        .get(JHAdminConfig.MR_HISTORY_INTERMEDIATE_DONE_DIR);
    if (doneDirPrefix == null) {
      doneDirPrefix = conf.get(MRJobConfig.MR_AM_STAGING_DIR,
          MRJobConfig.DEFAULT_MR_AM_STAGING_DIR)
          + "/history/done_intermediate";
    }
    return doneDirPrefix;
  }
  
  /**
   * Gets the configured directory prefix for Done history files.
   * @param conf the configuration object
   * @return the done history directory
   */
  public static String getConfiguredHistoryServerDoneDirPrefix(
      Configuration conf) {
    String doneDirPrefix = conf.get(JHAdminConfig.MR_HISTORY_DONE_DIR);
    if (doneDirPrefix == null) {
      doneDirPrefix = conf.get(MRJobConfig.MR_AM_STAGING_DIR,
          MRJobConfig.DEFAULT_MR_AM_STAGING_DIR)
          + "/history/done";
    }
    return doneDirPrefix;
  }

  /**
   * Gets the user directory for intermediate done history files.
   * @param conf the configuration object
   * @return the intermediate done directory for jobhistory files.
   */
  public static String getHistoryIntermediateDoneDirForUser(Configuration conf) throws IOException {
    return getConfiguredHistoryIntermediateDoneDirPrefix(conf) + File.separator
        + UserGroupInformation.getCurrentUser().getShortUserName();
  }

  public static boolean shouldCreateNonUserDirectory(Configuration conf) {
    // Returning true by default to allow non secure single node clusters to work
    // without any configuration change.
    return conf.getBoolean(MRJobConfig.MR_AM_CREATE_JH_INTERMEDIATE_BASE_DIR, true); 
  }

  /**
   * Get the job history file path for non Done history files.
   */
  public static Path getStagingJobHistoryFile(Path dir, JobId jobId, int attempt) {
    return getStagingJobHistoryFile(dir, TypeConverter.fromYarn(jobId).toString(), attempt);
  }
  
  /**
   * Get the job history file path for non Done history files.
   */
  public static Path getStagingJobHistoryFile(Path dir, String jobId, int attempt) {
    return new Path(dir, jobId + "_" + 
        attempt + JOB_HISTORY_FILE_EXTENSION);
  }
  
  /**
   * Get the done configuration file name for a job.
   * @param jobId the jobId.
   * @return the conf file name.
   */
  public static String getIntermediateConfFileName(JobId jobId) {
    return TypeConverter.fromYarn(jobId).toString() + CONF_FILE_NAME_SUFFIX;
  }
  
  /**
   * Get the done summary file name for a job.
   * @param jobId the jobId.
   * @return the conf file name.
   */
  public static String getIntermediateSummaryFileName(JobId jobId) {
    return TypeConverter.fromYarn(jobId).toString() + SUMMARY_FILE_NAME_SUFFIX;
  }
  
  /**
   * Gets the conf file path for jobs in progress.
   * 
   * @param logDir the log directory prefix.
   * @param jobId the jobId.
   * @param attempt attempt number for this job.
   * @return the conf file path for jobs in progress.
   */
  public static Path getStagingConfFile(Path logDir, JobId jobId, int attempt) {
    Path jobFilePath = null;
    if (logDir != null) {
      jobFilePath = new Path(logDir, TypeConverter.fromYarn(jobId).toString()
          + "_" + attempt + CONF_FILE_NAME_SUFFIX);
    }
    return jobFilePath;
  }
  
  /**
   * Gets the serial number part of the path based on the jobId and serialNumber format.
   * @param id
   * @param serialNumberFormat
   * @return the serial number part of the patch based on the jobId and serial number format.
   */
  public static String serialNumberDirectoryComponent(JobId id, String serialNumberFormat) {
    return String.format(serialNumberFormat,
        Integer.valueOf(jobSerialNumber(id))).substring(0,
        SERIAL_NUMBER_DIRECTORY_DIGITS);
  }
  
  /**Extracts the timstamp component from the path.
   * @param path
   * @return the timestamp component from the path
   */
  public static String getTimestampPartFromPath(String path) {
    Matcher matcher = TIMESTAMP_DIR_PATTERN.matcher(path);
    if (matcher.find()) {
      String matched = matcher.group();
      String ret = matched.intern();
      return ret;
    } else {
      return null;
    }
  }
  
  /**
   * Gets the history subdirectory based on the jobId, timestamp and serial number format.
   * @param id
   * @param timestampComponent
   * @param serialNumberFormat
   * @return the history sub directory based on the jobid, timestamp and serial number format
   */
  public static String historyLogSubdirectory(JobId id, String timestampComponent, String serialNumberFormat) {
//    String result = LOG_VERSION_STRING;
    String result = "";
    String serialNumberDirectory = serialNumberDirectoryComponent(id, serialNumberFormat);
    
    result = result 
      + timestampComponent
      + File.separator + serialNumberDirectory
      + File.separator;
    
    return result;
  }
  
  /**
   * Gets the timestamp component based on millisecond time.
   * @param millisecondTime
   * @return the timestamp component based on millisecond time
   */
  public static String timestampDirectoryComponent(long millisecondTime) {
    Calendar timestamp = Calendar.getInstance();
    timestamp.setTimeInMillis(millisecondTime);
    String dateString = null;
    dateString = String
        .format(TIMESTAMP_DIR_FORMAT,
            timestamp.get(Calendar.YEAR),
            // months are 0-based in Calendar, but people will expect January to
            // be month #1.
            timestamp.get(Calendar.MONTH) + 1,
            timestamp.get(Calendar.DAY_OF_MONTH));
    dateString = dateString.intern();
    return dateString;
  }
  
  public static String doneSubdirsBeforeSerialTail() {
    // date
    String result = "/*/*/*"; // YYYY/MM/DD ;
    return result;
  }
  
  /**
   * Computes a serial number used as part of directory naming for the given jobId.
   * @param id the jobId.
   * @return the serial number used as part of directory naming for the given jobid
   */
  public static int jobSerialNumber(JobId id) {
    return id.getId();
  }
  
  public static List<FileStatus> localGlobber(FileContext fc, Path root, String tail)
      throws IOException {
    return localGlobber(fc, root, tail, null);
  }

  public static List<FileStatus> localGlobber(FileContext fc, Path root, String tail,
      PathFilter filter) throws IOException {
    return localGlobber(fc, root, tail, filter, null);
  }

  // hasMismatches is just used to return a second value if you want
  // one. I would have used MutableBoxedBoolean if such had been provided.
  public static List<FileStatus> localGlobber(FileContext fc, Path root, String tail,
      PathFilter filter, AtomicBoolean hasFlatFiles) throws IOException {
    if (tail.equals("")) {
      return (listFilteredStatus(fc, root, filter));
    }

    if (tail.startsWith("/*")) {
      Path[] subdirs = filteredStat2Paths(
          remoteIterToList(fc.listStatus(root)), true, hasFlatFiles);

      List<List<FileStatus>> subsubdirs = new LinkedList<List<FileStatus>>();

      int subsubdirCount = 0;

      if (subdirs.length == 0) {
        return new LinkedList<FileStatus>();
      }

      String newTail = tail.substring(2);

      for (int i = 0; i < subdirs.length; ++i) {
        subsubdirs.add(localGlobber(fc, subdirs[i], newTail, filter, null));
        // subsubdirs.set(i, localGlobber(fc, subdirs[i], newTail, filter,
        // null));
        subsubdirCount += subsubdirs.get(i).size();
      }

      List<FileStatus> result = new LinkedList<FileStatus>();

      for (int i = 0; i < subsubdirs.size(); ++i) {
        result.addAll(subsubdirs.get(i));
      }

      return result;
    }

    if (tail.startsWith("/")) {
      int split = tail.indexOf('/', 1);

      if (split < 0) {
        return listFilteredStatus(fc, new Path(root, tail.substring(1)), filter);
      } else {
        String thisSegment = tail.substring(1, split);
        String newTail = tail.substring(split);
        return localGlobber(fc, new Path(root, thisSegment), newTail, filter,
            hasFlatFiles);
      }
    }

    IOException e = new IOException("localGlobber: bad tail");

    throw e;
  }

  private static List<FileStatus> listFilteredStatus(FileContext fc, Path root,
      PathFilter filter) throws IOException {
    List<FileStatus> fsList = remoteIterToList(fc.listStatus(root));
    if (filter == null) {
      return fsList;
    } else {
      List<FileStatus> filteredList = new LinkedList<FileStatus>();
      for (FileStatus fs : fsList) {
        if (filter.accept(fs.getPath())) {
          filteredList.add(fs);
        }
      }
      return filteredList;
    }
  }

  private static List<FileStatus> remoteIterToList(
      RemoteIterator<FileStatus> rIter) throws IOException {
    List<FileStatus> fsList = new LinkedList<FileStatus>();
    if (rIter == null)
      return fsList;
    while (rIter.hasNext()) {
      fsList.add(rIter.next());
    }
    return fsList;
  }
  
  // hasMismatches is just used to return a second value if you want
  // one. I would have used MutableBoxedBoolean if such had been provided.
  private static Path[] filteredStat2Paths(List<FileStatus> stats, boolean dirs,
      AtomicBoolean hasMismatches) {
    int resultCount = 0;

    if (hasMismatches == null) {
      hasMismatches = new AtomicBoolean(false);
    }

    for (int i = 0; i < stats.size(); ++i) {
      if (stats.get(i).isDirectory() == dirs) {
        stats.set(resultCount++, stats.get(i));
      } else {
        hasMismatches.set(true);
      }
    }

    Path[] result = new Path[resultCount];
    for (int i = 0; i < resultCount; i++) {
      result[i] = stats.get(i).getPath();
    }

    return result;
  }

  public static String getHistoryUrl(Configuration conf, ApplicationId appId) 
       throws UnknownHostException {
  //construct the history url for job
    String addr = conf.get(JHAdminConfig.MR_HISTORY_WEBAPP_ADDRESS,
        JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_ADDRESS);
    Iterator<String> it = ADDR_SPLITTER.split(addr).iterator();
    it.next(); // ignore the bind host
    String port = it.next();
    // Use hs address to figure out the host for webapp
    addr = conf.get(JHAdminConfig.MR_HISTORY_ADDRESS,
        JHAdminConfig.DEFAULT_MR_HISTORY_ADDRESS);
    String host = ADDR_SPLITTER.split(addr).iterator().next();
    String hsAddress = JOINER.join(host, ":", port);
    InetSocketAddress address = NetUtils.createSocketAddr(
      hsAddress, JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_PORT,
      JHAdminConfig.DEFAULT_MR_HISTORY_WEBAPP_ADDRESS);
    StringBuffer sb = new StringBuffer();
    if (address.getAddress().isAnyLocalAddress() || 
        address.getAddress().isLoopbackAddress()) {
      sb.append(InetAddress.getLocalHost().getCanonicalHostName());
    } else {
      sb.append(address.getHostName());
    }
    sb.append(":").append(address.getPort());
    sb.append("/jobhistory/job/");
    JobID jobId = TypeConverter.fromYarn(appId);
    sb.append(jobId.toString());
    return sb.toString();
  }
}
