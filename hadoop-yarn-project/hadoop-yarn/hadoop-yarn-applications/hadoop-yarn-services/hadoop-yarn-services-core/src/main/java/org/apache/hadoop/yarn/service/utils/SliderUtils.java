/*
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

package org.apache.hadoop.yarn.service.utils;

import com.google.common.base.Preconditions;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.service.client.params.Arguments;
import org.apache.hadoop.yarn.service.client.params.SliderActions;
import org.apache.hadoop.yarn.service.conf.YarnServiceConstants;
import org.apache.hadoop.yarn.service.containerlaunch.ClasspathConstructor;
import org.apache.hadoop.yarn.service.exceptions.BadClusterStateException;
import org.apache.hadoop.yarn.service.exceptions.BadCommandArgumentsException;
import org.apache.hadoop.yarn.service.exceptions.BadConfigException;
import org.apache.hadoop.yarn.service.exceptions.LauncherExitCodes;
import org.apache.hadoop.yarn.service.exceptions.SliderException;
import org.apache.zookeeper.server.util.KerberosUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.net.URLDecoder;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * These are slider-specific Util methods
 */
public final class SliderUtils {

  private static final Logger log = LoggerFactory.getLogger(SliderUtils.class);

  /**
   * Atomic bool to track whether or not process security has already been
   * turned on (prevents re-entrancy)
   */
  private static final AtomicBoolean processSecurityAlreadyInitialized =
      new AtomicBoolean(false);
  public static final String JAVA_SECURITY_KRB5_REALM =
      "java.security.krb5.realm";
  public static final String JAVA_SECURITY_KRB5_KDC = "java.security.krb5.kdc";

  /**
   * Winutils
   */
  public static final String WINUTILS = "WINUTILS.EXE";
  /**
   * name of openssl program
   */
  public static final String OPENSSL = "openssl";

  /**
   * name of python program
   */
  public static final String PYTHON = "python";

  /**
   * type of docker standalone service
   */
  public static final String DOCKER = "docker";
  /**
   * type of docker on yarn service
   */
  public static final String DOCKER_YARN = "yarn_docker";

  public static final int NODE_LIST_LIMIT = 10;

  private SliderUtils() {
  }

  /**
   * Implementation of set-ness, groovy definition of true/false for a string
   * @param s string
   * @return true iff the string is neither null nor empty
   */
  public static boolean isUnset(String s) {
    return s == null || s.isEmpty();
  }

  public static boolean isSet(String s) {
    return !isUnset(s);
  }

  public static boolean isEmpty(Collection l) {
    return l == null || l.isEmpty();
  }

  /**
   * Probe for a collection existing and not being empty
   * @param l collection
   * @return true if the reference is valid and it contains entries
   */

  public static boolean isNotEmpty(Collection l) {
    return l != null && !l.isEmpty();
  }

  /**
   * Probe for a map existing and not being empty
   * @param m map
   * @return true if the reference is valid and it contains map entries
   */
  public static boolean isNotEmpty(Map m) {
    return m != null && !m.isEmpty();
  }
  
  /*
   * Validates whether num is an integer
   * @param num
   * @param msg the message to be shown in exception
   */
  @SuppressWarnings("ResultOfMethodCallIgnored")
  private static void validateNumber(String num, String msg) throws
      BadConfigException {
    try {
      Integer.parseInt(num);
    } catch (NumberFormatException nfe) {
      throw new BadConfigException(msg + num);
    }
  }

  /*
   * Translates the trailing JVM heapsize unit: g, G, m, M
   * This assumes designated unit of 'm'
   * @param heapsize
   * @return heapsize in MB
   */
  public static String translateTrailingHeapUnit(String heapsize) throws
      BadConfigException {
    String errMsg = "Bad heapsize: ";
    if (heapsize.endsWith("m") || heapsize.endsWith("M")) {
      String num = heapsize.substring(0, heapsize.length() - 1);
      validateNumber(num, errMsg);
      return num;
    }
    if (heapsize.endsWith("g") || heapsize.endsWith("G")) {
      String num = heapsize.substring(0, heapsize.length() - 1) + "000";
      validateNumber(num, errMsg);
      return num;
    }
    // check if specified heap size is a number
    validateNumber(heapsize, errMsg);
    return heapsize;
  }

  /**
   * recursive directory delete
   * @param dir dir to delete
   * @throws IOException on any problem
   */
  public static void deleteDirectoryTree(File dir) throws IOException {
    if (dir.exists()) {
      if (dir.isDirectory()) {
        log.info("Cleaning up {}", dir);
        //delete the children
        File[] files = dir.listFiles();
        if (files == null) {
          throw new IOException("listfiles() failed for " + dir);
        }
        for (File file : files) {
          log.info("deleting {}", file);
          if (!file.delete()) {
            log.warn("Unable to delete " + file);
          }
        }
        if (!dir.delete()) {
          log.warn("Unable to delete " + dir);
        }
      } else {
        throw new IOException("Not a directory " + dir);
      }
    } else {
      //not found, do nothing
      log.debug("No output dir yet");
    }
  }

  /**
   * Find a containing JAR
   * @param clazz class to find
   * @return the file
   * @throws IOException any IO problem, including the class not having a
   * classloader
   * @throws FileNotFoundException if the class did not resolve to a file
   */
  public static File findContainingJarOrFail(Class clazz) throws IOException {
    File localFile = SliderUtils.findContainingJar(clazz);
    if (null == localFile) {
      throw new FileNotFoundException("Could not find JAR containing " + clazz);
    }
    return localFile;
  }


  /**
   * Find a containing JAR
   * @param my_class class to find
   * @return the file or null if it is not found
   * @throws IOException any IO problem, including the class not having a
   * classloader
   */
  public static File findContainingJar(Class my_class) throws IOException {
    ClassLoader loader = my_class.getClassLoader();
    if (loader == null) {
      throw new IOException(
          "Class " + my_class + " does not have a classloader!");
    }
    String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
    Enumeration<URL> urlEnumeration = loader.getResources(class_file);
    for (; urlEnumeration.hasMoreElements(); ) {
      URL url = urlEnumeration.nextElement();
      if ("jar".equals(url.getProtocol())) {
        String toReturn = url.getPath();
        if (toReturn.startsWith("file:")) {
          toReturn = toReturn.substring("file:".length());
        }
        // URLDecoder is a misnamed class, since it actually decodes
        // x-www-form-urlencoded MIME type rather than actual
        // URL encoding (which the file path has). Therefore it would
        // decode +s to ' 's which is incorrect (spaces are actually
        // either unencoded or encoded as "%20"). Replace +s first, so
        // that they are kept sacred during the decoding process.
        toReturn = toReturn.replaceAll("\\+", "%2B");
        toReturn = URLDecoder.decode(toReturn, "UTF-8");
        String jarFilePath = toReturn.replaceAll("!.*$", "");
        return new File(jarFilePath);
      } else {
        log.info("could not locate JAR containing {} URL={}", my_class, url);
      }
    }
    return null;
  }

  public static void checkPort(String hostname, int port, int connectTimeout)
      throws IOException {
    InetSocketAddress addr = new InetSocketAddress(hostname, port);
    checkPort(hostname, addr, connectTimeout);
  }

  @SuppressWarnings("SocketOpenedButNotSafelyClosed")
  public static void checkPort(String name,
      InetSocketAddress address,
      int connectTimeout)
      throws IOException {
    try(Socket socket = new Socket()) {
      socket.connect(address, connectTimeout);
    } catch (Exception e) {
      throw new IOException("Failed to connect to " + name
                            + " at " + address
                            + " after " + connectTimeout + "milliseconds"
                            + ": " + e,
          e);
    }
  }

  public static void checkURL(String name, String url, int timeout) throws
      IOException {
    InetSocketAddress address = NetUtils.createSocketAddr(url);
    checkPort(name, address, timeout);
  }

  /**
   * A required file
   * @param role role of the file (for errors)
   * @param filename the filename
   * @throws ExitUtil.ExitException if the file is missing
   * @return the file
   */
  public static File requiredFile(String filename, String role) throws
      IOException {
    if (filename.isEmpty()) {
      throw new ExitUtil.ExitException(-1, role + " file not defined");
    }
    File file = new File(filename);
    if (!file.exists()) {
      throw new ExitUtil.ExitException(-1,
          role + " file not found: " +
          file.getCanonicalPath());
    }
    return file;
  }

  private static final PatternValidator clusternamePattern
      = new PatternValidator("[a-z][a-z0-9_-]*");

  /**
   * Normalize a cluster name then verify that it is valid
   * @param name proposed cluster name
   * @return true iff it is valid
   */
  public static boolean isClusternameValid(String name) {
    return name != null && clusternamePattern.matches(name);
  }

  /**
   * Copy a directory to a new FS -both paths must be qualified. If
   * a directory needs to be created, supplied permissions can override
   * the default values. Existing directories are not touched
   * @param conf conf file
   * @param srcDirPath src dir
   * @param destDirPath dest dir
   * @param permission permission for the dest directory; null means "default"
   * @return # of files copies
   */
  @SuppressWarnings("deprecation")
  public static int copyDirectory(Configuration conf,
      Path srcDirPath,
      Path destDirPath,
      FsPermission permission) throws
      IOException,
      BadClusterStateException {
    FileSystem srcFS = FileSystem.get(srcDirPath.toUri(), conf);
    FileSystem destFS = FileSystem.get(destDirPath.toUri(), conf);
    //list all paths in the src.
    if (!srcFS.exists(srcDirPath)) {
      throw new FileNotFoundException("Source dir not found " + srcDirPath);
    }
    if (!srcFS.isDirectory(srcDirPath)) {
      throw new FileNotFoundException(
          "Source dir not a directory " + srcDirPath);
    }
    GlobFilter dotFilter = new GlobFilter("[!.]*");
    FileStatus[] entries = srcFS.listStatus(srcDirPath, dotFilter);
    int srcFileCount = entries.length;
    if (srcFileCount == 0) {
      return 0;
    }
    if (permission == null) {
      permission = FsPermission.getDirDefault();
    }
    if (!destFS.exists(destDirPath)) {
      new SliderFileSystem(destFS, conf).createWithPermissions(destDirPath,
          permission);
    }
    Path[] sourcePaths = new Path[srcFileCount];
    for (int i = 0; i < srcFileCount; i++) {
      FileStatus e = entries[i];
      Path srcFile = e.getPath();
      if (srcFS.isDirectory(srcFile)) {
        String msg = "Configuration dir " + srcDirPath
                     + " contains a directory " + srcFile;
        log.warn(msg);
        throw new IOException(msg);
      }
      log.debug("copying src conf file {}", srcFile);
      sourcePaths[i] = srcFile;
    }
    log.debug("Copying {} files from {} to dest {}", srcFileCount,
        srcDirPath,
        destDirPath);
    FileUtil.copy(srcFS, sourcePaths, destFS, destDirPath, false, true, conf);
    return srcFileCount;
  }

  /**
   * Copy a file to a new FS -both paths must be qualified.
   * @param conf conf file
   * @param srcFile src file
   * @param destFile dest file
   */
  @SuppressWarnings("deprecation")
  public static void copy(Configuration conf,
      Path srcFile,
      Path destFile) throws
      IOException,
      BadClusterStateException {
    FileSystem srcFS = FileSystem.get(srcFile.toUri(), conf);
    //list all paths in the src.
    if (!srcFS.exists(srcFile)) {
      throw new FileNotFoundException("Source file not found " + srcFile);
    }
    if (!srcFS.isFile(srcFile)) {
      throw new FileNotFoundException(
          "Source file not a file " + srcFile);
    }
    FileSystem destFS = FileSystem.get(destFile.toUri(), conf);
    FileUtil.copy(srcFS, srcFile, destFS, destFile, false, true, conf);
  }

  /**
   * Take a collection, return a list containing the string value of every
   * element in the collection.
   * @param c collection
   * @return a stringified list
   */
  public static List<String> collectionToStringList(Collection c) {
    List<String> l = new ArrayList<>(c.size());
    for (Object o : c) {
      l.add(o.toString());
    }
    return l;
  }

  /**
   * Join an collection of objects with a separator that appears after every
   * instance in the list -including at the end
   * @param collection collection to call toString() on each element
   * @param separator separator string
   * @return the joined entries
   */
  public static String join(Collection collection, String separator) {
    return join(collection, separator, true);
  }

  /**
   * Join an collection of objects with a separator that appears after every
   * instance in the list -optionally at the end
   * @param collection collection to call toString() on each element
   * @param separator separator string
   * @param trailing add a trailing entry or not
   * @return the joined entries
   */
  public static String join(Collection collection,
      String separator,
      boolean trailing) {
    StringBuilder b = new StringBuilder();
    // fast return on empty collection
    if (collection.isEmpty()) {
      return trailing ? separator : "";
    }
    for (Object o : collection) {
      b.append(o);
      b.append(separator);
    }
    int length = separator.length();
    String s = b.toString();
    return (trailing || s.isEmpty()) ?
           s : (b.substring(0, b.length() - length));
  }

  /**
   * Join an array of strings with a separator that appears after every
   * instance in the list -including at the end
   * @param collection strings
   * @param separator separator string
   * @return the joined entries
   */
  public static String join(String[] collection, String separator) {
    return join(collection, separator, true);


  }

  /**
   * Join an array of strings with a separator that appears after every
   * instance in the list -optionally at the end
   * @param collection strings
   * @param separator separator string
   * @param trailing add a trailing entry or not
   * @return the joined entries
   */
  public static String join(String[] collection, String separator,
      boolean trailing) {
    return join(Arrays.asList(collection), separator, trailing);
  }

  /**
   * Join an array of strings with a separator that appears after every
   * instance in the list -except at the end
   * @param collection strings
   * @param separator separator string
   * @return the list
   */
  public static String joinWithInnerSeparator(String separator,
      Object... collection) {
    StringBuilder b = new StringBuilder();
    boolean first = true;

    for (Object o : collection) {
      if (first) {
        first = false;
      } else {
        b.append(separator);
      }
      b.append(o.toString());
      b.append(separator);
    }
    return b.toString();
  }

  /**
   * Resolve a mandatory environment variable
   * @param key env var
   * @return the resolved value
   * @throws BadClusterStateException
   */
  public static String mandatoryEnvVariable(String key) throws
      BadClusterStateException {
    String v = System.getenv(key);
    if (v == null) {
      throw new BadClusterStateException("Missing Environment variable " + key);
    }
    return v;
  }

  public static String appReportToString(ApplicationReport r,
      String separator) {
    StringBuilder builder = new StringBuilder(512);
    builder.append("service ")
           .append(
               r.getName())
           .append("/")
           .append(r.getApplicationType())
           .append(separator);
    Set<String> tags = r.getApplicationTags();
    if (!tags.isEmpty()) {
      for (String tag : tags) {
        builder.append(tag).append(separator);
      }
    }
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");
    dateFormat.setTimeZone(TimeZone.getDefault());
    builder.append("state: ").append(r.getYarnApplicationState());
    String trackingUrl = r.getTrackingUrl();
    if (isSet(trackingUrl)) {
      builder.append(separator).append("URL: ").append(trackingUrl);
    }
    builder.append(separator)
           .append("Started: ")
           .append(dateFormat.format(new Date(r.getStartTime())));
    long finishTime = r.getFinishTime();
    if (finishTime > 0) {
      builder.append(separator)
             .append("Finished: ")
             .append(dateFormat.format(new Date(finishTime)));
    }
    String rpcHost = r.getHost();
    if (!isSet(rpcHost)) {
      builder.append(separator)
             .append("RPC :")
             .append(rpcHost)
             .append(':')
             .append(r.getRpcPort());
    }
    String diagnostics = r.getDiagnostics();
    if (!isSet(diagnostics)) {
      builder.append(separator).append("Diagnostics :").append(diagnostics);
    }
    return builder.toString();
  }

  /**
   * Filter a string value given a single filter
   * 
   * @param value
   *          the string value to check
   * @param filter
   *          a single string filter
   * @return return true if value should be trapped, false if it should be let
   *         through
   */
  public static boolean filter(String value, String filter) {
    return !(StringUtils.isEmpty(filter) || filter.equals(value));
  }

  /**
   * Filter a string value given a set of filters
   * 
   * @param value
   *          the string value to check
   * @param filters
   *          a set of string filters
   * @return return true if value should be trapped, false if it should be let
   *         through
   */
  public static boolean filter(String value, Set<String> filters) {
    return !(filters.isEmpty() || filters.contains(value));
  }

  /**
   * Sorts the given list of service reports, most recently started
   * or finished instance first.
   *
   * @param instances list of instances
   */
  public static void sortApplicationsByMostRecent(List<ApplicationReport> instances) {
    Collections.sort(instances, new MostRecentlyStartedOrFinishedFirst());
  }

  /**
   * Sorts the given list of service reports
   * Finished instances are ordered by finished time and running/accepted instances are
   * ordered by start time
   * Finally Instance are order by finished instances coming after running instances
   *
   * @param instances list of instances
   */
  public static void sortApplicationReport(List<ApplicationReport> instances) {
    if (instances.size() <= 1) {
      return;
    }
    List<ApplicationReport> nonLiveInstance =
        new ArrayList<>(instances.size());
    List<ApplicationReport> liveInstance =
        new ArrayList<>(instances.size());

    for (ApplicationReport report : instances) {
      if (report.getYarnApplicationState() == YarnApplicationState.RUNNING
          ||
          report.getYarnApplicationState() == YarnApplicationState.ACCEPTED) {
        liveInstance.add(report);
      } else {
        nonLiveInstance.add(report);
      }
    }

    if (liveInstance.size() > 1) {
      Collections.sort(liveInstance, new MostRecentlyStartedAppFirst());
    }
    if (nonLiveInstance.size() > 1) {
      Collections.sort(nonLiveInstance, new MostRecentAppFinishFirst());
    }
    instances.clear();
    instances.addAll(liveInstance);
    instances.addAll(nonLiveInstance);
  }

  /**
   * Merge in one map to another -all entries in the second map are
   * merged into the first -overwriting any duplicate keys.
   * @param first first map -the updated one.
   * @param second the map that is merged in
   * @return the first map
   */
  public static Map<String, String> mergeMap(Map<String, String> first,
      Map<String, String> second) {
    first.putAll(second);
    return first;
  }

  /**
   * Merge a set of entries into a map. This will take the entryset of
   * a map, or a Hadoop collection itself
   * @param dest destination
   * @param entries entries
   * @return dest -with the entries merged in
   */
  public static Map<String, String> mergeEntries(Map<String, String> dest,
      Iterable<Map.Entry<String, String>> entries) {
    for (Map.Entry<String, String> entry : entries) {
      dest.put(entry.getKey(), entry.getValue());
    }
    return dest;
  }

  /**
   * Generic map merge logic
   * @param first first map
   * @param second second map
   * @param <T1> key type
   * @param <T2> value type
   * @return 'first' merged with the second
   */
  public static <T1, T2> Map<T1, T2> mergeMaps(Map<T1, T2> first,
      Map<T1, T2> second) {
    first.putAll(second);
    return first;
  }

  /**
   * Generic map merge logic
   * @param first first map
   * @param second second map
   * @param <T1> key type
   * @param <T2> value type
   * @return 'first' merged with the second
   */
  public static <T1, T2> Map<T1, T2> mergeMapsIgnoreDuplicateKeys(Map<T1, T2> first,
      Map<T1, T2> second) {
    Preconditions.checkArgument(first != null, "Null 'first' value");
    Preconditions.checkArgument(second != null, "Null 'second' value");
    for (Map.Entry<T1, T2> entry : second.entrySet()) {
      T1 key = entry.getKey();
      if (!first.containsKey(key)) {
        first.put(key, entry.getValue());
      }
    }
    return first;
  }

  /**
   * Convert a map to a multi-line string for printing
   * @param map map to stringify
   * @return a string representation of the map
   */
  public static String stringifyMap(Map<String, String> map) {
    StringBuilder builder = new StringBuilder();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      builder.append(entry.getKey())
             .append("=\"")
             .append(entry.getValue())
             .append("\"\n");

    }
    return builder.toString();
  }

  /**
   * Parse an int value, replacing it with defval if undefined;
   * @param errorKey key to use in exceptions
   * @param defVal default value to use if the key is not in the map
   * @param min min value or -1 for do not check
   * @param max max value or -1 for do not check
   * @return the int value the integer value
   * @throws BadConfigException if the value could not be parsed
   */
  public static int parseAndValidate(String errorKey,
      String valS,
      int defVal,
      int min, int max) throws
      BadConfigException {
    if (valS == null) {
      valS = Integer.toString(defVal);
    }
    String trim = valS.trim();
    int val;
    try {
      val = Integer.decode(trim);
    } catch (NumberFormatException e) {
      throw new BadConfigException("Failed to parse value of "
                                   + errorKey + ": \"" + trim + "\"");
    }
    if (min >= 0 && val < min) {
      throw new BadConfigException("Value of "
                                   + errorKey + ": " + val + ""
                                   + "is less than the minimum of " + min);
    }
    if (max >= 0 && val > max) {
      throw new BadConfigException("Value of "
                                   + errorKey + ": " + val + ""
                                   + "is more than the maximum of " + max);
    }
    return val;
  }

  public static InetSocketAddress getRmAddress(Configuration conf) {
    return conf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADDRESS,
        YarnConfiguration.DEFAULT_RM_PORT);
  }

  public static InetSocketAddress getRmSchedulerAddress(Configuration conf) {
    return conf.getSocketAddr(YarnConfiguration.RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
  }

  /**
   * probe to see if the RM scheduler is defined
   * @param conf config
   * @return true if the RM scheduler address is set to
   * something other than 0.0.0.0
   */
  public static boolean isRmSchedulerAddressDefined(Configuration conf) {
    InetSocketAddress address = getRmSchedulerAddress(conf);
    return isAddressDefined(address);
  }

  /**
   * probe to see if the address
   * @param address network address
   * @return true if the scheduler address is set to
   * something other than 0.0.0.0
   */
  public static boolean isAddressDefined(InetSocketAddress address) {
    if (address == null || address.getHostString() == null) {
      return false;
    }
    return !(address.getHostString().equals("0.0.0.0"));
  }

  public static void setRmAddress(Configuration conf, String rmAddr) {
    conf.set(YarnConfiguration.RM_ADDRESS, rmAddr);
  }

  public static void setRmSchedulerAddress(Configuration conf, String rmAddr) {
    conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, rmAddr);
  }

  public static boolean hasAppFinished(ApplicationReport report) {
    return report == null ||
           report.getYarnApplicationState().ordinal() >=
           YarnApplicationState.FINISHED.ordinal();
  }

  public static String containerToString(Container container) {
    if (container == null) {
      return "null container";
    }
    return String.format(Locale.ENGLISH,
        "ContainerID=%s nodeID=%s http=%s priority=%s resource=%s",
        container.getId(),
        container.getNodeId(),
        container.getNodeHttpAddress(),
        container.getPriority(),
        container.getResource());
  }

  /**
   * convert an AM report to a string for diagnostics
   * @param report the report
   * @return the string value
   */
  public static String reportToString(ApplicationReport report) {
    if (report == null) {
      return "Null service report";
    }

    return "App " + report.getName() + "/" + report.getApplicationType() +
           "# " +
           report.getApplicationId() + " user " + report.getUser() +
           " is in state " + report.getYarnApplicationState() +
           " RPC: " + report.getHost() + ":" + report.getRpcPort() +
           " URL: " + report.getOriginalTrackingUrl();
  }

  /**
   * Convert a YARN URL into a string value of a normal URL
   * @param url URL
   * @return string representatin
   */
  public static String stringify(org.apache.hadoop.yarn.api.records.URL url) {
    StringBuilder builder = new StringBuilder();
    builder.append(url.getScheme()).append("://");
    if (url.getHost() != null) {
      builder.append(url.getHost()).append(":").append(url.getPort());
    }
    builder.append(url.getFile());
    return builder.toString();
  }

  /**
   * Get a random open port
   * @return true if the port was available for listening on
   */
  public static int getOpenPort() throws IOException {
    ServerSocket socket = null;
    try {
      socket = new ServerSocket(0);
      return socket.getLocalPort();
    } finally {
      if (socket != null) {
        socket.close();
      }
    }
  }

  /**
   * See if a port is available for listening on by trying to listen
   * on it and seeing if that works or fails.
   * @param port port to listen to
   * @return true if the port was available for listening on
   */
  public static boolean isPortAvailable(int port) {
    try {
      ServerSocket socket = new ServerSocket(port);
      socket.close();
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  // Build env map: key -> value;
  // value will be replaced by the corresponding value in tokenMap, if any.
  public static Map<String, String> buildEnvMap(
      org.apache.hadoop.yarn.service.api.records.Configuration conf,
      Map<String,String> tokenMap) {
    if (tokenMap == null) {
      return conf.getEnv();
    }
    Map<String, String> env = new HashMap<>();
    for (Map.Entry<String, String> entry : conf.getEnv().entrySet()) {
      String key = entry.getKey();
      String val = entry.getValue();
      for (Map.Entry<String,String> token : tokenMap.entrySet()) {
        val = val.replaceAll(Pattern.quote(token.getKey()),
            token.getValue());
      }
      env.put(key,val);
    }
    return env;
  }

  /**
   * Apply a set of command line options to a cluster role map
   * @param clusterRoleMap cluster role map to merge onto
   * @param commandOptions command opts
   */
  public static void applyCommandLineRoleOptsToRoleMap(
      Map<String, Map<String, String>> clusterRoleMap,
      Map<String, Map<String, String>> commandOptions) {
    for (Map.Entry<String, Map<String, String>> entry : commandOptions.entrySet()) {
      String key = entry.getKey();
      Map<String, String> optionMap = entry.getValue();
      Map<String, String> existingMap = clusterRoleMap.get(key);
      if (existingMap == null) {
        existingMap = new HashMap<String, String>();
      }
      log.debug("Overwriting role options with command line values {}",
          stringifyMap(optionMap));
      mergeMap(existingMap, optionMap);
      //set or overwrite the role
      clusterRoleMap.put(key, existingMap);
    }
  }

  /**
   * Verify that a Kerberos principal has been set -if not fail
   * with an error message that actually tells you what is missing
   * @param conf configuration to look at
   * @param principal key of principal
   * @throws BadConfigException if the key is not set
   */
  public static void verifyPrincipalSet(Configuration conf,
      String principal) throws
      BadConfigException {
    String principalName = conf.get(principal);
    if (principalName == null) {
      throw new BadConfigException("Unset Kerberos principal : %s",
          principal);
    }
    log.debug("Kerberos princial {}={}", principal, principalName);
  }

  /**
   * Flag to indicate whether the cluster is in secure mode
   * @param conf configuration to look at
   * @return true if the slider client/service should be in secure mode
   */
  public static boolean isHadoopClusterSecure(Configuration conf) {
    return SecurityUtil.getAuthenticationMethod(conf) !=
           UserGroupInformation.AuthenticationMethod.SIMPLE;
  }

  /**
   * Init security if the cluster configuration declares the cluster is secure
   * @param conf configuration to look at
   * @return true if the cluster is secure
   * @throws IOException cluster is secure
   * @throws SliderException the configuration/process is invalid
   */
  public static boolean maybeInitSecurity(Configuration conf) throws
      IOException,
      SliderException {
    boolean clusterSecure = isHadoopClusterSecure(conf);
    if (clusterSecure) {
      log.debug("Enabling security");
      initProcessSecurity(conf);
    }
    return clusterSecure;
  }

  /**
   * Turn on security. This is setup to only run once.
   * @param conf configuration to build up security
   * @return true if security was initialized in this call
   * @throws IOException IO/Net problems
   * @throws BadConfigException the configuration and system state are inconsistent
   */
  public static boolean initProcessSecurity(Configuration conf) throws
      IOException,
      SliderException {

    if (processSecurityAlreadyInitialized.compareAndSet(true, true)) {
      //security is already inited
      return false;
    }

    log.info("JVM initialized into secure mode with kerberos realm {}",
        SliderUtils.getKerberosRealm());
    //this gets UGI to reset its previous world view (i.e simple auth)
    //security
    log.debug("java.security.krb5.realm={}",
        System.getProperty(JAVA_SECURITY_KRB5_REALM, ""));
    log.debug("java.security.krb5.kdc={}",
        System.getProperty(JAVA_SECURITY_KRB5_KDC, ""));
    log.debug("hadoop.security.authentication={}",
        conf.get(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION));
    log.debug("hadoop.security.authorization={}",
        conf.get(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION));
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation authUser = UserGroupInformation.getCurrentUser();
    log.debug("Authenticating as {}", authUser);
    log.debug("Login user is {}", UserGroupInformation.getLoginUser());
    if (!UserGroupInformation.isSecurityEnabled()) {
      throw new SliderException(LauncherExitCodes.EXIT_UNAUTHORIZED,
          "Although secure mode is enabled," +
         "the service has already set up its user as an insecure entity %s",
          authUser);
    }
    if (authUser.getAuthenticationMethod() ==
        UserGroupInformation.AuthenticationMethod.SIMPLE) {
      throw new BadConfigException("Auth User is not Kerberized %s" +
         " -security has already been set up with the wrong authentication method. "
         + "This can occur if a file system has already been created prior to the loading of "
         + "the security configuration.",
          authUser);

    }

    SliderUtils.verifyPrincipalSet(conf, YarnConfiguration.RM_PRINCIPAL);
    SliderUtils.verifyPrincipalSet(conf, "dfs.namenode.kerberos.principal");
    return true;
  }

  /**
   * Force an early login: This catches any auth problems early rather than
   * in RPC operations
   * @throws IOException if the login fails
   */
  public static void forceLogin() throws IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      if (UserGroupInformation.isLoginKeytabBased()) {
        UserGroupInformation.getLoginUser().reloginFromKeytab();
      } else {
        UserGroupInformation.getLoginUser().reloginFromTicketCache();
      }
    }
  }

  public static String getLibDir() {
    String[] libDirs = getLibDirs();
    if (libDirs == null || libDirs.length == 0) {
      return null;
    }
    return libDirs[0];
  }

  public static String[] getLibDirs() {
    String libDirStr = System.getProperty(YarnServiceConstants.PROPERTY_LIB_DIR);
    if (isUnset(libDirStr)) {
      return ArrayUtils.EMPTY_STRING_ARRAY;
    }
    return StringUtils.split(libDirStr, ',');
  }

  /**
   * Submit a JAR containing a specific class and map it
   * @param providerResources provider map to build up
   * @param sliderFileSystem remote fs
   * @param clazz class to look for
   * @param libdir lib directory
   * @param jarName <i>At the destination</i>
   * @return the local resource ref
   * @throws IOException trouble copying to HDFS
   */
  public static LocalResource putJar(Map<String, LocalResource> providerResources,
      SliderFileSystem sliderFileSystem,
      Class clazz,
      Path tempPath,
      String libdir,
      String jarName
  )
      throws IOException, SliderException {
    LocalResource res = sliderFileSystem.submitJarWithClass(
        clazz,
        tempPath,
        libdir,
        jarName);
    providerResources.put(libdir + "/" + jarName, res);
    return res;
  }

  /**
   * Submit a JAR containing and map it
   * @param providerResources provider map to build up
   * @param sliderFileSystem remote fs
   * @param libDir lib directory
   * @param srcPath copy jars from
   */
  public static void putAllJars(Map<String, LocalResource> providerResources,
                                SliderFileSystem sliderFileSystem,
                                Path tempPath,
                                String libDir,
                                String srcPath) throws IOException, SliderException {
    log.info("Loading all dependencies from {}", srcPath);
    if (SliderUtils.isSet(srcPath)) {
      File srcFolder = new File(srcPath);
      FilenameFilter jarFilter = createJarFilter();
      File[] listOfJars = srcFolder.listFiles(jarFilter);
      if (listOfJars == null || listOfJars.length == 0) {
        return;
      }
      for (File jarFile : listOfJars) {
        LocalResource res = sliderFileSystem.submitFile(jarFile, tempPath, libDir, jarFile.getName());
        providerResources.put(libDir + "/" + jarFile.getName(), res);
      }
    }
  }

  /**
   * Accept all filenames ending with {@code .jar}
   * @return a filename filter
   */
  public static FilenameFilter createJarFilter() {
    return new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.toLowerCase(Locale.ENGLISH).endsWith(".jar");
      }
    };
  }

  /**
   * Submit the AM tar.gz containing all dependencies and map it
   * @param providerResources provider map to build up
   * @param sliderFileSystem remote fs
   */
  public static void putAmTarGzipAndUpdate(
      Map<String, LocalResource> providerResources,
      SliderFileSystem sliderFileSystem
  ) throws IOException, SliderException {
    log.info("Loading all dependencies from {}{}",
        YarnServiceConstants.DEPENDENCY_TAR_GZ_FILE_NAME,
        YarnServiceConstants.DEPENDENCY_TAR_GZ_FILE_EXT);
    sliderFileSystem.submitTarGzipAndUpdate(providerResources);
  }

  public static Map<String, Map<String, String>> deepClone(Map<String, Map<String, String>> src) {
    Map<String, Map<String, String>> dest = new HashMap<>();
    for (Map.Entry<String, Map<String, String>> entry : src.entrySet()) {
      dest.put(entry.getKey(), stringMapClone(entry.getValue()));
    }
    return dest;
  }

  public static Map<String, String> stringMapClone(Map<String, String> src) {
    Map<String, String> dest = new HashMap<>();
    return mergeEntries(dest, src.entrySet());
  }

  /**
   * List a directory in the local filesystem
   * @param dir directory
   * @return a listing, one to a line
   */
  public static String listDir(File dir) {
    if (dir == null) {
      return "";
    }
    String[] confDirEntries = dir.list();
    if (confDirEntries == null) {
      return "";
    }
    StringBuilder builder = new StringBuilder();
    for (String entry : confDirEntries) {
      builder.append(entry).append("\n");
    }
    return builder.toString();
  }

  /**
   * Create a file:// path from a local file
   * @param file file to point the path
   * @return a new Path
   */
  public static Path createLocalPath(File file) {
    return new Path(file.toURI());
  }

  public static String getKerberosRealm() {
    try {
      return KerberosUtil.getDefaultRealm();
    } catch (Exception e) {
      log.debug("introspection into JVM internals failed", e);
      return "(unknown)";

    }
  }

  /**
   * Build up the classpath for execution
   * -behaves very differently on a mini test cluster vs a production
   * production one.
   *
   * @param sliderConfDir relative path to the dir containing slider config
   *                      options to put on the classpath -or null
   * @param libdir directory containing the JAR files
   * @param usingMiniMRCluster flag to indicate the MiniMR cluster is in use
   * (and hence the current classpath should be used, not anything built up)
   * @return a classpath
   */
  public static ClasspathConstructor buildClasspath(String sliderConfDir,
      String libdir,
      SliderFileSystem sliderFileSystem,
      boolean usingMiniMRCluster) {

    ClasspathConstructor classpath = new ClasspathConstructor();
    classpath.append(YarnServiceConstants.YARN_SERVICE_LOG4J_FILENAME);

    // add the runtime classpath needed for tests to work
    if (usingMiniMRCluster) {
      // for mini cluster we pass down the java CP properties
      // and nothing else
      classpath.appendAll(classpath.localJVMClasspath());
    } else {
      if (sliderConfDir != null) {
        classpath.addClassDirectory(sliderConfDir);
      }
      classpath.addLibDir(libdir);
      if (sliderFileSystem.isFile(sliderFileSystem.getDependencyTarGzip())) {
        classpath.addLibDir(YarnServiceConstants.DEPENDENCY_LOCALIZED_DIR_LINK);
      } else {
        log.info(
            "For faster submission of apps, upload dependencies using cmd {} {}",
            SliderActions.ACTION_DEPENDENCY, Arguments.ARG_UPLOAD);
      }
      classpath.addRemoteClasspathEnvVar();
      classpath.append(ApplicationConstants.Environment.HADOOP_CONF_DIR.$$());
    }
    return classpath;
  }

  /**
   * Verify that a path refers to a directory. If not
   * logs the parent dir then throws an exception
   * @param dir the directory
   * @param errorlog log for output on an error
   * @throws FileNotFoundException if it is not a directory
   */
  public static void verifyIsDir(File dir, Logger errorlog) throws
      FileNotFoundException {
    if (!dir.exists()) {
      errorlog.warn("contents of {}: {}", dir,
          listDir(dir.getParentFile()));
      throw new FileNotFoundException(dir.toString());
    }
    if (!dir.isDirectory()) {
      errorlog.info("contents of {}: {}", dir,
          listDir(dir.getParentFile()));
      throw new FileNotFoundException(
          "Not a directory: " + dir);
    }
  }

  /**
   * Verify that a file exists
   * @param file file
   * @param errorlog log for output on an error
   * @throws FileNotFoundException
   */
  public static void verifyFileExists(File file, Logger errorlog) throws
      FileNotFoundException {
    if (!file.exists()) {
      errorlog.warn("contents of {}: {}", file,
          listDir(file.getParentFile()));
      throw new FileNotFoundException(file.toString());
    }
    if (!file.isFile()) {
      throw new FileNotFoundException("Not a file: " + file.toString());
    }
  }

  /**
   * verify that a config option is set
   * @param configuration config
   * @param key key
   * @return the value, in case it needs to be verified too
   * @throws BadConfigException if the key is missing
   */
  public static String verifyOptionSet(Configuration configuration, String key,
      boolean allowEmpty) throws BadConfigException {
    String val = configuration.get(key);
    if (val == null) {
      throw new BadConfigException(
          "Required configuration option \"%s\" not defined ", key);
    }
    if (!allowEmpty && val.isEmpty()) {
      throw new BadConfigException(
          "Configuration option \"%s\" must not be empty", key);
    }
    return val;
  }

  /**
   * Verify that a keytab property is defined and refers to a non-empty file
   *
   * @param siteConf configuration
   * @param prop property to look for
   * @return the file referenced
   * @throws BadConfigException on a failure
   */
  public static File verifyKeytabExists(Configuration siteConf,
      String prop) throws
      BadConfigException {
    String keytab = siteConf.get(prop);
    if (keytab == null) {
      throw new BadConfigException("Missing keytab property %s",
          prop);

    }
    File keytabFile = new File(keytab);
    if (!keytabFile.exists()) {
      throw new BadConfigException("Missing keytab file %s defined in %s",
          keytabFile,
          prop);
    }
    if (keytabFile.length() == 0 || !keytabFile.isFile()) {
      throw new BadConfigException("Invalid keytab file %s defined in %s",
          keytabFile,
          prop);
    }
    return keytabFile;
  }

  /**
   * Add a subpath to an existing URL. This extends
   * the path, inserting a / between all entries
   * if needed.
   * @param base base path/URL
   * @param path subpath
   * @return base+"/"+subpath
   */
  public static String appendToURL(String base, String path) {
    StringBuilder fullpath = new StringBuilder(base);
    if (!base.endsWith("/")) {
      fullpath.append("/");
    }
    if (path.startsWith("/")) {
      fullpath.append(path.substring(1));
    } else {
      fullpath.append(path);
    }
    return fullpath.toString();
  }

  /**
   * Truncate the given string to a maximum length provided
   * with a pad (...) added to the end if expected size if more than 10.
   * @param toTruncate string to truncate; may be null
   * @param maxSize maximum size
   * @return the truncated/padded string. 
   */
  public static String truncate(String toTruncate, int maxSize) {
    if (toTruncate == null || maxSize < 1
        || toTruncate.length() <= maxSize) {
      return toTruncate;
    }

    String pad = "...";
    if (maxSize < 10) {
      pad = "";
    }
    return toTruncate.substring(0, maxSize - pad.length()).concat(pad);
  }

  /**
   * Given a source folder create zipped file
   *
   * @param srcFolder
   * @param zipFile
   *
   * @throws IOException
   */
  public static void zipFolder(File srcFolder, File zipFile) throws IOException {
    log.info("Zipping folder {} to {}", srcFolder.getAbsolutePath(), zipFile.getAbsolutePath());
    List<String> files = new ArrayList<>();
    generateFileList(files, srcFolder, srcFolder, true);

    byte[] buffer = new byte[1024];

    try (FileOutputStream fos = new FileOutputStream(zipFile)) {
      try (ZipOutputStream zos = new ZipOutputStream(fos)) {

        for (String file : files) {
          ZipEntry ze = new ZipEntry(file);
          zos.putNextEntry(ze);
          try (FileInputStream in = new FileInputStream(srcFolder + File.separator + file)) {
            int len;
            while ((len = in.read(buffer)) > 0) {
              zos.write(buffer, 0, len);
            }
          }
        }
      }
    }
  }

  /**
   * Given a source folder create a tar.gz file
   * 
   * @param libDirs
   * @param tarGzipFile
   * 
   * @throws IOException
   */
  public static void tarGzipFolder(String[] libDirs, File tarGzipFile,
      FilenameFilter filter) throws IOException {
    log.info("Tar-gzipping folders {} to {}", libDirs,
        tarGzipFile.getAbsolutePath());

    try(TarArchiveOutputStream taos =
            new TarArchiveOutputStream(new GZIPOutputStream(
        new BufferedOutputStream(new FileOutputStream(tarGzipFile))))) {
      for (String libDir : libDirs) {
        File srcFolder = new File(libDir);
        List<String> files = new ArrayList<>();
        generateFileList(files, srcFolder, srcFolder, true, filter);
        for (String file : files) {
          File srcFile = new File(srcFolder, file);
          TarArchiveEntry tarEntry = new TarArchiveEntry(
              srcFile, file);
          taos.putArchiveEntry(tarEntry);
          try(FileInputStream in = new FileInputStream(srcFile)) {
            org.apache.commons.io.IOUtils.copy(in, taos);
          }
          taos.flush();
          taos.closeArchiveEntry();
        }
      }
    }
  }

  private static void generateFileList(List<String> fileList, File node,
      File rootFolder, Boolean relative) {
    generateFileList(fileList, node, rootFolder, relative, null);
  }

  private static void generateFileList(List<String> fileList, File node,
      File rootFolder, Boolean relative, FilenameFilter filter) {
    if (node.isFile()) {
      String fileFullPath = node.toString();
      if (relative) {
        fileList.add(fileFullPath.substring(rootFolder.toString().length() + 1,
            fileFullPath.length()));
      } else {
        fileList.add(fileFullPath);
      }
    }

    if (node.isDirectory()) {
      String[] subNode = node.list(filter);
      if (subNode == null || subNode.length == 0) {
          return;
      }
      for (String filename : subNode) {
        generateFileList(fileList, new File(node, filename), rootFolder,
            relative, filter);
      }
    }
  }

  /**
   * Check for any needed libraries being present. On Unix none are needed;
   * on windows they must be present
   * @return true if all is well
   */
  public static String checkForRequiredNativeLibraries() {

    if (!Shell.WINDOWS) {
      return "";
    }
    StringBuilder errorText = new StringBuilder("");
    if (!NativeIO.isAvailable()) {
      errorText.append("No native IO library. ");
    }
    try {
      String path = Shell.getQualifiedBinPath(WINUTILS);
      log.debug("winutils is at {}", path);
    } catch (IOException e) {
      errorText.append("No " + WINUTILS);
      log.warn("No winutils: {}", e, e);
    }
    try {
      File target = new File("target");
      FileUtil.canRead(target);
    } catch (UnsatisfiedLinkError e) {
      log.warn("Failing to link to native IO methods: {}", e, e);
      errorText.append("No native IO methods");
    }
    return errorText.toString();
  }

  /**
   * Strictly verify that windows utils is present.
   * Checks go as far as opening the file and looking for
   * the headers. 
   * @throws IOException on any problem reading the file
   * @throws FileNotFoundException if the file is not considered valid
   */
  public static void maybeVerifyWinUtilsValid() throws
      IOException,
      SliderException {
    String errorText = SliderUtils.checkForRequiredNativeLibraries();
    if (!errorText.isEmpty()) {
      throw new BadClusterStateException(errorText);
    }
  }

  /**
   * Write bytes to a file
   * @param outfile output file
   * @param data data to write
   * @throws IOException on any IO problem
   */
  public static void write(File outfile, byte[] data)
      throws IOException {
    File parentDir = outfile.getCanonicalFile().getParentFile();
    if (parentDir == null) {
      throw new IOException(outfile.getPath() + " has no parent dir");
    }
    if (!parentDir.exists()) {
      if(!parentDir.mkdirs()) {
        throw new IOException("Failed to create parent directory " + parentDir);
      }
    }
    SliderUtils.verifyIsDir(parentDir, log);
    try(FileOutputStream out = new FileOutputStream(outfile)) {
      out.write(data);
    }
  }

  /**
   * Compare the times of two applications: most recent app comes first
   * Specifically: the one whose start time value is greater.
   */
  private static class MostRecentlyStartedAppFirst
      implements Comparator<ApplicationReport>, Serializable {
    @Override
    public int compare(ApplicationReport r1, ApplicationReport r2) {
      long x = r1.getStartTime();
      long y = r2.getStartTime();
      return compareTwoLongsReverse(x, y);
    }
  }
  
  /**
   * Compare the times of two applications: most recent app comes first.
   * "Recent"== the app whose start time <i>or finish time</i> is the greatest.
   */
  private static class MostRecentlyStartedOrFinishedFirst
      implements Comparator<ApplicationReport>, Serializable {
    @Override
    public int compare(ApplicationReport r1, ApplicationReport r2) {
      long started1 = r1.getStartTime();
      long started2 = r2.getStartTime();
      long finished1 = r1.getFinishTime();
      long finished2 = r2.getFinishTime();
      long lastEvent1 = Math.max(started1, finished1);
      long lastEvent2 = Math.max(started2, finished2);
      return compareTwoLongsReverse(lastEvent1, lastEvent2);
    }
  }

  /**
   * Compare the times of two applications: most recently finished app comes first
   * Specifically: the one whose finish time value is greater.
   */
  private static class MostRecentAppFinishFirst
      implements Comparator<ApplicationReport>, Serializable {
    @Override
    public int compare(ApplicationReport r1, ApplicationReport r2) {
      long x = r1.getFinishTime();
      long y = r2.getFinishTime();
      return compareTwoLongsReverse(x, y);
    }
  }

  /**
   * Compare two long values for sorting. As the return value for 
   * comparators must be int, the simple value of <code>x-y</code>
   * is inapplicable
   * @param x x value
   * @param y y value
   * @return +ve if x is less than y, -ve if y is greater than x; 0 for equality
   */
  public static int compareTwoLongsReverse(long x, long y) {
    return (x < y) ? 1 : ((x == y) ? 0 : -1);
  }

  public static String createNameTag(String name) {
    return "Name: " + name;
  }

  public static String createVersionTag(String version) {
    return "Version: " + version;
  }

  public static String createDescriptionTag(String description) {
    return "Description: " + description;
  }

  public static final String DAYS = ".days";
  public static final String HOURS = ".hours";
  public static final String MINUTES = ".minutes";
  public static final String SECONDS = ".seconds";
}
