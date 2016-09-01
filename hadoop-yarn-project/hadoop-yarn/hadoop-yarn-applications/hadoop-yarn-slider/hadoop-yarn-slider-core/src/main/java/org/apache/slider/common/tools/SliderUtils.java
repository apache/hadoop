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

package org.apache.slider.common.tools;

import com.google.common.base.Preconditions;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.slider.Slider;
import org.apache.slider.api.InternalKeys;
import org.apache.slider.api.RoleKeys;
import org.apache.slider.api.types.ContainerInformation;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.SliderXmlConfKeys;
import org.apache.slider.common.params.Arguments;
import org.apache.slider.common.params.SliderActions;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.BadClusterStateException;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.core.exceptions.ErrorStrings;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.launch.ClasspathConstructor;
import org.apache.slider.core.main.LauncherExitCodes;
import org.apache.slider.providers.agent.AgentKeys;
import org.apache.slider.providers.agent.application.metadata.Component;
import org.apache.slider.server.services.utility.PatternValidator;
import org.apache.slider.server.services.workflow.ForkedProcessService;
import org.apache.zookeeper.server.util.KerberosUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
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
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.apache.slider.common.SliderKeys.COMPONENT_SEPARATOR;

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
   * type of docker standalone application
   */
  public static final String DOCKER = "docker";
  /**
   * type of docker on yarn application
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

  public static boolean isEmpty(List l) {
    return l == null || l.isEmpty();
  }

  /**
   * Probe for a list existing and not being empty
   * @param l list
   * @return true if the reference is valid and it contains entries
   */

  public static boolean isNotEmpty(List l) {
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
    if (urlEnumeration == null) {
      throw new IOException("Unable to find resources for class " + my_class);
    }

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

  public static boolean oldIsClusternameValid(String name) {
    if (name == null || name.isEmpty()) {
      return false;
    }
    int first = name.charAt(0);
    if (0 == (Character.getType(first) & Character.LOWERCASE_LETTER)) {
      return false;
    }

    for (int i = 0; i < name.length(); i++) {
      int elt = (int) name.charAt(i);
      int t = Character.getType(elt);
      if (0 == (t & Character.LOWERCASE_LETTER)
          && 0 == (t & Character.DECIMAL_DIGIT_NUMBER)
          && elt != '-'
          && elt != '_') {
        return false;
      }
      if (!Character.isLetterOrDigit(elt) && elt != '-' && elt != '_') {
        return false;
      }
    }
    return true;
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
    if (destFS.exists(destFile)) {
      throw new IOException("Dest file already exists " + destFile);
    }
    FileUtil.copy(srcFS, srcFile, destFS, destFile, false, true, conf);
  }

  public static String stringify(Throwable t) {
    StringWriter sw = new StringWriter();
    sw.append(t.toString()).append('\n');
    t.printStackTrace(new PrintWriter(sw));
    return sw.toString();
  }

  /**
   * Create a configuration with Slider-specific tuning.
   * This is done rather than doing custom configs.
   * @return the config
   */
  public static YarnConfiguration createConfiguration() {
    YarnConfiguration conf = new YarnConfiguration();
    patchConfiguration(conf);
    return conf;
  }

  /**
   * Take an existing conf and patch it for Slider's needs. Useful
   * in Service.init & RunService methods where a shared config is being
   * passed in
   * @param conf configuration
   * @return the patched configuration
   */
  public static Configuration patchConfiguration(Configuration conf) {

    //if the fallback option is NOT set, enable it.
    //if it is explicitly set to anything -leave alone
    if (conf.get(SliderXmlConfKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH) == null) {
      conf.set(SliderXmlConfKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH, "true");
    }
    return conf;
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
    builder.append("application ")
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
   * Convert the instance details of an application to a string
   * @param name instance name
   * @param report the application report
   * @param verbose verbose output
   * @return a string
   */
  public static String instanceDetailsToString(String name,
      ApplicationReport report,
      List<ContainerInformation> containers,
      String version,
      Set<String> components,
      boolean verbose) {
    // format strings
    String staticf = "%-30s";
    String reportedf = staticf + "  %10s  %-42s";
    String livef = reportedf + "  %s";
    StringBuilder builder = new StringBuilder(200);
    if (report == null) {
      builder.append(String.format(staticf, name));
    } else {
      // there's a report to look at
      String appId = report.getApplicationId().toString();
      String state = report.getYarnApplicationState().toString();
      if (report.getYarnApplicationState() == YarnApplicationState.RUNNING) {
        // running: there's a URL
        builder.append(
            String.format(livef, name, state, appId, report.getTrackingUrl()));
      } else {
        builder.append(String.format(reportedf, name, state, appId));
      }
      if (verbose) {
        builder.append('\n');
        builder.append(SliderUtils.appReportToString(report, "\n  "));
      }
      if (containers != null) {
        builder.append('\n');
        builder.append(SliderUtils.containersToString(containers, version,
            components));
      }
    }

    builder.append('\n');
    return builder.toString();
  }

  public static String containersToString(
      List<ContainerInformation> containers, String version,
      Set<String> components) {
    String containerf = "  %-28s  %30s  %45s  %s\n";
    StringBuilder builder = new StringBuilder(512);
    builder.append("Containers:\n");
    builder.append(String.format("  %-28s  %30s  %45s  %s\n", "Component Name",
        "App Version", "Container Id", "Container Info/Logs"));
    for (ContainerInformation container : containers) {
      if (filter(container.appVersion, version)
          || filter(container.component, components)) {
        continue;
      }
      builder.append(String.format(containerf, container.component,
          container.appVersion, container.containerId, container.host
              + SliderKeys.YARN_CONTAINER_PATH + container.containerId));
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
   * Sorts the given list of application reports, most recently started 
   * or finished instance first.
   *
   * @param instances list of instances
   */
  public static void sortApplicationsByMostRecent(List<ApplicationReport> instances) {
    Collections.sort(instances, new MostRecentlyStartedOrFinishedFirst());
  }

  /**
   * Sorts the given list of application reports
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
   * Built a (sorted) map of application reports, mapped to the instance name
   * The list is sorted, and the addition process does not add a report
   * if there is already one that exists. If the list handed in is sorted,
   * those that are listed first form the entries returned
   * @param instances list of intances
   * @param minState minimum YARN state to be included
   * @param maxState maximum YARN state to be included
   * @return all reports in the list whose state &gt;= minimum and &lt;= maximum
   */
  public static Map<String, ApplicationReport> buildApplicationReportMap(
      List<ApplicationReport> instances,
      YarnApplicationState minState, YarnApplicationState maxState) {
    TreeMap<String, ApplicationReport> map = new TreeMap<>();
    for (ApplicationReport report : instances) {
      YarnApplicationState state = report.getYarnApplicationState();
      if (state.ordinal() >= minState.ordinal() &&
          state.ordinal() <= maxState.ordinal() &&
          map.get(report.getName()) == null) {
        map.put(report.getName(), report);
      }
    }
    return map;
  }

  /**
   * Take a map and produce a sorted equivalent
   * @param source source map
   * @return a map whose iterator returns the string-sorted ordering of entries
   */
  public static Map<String, String> sortedMap(Map<String, String> source) {
    Map<String, String> out = new TreeMap<>(source);
    return out;
  }

  /**
   * Convert a properties instance to a string map.
   * @param properties source property object
   * @return a string map
   */
  public static Map<String, String> toMap(Properties properties) {
    Map<String, String> out = new HashMap<>(properties.size());
    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      out.put(entry.getKey().toString(), entry.getValue().toString());
    }
    return out;
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
   * Merge string maps excluding prefixes
   * @param first first map
   * @param second second map
   * @param  prefixes prefixes to ignore
   * @return 'first' merged with the second
   */
  public static Map<String, String> mergeMapsIgnoreDuplicateKeysAndPrefixes(
      Map<String, String> first, Map<String, String> second,
      String... prefixes) {
    Preconditions.checkArgument(first != null, "Null 'first' value");
    Preconditions.checkArgument(second != null, "Null 'second' value");
    Preconditions.checkArgument(prefixes != null, "Null 'prefixes' value");
    for (Map.Entry<String, String> entry : second.entrySet()) {
      String key = entry.getKey();
      boolean hasPrefix = false;
      for (String prefix : prefixes) {
        if (key.startsWith(prefix)) {
          hasPrefix = true;
          break;
        }
      }
      if (hasPrefix) {
        continue;
      }
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
   * Get the int value of a role
   * @param roleMap map of role key->val entries
   * @param key key the key to look for
   * @param defVal default value to use if the key is not in the map
   * @param min min value or -1 for do not check
   * @param max max value or -1 for do not check
   * @return the int value the integer value
   * @throws BadConfigException if the value could not be parsed
   */
  public static int getIntValue(Map<String, String> roleMap,
      String key,
      int defVal,
      int min,
      int max
  ) throws BadConfigException {
    String valS = roleMap.get(key);
    return parseAndValidate(key, valS, defVal, min, max);

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
      return "Null application report";
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

  public static int findFreePort(int start, int limit) {
    if (start == 0) {
      //bail out if the default is "dont care"
      return 0;
    }
    int found = 0;
    int port = start;
    int finish = start + limit;
    while (found == 0 && port < finish) {
      if (isPortAvailable(port)) {
        found = port;
      } else {
        port++;
      }
    }
    return found;
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

  /**
   * Build the environment map from a role option map, finding all entries
   * beginning with "env.", adding them to a map of (prefix-removed)
   * env vars
   * @param roleOpts role options. This can be null, meaning the
   * role is undefined
   * @return a possibly empty map of environment variables.
   */
  public static Map<String, String> buildEnvMap(Map<String, String> roleOpts) {
    return buildEnvMap(roleOpts, null);
  }

  public static Map<String, String> buildEnvMap(Map<String, String> roleOpts,
      Map<String,String> tokenMap) {
    Map<String, String> env = new HashMap<>();
    if (roleOpts != null) {
      for (Map.Entry<String, String> entry : roleOpts.entrySet()) {
        String key = entry.getKey();
        if (key.startsWith(RoleKeys.ENV_PREFIX)) {
          String envName = key.substring(RoleKeys.ENV_PREFIX.length());
          if (!envName.isEmpty()) {
            String value = entry.getValue();
            if (tokenMap != null) {
              for (Map.Entry<String,String> token : tokenMap.entrySet()) {
                value = value.replaceAll(Pattern.quote(token.getKey()),
                    token.getValue());
              }
            }
            env.put(envName, value);
          }
        }
      }
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
   * verify that the supplied cluster name is valid
   * @param clustername cluster name
   * @throws BadCommandArgumentsException if it is invalid
   */
  public static void validateClusterName(String clustername) throws
      BadCommandArgumentsException {
    if (!isClusternameValid(clustername)) {
      throw new BadCommandArgumentsException(
          "Illegal cluster name: " + clustername);
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
         "the application has already set up its user as an insecure entity %s",
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
    SliderUtils.verifyPrincipalSet(conf, SliderXmlConfKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY);
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
   * @throws IOException, SliderException trouble copying to HDFS
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
   * @throws IOException, SliderException trouble copying to HDFS
   */
  public static void putAmTarGzipAndUpdate(
      Map<String, LocalResource> providerResources,
      SliderFileSystem sliderFileSystem
  ) throws IOException, SliderException {
    log.info("Loading all dependencies from {}{}",
        SliderKeys.SLIDER_DEPENDENCY_TAR_GZ_FILE_NAME,
        SliderKeys.SLIDER_DEPENDENCY_TAR_GZ_FILE_EXT);
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

  /**
   * Get the current user -relays to
   * {@link UserGroupInformation#getCurrentUser()}
   * with any Slider-specific post processing and exception handling
   * @return user info
   * @throws IOException on a failure to get the credentials
   */
  public static UserGroupInformation getCurrentUser() throws IOException {

    try {
      UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
      return currentUser;
    } catch (IOException e) {
      log.info("Failed to get user info", e);
      throw e;
    }
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
   * Register the client resource in
   * {@link SliderKeys#SLIDER_CLIENT_XML}
   * for Configuration instances.
   *
   * @return true if the resource could be loaded
   */
  public static URL registerClientResource() {
    return ConfigHelper.registerDefaultResource(SliderKeys.SLIDER_CLIENT_XML);
  }
  
  /**
   * Attempt to load the slider client resource. If the
   * resource is not on the CP an empty config is returned.
   * @return a config
   */
  public static Configuration loadSliderClientXML() {
    return ConfigHelper.loadFromResource(SliderKeys.SLIDER_CLIENT_XML);
  }

  /**
   * Convert a char sequence to a string.
   * This ensures that comparisons work
   * @param charSequence source
   * @return the string equivalent
   */
  public static String sequenceToString(CharSequence charSequence) {
    StringBuilder stringBuilder = new StringBuilder(charSequence);
    return stringBuilder.toString();
  }

  /**
   * Build up the classpath for execution
   * -behaves very differently on a mini test cluster vs a production
   * production one.
   *
   * @param sliderConfDir relative path to the dir containing slider config
   *                      options to put on the classpath -or null
   * @param libdir directory containing the JAR files
   * @param config the configuration
   * @param usingMiniMRCluster flag to indicate the MiniMR cluster is in use
   * (and hence the current classpath should be used, not anything built up)
   * @return a classpath
   */
  public static ClasspathConstructor buildClasspath(String sliderConfDir,
      String libdir,
      Configuration config,
      SliderFileSystem sliderFileSystem,
      boolean usingMiniMRCluster) {

    ClasspathConstructor classpath = new ClasspathConstructor();

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
        classpath.addLibDir(SliderKeys.SLIDER_DEPENDENCY_LOCALIZED_DIR_LINK);
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
   * Convert an epoch time to a GMT time. This
   * uses the deprecated Date.toString() operation,
   * so is in one place to reduce the number of deprecation warnings.
   * @param time timestamp
   * @return string value as ISO-9601
   */
  @SuppressWarnings({"CallToDateToString", "deprecation"})
  public static String toGMTString(long time) {
    return new Date(time).toGMTString();
  }

  /**
   * Add the cluster build information; this will include Hadoop details too
   * @param info cluster info
   * @param prefix prefix for the build info
   */
  public static void addBuildInfo(Map<String, String> info, String prefix) {

    Properties props = SliderVersionInfo.loadVersionProperties();
    info.put(prefix + "." + SliderVersionInfo.APP_BUILD_INFO, props.getProperty(
        SliderVersionInfo.APP_BUILD_INFO));
    info.put(prefix + "." + SliderVersionInfo.HADOOP_BUILD_INFO,
        props.getProperty(SliderVersionInfo.HADOOP_BUILD_INFO));

    info.put(prefix + "." + SliderVersionInfo.HADOOP_DEPLOYED_INFO,
        VersionInfo.getBranch() + " @" + VersionInfo.getSrcChecksum());
  }

  /**
   * Set the time for an information (human, machine) timestamp pair of fields.
   * The human time is the time in millis converted via the {@link Date} class.
   * @param info info fields
   * @param keyHumanTime name of human time key
   * @param keyMachineTime name of machine time
   * @param time timestamp
   */
  public static void setInfoTime(Map info,
      String keyHumanTime,
      String keyMachineTime,
      long time) {
    info.put(keyHumanTime, SliderUtils.toGMTString(time));
    info.put(keyMachineTime, Long.toString(time));
  }

  public static Path extractImagePath(CoreFileSystem fs,
      MapOperations internalOptions) throws
      SliderException, IOException {
    Path imagePath;
    String imagePathOption =
        internalOptions.get(InternalKeys.INTERNAL_APPLICATION_IMAGE_PATH);
    String appHomeOption =
        internalOptions.get(InternalKeys.INTERNAL_APPLICATION_HOME);
    if (!isUnset(imagePathOption)) {
      if (!isUnset(appHomeOption)) {
        throw new BadClusterStateException(
            ErrorStrings.E_BOTH_IMAGE_AND_HOME_DIR_SPECIFIED);
      }
      imagePath = fs.createPathThatMustExist(imagePathOption);
    } else {
      imagePath = null;
      if (isUnset(appHomeOption)) {
        throw new BadClusterStateException(
            ErrorStrings.E_NO_IMAGE_OR_HOME_DIR_SPECIFIED);
      }
    }
    return imagePath;
  }

  /**
   * trigger a  JVM halt with no clean shutdown at all
   * @param status status code for exit
   * @param text text message
   * @param delay delay in millis
   * @return the timer (assuming the JVM hasn't halted yet)
   *
   */
  public static Timer haltAM(int status, String text, int delay) {

    Timer timer = new Timer("halt timer", false);
    timer.schedule(new DelayedHalt(status, text), delay);
    return timer;
  }

  public static String propertiesToString(Properties props) {
    TreeSet<String> keys = new TreeSet<>(props.stringPropertyNames());
    StringBuilder builder = new StringBuilder();
    for (String key : keys) {
      builder.append(key)
             .append("=")
             .append(props.getProperty(key))
             .append("\n");
    }
    return builder.toString();
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
   * Append a list of paths, inserting "/" signs as appropriate
   * @param base base path/URL
   * @param paths subpaths
   * @return base+"/"+paths[0]+"/"+paths[1]...
   */
  public static String appendToURL(String base, String... paths) {
    String result = base;
    for (String path : paths) {
      result = appendToURL(result, path);
    }
    return result;
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
   * Get a string node label value from a node report
   * @param report node report
   * @return a single trimmed label or ""
   */
  public static String extractNodeLabel(NodeReport report) {
    Set<String> newlabels = report.getNodeLabels();
    if (newlabels != null && !newlabels.isEmpty()) {
      return newlabels.iterator().next().trim();
    } else {
      return "";
    }
  }

  /**
   * Callable for async/scheduled halt
   */
  public static class DelayedHalt extends TimerTask {
    private final int status;
    private final String text;

    public DelayedHalt(int status, String text) {
      this.status = status;
      this.text = text;
    }

    @Override
    public void run() {
      try {
        ExitUtil.halt(status, text);
        //this should never be reached
      } catch (ExitUtil.HaltException e) {
        log.info("Halt failed");
      }
    }
  }

  /**
   * A compareTo function that converts the result of a long
   * comparision into the integer that <code>Comparable</code>
   * expects.
   * @param left left side
   * @param right right side
   * @return -1, 0, 1 depending on the diff
   */
  public static int compareTo(long left, long right) {
    long diff = left - right;
    if (diff < 0) {
      return -1;
    }
    if (diff > 0) {
      return 1;
    }
    return 0;
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
   * @param srcFolder
   * @param tarGzipFile
   * 
   * @throws IOException
   */
  public static void tarGzipFolder(File srcFolder, File tarGzipFile,
      FilenameFilter filter) throws IOException {
    log.info("Tar-gzipping folder {} to {}", srcFolder.getAbsolutePath(),
        tarGzipFile.getAbsolutePath());
    List<String> files = new ArrayList<>();
    generateFileList(files, srcFolder, srcFolder, true, filter);

    try(TarArchiveOutputStream taos =
            new TarArchiveOutputStream(new GZIPOutputStream(
        new BufferedOutputStream(new FileOutputStream(tarGzipFile))))) {
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

  /**
   * Retrieve the HDP version if it is an HDP cluster, or null otherwise. It
   * first checks if system property HDP_VERSION is defined. If not it checks if
   * system env HDP_VERSION is defined.
   * 
   * @return HDP version (if defined) or null otherwise
   */
  public static String getHdpVersion() {
    String hdpVersion = System
        .getProperty(SliderKeys.HDP_VERSION_PROP_NAME);
    if (StringUtils.isEmpty(hdpVersion)) {
      hdpVersion = System.getenv(SliderKeys.HDP_VERSION_PROP_NAME);
    }
    return hdpVersion;
  }

  /**
   * Query to find if it is an HDP cluster
   * 
   * @return true if this is invoked in an HDP cluster or false otherwise
   */
  public static boolean isHdp() {
    return StringUtils.isNotEmpty(getHdpVersion());
  }

  /**
   * Retrieve the version of the current Slider install
   * 
   * @return the version string of the Slider release
   */
  public static String getSliderVersion() {
    if (isHdp()) {
      return getHdpVersion();
    } else {
      Properties props = SliderVersionInfo.loadVersionProperties();
      return props.getProperty(SliderVersionInfo.APP_VERSION);
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
   * This wraps ApplicationReports and generates a string version
   * iff the toString() operator is invoked
   */
  public static class OnDemandReportStringifier {
    private final ApplicationReport report;

    public OnDemandReportStringifier(ApplicationReport report) {
      this.report = report;
    }

    @Override
    public String toString() {
      return appReportToString(report, "\n");
    }
  }

  public static InputStream getApplicationResourceInputStream(FileSystem fs,
      Path appPath,
      String entry)
      throws IOException {
    InputStream is = null;
    try(FSDataInputStream appStream = fs.open(appPath)) {
      ZipArchiveInputStream zis = new ZipArchiveInputStream(appStream);
      ZipArchiveEntry zipEntry;
      boolean done = false;
      while (!done && (zipEntry = zis.getNextZipEntry()) != null) {
        if (entry.equals(zipEntry.getName())) {
          int size = (int) zipEntry.getSize();
          if (size != -1) {
            log.info("Reading {} of size {}", zipEntry.getName(),
                zipEntry.getSize());
            byte[] content = new byte[size];
            int offset = 0;
            while (offset < size) {
              offset += zis.read(content, offset, size - offset);
            }
            is = new ByteArrayInputStream(content);
          } else {
            log.debug("Size unknown. Reading {}", zipEntry.getName());
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            while (true) {
              int byteRead = zis.read();
              if (byteRead == -1) {
                break;
              }
              baos.write(byteRead);
            }
            is = new ByteArrayInputStream(baos.toByteArray());
          }
          done = true;
        }
      }
    }

    return is;
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

  public static void verifyIsFile(String program, File exe) throws
      FileNotFoundException {
    if (!exe.isFile()) {
      throw new FileNotFoundException(program
                                      + " at " + exe
                                      + " is not a file");

    }
  }

  public static void verifyFileSize(String program,
      File exe,
      int minFileSize) throws FileNotFoundException {
    if (exe.length() < minFileSize) {
      throw new FileNotFoundException(program
                                      + " at " + exe
                                      + " is too short to be an executable");
    }
  }

  /**
   * Look for the windows executable and check it has the right headers.
   * <code>File.canRead()</code> doesn't work on windows, so the reading
   * is mandatory.
   *
   * @param program program name for errors
   * @param exe executable
   * @throws IOException IOE
   */
  public static void verifyWindowsExe(String program, File exe)
      throws IOException {
    verifyIsFile(program, exe);

    verifyFileSize(program, exe, 0x100);

    // now read two bytes and verify the header.
    try(FileReader reader = new FileReader(exe)) {
      int[] header = new int[2];
      header[0] = reader.read();
      header[1] = reader.read();
      if ((header[0] != 'M' || header[1] != 'Z')) {
        throw new FileNotFoundException(program
                                        + " at " + exe
                                        + " is not a windows executable file");
      }
    }
  }

  /**
   * Verify that a Unix exe works
   * @param program program name for errors
   * @param exe executable
   * @throws IOException IOE

   */
  public static void verifyUnixExe(String program, File exe)
      throws IOException {
    verifyIsFile(program, exe);

    // read flag
    if (!exe.canRead()) {
      throw new IOException("Cannot read " + program + " at " + exe);
    }
    // exe flag
    if (!exe.canExecute()) {
      throw new IOException("Cannot execute " + program + " at " + exe);
    }
  }

  /**
   * Validate an executable
   * @param program program name for errors
   * @param exe program to look at
   * @throws IOException
   */
  public static void validateExe(String program, File exe) throws IOException {
    if (!Shell.WINDOWS) {
      verifyWindowsExe(program, exe);
    } else {
      verifyUnixExe(program, exe);
    }
  }

  /**
   * Write bytes to a file
   * @param outfile output file
   * @param data data to write
   * @param createParent flag to indicate that the parent dir should
   * be created
   * @throws IOException on any IO problem
   */
  public static void write(File outfile, byte[] data, boolean createParent)
      throws IOException {
    File parentDir = outfile.getCanonicalFile().getParentFile();
    if (parentDir == null) {
      throw new IOException(outfile.getPath() + " has no parent dir");
    }
    if (createParent) {
      parentDir.mkdirs();
    }
    SliderUtils.verifyIsDir(parentDir, log);
    try(FileOutputStream out = new FileOutputStream(outfile)) {
      out.write(data);
    }

  }

  /**
   * Execute a command for a test operation
   * @param name name in error
   * @param status status code expected
   * @param timeoutMillis timeout in millis for process to finish
   * @param logger
   * @param outputString optional string to grep for (must not span a line)
   * @param commands commands   @return the process
   * @throws IOException on any failure.
   */
  public static ForkedProcessService execCommand(String name,
      int status,
      long timeoutMillis,
      Logger logger,
      String outputString,
      String... commands) throws IOException, SliderException {
    Preconditions.checkArgument(isSet(name), "no name");
    Preconditions.checkArgument(commands.length > 0, "no commands");
    Preconditions.checkArgument(isSet(commands[0]), "empty command");

    ForkedProcessService process;


    process = new ForkedProcessService(
        name,
        new HashMap<String, String>(),
        Arrays.asList(commands));
    process.setProcessLog(logger);
    process.init(new Configuration());
    String errorText = null;
    process.start();
    try {
      if (!process.waitForServiceToStop(timeoutMillis)) {
        throw new TimeoutException(
            "Process did not stop in " + timeoutMillis + "mS");
      }
      int exitCode = process.getExitCode();
      List<String> recentOutput = process.getRecentOutput();
      if (status != exitCode) {
        // error condition
        errorText = "Expected exit code={" + status + "}, "
                    + "actual exit code={" + exitCode + "}";
      } else {
        if (isSet(outputString)) {
          boolean found = false;
          for (String line : recentOutput) {
            if (line.contains(outputString)) {
              found = true;
              break;
            }
          }
          if (!found) {
            errorText = "Did not find \"" + outputString + "\""
                        + " in output";
          }
        }
      }
      if (errorText == null) {
        return process;
      }

    } catch (TimeoutException e) {
      errorText = e.toString();
    }
    // error text: non null ==> operation failed
    log.warn(errorText);
    List<String> recentOutput = process.getRecentOutput();
    for (String line : recentOutput) {
      log.info(line);
    }
    throw new SliderException(LauncherExitCodes.EXIT_OTHER_FAILURE,
        "Process %s failed: %s", name, errorText);

  }


  /**
   * Validate the slider client-side execution environment.
   * This looks for everything felt to be critical for execution, including
   * native binaries and other essential dependencies.
   * @param logger logger to log to on normal execution
   * @throws IOException on IO failures
   * @throws SliderException on validation failures
   */
  public static void validateSliderClientEnvironment(Logger logger) throws
      IOException,
      SliderException {
    maybeVerifyWinUtilsValid();
  }

  /**
   * Validate the slider server-side execution environment.
   * This looks for everything felt to be critical for execution, including
   * native binaries and other essential dependencies.
   * @param logger logger to log to on normal execution
   * @param dependencyChecks flag to indicate checks for agent dependencies
   * @throws IOException on IO failures
   * @throws SliderException on validation failures
   */
  public static void validateSliderServerEnvironment(Logger logger,
      boolean dependencyChecks) throws
      IOException,
      SliderException {
    maybeVerifyWinUtilsValid();
    if (dependencyChecks) {
      validatePythonEnv(logger);
      validateOpenSSLEnv(logger);
    }
  }

  public static void validateOpenSSLEnv(Logger logger) throws
      IOException,
      SliderException {
    execCommand(OPENSSL, 0, 5000, logger, "OpenSSL", OPENSSL, "version");
  }

  public static void validatePythonEnv(Logger logger) throws
      IOException,
      SliderException {
    execCommand(PYTHON, 0, 5000, logger, "Python", PYTHON, "-V");
  }

  /**
   * return the path to the currently running slider command
   *
   * @throws NullPointerException
   *             - If the pathname argument is null
   * @throws SecurityException
   *             - if a security manager exists and its checkPermission method
   *             doesn't allow getting the ProtectionDomain
   */
  public static String getCurrentCommandPath() {
    File f = new File(Slider.class.getProtectionDomain().getCodeSource()
                                  .getLocation().getPath());
    return f.getAbsolutePath();
  }

  /**
   * return the HDFS path where the application package has been uploaded
   * manually or by using slider client (install package command)
   * 
   * @param conf configuration
   * @return
   */
  public static String getApplicationDefinitionPath(ConfTreeOperations conf)
      throws BadConfigException {
    return getApplicationDefinitionPath(conf, null);
  }

  /**
   * return the HDFS path where the application package has been uploaded
   * manually or by using slider client (install package command)
   *
   * @param conf configuration
   * @param roleGroup name of component
   * @return
   */
  public static String getApplicationDefinitionPath(ConfTreeOperations conf,
      String roleGroup)
      throws BadConfigException {
    String appDefPath = conf.getGlobalOptions().getMandatoryOption(
        AgentKeys.APP_DEF);
    if (roleGroup != null) {
      MapOperations component = conf.getComponent(roleGroup);
      if (component != null) {
        appDefPath = component.getOption(AgentKeys.APP_DEF, appDefPath);
      }
    }
    return appDefPath;
  }

  /**
   * return the path to the slider-client.xml used by the current running
   * slider command
   *
   * @throws SecurityException
   *             - if a security manager exists and its checkPermission method
   *             denies access to the class loader for the class
   */
  public static String getClientConfigPath() {
    URL path = ConfigHelper.class.getClassLoader().getResource(
        SliderKeys.SLIDER_CLIENT_XML);
    Preconditions.checkNotNull(path, "Failed to locate resource " + SliderKeys.SLIDER_CLIENT_XML);
    return path.toString();
  }

  /**
   * validate if slider-client.xml under the path can be opened
   *
   * @throws IOException
   *             : the file can't be found or open
   */
  public static void validateClientConfigFile() throws IOException {
    URL resURL = SliderVersionInfo.class.getClassLoader().getResource(
        SliderKeys.SLIDER_CLIENT_XML);
    if (resURL == null) {
      throw new IOException(
          "slider-client.xml doesn't exist on the path: "
          + getClientConfigPath());
    }

    try {
      InputStream inStream = resURL.openStream();
      if (inStream == null) {
        throw new IOException("slider-client.xml can't be opened");
      }
    } catch (IOException e) {
      throw new IOException("slider-client.xml can't be opened: "
                            + e.toString());
    }
  }

  /**
   * validate if a file on HDFS can be open
   *
   * @throws IOException the file can't be found or opened
   * @throws URISyntaxException
   */
  public static void validateHDFSFile(SliderFileSystem sliderFileSystem,
      String pathStr)
      throws IOException, URISyntaxException {
    try(InputStream inputStream =
            sliderFileSystem.getFileSystem().open(new Path(new URI(pathStr)))) {
      if (inputStream == null) {
        throw new IOException("HDFS file " + pathStr + " can't be opened");
      }
    }
  }

  /**
   * return the version and path of the JDK invoking the current running
   * slider command
   *
   * @throws SecurityException
   *             - if a security manager exists and its checkPropertyAccess
   *             method doesn't allow access to the specified system property.
   */
  public static String getJDKInfo() {
    String version = System.getProperty("java.version");
    String javaHome = System.getProperty("java.home");
    return
        "The version of the JDK invoking the current running slider command: "
        + version + "; The path to it is: " + javaHome;
  }

  /**
   * return a description of whether the current user has created credential
   * cache files from kerberos servers
   *
   * @throws IOException
   * @throws BadConfigException
   * @throws SecurityException
   *             - if a security manager exists and its checkPropertyAccess
   *             method doesn't allow access to the specified system property.
   */
  public static String checkCredentialCacheFile() throws IOException,
      BadConfigException {
    String result = null;
    if (!Shell.WINDOWS) {
      result = Shell.execCommand("klist");
    }
    return result;
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

  public static String getSystemEnv(String property) {
    return System.getenv(property);
  }

  public static Map<String, String> getSystemEnv() {
    return System.getenv();
  }

  public static String requestToString(AMRMClient.ContainerRequest request) {
    Preconditions.checkArgument(request != null, "Null request");
    StringBuilder buffer = new StringBuilder(request.toString());
    buffer.append("; ");
    buffer.append("relaxLocality=").append(request.getRelaxLocality()).append("; ");
    String labels = request.getNodeLabelExpression();
    if (labels != null) {
      buffer.append("nodeLabels=").append(labels).append("; ");
    }
    List<String> nodes = request.getNodes();
    if (nodes != null) {
      buffer.append("Nodes = [ ");
      int size = nodes.size();
      for (int i = 0; i < Math.min(NODE_LIST_LIMIT, size); i++) {
        buffer.append(nodes.get(i)).append(' ');
      }
      if (size > NODE_LIST_LIMIT) {
        buffer.append(String.format("...(total %d entries)", size));
      }
      buffer.append("]; ");
    }
    List<String> racks = request.getRacks();
    if (racks != null) {
      buffer.append("racks = [")
          .append(join(racks, ", ", false))
          .append("]; ");
    }
    return buffer.toString();
  }

  public static String trimPrefix(String prefix) {
    if (prefix != null && prefix.endsWith(COMPONENT_SEPARATOR)) {
      return prefix.substring(0, prefix.length()-1);
    }
    return prefix;
  }
}
