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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.service.conf.YarnServiceConstants;
import org.apache.hadoop.yarn.service.containerlaunch.ClasspathConstructor;
import org.apache.hadoop.yarn.service.exceptions.BadClusterStateException;
import org.apache.hadoop.yarn.service.exceptions.SliderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic
    .HADOOP_SECURITY_DNS_INTERFACE_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic
    .HADOOP_SECURITY_DNS_NAMESERVER_KEY;

/**
 * These are slider-specific Util methods
 */
public final class ServiceUtils {

  private static final Logger log = LoggerFactory.getLogger(ServiceUtils.class);

  private ServiceUtils() {
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
   * Find a containing JAR
   * @param clazz class to find
   * @return the file
   * @throws IOException any IO problem, including the class not having a
   * classloader
   * @throws FileNotFoundException if the class did not resolve to a file
   */
  public static File findContainingJarOrFail(Class clazz) throws IOException {
    File localFile = ServiceUtils.findContainingJar(clazz);
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
    log.debug("Loading all dependencies from {}", srcPath);
    if (ServiceUtils.isSet(srcPath)) {
      File srcFolder = new File(srcPath);
      FilenameFilter jarFilter = createJarFilter();
      File[] listOfJars = srcFolder.listFiles(jarFilter);
      if (listOfJars == null || listOfJars.length == 0) {
        return;
      }
      for (File jarFile : listOfJars) {
        if (!jarFile.exists()) {
          log.debug("File does not exist, skipping: " + jarFile);
          continue;
        }
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
   * Create a file:// path from a local file
   * @param file file to point the path
   * @return a new Path
   */
  public static Path createLocalPath(File file) {
    return new Path(file.toURI());
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
      }
      classpath.addRemoteClasspathEnvVar();
      classpath.append(ApplicationConstants.Environment.HADOOP_CONF_DIR.$$());
    }
    return classpath;
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

  public static String createNameTag(String name) {
    return "Name: " + name;
  }

  public static String createVersionTag(String version) {
    return "Version: " + version;
  }

  public static String createDescriptionTag(String description) {
    return "Description: " + description;
  }

  // Copied from SecurityUtil because it is not public
  public static String getLocalHostName(@Nullable Configuration conf)
      throws UnknownHostException {
    if (conf != null) {
      String dnsInterface = conf.get(HADOOP_SECURITY_DNS_INTERFACE_KEY);
      String nameServer = conf.get(HADOOP_SECURITY_DNS_NAMESERVER_KEY);

      if (dnsInterface != null) {
        return DNS.getDefaultHost(dnsInterface, nameServer, true);
      } else if (nameServer != null) {
        throw new IllegalArgumentException(HADOOP_SECURITY_DNS_NAMESERVER_KEY +
            " requires " + HADOOP_SECURITY_DNS_INTERFACE_KEY + ". Check your" +
            "configuration.");
      }
    }

    // Fallback to querying the default hostname as we did before.
    return InetAddress.getLocalHost().getCanonicalHostName();
  }
}
