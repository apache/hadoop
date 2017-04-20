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

package org.apache.slider.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.slider.api.resource.Application;
import org.apache.slider.api.resource.Container;
import org.apache.slider.client.SliderClient;
import org.apache.slider.common.params.Arguments;
import org.apache.slider.common.tools.Duration;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.main.LauncherExitCodes;
import org.apache.slider.core.main.ServiceLaunchException;
import org.apache.slider.core.main.ServiceLauncher;
import org.apache.slider.core.persist.JsonSerDeser;
import org.apache.slider.core.registry.docstore.PublishedConfigSet;
import org.apache.slider.core.registry.docstore.PublishedConfiguration;
import org.apache.slider.server.services.workflow.ForkedProcessService;
import org.codehaus.jackson.map.PropertyNamingStrategy;
import org.junit.Assert;
import org.junit.Assume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;

import static org.apache.slider.common.params.Arguments.ARG_OPTION;

/**
 * Static utils for tests in this package and in other test projects.
 *
 * It is designed to work with mini clusters as well as remote ones
 *
 * This class is not final and may be extended for test cases.
 *
 * Some of these methods are derived from the SwiftUtils and SwiftTestUtils
 * classes -replicated here so that they are available in Hadoop-2.0 code
 */
public class SliderTestUtils extends Assert {
  private static final Logger LOG =
      LoggerFactory.getLogger(SliderTestUtils.class);
  public static final String DEFAULT_SLIDER_CLIENT = SliderClient.class
      .getName();
  private static String sliderClientClassName = DEFAULT_SLIDER_CLIENT;

  public static final Map<String, String> EMPTY_MAP = Collections.emptyMap();
  public static final Map<String, Integer> EMPTY_INT_MAP = Collections
      .emptyMap();
  public static final List<String> EMPTY_LIST = Collections.emptyList();

  public static final ObjectReader OBJECT_READER;
  public static final ObjectWriter OBJECT_WRITER;

  public static final JsonSerDeser<Application> JSON_SER_DESER =
      new JsonSerDeser<>(Application.class,
          PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

  static {
    ObjectMapper mapper = new ObjectMapper();
    OBJECT_READER = mapper.readerFor(Object.class);
    OBJECT_WRITER = mapper.writer();
  }

  /**
   * Action that returns an object.
   */
  public interface Action {
    Object invoke() throws Exception;
  }

  /**
   * Probe that returns an Outcome.
   */
  public interface Probe {
    Outcome invoke(Map args) throws Exception;
  }

  public static void setSliderClientClassName(String sliderClientClassName) {
    sliderClientClassName = sliderClientClassName;
  }

  public static void describe(String s) {
    LOG.info("");
    LOG.info("===============================");
    LOG.info(s);
    LOG.info("===============================");
    LOG.info("");
  }

  /**
   * Convert a JSON string to something readable.
   * @param json
   * @return a string for printing
   */
  public static String prettyPrintJson(String json) {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    return gson.toJson(new JsonParser().parse(json));
  }

  /**
   * Convert an object to something readable.
   * @param src
   * @return a string for printing
   */
  public static String prettyPrintAsJson(Object src)
      throws JsonProcessingException, UnsupportedEncodingException {
    return new String(OBJECT_WRITER.writeValueAsBytes(src), "UTF8");
  }

  /**
   * Skip the test with a message.
   * @param message message logged and thrown
   */
  public static void skip(String message) {
    LOG.warn("Skipping test: {}", message);
    Assume.assumeTrue(message, false);
  }

  /**
   * Skip the test with a message if condition holds.
   * @param condition predicate
   * @param message message logged and thrown
   */
  public static void assume(boolean condition, String message) {
    if (!condition) {
      skip(message);
    }
  }

  /**
   * Skip a test if not running on Windows.
   */
  public static void assumeWindows() {
    assume(Shell.WINDOWS, "not windows");
  }

  /**
   * Skip a test if running on Windows.
   */
  public static void assumeNotWindows() {
    assume(!Shell.WINDOWS, "windows");
  }

  /**
   * Skip a test on windows.
   */
  public static void skipOnWindows() {
    assumeNotWindows();
  }

  /**
   * Equality size for a list.
   * @param left
   * @param right
   */
  public static void assertListEquals(List left, List right) {
    String lval = collectionToString(left);
    String rval = collectionToString(right);
    String text = "comparing " + lval + " to " + rval;
    assertEquals(text, left.size(), right.size());
    for (int i = 0; i < left.size(); i++) {
      assertEquals(text, left.get(i), right.get(i));
    }
  }

  /**
   * Assert a list has a given length.
   * @param list list
   * @param size size to have
   */
  public static void assertListLength(List list, int size) {
    String lval = collectionToString(list);
    assertEquals(lval, size, list.size());
  }

  /**
   * Stringify a collection with [ ] at either end.
   * @param collection collection
   * @return string value
   */
  public static String collectionToString(List collection) {
    return "[" + SliderUtils.join(collection, ", ", false) + "]";
  }

  /**
   * Assume that a string option is set and not equal to "".
   * @param conf configuration file
   * @param key key to look for
   */
  public static void assumeStringOptionSet(Configuration conf, String key) {
    if (SliderUtils.isUnset(conf.getTrimmed(key))) {
      skip("Configuration key " + key + " not set");
    }
  }

  /**
   * assert that a string option is set and not equal to "".
   * @param conf configuration file
   * @param key key to look for
   */
  public static void assertStringOptionSet(Configuration conf, String key) {
    getRequiredConfOption(conf, key);
  }

  /**
   * Assume that a boolean option is set and true.
   * Unset or false triggers a test skip
   * @param conf configuration file
   * @param key key to look for
   */
  public static void assumeBoolOptionTrue(Configuration conf, String key) {
    assumeBoolOption(conf, key, false);
  }

  /**
   * Assume that a boolean option is true.
   * False triggers a test skip
   * @param conf configuration file
   * @param key key to look for
   * @param defval default value if the property is not defined
   */
  public static void assumeBoolOption(
      Configuration conf, String key, boolean defval) {
    assume(conf.getBoolean(key, defval),
        "Configuration key " + key + " is false");
  }

  /**
   * Get a required config option (trimmed, incidentally).
   * Test will fail if not set
   * @param conf configuration
   * @param key key
   * @return the string
   */
  public static String getRequiredConfOption(Configuration conf, String key) {
    String val = conf.getTrimmed(key);
    if (SliderUtils.isUnset(val)) {
      fail("Missing configuration option " + key);
    }
    return val;
  }

  /**
   * Fails a test because required behavior has not been implemented.
   */
  public static void failNotImplemented() {
    fail("Not implemented");
  }

  /**
   * Assert that any needed libraries being present. On Unix none are needed;
   * on windows they must be present
   */
  public static void assertNativeLibrariesPresent() {
    String errorText = SliderUtils.checkForRequiredNativeLibraries();
    if (SliderUtils.isSet(errorText)) {
      fail(errorText);
    }
  }

  protected static String[] toArray(List<Object> args) {
    String[] converted = new String[args.size()];
    for (int i = 0; i < args.size(); i++) {
      Object elt = args.get(i);
      assertNotNull(args.get(i));
      converted[i] = elt.toString();
    }
    return converted;
  }

  public static void waitWhileClusterLive(SliderClient client, int timeout)
      throws IOException, YarnException {
    Duration duration = new Duration(timeout);
    duration.start();
    while (client.actionExists(client.getDeployedClusterName(), true) ==
        LauncherExitCodes.EXIT_SUCCESS && !duration.getLimitExceeded()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
    }
    if (duration.getLimitExceeded()) {
      fail("Cluster " + client.getDeployedClusterName() + " still live after " +
          timeout + " ms");
    }
  }

  public static void waitUntilClusterLive(SliderClient client, int timeout)
      throws IOException, YarnException {
    Duration duration = new Duration(timeout);
    duration.start();
    while (LauncherExitCodes.EXIT_SUCCESS != client.actionExists(
        client.getDeployedClusterName(), true) &&
           !duration.getLimitExceeded()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
    }
    if (duration.getLimitExceeded()) {
      fail("Cluster " + client.getDeployedClusterName() + " not live after " +
          timeout + " ms");
    }
  }

  public static void dumpClusterDescription(
      String text,
      Application status) throws IOException {
    describe(text);
    LOG.info(JSON_SER_DESER.toJson(status));
  }

  /**
   * Assert that a service operation succeeded.
   * @param service service
   */
  public static void assertSucceeded(ServiceLauncher service) {
    assertEquals(0, service.getServiceExitCode());
  }

  public static void assertContainersLive(Application application,
      String component, int expected) {
    LOG.info("Asserting component {} expected count {}", component, expected);
    int actual = extractLiveContainerCount(application, component);
    if (expected != actual) {
      LOG.warn("{} actual={}, expected {} in \n{}\n", component, actual,
          expected, application);
    }
    assertEquals(expected, actual);
  }

  /**
   * Robust extraction of live container count.
   * @param application status
   * @param component component to resolve
   * @return the number of containers live.
   */
  public static int extractLiveContainerCount(
      Application application,
      String component) {
    int actual = 0;
    if (application.getContainers() != null) {
      for (Container container : application.getContainers()) {
        if (container.getComponentName().equals(component)) {
          actual++;
        }
      }
    }
    return actual;
  }

  /**
   * Exec a set of commands, wait a few seconds for it to finish.
   * @param status code
   * @param commands
   * @return the process
   */
  public static ForkedProcessService exec(int status, List<String> commands)
      throws IOException, TimeoutException {
    ForkedProcessService process = exec(commands);

    Integer exitCode = process.getExitCode();
    assertNotNull(exitCode);
    assertEquals(status, exitCode.intValue());
    return process;
  }

  /**
   * Exec a set of commands, wait a few seconds for it to finish.
   * @param commands
   * @return
   */
  public static ForkedProcessService exec(List<String> commands)
      throws IOException, TimeoutException {
    ForkedProcessService process;
    process = new ForkedProcessService(
        commands.get(0),
        EMPTY_MAP,
        commands);
    process.init(new Configuration());
    process.start();
    int timeoutMillis = 5000;
    if (!process.waitForServiceToStop(timeoutMillis)) {
      throw new TimeoutException(
          "Process did not stop in " + timeoutMillis + "mS");
    }
    return process;
  }

  /**
   * Determine whether an application exists. Run the commands and if the
   * operation fails with a FileNotFoundException, then
   * this method returns false.
   * <p>
   *   Run something harmless like a -version command, something
   *   which must return 0
   *
   * @param commands
   * @return true if the command sequence succeeded
   * false if they failed with no file
   * @throws Exception on any other failure cause
   */
  public static boolean doesAppExist(List<String> commands)
      throws IOException, TimeoutException {
    try {
      exec(0, commands);
      return true;
    } catch (ServiceStateException e) {
      if (!(e.getCause() instanceof FileNotFoundException)) {
        throw e;
      }
      return false;
    }
  }

  /**
   * Locate an executable on the path.
   * @param exe executable name. If it is an absolute path which
   * exists then it will returned direct
   * @return the path to an exe or null for no match
   */
  public static File locateExecutable(String exe) {
    File exeNameAsPath = new File(exe).getAbsoluteFile();
    if (exeNameAsPath.exists()) {
      return exeNameAsPath;
    }

    File exepath = null;
    String path = extractPath();
    String[] dirs = path.split(System.getProperty("path.separator"));
    for (String dirname : dirs) {
      File dir = new File(dirname);

      File possible = new File(dir, exe);
      if (possible.exists()) {
        exepath = possible;
      }
    }
    return exepath;
  }

  /**
   * Lookup the PATH env var.
   * @return the path or null
   */
  public static String extractPath() {
    return extractEnvVar("PATH");
  }

  /**
   * Find an environment variable. Uses case independent checking for
   * the benefit of windows.
   * Will fail if the var is not found.
   * @param var path variable <i>in upper case</i>
   * @return the env var
   */
  public static String extractEnvVar(String var) {
    String realkey = "";

    for (String it : System.getenv().keySet()) {
      if (it.toUpperCase(Locale.ENGLISH).equals(var)) {
        realkey = it;
      }
    }

    if (SliderUtils.isUnset(realkey)) {
      fail("No environment variable " + var + " found");
    }
    String val = System.getenv(realkey);

    LOG.info("{} = {}", realkey, val);
    return val;
  }

  /**
   * Create a temp JSON file. After coming up with the name, the file
   * is deleted
   * @return the filename
   */
  public static  File createTempJsonFile() throws IOException {
    return tmpFile(".json");
  }

  /**
   * Create a temp file with the specific name. It's deleted after creation,
   * to avoid  "file exists exceptions"
   * @param suffix suffix, e.g. ".txt"
   * @return a path to a file which may be created
   */
  public static File tmpFile(String suffix) throws IOException {
    File reportFile = File.createTempFile(
        "temp",
        suffix,
        new File("target"));
    reportFile.delete();
    return reportFile;
  }

  /**
   * Execute a closure, assert it fails with a given exit code and text.
   * @param exitCode exit code
   * @param text text (can be "")
   * @param action action
   * @return
   */
  public void  assertFailsWithException(int exitCode,
      String text,
      Action action) throws Exception {
    try {
      action.invoke();
      fail("Operation was expected to fail —but it succeeded");
    } catch (ServiceLaunchException e) {
      assertExceptionDetails(e, exitCode, text);
    }
  }

  /**
   * Execute a closure, assert it fails with a given exit code and text.
   * @param text text (can be "")
   * @param action action
   * @return
   */
  public void assertFailsWithExceptionClass(Class clazz,
      String text,
      Action action) throws Exception {
    try {
      action.invoke();
      fail("Operation was expected to fail —but it succeeded");
    } catch (Exception e) {
      assertExceptionDetails(e, clazz, text);
    }
  }

  public static void assertExceptionDetails(
      ServiceLaunchException ex,
      int exitCode) {
    assertExceptionDetails(ex, exitCode, null);
  }

  /**
   * Make an assertion about the exit code of an exception.
   * @param ex exception
   * @param exitCode exit code
   * @param text error text to look for in the exception
   */
  public static void assertExceptionDetails(
      ServiceLaunchException ex,
      int exitCode,
      String text) {
    if (exitCode != ex.getExitCode()) {
      String message = String.format("Wrong exit code, expected %d but" +
              " got %d in %s", exitCode, ex.getExitCode(), ex);
      LOG.warn(message, ex);
      throw new AssertionError(message, ex);
    }
    if (SliderUtils.isSet(text)) {
      if (!(ex.toString().contains(text))) {
        String message = String.format("String match for \"%s\"failed in %s",
            text, ex);
        LOG.warn(message, ex);
        throw new AssertionError(message, ex);
      }
    }
  }

  /**
   * Make an assertion about the class of an exception.
   * @param ex exception
   * @param clazz exit code
   * @param text error text to look for in the exception
   */
  static void assertExceptionDetails(
      Exception ex,
      Class clazz,
      String text) throws Exception {
    if (ex.getClass() != clazz) {
      throw ex;
    }
    if (SliderUtils.isSet(text) && !(ex.toString().contains(text))) {
      throw ex;
    }
  }

  /**
   * Launch the slider client with the specific args; no validation
   * of return code takes place.
   * @param conf configuration
   * @param args arg list
   * @return the launcher
   */
  protected static ServiceLauncher<SliderClient> execSliderCommand(
      Configuration conf,
      List args) throws Throwable {
    ServiceLauncher<SliderClient> serviceLauncher =
        new ServiceLauncher<>(sliderClientClassName);

    LOG.debug("slider {}", SliderUtils.join(args, " ", false));
    serviceLauncher.launchService(conf,
        toArray(args),
        false);
    return serviceLauncher;
  }

  /**
   * Launch a slider command to a given exit code.
   * Most failures will trigger exceptions; this is for the exit code of the
   * runService() call.
   * @param exitCode desired exit code
   * @param conf configuration
   * @param args arg list
   * @return the launcher
   */
  protected static ServiceLauncher<SliderClient> execSliderCommand(
      int exitCode,
      Configuration conf,
      List args) throws Throwable {
    ServiceLauncher<SliderClient> serviceLauncher = execSliderCommand(conf,
        args);
    assertEquals(exitCode, serviceLauncher.getServiceExitCode());
    return serviceLauncher;
  }

  public static ServiceLauncher launch(Class serviceClass,
      Configuration conf,
      List<Object> args) throws
      Throwable {
    ServiceLauncher serviceLauncher =
        new ServiceLauncher(serviceClass.getName());

    String joinedArgs = SliderUtils.join(args, " ", false);
    LOG.debug("slider {}", joinedArgs);

    serviceLauncher.launchService(conf,
        toArray(args),
        false);
    return serviceLauncher;
  }

  public static Throwable launchExpectingException(Class serviceClass,
      Configuration conf,
      String expectedText,
      List args)
      throws Throwable {
    try {
      ServiceLauncher launch = launch(serviceClass, conf, args);
      throw new AssertionError("Expected an exception with text containing " +
          expectedText + " -but the service completed with exit code " +
          launch.getServiceExitCode());
    } catch (AssertionError error) {
      throw error;
    } catch (Throwable thrown) {
      if (SliderUtils.isSet(expectedText) && !thrown.toString().contains(
          expectedText)) {
        //not the right exception -rethrow
        LOG.warn("Caught Exception did not contain expected text" +
                 "\"" + expectedText + "\"");
        throw thrown;
      }
      return thrown;
    }
  }


  public static ServiceLauncher<SliderClient> launchClientAgainstRM(
      String address,
      List<String> args,
      Configuration conf) throws Throwable {
    assertNotNull(address);
    LOG.info("Connecting to rm at {}", address);
    if (!args.contains(Arguments.ARG_MANAGER)) {
      args.add(Arguments.ARG_MANAGER);
      args.add(address);
    }
    ServiceLauncher<SliderClient> launcher = execSliderCommand(conf, args);
    return launcher;
  }

  /**
   * Add a configuration parameter as a cluster configuration option.
   * @param extraArgs extra arguments
   * @param conf config
   * @param option option
   */
  public static void addClusterConfigOption(
      List<String> extraArgs,
      YarnConfiguration conf,
      String option) {

    conf.getTrimmed(option);
    extraArgs.add(ARG_OPTION);
    extraArgs.add(option);
    extraArgs.add(getRequiredConfOption(conf, option));
  }

  /**
   * Assert that a path refers to a directory.
   * @param fs filesystem
   * @param path path of the directory
   * @throws IOException on File IO problems
   */
  public static void assertIsDirectory(FileSystem fs,
      Path path) throws IOException {
    FileStatus fileStatus = fs.getFileStatus(path);
    assertIsDirectory(fileStatus);
  }

  /**
   * Assert that a path refers to a directory.
   * @param fileStatus stats to check
   */
  public static void assertIsDirectory(FileStatus fileStatus) {
    assertTrue("Should be a dir -but isn't: " + fileStatus,
        fileStatus.isDirectory());
  }

  /**
   * Assert that a path exists -but make no assertions as to the
   * type of that entry.
   *
   * @param fileSystem filesystem to examine
   * @param message message to include in the assertion failure message
   * @param path path in the filesystem
   * @throws IOException IO problems
   */
  public static void assertPathExists(
      FileSystem fileSystem,
      String message,
      Path path) throws IOException {
    if (!fileSystem.exists(path)) {
      //failure, report it
      fail(
          message + ": not found \"" + path + "\" in " + path.getParent() +
          "-" +
          ls(fileSystem, path.getParent()));
    }
  }

  /**
   * Assert that a path does not exist.
   *
   * @param fileSystem filesystem to examine
   * @param message message to include in the assertion failure message
   * @param path path in the filesystem
   * @throws IOException IO problems
   */
  public static void assertPathDoesNotExist(
      FileSystem fileSystem,
      String message,
      Path path) throws IOException {
    try {
      FileStatus status = fileSystem.getFileStatus(path);
      // a status back implies there is a file here
      fail(message + ": unexpectedly found " + path + " as  " + status);
    } catch (FileNotFoundException expected) {
      //this is expected

    }
  }

  /**
   * Assert that a FileSystem.listStatus on a dir finds the subdir/child entry.
   * @param fs filesystem
   * @param dir directory to scan
   * @param subdir full path to look for
   * @throws IOException IO probles
   */
  public static void assertListStatusFinds(FileSystem fs,
      Path dir,
      Path subdir) throws IOException {
    FileStatus[] stats = fs.listStatus(dir);
    boolean found = false;
    StringBuilder builder = new StringBuilder();
    for (FileStatus stat : stats) {
      builder.append(stat.toString()).append('\n');
      if (stat.getPath().equals(subdir)) {
        found = true;
      }
    }
    assertTrue("Path " + subdir
        + " not found in directory " + dir + ":" + builder,
        found);
  }

  /**
   * List a a path to string.
   * @param fileSystem filesystem
   * @param path directory
   * @return a listing of the filestatuses of elements in the directory, one
   * to a line, precedeed by the full path of the directory
   * @throws IOException connectivity problems
   */
  public static String ls(FileSystem fileSystem, Path path)
      throws IOException {
    if (path == null) {
      //surfaces when someone calls getParent() on something at the top of
      // the path
      return "/";
    }
    FileStatus[] stats;
    String pathtext = "ls " + path;
    try {
      stats = fileSystem.listStatus(path);
    } catch (FileNotFoundException e) {
      return pathtext + " -file not found";
    } catch (IOException e) {
      return pathtext + " -failed: " + e;
    }
    return pathtext + fileStatsToString(stats, "\n");
  }

  /**
   * Take an array of filestats and convert to a string (prefixed w/ a [01]
   * counter).
   * @param stats array of stats
   * @param separator separator after every entry
   * @return a stringified set
   */
  public static String fileStatsToString(FileStatus[] stats, String separator) {
    StringBuilder buf = new StringBuilder(stats.length * 128);
    for (int i = 0; i < stats.length; i++) {
      buf.append(String.format("[%02d] %s", i, stats[i])).append(separator);
    }
    return buf.toString();
  }

  public static void waitWhileClusterLive(SliderClient sliderClient)
      throws IOException, YarnException {
    waitWhileClusterLive(sliderClient, 30000);
  }

  public static void dumpRegistryInstances(
      Map<String, ServiceRecord> instances) {
    describe("service registry slider instances");
    for (Entry<String, ServiceRecord> it : instances.entrySet()) {
      LOG.info(" {} : {}", it.getKey(), it.getValue());
    }
    describe("end list service registry slider instances");
  }


  public static void dumpRegistryInstanceIDs(List<String> instanceIds) {
    describe("service registry instance IDs");
    dumpCollection(instanceIds);
  }

  public static void dumpRegistryServiceTypes(Collection<String> entries) {
    describe("service registry types");
    dumpCollection(entries);
  }

  public static <V> void dumpCollection(Collection<V> entries) {
    LOG.info("number of entries: {}", entries.size());
    for (V it : entries) {
      LOG.info(it.toString());
    }
  }

  public static void dumpArray(Object[] entries) {
    LOG.info("number of entries: {}", entries.length);
    for (Object it : entries) {
      LOG.info(it.toString());
    }
  }

  public static <K, V> void dumpMap(Map<K, V> map) {
    for (Entry<K, V> it : map.entrySet()) {
      LOG.info("\"{}\": \"{}\"", it.getKey().toString(), it.getValue()
          .toString());
    }
  }

  /**
   * Get a time option in seconds if set, otherwise the default value (also
   * in seconds).
   * This operation picks up the time value as a system property if set -that
   * value overrides anything in the test file
   * @param conf
   * @param key
   * @param defValMillis
   * @return
   */
  public static int getTimeOptionMillis(
      Configuration conf,
      String key,
      int defValMillis) {
    int val = conf.getInt(key, 0);
    val = Integer.getInteger(key, val);
    int time = 1000 * val;
    if (time == 0) {
      time = defValMillis;
    }
    return time;
  }

  public void dumpConfigurationSet(PublishedConfigSet confSet) {
    for (String key : confSet.keys()) {
      PublishedConfiguration config = confSet.get(key);
      LOG.info("{} -- {}", key, config.description);
    }
  }

  /**
   * Convert a file to a URI suitable for use in an argument.
   * @param file file
   * @return a URI string valid on all platforms
   */
  public String toURIArg(File file) {
    return file.getAbsoluteFile().toURI().toString();
  }

  /**
   * Assert a file exists; fails with a listing of the parent dir.
   * @param text text for front of message
   * @param file file to look for
   * @throws FileNotFoundException
   */
  public void assertFileExists(String text, File file)
      throws FileNotFoundException {
    if (!file.exists()) {
      File parent = file.getParentFile();
      String[] files = parent.list();
      StringBuilder builder = new StringBuilder();
      builder.append(parent.getAbsolutePath());
      builder.append(":\n");
      for (String name : files) {
        builder.append("  ");
        builder.append(name);
        builder.append("\n");
      }
      throw new FileNotFoundException(text + ": " + file + " not found in " +
          builder);
    }
  }

  /**
   * Repeat a probe until it succeeds, if it does not execute a failure
   * closure then raise an exception with the supplied message.
   * @param probe probe
   * @param timeout time in millis before giving up
   * @param sleepDur sleep between failing attempts
   * @param args map of arguments to the probe
   * @param failIfUnsuccessful if the probe fails after all the attempts
   * —should it raise an exception
   * @param failureMessage message to include in exception raised
   * @param failureHandler closure to invoke prior to the failure being raised
   */
  protected void repeatUntilSuccess(
      String action,
      Probe probe,
      int timeout,
      int sleepDur,
      Map args,
      boolean failIfUnsuccessful,
      String failureMessage,
      Action failureHandler) throws Exception {
    LOG.debug("Probe {} timelimit {}", action, timeout);
    if (timeout < 1000) {
      fail("Timeout " + timeout + " too low: milliseconds are expected, not " +
          "seconds");
    }
    int attemptCount = 1;
    boolean succeeded = false;
    boolean completed = false;
    Duration duration = new Duration(timeout);
    duration.start();
    while (!completed) {
      Outcome outcome = probe.invoke(args);
      if (outcome.equals(Outcome.SUCCESS)) {
        // success
        LOG.debug("Success after {} attempt(s)", attemptCount);
        succeeded = true;
        completed = true;
      } else if (outcome.equals(Outcome.RETRY)) {
        // failed but retry possible
        attemptCount++;
        completed = duration.getLimitExceeded();
        if (!completed) {
          LOG.debug("Attempt {} failed", attemptCount);
          try {
            Thread.sleep(sleepDur);
          } catch (InterruptedException e) {
          }
        }
      } else if (outcome.equals(Outcome.FAIL)) {
        // fast fail
        LOG.debug("Fast fail of probe");
        completed = true;
      }
    }
    if (!succeeded) {
      if (duration.getLimitExceeded()) {
        LOG.info("probe timed out after {} and {} attempts", timeout,
            attemptCount);
      }
      if (failureHandler != null) {
        failureHandler.invoke();
      }
      if (failIfUnsuccessful) {
        fail(failureMessage);
      }
    }
  }

  /**
   * Get a value from a map; raise an assertion if it is not there.
   * @param map map to look up
   * @param key key
   * @return the string value
   */
  public <K, V> String requiredMapValue(Map<K, V> map, String key) {
    assertNotNull(map.get(key));
    return map.get(key).toString();
  }

  public static void assertStringContains(String expected, String text) {
    assertNotNull("null text", text);
    if (!text.contains(expected)) {
      String message = String.format("did not find %s in \"%s\"", expected,
          text);
      LOG.error(message);
      fail(message);
    }
  }
}
