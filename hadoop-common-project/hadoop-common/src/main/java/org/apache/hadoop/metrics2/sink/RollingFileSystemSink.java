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

package org.apache.hadoop.metrics2.sink;

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * <p>This class is a metrics sink that uses
 * {@link org.apache.hadoop.fs.FileSystem} to write the metrics logs.  Every
 * hour a new directory will be created under the path specified by the
 * <code>basepath</code> property. All metrics will be logged to a file in the
 * current hour's directory in a file named &lt;hostname&gt;.log, where
 * &lt;hostname&gt; is the name of the host on which the metrics logging
 * process is running. The base path is set by the
 * <code>&lt;prefix&gt;.sink.&lt;instance&gt;.basepath</code> property.  The
 * time zone used to create the current hour's directory name is GMT.  If the
 * <code>basepath</code> property isn't specified, it will default to
 * &quot;/tmp&quot;, which is the temp directory on whatever default file
 * system is configured for the cluster.</p>
 *
 * <p>The <code>&lt;prefix&gt;.sink.&lt;instance&gt;.ignore-error</code>
 * property controls whether an exception is thrown when an error is encountered
 * writing a log file.  The default value is <code>true</code>.  When set to
 * <code>false</code>, file errors are quietly swallowed.</p>
 *
 * <p>The primary use of this class is for logging to HDFS.  As it uses
 * {@link org.apache.hadoop.fs.FileSystem} to access the target file system,
 * however, it can be used to write to the local file system, Amazon S3, or any
 * other supported file system.  The base path for the sink will determine the
 * file system used.  An unqualified path will write to the default file system
 * set by the configuration.</p>
 *
 * <p>Not all file systems support the ability to append to files.  In file
 * systems without the ability to append to files, only one writer can write to
 * a file at a time.  To allow for concurrent writes from multiple daemons on a
 * single host, the <code>source</code> property should be set to the name of
 * the source daemon, e.g. <i>namenode</i>.  The value of the
 * <code>source</code> property should typically be the same as the property's
 * prefix.  If this property is not set, the source is taken to be
 * <i>unknown</i>.</p>
 *
 * <p>Instead of appending to an existing file, by default the sink
 * will create a new file with a suffix of &quot;.&lt;n&gt;&quet;, where
 * <i>n</i> is the next lowest integer that isn't already used in a file name,
 * similar to the Hadoop daemon logs.  NOTE: the file with the <b>highest</b>
 * sequence number is the <b>newest</b> file, unlike the Hadoop daemon logs.</p>
 *
 * <p>For file systems that allow append, the sink supports appending to the
 * existing file instead. If the <code>allow-append</code> property is set to
 * true, the sink will instead append to the existing file on file systems that
 * support appends. By default, the <code>allow-append</code> property is
 * false.</p>
 *
 * <p>Note that when writing to HDFS with <code>allow-append</code> set to true,
 * there is a minimum acceptable number of data nodes.  If the number of data
 * nodes drops below that minimum, the append will succeed, but reading the
 * data will fail with an IOException in the DataStreamer class.  The minimum
 * number of data nodes required for a successful append is generally 2 or
 * 3.</p>
 *
 * <p>Note also that when writing to HDFS, the file size information is not
 * updated until the file is closed (e.g. at the top of the hour) even though
 * the data is being written successfully. This is a known HDFS limitation that
 * exists because of the performance cost of updating the metadata.  See
 * <a href="https://issues.apache.org/jira/browse/HDFS-5478">HDFS-5478</a>.</p>
 *
 * <p>When using this sink in a secure (Kerberos) environment, two additional
 * properties must be set: <code>keytab-key</code> and
 * <code>principal-key</code>. <code>keytab-key</code> should contain the key by
 * which the keytab file can be found in the configuration, for example,
 * <code>yarn.nodemanager.keytab</code>. <code>principal-key</code> should
 * contain the key by which the principal can be found in the configuration,
 * for example, <code>yarn.nodemanager.principal</code>.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RollingFileSystemSink implements MetricsSink, Closeable {
  private static final String BASEPATH_KEY = "basepath";
  private static final String SOURCE_KEY = "source";
  private static final String IGNORE_ERROR_KEY = "ignore-error";
  private static final String ALLOW_APPEND_KEY = "allow-append";
  private static final String KEYTAB_PROPERTY_KEY = "keytab-key";
  private static final String USERNAME_PROPERTY_KEY = "principal-key";
  private static final String SOURCE_DEFAULT = "unknown";
  private static final String BASEPATH_DEFAULT = "/tmp";
  private static final FastDateFormat DATE_FORMAT =
      FastDateFormat.getInstance("yyyyMMddHH", TimeZone.getTimeZone("GMT"));
  private final Object lock = new Object();
  private boolean initialized = false;
  private SubsetConfiguration properties;
  private Configuration conf;
  private String source;
  private boolean ignoreError;
  private boolean allowAppend;
  private Path basePath;
  private FileSystem fileSystem;
  // The current directory path into which we're writing files
  private Path currentDirPath;
  // The path to the current file into which we're writing data
  private Path currentFilePath;
  // The stream to which we're currently writing.
  private PrintStream currentOutStream;
  // We keep this only to be able to call hsynch() on it.
  private FSDataOutputStream currentFSOutStream;
  private Timer flushTimer;

  // This flag is used during testing to make the flusher thread run after only
  // a short pause instead of waiting for the top of the hour.
  @VisibleForTesting
  protected static boolean flushQuickly = false;
  // This flag is used by the flusher thread to indicate that it has run. Used
  // only for testing purposes.
  @VisibleForTesting
  protected static volatile boolean hasFlushed = false;
  // Use this configuration instead of loading a new one.
  @VisibleForTesting
  protected static Configuration suppliedConf = null;
  // Use this file system instead of getting a new one.
  @VisibleForTesting
  protected static FileSystem suppliedFilesystem = null;

  @Override
  public void init(SubsetConfiguration metrics2Properties) {
    properties = metrics2Properties;
    basePath = new Path(properties.getString(BASEPATH_KEY, BASEPATH_DEFAULT));
    source = properties.getString(SOURCE_KEY, SOURCE_DEFAULT);
    ignoreError = properties.getBoolean(IGNORE_ERROR_KEY, false);
    allowAppend = properties.getBoolean(ALLOW_APPEND_KEY, false);

    conf = loadConf();
    UserGroupInformation.setConfiguration(conf);

    // Don't do secure setup if it's not needed.
    if (UserGroupInformation.isSecurityEnabled()) {
      // Validate config so that we don't get an NPE
      checkForProperty(properties, KEYTAB_PROPERTY_KEY);
      checkForProperty(properties, USERNAME_PROPERTY_KEY);


      try {
        // Login as whoever we're supposed to be and let the hostname be pulled
        // from localhost. If security isn't enabled, this does nothing.
        SecurityUtil.login(conf, properties.getString(KEYTAB_PROPERTY_KEY),
            properties.getString(USERNAME_PROPERTY_KEY));
      } catch (IOException ex) {
        throw new MetricsException("Error logging in securely: ["
            + ex.toString() + "]", ex);
      }
    }
  }

  /**
   * Initialize the connection to HDFS and create the base directory. Also
   * launch the flush thread.
   */
  private boolean initFs() {
    boolean success = false;

    fileSystem = getFileSystem();

    // This step isn't strictly necessary, but it makes debugging issues much
    // easier. We try to create the base directory eagerly and fail with
    // copious debug info if it fails.
    try {
      fileSystem.mkdirs(basePath);
      success = true;
    } catch (Exception ex) {
      if (!ignoreError) {
        throw new MetricsException("Failed to create " + basePath + "["
            + SOURCE_KEY + "=" + source + ", "
            + ALLOW_APPEND_KEY + "=" + allowAppend + ", "
            + stringifySecurityProperty(KEYTAB_PROPERTY_KEY) + ", "
            + stringifySecurityProperty(USERNAME_PROPERTY_KEY)
            + "] -- " + ex.toString(), ex);
      }
    }

    if (success) {
      // If we're permitted to append, check if we actually can
      if (allowAppend) {
        allowAppend = checkAppend(fileSystem);
      }

      flushTimer = new Timer("RollingFileSystemSink Flusher", true);
    }

    return success;
  }

  /**
   * Turn a security property into a nicely formatted set of <i>name=value</i>
   * strings, allowing for either the property or the configuration not to be
   * set.
   *
   * @param properties the sink properties
   * @param conf the conf
   * @param property the property to stringify
   * @return the stringified property
   */
  private String stringifySecurityProperty(String property) {
    String securityProperty;

    if (properties.containsKey(property)) {
      String propertyValue = properties.getString(property);
      String confValue = conf.get(properties.getString(property));

      if (confValue != null) {
        securityProperty = property + "=" + propertyValue
            + ", " + properties.getString(property) + "=" + confValue;
      } else {
        securityProperty = property + "=" + propertyValue
            + ", " + properties.getString(property) + "=<NOT SET>";
      }
    } else {
      securityProperty = property + "=<NOT SET>";
    }

    return securityProperty;
  }

  /**
   * Throw a {@link MetricsException} if the given property is not set.
   *
   * @param conf the configuration to test
   * @param key the key to validate
   */
  private static void checkForProperty(SubsetConfiguration conf, String key) {
    if (!conf.containsKey(key)) {
      throw new MetricsException("Configuration is missing " + key
          + " property");
    }
  }

  /**
   * Return the supplied configuration for testing or otherwise load a new
   * configuration.
   *
   * @return the configuration to use
   */
  private Configuration loadConf() {
    Configuration c;

    if (suppliedConf != null) {
      c = suppliedConf;
    } else {
      // The config we're handed in init() isn't the one we want here, so we
      // create a new one to pick up the full settings.
      c = new Configuration();
    }

    return c;
  }

  /**
   * Return the supplied file system for testing or otherwise get a new file
   * system.
   *
   * @param conf the configuration
   * @return the file system to use
   * @throws MetricsException thrown if the file system could not be retrieved
   */
  private FileSystem getFileSystem() throws MetricsException {
    FileSystem fs = null;

    if (suppliedFilesystem != null) {
      fs = suppliedFilesystem;
    } else {
      try {
        fs = FileSystem.get(new URI(basePath.toString()), conf);
      } catch (URISyntaxException ex) {
        throw new MetricsException("The supplied filesystem base path URI"
            + " is not a valid URI: " + basePath.toString(), ex);
      } catch (IOException ex) {
        throw new MetricsException("Error connecting to file system: "
            + basePath + " [" + ex.toString() + "]", ex);
      }
    }

    return fs;
  }

  /**
   * Test whether the file system supports append and return the answer.
   * @param fs the target file system
   */
  private boolean checkAppend(FileSystem fs) {
    boolean canAppend = true;

    try {
      fs.append(basePath);
    } catch (IOException ex) {
      if (ex.getMessage().equals("Not supported")) {
        canAppend = false;
      }
    }

    return canAppend;
  }

  /**
   * Check the current directory against the time stamp.  If they're not
   * the same, create a new directory and a new log file in that directory.
   *
   * @throws MetricsException thrown if an error occurs while creating the
   * new directory or new log file
   */
  private void rollLogDirIfNeeded() throws MetricsException {
    Date now = new Date();
    String currentDir = DATE_FORMAT.format(now);
    Path path = new Path(basePath, currentDir);

    // We check whether currentOutStream is null instead of currentDirPath,
    // because if currentDirPath is null, then currentOutStream is null, but
    // currentOutStream can be null for other reasons.
    if ((currentOutStream == null) || !path.equals(currentDirPath)) {
      // If we're not yet connected to HDFS, create the connection
      if (!initialized) {
        initialized = initFs();
      }

      if (initialized) {
        // Close the stream. This step could have been handled already by the
        // flusher thread, but if it has, the PrintStream will just swallow the
        // exception, which is fine.
        if (currentOutStream != null) {
          currentOutStream.close();
        }

        currentDirPath = path;

        try {
          rollLogDir();
        } catch (IOException ex) {
          throwMetricsException("Failed to create new log file", ex);
        }

        scheduleFlush(now);
      }
    }
  }

  /**
   * Schedule the current hour's directory to be flushed at the top of the next
   * hour. If this ends up running after the top of the next hour, it will
   * execute immediately.
   *
   * @param now the current time
   */
  private void scheduleFlush(Date now) {
    // Store the current currentDirPath to close later
    final PrintStream toClose = currentOutStream;
    Calendar next = Calendar.getInstance();

    next.setTime(now);

    if (flushQuickly) {
      // If we're running unit tests, flush after a short pause
      next.add(Calendar.MILLISECOND, 400);
    } else {
      // Otherwise flush at the top of the hour
      next.set(Calendar.SECOND, 0);
      next.set(Calendar.MINUTE, 0);
      next.add(Calendar.HOUR, 1);
    }

    flushTimer.schedule(new TimerTask() {
      @Override
      public void run() {
        synchronized (lock) {
          // This close may have already been done by a putMetrics() call. If it
          // has, the PrintStream will swallow the exception, which is fine.
          toClose.close();
        }

        hasFlushed = true;
      }
    }, next.getTime());
  }

  /**
   * Create a new directory based on the current hour and a new log file in
   * that directory.
   *
   * @throws IOException thrown if an error occurs while creating the
   * new directory or new log file
   */
  private void rollLogDir() throws IOException {
    String fileName =
        source + "-" + InetAddress.getLocalHost().getHostName() + ".log";

    Path targetFile = new Path(currentDirPath, fileName);
    fileSystem.mkdirs(currentDirPath);

    if (allowAppend) {
      createOrAppendLogFile(targetFile);
    } else {
      createLogFile(targetFile);
    }
  }

  /**
   * Create a new log file and return the {@link FSDataOutputStream}. If a
   * file with the specified path already exists, add a suffix, starting with 1
   * and try again. Keep incrementing the suffix until a nonexistent target
   * path is found.
   *
   * Once the file is open, update {@link #currentFSOutStream},
   * {@link #currentOutStream}, and {@#link #currentFile} are set appropriately.
   *
   * @param initial the target path
   * @throws IOException thrown if the call to see if the exists fails
   */
  private void createLogFile(Path initial) throws IOException {
    Path currentAttempt = initial;
    // Start at 0 so that if the base filname exists, we start with the suffix
    // ".1".
    int id = 0;

    while (true) {
      // First try blindly creating the file. If we fail, it either means
      // the file exists, or the operation actually failed.  We do it this way
      // because if we check whether the file exists, it might still be created
      // by the time we try to create it. Creating first works like a
      // test-and-set.
      try {
        currentFSOutStream = fileSystem.create(currentAttempt, false);
        currentOutStream = new PrintStream(currentFSOutStream, true,
            StandardCharsets.UTF_8.name());
        currentFilePath = currentAttempt;
        break;
      } catch (IOException ex) {
        // Now we can check to see if the file exists to know why we failed
        if (fileSystem.exists(currentAttempt)) {
          id = getNextIdToTry(initial, id);
          currentAttempt = new Path(initial.toString() + "." + id);
        } else {
          throw ex;
        }
      }
    }
  }

  /**
   * Return the next ID suffix to use when creating the log file. This method
   * will look at the files in the directory, find the one with the highest
   * ID suffix, and 1 to that suffix, and return it. This approach saves a full
   * linear probe, which matters in the case where there are a large number of
   * log files.
   *
   * @param initial the base file path
   * @param lastId the last ID value that was used
   * @return the next ID to try
   * @throws IOException thrown if there's an issue querying the files in the
   * directory
   */
  private int getNextIdToTry(Path initial, int lastId)
      throws IOException {
    RemoteIterator<LocatedFileStatus> files =
        fileSystem.listFiles(currentDirPath, true);
    String base = initial.toString();
    int id = lastId;

    while (files.hasNext()) {
      String file = files.next().getPath().getName();

      if (file.startsWith(base)) {
        int fileId = extractId(file);

        if (fileId > id) {
          id = fileId;
        }
      }
    }

    // Return either 1 more than the highest we found or 1 more than the last
    // ID used (if no ID was found).
    return id + 1;
  }

  /**
   * Extract the ID from the suffix of the given file name.
   *
   * @param file the file name
   * @return the ID or -1 if no ID could be extracted
   */
  private int extractId(String file) {
    int index = file.lastIndexOf(".");
    int id = -1;

    // A hostname has to have at least 1 character
    if (index > 0) {
      try {
        id = Integer.parseInt(file.substring(index + 1));
      } catch (NumberFormatException ex) {
        // This can happen if there's no suffix, but there is a dot in the
        // hostname.  Just ignore it.
      }
    }

    return id;
  }

  /**
   * Create a new log file and return the {@link FSDataOutputStream}. If a
   * file with the specified path already exists, open the file for append
   * instead.
   *
   * Once the file is open, update {@link #currentFSOutStream},
   * {@link #currentOutStream}, and {@#link #currentFile} are set appropriately.
   *
   * @param initial the target path
   * @throws IOException thrown if the call to see the append operation fails.
   */
  private void createOrAppendLogFile(Path targetFile) throws IOException {
    // First try blindly creating the file. If we fail, it either means
    // the file exists, or the operation actually failed.  We do it this way
    // because if we check whether the file exists, it might still be created
    // by the time we try to create it. Creating first works like a
    // test-and-set.
    try {
      currentFSOutStream = fileSystem.create(targetFile, false);
      currentOutStream = new PrintStream(currentFSOutStream, true,
          StandardCharsets.UTF_8.name());
    } catch (IOException ex) {
      // Try appending instead.  If we fail, if means the file doesn't
      // actually exist yet or the operation actually failed.
      try {
        currentFSOutStream = fileSystem.append(targetFile);
        currentOutStream = new PrintStream(currentFSOutStream, true,
            StandardCharsets.UTF_8.name());
      } catch (IOException ex2) {
        // If the original create failed for a legit but transitory
        // reason, the append will fail because the file now doesn't exist,
        // resulting in a confusing stack trace.  To avoid that, we set
        // the cause of the second exception to be the first exception.
        // It's still a tiny bit confusing, but it's enough
        // information that someone should be able to figure it out.
        ex2.initCause(ex);

        throw ex2;
      }
    }

    currentFilePath = targetFile;
  }

  @Override
  public void putMetrics(MetricsRecord record) {
    synchronized (lock) {
      rollLogDirIfNeeded();

      if (currentOutStream != null) {
        currentOutStream.printf("%d %s.%s", record.timestamp(),
            record.context(), record.name());

        String separator = ": ";

        for (MetricsTag tag : record.tags()) {
          currentOutStream.printf("%s%s=%s", separator, tag.name(),
              tag.value());
          separator = ", ";
        }

        for (AbstractMetric metric : record.metrics()) {
          currentOutStream.printf("%s%s=%s", separator, metric.name(),
              metric.value());
        }

        currentOutStream.println();

        // If we don't hflush(), the data may not be written until the file is
        // closed. The file won't be closed until the top of the hour *AND*
        // another record is received. Calling hflush() makes sure that the data
        // is complete at the top of the hour.
        try {
          currentFSOutStream.hflush();
        } catch (IOException ex) {
          throwMetricsException("Failed flushing the stream", ex);
        }

        checkForErrors("Unable to write to log file");
      } else if (!ignoreError) {
        throwMetricsException("Unable to write to log file");
      }
    }
  }

  @Override
  public void flush() {
    synchronized (lock) {
      // currentOutStream is null if currentFSOutStream is null
      if (currentFSOutStream != null) {
        try {
          currentFSOutStream.hflush();
        } catch (IOException ex) {
          throwMetricsException("Unable to flush log file", ex);
        }
      }
    }
  }

  @Override
  public void close() {
    synchronized (lock) {
      if (currentOutStream != null) {
        currentOutStream.close();

        try {
          checkForErrors("Unable to close log file");
        } finally {
          // Null out the streams just in case someone tries to reuse us.
          currentOutStream = null;
          currentFSOutStream = null;
        }
      }
    }
  }

  /**
   * If the sink isn't set to ignore errors, throw a {@link MetricsException}
   * if the stream encountered an exception.  The message parameter will be used
   * as the new exception's message with the current file name
   * ({@link #currentFilePath}) appended to it.
   *
   * @param message the exception message. The message will have the current
   * file name ({@link #currentFilePath}) appended to it.
   * @throws MetricsException thrown if there was an error and the sink isn't
   * ignoring errors
   */
  private void checkForErrors(String message)
      throws MetricsException {
    if (!ignoreError && currentOutStream.checkError()) {
      throw new MetricsException(message + ": " + currentFilePath);
    }
  }

  /**
   * If the sink isn't set to ignore errors, wrap the Throwable in a
   * {@link MetricsException} and throw it.  The message parameter will be used
   * as the new exception's message with the current file name
   * ({@link #currentFilePath}) and the Throwable's string representation
   * appended to it.
   *
   * @param message the exception message. The message will have the current
   * file name ({@link #currentFilePath}) and the Throwable's string
   * representation appended to it.
   * @param t the Throwable to wrap
   */
  private void throwMetricsException(String message, Throwable t) {
    if (!ignoreError) {
      throw new MetricsException(message + ": " + currentFilePath + " ["
          + t.toString() + "]", t);
    }
  }

  /**
   * If the sink isn't set to ignore errors, throw a new
   * {@link MetricsException}.  The message parameter will be used  as the
   * new exception's message with the current file name
   * ({@link #currentFilePath}) appended to it.
   *
   * @param message the exception message. The message will have the current
   * file name ({@link #currentFilePath}) appended to it.
   */
  private void throwMetricsException(String message) {
    if (!ignoreError) {
      throw new MetricsException(message + ": " + currentFilePath);
    }
  }
}
