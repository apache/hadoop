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

package org.apache.hadoop.yarn.logaggregation;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.Writer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.file.tfile.TFile;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class AggregatedLogFormat {

  static final Log LOG = LogFactory.getLog(AggregatedLogFormat.class);
  private static final LogKey APPLICATION_ACL_KEY = new LogKey("APPLICATION_ACL");
  private static final LogKey APPLICATION_OWNER_KEY = new LogKey("APPLICATION_OWNER");
  private static final LogKey VERSION_KEY = new LogKey("VERSION");
  private static final Map<String, LogKey> RESERVED_KEYS;
  //Maybe write out the retention policy.
  //Maybe write out a list of containerLogs skipped by the retention policy.
  private static final int VERSION = 1;

  /**
   * Umask for the log file.
   */
  private static final FsPermission APP_LOG_FILE_UMASK = FsPermission
      .createImmutable((short) (0640 ^ 0777));


  static {
    RESERVED_KEYS = new HashMap<String, AggregatedLogFormat.LogKey>();
    RESERVED_KEYS.put(APPLICATION_ACL_KEY.toString(), APPLICATION_ACL_KEY);
    RESERVED_KEYS.put(APPLICATION_OWNER_KEY.toString(), APPLICATION_OWNER_KEY);
    RESERVED_KEYS.put(VERSION_KEY.toString(), VERSION_KEY);
  }
  
  public static class LogKey implements Writable {

    private String keyString;

    public LogKey() {

    }

    public LogKey(ContainerId containerId) {
      this.keyString = containerId.toString();
    }

    public LogKey(String keyString) {
      this.keyString = keyString;
    }
    
    @Override
    public int hashCode() {
      return keyString == null ? 0 : keyString.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof LogKey) {
        LogKey other = (LogKey) obj;
        if (this.keyString == null) {
          return other.keyString == null;
        }
        return this.keyString.equals(other.keyString);
      }
      return false;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(this.keyString);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.keyString = in.readUTF();
    }

    @Override
    public String toString() {
      return this.keyString;
    }
  }

  public static class LogValue {

    private final List<String> rootLogDirs;
    private final ContainerId containerId;
    // TODO Maybe add a version string here. Instead of changing the version of
    // the entire k-v format

    public LogValue(List<String> rootLogDirs, ContainerId containerId) {
      this.rootLogDirs = new ArrayList<String>(rootLogDirs);
      this.containerId = containerId;

      // Ensure logs are processed in lexical order
      Collections.sort(this.rootLogDirs);
    }

    public void write(DataOutputStream out) throws IOException {
      for (String rootLogDir : this.rootLogDirs) {
        File appLogDir =
            new File(rootLogDir, 
                ConverterUtils.toString(
                    this.containerId.getApplicationAttemptId().
                        getApplicationId())
                );
        File containerLogDir =
            new File(appLogDir, ConverterUtils.toString(this.containerId));

        if (!containerLogDir.isDirectory()) {
          continue; // ContainerDir may have been deleted by the user.
        }

        // Write out log files in lexical order
        File[] logFiles = containerLogDir.listFiles();
        Arrays.sort(logFiles);
        for (File logFile : logFiles) {

          // Write the logFile Type
          out.writeUTF(logFile.getName());

          // Write the log length as UTF so that it is printable
          out.writeUTF(String.valueOf(logFile.length()));

          // Write the log itself
          FileInputStream in = null;
          try {
            in = new FileInputStream(logFile);
            byte[] buf = new byte[65535];
            int len = 0;
            while ((len = in.read(buf)) != -1) {
              out.write(buf, 0, len);
            }
          } finally {
            in.close();
          }
        }
      }
    }
  }

  public static class LogWriter {

    private final FSDataOutputStream fsDataOStream;
    private final TFile.Writer writer;

    public LogWriter(final Configuration conf, final Path remoteAppLogFile,
        UserGroupInformation userUgi) throws IOException {
      try {
        this.fsDataOStream =
            userUgi.doAs(new PrivilegedExceptionAction<FSDataOutputStream>() {
              @Override
              public FSDataOutputStream run() throws Exception {
                FileContext fc = FileContext.getFileContext(conf);
                fc.setUMask(APP_LOG_FILE_UMASK);
                return fc.create(
                    remoteAppLogFile,
                    EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE),
                    new Options.CreateOpts[] {});
              }
            });
      } catch (InterruptedException e) {
        throw new IOException(e);
      }

      // Keys are not sorted: null arg
      // 256KB minBlockSize : Expected log size for each container too
      this.writer =
          new TFile.Writer(this.fsDataOStream, 256 * 1024, conf.get(
              YarnConfiguration.NM_LOG_AGG_COMPRESSION_TYPE,
              YarnConfiguration.DEFAULT_NM_LOG_AGG_COMPRESSION_TYPE), null, conf);
      //Write the version string
      writeVersion();
    }

    private void writeVersion() throws IOException {
      DataOutputStream out = this.writer.prepareAppendKey(-1);
      VERSION_KEY.write(out);
      out.close();
      out = this.writer.prepareAppendValue(-1);
      out.writeInt(VERSION);
      out.close();
    }

    public void writeApplicationOwner(String user) throws IOException {
      DataOutputStream out = this.writer.prepareAppendKey(-1);
      APPLICATION_OWNER_KEY.write(out);
      out.close();
      out = this.writer.prepareAppendValue(-1);
      out.writeUTF(user);
      out.close();
    }

    public void writeApplicationACLs(Map<ApplicationAccessType, String> appAcls)
        throws IOException {
      DataOutputStream out = this.writer.prepareAppendKey(-1);
      APPLICATION_ACL_KEY.write(out);
      out.close();
      out = this.writer.prepareAppendValue(-1);
      for (Entry<ApplicationAccessType, String> entry : appAcls.entrySet()) {
        out.writeUTF(entry.getKey().toString());
        out.writeUTF(entry.getValue());
      }
      out.close();
    }

    public void append(LogKey logKey, LogValue logValue) throws IOException {
      DataOutputStream out = this.writer.prepareAppendKey(-1);
      logKey.write(out);
      out.close();
      out = this.writer.prepareAppendValue(-1);
      logValue.write(out);
      out.close();
    }

    public void closeWriter() {
      try {
        this.writer.close();
      } catch (IOException e) {
        LOG.warn("Exception closing writer", e);
      }
      try {
        this.fsDataOStream.close();
      } catch (IOException e) {
        LOG.warn("Exception closing output-stream", e);
      }
    }
  }

  public static class LogReader {

    private final FSDataInputStream fsDataIStream;
    private final TFile.Reader.Scanner scanner;
    private final TFile.Reader reader;

    public LogReader(Configuration conf, Path remoteAppLogFile)
        throws IOException {
      FileContext fileContext = FileContext.getFileContext(conf);
      this.fsDataIStream = fileContext.open(remoteAppLogFile);
      reader =
          new TFile.Reader(this.fsDataIStream, fileContext.getFileStatus(
              remoteAppLogFile).getLen(), conf);
      this.scanner = reader.createScanner();
    }

    private boolean atBeginning = true;

    /**
     * Returns the owner of the application.
     * 
     * @return the application owner.
     * @throws IOException
     */
    public String getApplicationOwner() throws IOException {
      TFile.Reader.Scanner ownerScanner = reader.createScanner();
      LogKey key = new LogKey();
      while (!ownerScanner.atEnd()) {
        TFile.Reader.Scanner.Entry entry = ownerScanner.entry();
        key.readFields(entry.getKeyStream());
        if (key.toString().equals(APPLICATION_OWNER_KEY.toString())) {
          DataInputStream valueStream = entry.getValueStream();
          return valueStream.readUTF();
        }
        ownerScanner.advance();
      }
      return null;
    }

    /**
     * Returns ACLs for the application. An empty map is returned if no ACLs are
     * found.
     * 
     * @return a map of the Application ACLs.
     * @throws IOException
     */
    public Map<ApplicationAccessType, String> getApplicationAcls()
        throws IOException {
      // TODO Seek directly to the key once a comparator is specified.
      TFile.Reader.Scanner aclScanner = reader.createScanner();
      LogKey key = new LogKey();
      Map<ApplicationAccessType, String> acls =
          new HashMap<ApplicationAccessType, String>();
      while (!aclScanner.atEnd()) {
        TFile.Reader.Scanner.Entry entry = aclScanner.entry();
        key.readFields(entry.getKeyStream());
        if (key.toString().equals(APPLICATION_ACL_KEY.toString())) {
          DataInputStream valueStream = entry.getValueStream();
          while (true) {
            String appAccessOp = null;
            String aclString = null;
            try {
              appAccessOp = valueStream.readUTF();
            } catch (EOFException e) {
              // Valid end of stream.
              break;
            }
            try {
              aclString = valueStream.readUTF();
            } catch (EOFException e) {
              throw new YarnException("Error reading ACLs", e);
            }
            acls.put(ApplicationAccessType.valueOf(appAccessOp), aclString);
          }

        }
        aclScanner.advance();
      }
      return acls;
    }
    
    /**
     * Read the next key and return the value-stream.
     * 
     * @param key
     * @return the valueStream if there are more keys or null otherwise.
     * @throws IOException
     */
    public DataInputStream next(LogKey key) throws IOException {
      if (!this.atBeginning) {
        this.scanner.advance();
      } else {
        this.atBeginning = false;
      }
      if (this.scanner.atEnd()) {
        return null;
      }
      TFile.Reader.Scanner.Entry entry = this.scanner.entry();
      key.readFields(entry.getKeyStream());
      // Skip META keys
      if (RESERVED_KEYS.containsKey(key.toString())) {
        return next(key);
      }
      DataInputStream valueStream = entry.getValueStream();
      return valueStream;
    }

    /**
     * Get a ContainerLogsReader to read the logs for
     * the specified container.
     *
     * @param containerId
     * @return object to read the container's logs or null if the
     *         logs could not be found
     * @throws IOException
     */
    public ContainerLogsReader getContainerLogsReader(
        ContainerId containerId) throws IOException {
      ContainerLogsReader logReader = null;

      final LogKey containerKey = new LogKey(containerId);
      LogKey key = new LogKey();
      DataInputStream valueStream = next(key);
      while (valueStream != null && !key.equals(containerKey)) {
        valueStream = next(key);
      }

      if (valueStream != null) {
        logReader = new ContainerLogsReader(valueStream);
      }

      return logReader;
    }

    //TODO  Change Log format and interfaces to be containerId specific.
    // Avoid returning completeValueStreams.
//    public List<String> getTypesForContainer(DataInputStream valueStream){}
//    
//    /**
//     * @param valueStream
//     *          The Log stream for the container.
//     * @param fileType
//     *          the log type required.
//     * @return An InputStreamReader for the required log type or null if the
//     *         type is not found.
//     * @throws IOException
//     */
//    public InputStreamReader getLogStreamForType(DataInputStream valueStream,
//        String fileType) throws IOException {
//      valueStream.reset();
//      try {
//        while (true) {
//          String ft = valueStream.readUTF();
//          String fileLengthStr = valueStream.readUTF();
//          long fileLength = Long.parseLong(fileLengthStr);
//          if (ft.equals(fileType)) {
//            BoundedInputStream bis =
//                new BoundedInputStream(valueStream, fileLength);
//            return new InputStreamReader(bis);
//          } else {
//            long totalSkipped = 0;
//            long currSkipped = 0;
//            while (currSkipped != -1 && totalSkipped < fileLength) {
//              currSkipped = valueStream.skip(fileLength - totalSkipped);
//              totalSkipped += currSkipped;
//            }
//            // TODO Verify skip behaviour.
//            if (currSkipped == -1) {
//              return null;
//            }
//          }
//        }
//      } catch (EOFException e) {
//        return null;
//      }
//    }

    /**
     * Writes all logs for a single container to the provided writer.
     * @param valueStream
     * @param writer
     * @throws IOException
     */
    public static void readAcontainerLogs(DataInputStream valueStream,
        Writer writer) throws IOException {
      int bufferSize = 65536;
      char[] cbuf = new char[bufferSize];
      String fileType;
      String fileLengthStr;
      long fileLength;

      while (true) {
        try {
          fileType = valueStream.readUTF();
        } catch (EOFException e) {
          // EndOfFile
          return;
        }
        fileLengthStr = valueStream.readUTF();
        fileLength = Long.parseLong(fileLengthStr);
        writer.write("\n\nLogType:");
        writer.write(fileType);
        writer.write("\nLogLength:");
        writer.write(fileLengthStr);
        writer.write("\nLog Contents:\n");
        // ByteLevel
        BoundedInputStream bis =
            new BoundedInputStream(valueStream, fileLength);
        InputStreamReader reader = new InputStreamReader(bis);
        int currentRead = 0;
        int totalRead = 0;
        while ((currentRead = reader.read(cbuf, 0, bufferSize)) != -1) {
          writer.write(cbuf, 0, currentRead);
          totalRead += currentRead;
        }
      }
    }

    /**
     * Keep calling this till you get a {@link EOFException} for getting logs of
     * all types for a single container.
     * 
     * @param valueStream
     * @param out
     * @throws IOException
     */
    public static void readAContainerLogsForALogType(
        DataInputStream valueStream, DataOutputStream out)
          throws IOException {

      byte[] buf = new byte[65535];

      String fileType = valueStream.readUTF();
      String fileLengthStr = valueStream.readUTF();
      long fileLength = Long.parseLong(fileLengthStr);
      out.writeUTF("\nLogType:");
      out.writeUTF(fileType);
      out.writeUTF("\nLogLength:");
      out.writeUTF(fileLengthStr);
      out.writeUTF("\nLog Contents:\n");

      int curRead = 0;
      long pendingRead = fileLength - curRead;
      int toRead =
                pendingRead > buf.length ? buf.length : (int) pendingRead;
      int len = valueStream.read(buf, 0, toRead);
      while (len != -1 && curRead < fileLength) {
        out.write(buf, 0, len);
        curRead += len;

        pendingRead = fileLength - curRead;
        toRead =
                  pendingRead > buf.length ? buf.length : (int) pendingRead;
        len = valueStream.read(buf, 0, toRead);
      }
    }

    public void close() throws IOException {
      this.scanner.close();
      this.fsDataIStream.close();
    }
  }

  public static class ContainerLogsReader {
    private DataInputStream valueStream;
    private String currentLogType = null;
    private long currentLogLength = 0;
    private BoundedInputStream currentLogData = null;
    private InputStreamReader currentLogISR;

    public ContainerLogsReader(DataInputStream stream) {
      valueStream = stream;
    }

    public String nextLog() throws IOException {
      if (currentLogData != null && currentLogLength > 0) {
        // seek to the end of the current log, relying on BoundedInputStream
        // to prevent seeking past the end of the current log
        do {
          if (currentLogData.skip(currentLogLength) < 0) {
            break;
          }
        } while (currentLogData.read() != -1);
      }

      currentLogType = null;
      currentLogLength = 0;
      currentLogData = null;
      currentLogISR = null;

      try {
        String logType = valueStream.readUTF();
        String logLengthStr = valueStream.readUTF();
        currentLogLength = Long.parseLong(logLengthStr);
        currentLogData =
            new BoundedInputStream(valueStream, currentLogLength);
        currentLogData.setPropagateClose(false);
        currentLogISR = new InputStreamReader(currentLogData);
        currentLogType = logType;
      } catch (EOFException e) {
      }

      return currentLogType;
    }

    public String getCurrentLogType() {
      return currentLogType;
    }

    public long getCurrentLogLength() {
      return currentLogLength;
    }

    public long skip(long n) throws IOException {
      return currentLogData.skip(n);
    }

    public int read(byte[] buf, int off, int len) throws IOException {
      return currentLogData.read(buf, off, len);
    }

    public int read(char[] buf, int off, int len) throws IOException {
      return currentLogISR.read(buf, off, len);
    }
  }
}
