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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.EnumSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.file.tfile.TFile;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class AggregatedLogFormat {

  static final Log LOG = LogFactory.getLog(AggregatedLogFormat.class);

  public static class LogKey implements Writable {

    private String containerId;

    public LogKey() {

    }

    public LogKey(ContainerId containerId) {
      this.containerId = ConverterUtils.toString(containerId);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(this.containerId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.containerId = in.readUTF();
    }

    @Override
    public String toString() {
      return this.containerId;
    }
  }

  public static class LogValue {

    private final String[] rootLogDirs;
    private final ContainerId containerId;

    public LogValue(String[] rootLogDirs, ContainerId containerId) {
      this.rootLogDirs = rootLogDirs;
      this.containerId = containerId;
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

        for (File logFile : containerLogDir.listFiles()) {

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
                return FileContext.getFileContext(conf).create(
                    remoteAppLogFile,
                    EnumSet.of(CreateFlag.CREATE), new Options.CreateOpts[] {});
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
    }

    public void append(LogKey logKey, LogValue logValue) throws IOException {
      DataOutputStream out = this.writer.prepareAppendKey(-1);
      logKey.write(out);
      out.close();
      out = this.writer.prepareAppendValue(-1);
      logValue.write(out);
      out.close();
      this.fsDataOStream.hflush();
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

    public LogReader(Configuration conf, Path remoteAppLogFile)
        throws IOException {
      FileContext fileContext = FileContext.getFileContext(conf);
      this.fsDataIStream = fileContext.open(remoteAppLogFile);
      TFile.Reader reader =
          new TFile.Reader(this.fsDataIStream, fileContext.getFileStatus(
              remoteAppLogFile).getLen(), conf);
      this.scanner = reader.createScanner();
    }

    private boolean atBeginning = true;

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
      DataInputStream valueStream = entry.getValueStream();
      return valueStream;
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
}
