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

package org.apache.hadoop.sqoop.manager;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.sqoop.SqoopOptions;
import org.apache.hadoop.sqoop.io.SplittableBufferedWriter;
import org.apache.hadoop.sqoop.util.AsyncSink;
import org.apache.hadoop.sqoop.util.DirectImportUtils;
import org.apache.hadoop.sqoop.util.ErrorableAsyncSink;
import org.apache.hadoop.sqoop.util.ErrorableThread;
import org.apache.hadoop.sqoop.util.Executor;
import org.apache.hadoop.sqoop.util.ImportException;
import org.apache.hadoop.sqoop.util.JdbcUrl;
import org.apache.hadoop.sqoop.util.LoggingAsyncSink;
import org.apache.hadoop.sqoop.util.PerfCounters;

/**
 * Manages direct dumps from Postgresql databases via psql COPY TO STDOUT
 * commands.
 */
public class DirectPostgresqlManager extends PostgresqlManager {
  public static final Log LOG = LogFactory.getLog(DirectPostgresqlManager.class.getName());

  public DirectPostgresqlManager(final SqoopOptions opts) {
    // Inform superclass that we're overriding import method via alt. constructor.
    super(opts, true);
  }

  private static final String PSQL_CMD = "psql";

  /** Copies data directly into HDFS, adding the user's chosen line terminator
      char to each record.
    */
  static class PostgresqlAsyncSink extends ErrorableAsyncSink {
    private final SplittableBufferedWriter writer;
    private final PerfCounters counters;
    private final SqoopOptions options;

    PostgresqlAsyncSink(final SplittableBufferedWriter w, final SqoopOptions opts,
        final PerfCounters ctrs) {
      this.writer = w;
      this.options = opts;
      this.counters = ctrs;
    }

    public void processStream(InputStream is) {
      child = new PostgresqlStreamThread(is, writer, options, counters);
      child.start();
    }

    private static class PostgresqlStreamThread extends ErrorableThread {
      public static final Log LOG = LogFactory.getLog(PostgresqlStreamThread.class.getName());

      private final SplittableBufferedWriter writer;
      private final InputStream stream;
      private final SqoopOptions options;
      private final PerfCounters counters;

      PostgresqlStreamThread(final InputStream is, final SplittableBufferedWriter w,
          final SqoopOptions opts, final PerfCounters ctrs) {
        this.stream = is;
        this.writer = w;
        this.options = opts;
        this.counters = ctrs;
      }

      public void run() {
        BufferedReader r = null;
        SplittableBufferedWriter w = this.writer;

        char recordDelim = this.options.getOutputRecordDelim();

        try {
          r = new BufferedReader(new InputStreamReader(this.stream));

          // read/write transfer loop here.
          while (true) {
            String inLine = r.readLine();
            if (null == inLine) {
              break; // EOF
            }

            w.write(inLine);
            w.write(recordDelim);
            w.allowSplit();
            counters.addBytes(1 + inLine.length());
          }
        } catch (IOException ioe) {
          LOG.error("IOException reading from psql: " + ioe.toString());
          // set the error bit so our caller can see that something went wrong.
          setError();
        } finally {
          if (null != r) {
            try {
              r.close();
            } catch (IOException ioe) {
              LOG.info("Error closing FIFO stream: " + ioe.toString());
            }
          }

          if (null != w) {
            try {
              w.close();
            } catch (IOException ioe) {
              LOG.info("Error closing HDFS stream: " + ioe.toString());
            }
          }
        }
      }
    }
  }

  /**
    Takes a list of columns and turns them into a string like "col1, col2, col3..."
   */
  private String getColumnListStr(String [] cols) {
    if (null == cols) {
      return null;
    }

    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (String col : cols) {
      if (!first) {
        sb.append(", ");
      }
      sb.append(col);
      first = false;
    }

    return sb.toString();
  }

  /**
   * @return the Postgresql-specific SQL command to copy the table ("COPY .... TO STDOUT")
   */
  private String getCopyCommand(String tableName) {
    /*
       Format of this command is:

          COPY table(col, col....) TO STDOUT 
      or  COPY ( query ) TO STDOUT
        WITH DELIMITER 'fieldsep'
        CSV
        QUOTE 'quotechar'
        ESCAPE 'escapechar'
        FORCE QUOTE col, col, col....
    */

    StringBuilder sb = new StringBuilder();
    String [] cols = getColumnNames(tableName);

    sb.append("COPY ");
    String whereClause = this.options.getWhereClause();
    if (whereClause != null && whereClause.length() > 0) {
      // Import from a SELECT QUERY
      sb.append("(");
      sb.append("SELECT ");
      if (null != cols) {
        sb.append(getColumnListStr(cols));
      } else {
        sb.append("*");
      }

      sb.append(" FROM ");
      sb.append(tableName);
      sb.append(" WHERE ");
      sb.append(whereClause);
      sb.append(")");
    } else {
      // Import just the table.
      sb.append(tableName);
      if (null != cols) {
        // specify columns.
        sb.append("(");
        sb.append(getColumnListStr(cols));
        sb.append(")");
      }
    }

    // Translate delimiter characters to '\ooo' octal representation.
    sb.append(" TO STDOUT WITH DELIMITER E'\\");
    sb.append(Integer.toString((int) this.options.getOutputFieldDelim(), 8));
    sb.append("' CSV ");
    if (this.options.getOutputEnclosedBy() != '\0') {
      sb.append("QUOTE E'\\");
      sb.append(Integer.toString((int) this.options.getOutputEnclosedBy(), 8));
      sb.append("' ");
    }
    if (this.options.getOutputEscapedBy() != '\0') {
      sb.append("ESCAPE E'\\");
      sb.append(Integer.toString((int) this.options.getOutputEscapedBy(), 8));
      sb.append("' ");
    }

    // add the "FORCE QUOTE col, col, col..." clause if quotes are required.
    if (null != cols && this.options.isOutputEncloseRequired()) {
      sb.append("FORCE QUOTE ");
      sb.append(getColumnListStr(cols));
    }

    sb.append(";");

    String copyCmd = sb.toString();
    LOG.debug("Copy command is " + copyCmd);
    return copyCmd;
  }

  /** Write the COPY command to a temp file
    * @return the filename we wrote to.
    */
  private String writeCopyCommand(String command) throws IOException {
    String tmpDir = options.getTempDir();
    File tempFile = File.createTempFile("tmp-",".sql", new File(tmpDir));
    BufferedWriter w = new BufferedWriter(
        new OutputStreamWriter(new FileOutputStream(tempFile)));
    w.write(command);
    w.newLine();
    w.close();
    return tempFile.toString();
  }

  /** Write the user's password to a file that is chmod 0600.
      @return the filename.
    */
  private String writePasswordFile(String password) throws IOException {

    String tmpDir = options.getTempDir();
    File tempFile = File.createTempFile("pgpass",".pgpass", new File(tmpDir));
    LOG.debug("Writing password to tempfile: " + tempFile);

    // Make sure it's only readable by the current user.
    DirectImportUtils.setFilePermissions(tempFile, "0600");

    // Actually write the password data into the file.
    BufferedWriter w = new BufferedWriter(
        new OutputStreamWriter(new FileOutputStream(tempFile)));
    w.write("*:*:*:*:" + password);
    w.close();
    return tempFile.toString();
  }

  @Override
  /**
   * Import the table into HDFS by using psql to pull the data out of the db
   * via COPY FILE TO STDOUT.
   */
  public void importTable(ImportJobContext context)
    throws IOException, ImportException {

    String tableName = context.getTableName();
    String jarFile = context.getJarFile();
    SqoopOptions options = context.getOptions();

    LOG.info("Beginning psql fast path import");

    if (options.getFileLayout() != SqoopOptions.FileLayout.TextFile) {
      // TODO(aaron): Support SequenceFile-based load-in
      LOG.warn("File import layout" + options.getFileLayout()
          + " is not supported by");
      LOG.warn("Postgresql direct import; import will proceed as text files.");
    }

    String commandFilename = null;
    String passwordFilename = null;
    Process p = null;
    AsyncSink sink = null;
    AsyncSink errSink = null;
    PerfCounters counters = new PerfCounters();

    try {
      // Get the COPY TABLE command to issue, write this to a file, and pass it
      // in to psql with -f filename.
      // Then make sure we delete this file in our finally block.
      String copyCmd = getCopyCommand(tableName);
      commandFilename = writeCopyCommand(copyCmd);

      // Arguments to pass to psql on the command line.
      ArrayList<String> args = new ArrayList<String>();

      // Environment to pass to psql.
      List<String> envp = Executor.getCurEnvpStrings();

      // We need to parse the connect string URI to determine the database name
      // and the host and port. If the host is localhost and the port is not specified,
      // we don't want to pass this to psql, because we want to force the use of a
      // UNIX domain socket, not a TCP/IP socket.
      String connectString = options.getConnectString();
      String databaseName = JdbcUrl.getDatabaseName(connectString);
      String hostname = JdbcUrl.getHostName(connectString);
      int port = JdbcUrl.getPort(connectString);

      if (null == databaseName) {
        throw new ImportException("Could not determine database name");
      }

      LOG.info("Performing import of table " + tableName + " from database " + databaseName);
      args.add(PSQL_CMD); // requires that this is on the path.
      args.add("--tuples-only");
      args.add("--quiet");

      String username = options.getUsername();
      if (username != null) {
        args.add("--username");
        args.add(username);
        String password = options.getPassword();
        if (null != password) {
          passwordFilename = writePasswordFile(password);
          // Need to send PGPASSFILE environment variable specifying
          // location of our postgres file.
          envp.add("PGPASSFILE=" + passwordFilename);
        }
      }

      if (!DirectImportUtils.isLocalhost(hostname) || port != -1) {
        args.add("--host");
        args.add(hostname);
        args.add("--port");
        args.add(Integer.toString(port));
      }

      if (null != databaseName && databaseName.length() > 0) {
        args.add(databaseName);
      }

      // The COPY command is in a script file.
      args.add("-f");
      args.add(commandFilename);

      // begin the import in an external process.
      LOG.debug("Starting psql with arguments:");
      for (String arg : args) {
        LOG.debug("  " + arg);
      }

      // This writer will be closed by AsyncSink.
      SplittableBufferedWriter w = DirectImportUtils.createHdfsSink(
          options.getConf(), options, tableName);

      // Actually start the psql dump.
      p = Runtime.getRuntime().exec(args.toArray(new String[0]),
          envp.toArray(new String[0]));

      // read from the stdout pipe into the HDFS writer.
      InputStream is = p.getInputStream();
      sink = new PostgresqlAsyncSink(w, options, counters);

      LOG.debug("Starting stream sink");
      counters.startClock();
      sink.processStream(is);
      errSink = new LoggingAsyncSink(LOG);
      errSink.processStream(p.getErrorStream());
    } finally {
      // block until the process is done.
      LOG.debug("Waiting for process completion");
      int result = 0;
      if (null != p) {
        while (true) {
          try {
            result = p.waitFor();
          } catch (InterruptedException ie) {
            // interrupted; loop around.
            continue;
          }

          break;
        }
      }

      // Remove any password file we wrote
      if (null != passwordFilename) {
        if (!new File(passwordFilename).delete()) {
          LOG.error("Could not remove postgresql password file " + passwordFilename);
          LOG.error("You should remove this file to protect your credentials.");
        }
      }

      if (null != commandFilename) {
        // We wrote the COPY comand to a tmpfile. Remove it.
        if (!new File(commandFilename).delete()) {
          LOG.info("Could not remove temp file: " + commandFilename);
        }
      }

      // block until the stream sink is done too.
      int streamResult = 0;
      if (null != sink) {
        while (true) {
          try {
            streamResult = sink.join();
          } catch (InterruptedException ie) {
            // interrupted; loop around.
            continue;
          }

          break;
        }
      }

      // Attempt to block for stderr stream sink; errors are advisory.
      if (null != errSink) {
        try {
          if (0 != errSink.join()) {
            LOG.info("Encountered exception reading stderr stream");
          }
        } catch (InterruptedException ie) {
          LOG.info("Thread interrupted waiting for stderr to complete: "
              + ie.toString());
        }
      }

      LOG.info("Transfer loop complete.");

      if (0 != result) {
        throw new IOException("psql terminated with status "
            + Integer.toString(result));
      }

      if (0 != streamResult) {
        throw new IOException("Encountered exception in stream sink");
      }

      counters.stopClock();
      LOG.info("Transferred " + counters.toString());
    }
  }
}
