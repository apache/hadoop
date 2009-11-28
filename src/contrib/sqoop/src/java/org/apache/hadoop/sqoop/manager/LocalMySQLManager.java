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
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.sqoop.ImportOptions;
import org.apache.hadoop.sqoop.io.SplittableBufferedWriter;
import org.apache.hadoop.sqoop.lib.FieldFormatter;
import org.apache.hadoop.sqoop.lib.RecordParser;
import org.apache.hadoop.sqoop.util.AsyncSink;
import org.apache.hadoop.sqoop.util.DirectImportUtils;
import org.apache.hadoop.sqoop.util.ErrorableAsyncSink;
import org.apache.hadoop.sqoop.util.ErrorableThread;
import org.apache.hadoop.sqoop.util.ImportError;
import org.apache.hadoop.sqoop.util.JdbcUrl;
import org.apache.hadoop.sqoop.util.LoggingAsyncSink;
import org.apache.hadoop.sqoop.util.PerfCounters;

/**
 * Manages direct connections to MySQL databases
 * so we can use mysqldump to get really fast dumps.
 */
public class LocalMySQLManager extends MySQLManager {

  public static final Log LOG = LogFactory.getLog(LocalMySQLManager.class.getName());

  // AsyncSinks used to import data from mysqldump directly into HDFS.

  /**
   * Copies data directly from mysqldump into HDFS, after stripping some
   * header and footer characters that are attached to each line in mysqldump.
   */
  static class CopyingAsyncSink extends ErrorableAsyncSink {
    private final SplittableBufferedWriter writer;
    private final PerfCounters counters;

    CopyingAsyncSink(final SplittableBufferedWriter w,
        final PerfCounters ctrs) {
      this.writer = w;
      this.counters = ctrs;
    }

    public void processStream(InputStream is) {
      child = new CopyingStreamThread(is, writer, counters);
      child.start();
    }

    private static class CopyingStreamThread extends ErrorableThread {
      public static final Log LOG = LogFactory.getLog(
          CopyingStreamThread.class.getName());

      private final SplittableBufferedWriter writer;
      private final InputStream stream;
      private final PerfCounters counters;

      CopyingStreamThread(final InputStream is,
          final SplittableBufferedWriter w, final PerfCounters ctrs) {
        this.writer = w;
        this.stream = is;
        this.counters = ctrs;
      }

      public void run() {
        BufferedReader r = null;
        SplittableBufferedWriter w = this.writer;

        try {
          r = new BufferedReader(new InputStreamReader(this.stream));

          // Actually do the read/write transfer loop here.
          int preambleLen = -1; // set to this for "undefined"
          while (true) {
            String inLine = r.readLine();
            if (null == inLine) {
              break; // EOF.
            }

            // this line is of the form "INSERT .. VALUES ( actual value text );"
            // strip the leading preamble up to the '(' and the trailing ');'.
            if (preambleLen == -1) {
              // we haven't determined how long the preamble is. It's constant
              // across all lines, so just figure this out once.
              String recordStartMark = "VALUES (";
              preambleLen = inLine.indexOf(recordStartMark) + recordStartMark.length();
            }

            // chop off the leading and trailing text as we write the
            // output to HDFS.
            int len = inLine.length() - 2 - preambleLen;
            w.write(inLine, preambleLen, len);
            w.newLine();
            counters.addBytes(1 + len);
          }
        } catch (IOException ioe) {
          LOG.error("IOException reading from mysqldump: " + ioe.toString());
          // flag this error so we get an error status back in the caller.
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
   * The ReparsingAsyncSink will instantiate a RecordParser to read mysqldump's
   * output, and re-emit the text in the user's specified output format.
   */
  static class ReparsingAsyncSink extends ErrorableAsyncSink {
    private final SplittableBufferedWriter writer;
    private final ImportOptions options;
    private final PerfCounters counters;

    ReparsingAsyncSink(final SplittableBufferedWriter w,
        final ImportOptions opts, final PerfCounters ctrs) {
      this.writer = w;
      this.options = opts;
      this.counters = ctrs;
    }

    public void processStream(InputStream is) {
      child = new ReparsingStreamThread(is, writer, options, counters);
      child.start();
    }

    private static class ReparsingStreamThread extends ErrorableThread {
      public static final Log LOG = LogFactory.getLog(
          ReparsingStreamThread.class.getName());

      private final SplittableBufferedWriter writer;
      private final ImportOptions options;
      private final InputStream stream;
      private final PerfCounters counters;

      ReparsingStreamThread(final InputStream is,
          final SplittableBufferedWriter w, final ImportOptions opts,
          final PerfCounters ctrs) {
        this.writer = w;
        this.options = opts;
        this.stream = is;
        this.counters = ctrs;
      }

      private static final char MYSQL_FIELD_DELIM = ',';
      private static final char MYSQL_RECORD_DELIM = '\n';
      private static final char MYSQL_ENCLOSE_CHAR = '\'';
      private static final char MYSQL_ESCAPE_CHAR = '\\';
      private static final boolean MYSQL_ENCLOSE_REQUIRED = false;

      private static final RecordParser MYSQLDUMP_PARSER;

      static {
        // build a record parser for mysqldump's format
        MYSQLDUMP_PARSER = new RecordParser(MYSQL_FIELD_DELIM,
            MYSQL_RECORD_DELIM, MYSQL_ENCLOSE_CHAR, MYSQL_ESCAPE_CHAR,
            MYSQL_ENCLOSE_REQUIRED);
      }

      public void run() {
        BufferedReader r = null;
        SplittableBufferedWriter w = this.writer;

        try {
          r = new BufferedReader(new InputStreamReader(this.stream));

          char outputFieldDelim = options.getOutputFieldDelim();
          char outputRecordDelim = options.getOutputRecordDelim();
          String outputEnclose = "" + options.getOutputEnclosedBy();
          String outputEscape = "" + options.getOutputEscapedBy();
          boolean outputEncloseRequired = options.isOutputEncloseRequired(); 
          char [] encloseFor = { outputFieldDelim, outputRecordDelim };

          // Actually do the read/write transfer loop here.
          int preambleLen = -1; // set to this for "undefined"
          while (true) {
            String inLine = r.readLine();
            if (null == inLine) {
              break; // EOF.
            }

            // this line is of the form "INSERT .. VALUES ( actual value text );"
            // strip the leading preamble up to the '(' and the trailing ');'.
            if (preambleLen == -1) {
              // we haven't determined how long the preamble is. It's constant
              // across all lines, so just figure this out once.
              String recordStartMark = "VALUES (";
              preambleLen = inLine.indexOf(recordStartMark) + recordStartMark.length();
            }

            // Wrap the input string in a char buffer that ignores the leading and trailing
            // text.
            CharBuffer charbuf = CharBuffer.wrap(inLine, preambleLen, inLine.length() - 2);

            // Pass this along to the parser
            List<String> fields = null;
            try {
              fields = MYSQLDUMP_PARSER.parseRecord(charbuf);
            } catch (RecordParser.ParseError pe) {
              LOG.warn("ParseError reading from mysqldump: " + pe.toString() + "; record skipped");
            }

            // For all of the output fields, emit them using the delimiters the user chooses.
            boolean first = true;
            int recordLen = 1; // for the delimiter.
            for (String field : fields) {
              if (!first) {
                w.write(outputFieldDelim);
              } else {
                first = false;
              }

              String fieldStr = FieldFormatter.escapeAndEnclose(field, outputEscape, outputEnclose,
                  encloseFor, outputEncloseRequired);
              w.write(fieldStr);
              recordLen += fieldStr.length();
            }

            w.write(outputRecordDelim);
            w.allowSplit();
            counters.addBytes(recordLen);
          }
        } catch (IOException ioe) {
          LOG.error("IOException reading from mysqldump: " + ioe.toString());
          // flag this error so the parent can handle it appropriately.
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


  public LocalMySQLManager(final ImportOptions options) {
    super(options, false);
  }

  private static final String MYSQL_DUMP_CMD = "mysqldump";

  /**
   * @return true if the user's output delimiters match those used by mysqldump.
   * fields: ,
   * lines: \n
   * optional-enclose: \'
   * escape: \\
   */
  private boolean outputDelimsAreMySQL() {
    return options.getOutputFieldDelim() == ','
        && options.getOutputRecordDelim() == '\n'
        && options.getOutputEnclosedBy() == '\''
        && options.getOutputEscapedBy() == '\\'
        && !options.isOutputEncloseRequired(); // encloser is optional
  }
  
  /**
   * Writes the user's password to a tmp file with 0600 permissions.
   * @return the filename used.
   */
  private String writePasswordFile() throws IOException {
    // Create the temp file to hold the user's password.
    String tmpDir = options.getTempDir();
    File tempFile = File.createTempFile("mysql-cnf",".cnf", new File(tmpDir));

    // Make the password file only private readable.
    DirectImportUtils.setFilePermissions(tempFile, "0600");

    // If we're here, the password file is believed to be ours alone.
    // The inability to set chmod 0600 inside Java is troublesome. We have to trust
    // that the external 'chmod' program in the path does the right thing, and returns
    // the correct exit status. But given our inability to re-read the permissions
    // associated with a file, we'll have to make do with this.
    String password = options.getPassword();
    BufferedWriter w = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tempFile)));
    w.write("[client]\n");
    w.write("password=" + password + "\n");
    w.close();

    return tempFile.toString();
  }

  /**
   * Import the table into HDFS by using mysqldump to pull out the data from
   * the database and upload the files directly to HDFS.
   */
  public void importTable(ImportJobContext context)
      throws IOException, ImportError {

    String tableName = context.getTableName();
    String jarFile = context.getJarFile();
    ImportOptions options = context.getOptions();

    LOG.info("Beginning mysqldump fast path import");

    if (options.getFileLayout() != ImportOptions.FileLayout.TextFile) {
      // TODO(aaron): Support SequenceFile-based load-in
      LOG.warn("File import layout " + options.getFileLayout()
          + " is not supported by");
      LOG.warn("MySQL direct import; import will proceed as text files.");
    }

    ArrayList<String> args = new ArrayList<String>();

    // We need to parse the connect string URI to determine the database
    // name. Using java.net.URL directly on the connect string will fail because
    // Java doesn't respect arbitrary JDBC-based schemes. So we chop off the scheme
    // (everything before '://') and replace it with 'http', which we know will work.
    String connectString = options.getConnectString();
    String databaseName = JdbcUrl.getDatabaseName(connectString);
    String hostname = JdbcUrl.getHostName(connectString);
    int port = JdbcUrl.getPort(connectString);

    if (null == databaseName) {
      throw new ImportError("Could not determine database name");
    }

    LOG.info("Performing import of table " + tableName + " from database " + databaseName);
    args.add(MYSQL_DUMP_CMD); // requires that this is on the path.

    String password = options.getPassword();
    String passwordFile = null;

    Process p = null;
    AsyncSink sink = null;
    AsyncSink errSink = null;
    PerfCounters counters = new PerfCounters();
    try {
      // --defaults-file must be the first argument.
      if (null != password && password.length() > 0) {
        passwordFile = writePasswordFile();
        args.add("--defaults-file=" + passwordFile);
      }

      String whereClause = options.getWhereClause();
      if (null != whereClause) {
        // Don't use the --where="<whereClause>" version because spaces in it can confuse
        // Java, and adding in surrounding quotes confuses Java as well.
        args.add("-w");
        args.add(whereClause);
      }

      if (!DirectImportUtils.isLocalhost(hostname) || port != -1) {
        args.add("--host=" + hostname);
        args.add("--port=" + Integer.toString(port));
      }

      args.add("--skip-opt");
      args.add("--compact");
      args.add("--no-create-db");
      args.add("--no-create-info");
      args.add("--quick"); // no buffering
      args.add("--single-transaction"); 

      String username = options.getUsername();
      if (null != username) {
        args.add("--user=" + username);
      }

      // If the user supplied extra args, add them here.
      String [] extra = options.getExtraArgs();
      if (null != extra) {
        for (String arg : extra) {
          args.add(arg);
        }
      }

      args.add(databaseName);
      args.add(tableName);

      // begin the import in an external process.
      LOG.debug("Starting mysqldump with arguments:");
      for (String arg : args) {
        LOG.debug("  " + arg);
      }

      // This writer will be closed by AsyncSink.
      SplittableBufferedWriter w = DirectImportUtils.createHdfsSink(
          options.getConf(), options, tableName);

      // Actually start the mysqldump.
      p = Runtime.getRuntime().exec(args.toArray(new String[0]));

      // read from the stdout pipe into the HDFS writer.
      InputStream is = p.getInputStream();

      if (outputDelimsAreMySQL()) {
        LOG.debug("Output delimiters conform to mysqldump; using straight copy"); 
        sink = new CopyingAsyncSink(w, counters);
      } else {
        LOG.debug("User-specified delimiters; using reparsing import");
        LOG.info("Converting data to use specified delimiters.");
        LOG.info("(For the fastest possible import, use");
        LOG.info("--mysql-delimiters to specify the same field");
        LOG.info("delimiters as are used by mysqldump.)");
        sink = new ReparsingAsyncSink(w, options, counters);
      }

      // Start an async thread to read and upload the whole stream.
      counters.startClock();
      sink.processStream(is);

      // Start an async thread to send stderr to log4j.
      errSink = new LoggingAsyncSink(LOG);
      errSink.processStream(p.getErrorStream());
    } finally {

      // block until the process is done.
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

      // Remove the password file.
      if (null != passwordFile) {
        if (!new File(passwordFile).delete()) {
          LOG.error("Could not remove mysql password file " + passwordFile);
          LOG.error("You should remove this file to protect your credentials.");
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

      // Try to wait for stderr to finish, but regard any errors as advisory.
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
        throw new IOException("mysqldump terminated with status "
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

