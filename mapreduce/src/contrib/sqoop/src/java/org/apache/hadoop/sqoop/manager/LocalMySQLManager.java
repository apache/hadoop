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
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.CharBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.sqoop.ImportOptions;
import org.apache.hadoop.sqoop.lib.FieldFormatter;
import org.apache.hadoop.sqoop.lib.RecordParser;
import org.apache.hadoop.sqoop.util.DirectImportUtils;
import org.apache.hadoop.sqoop.util.ImportError;
import org.apache.hadoop.sqoop.util.JdbcUrl;
import org.apache.hadoop.sqoop.util.PerfCounters;
import org.apache.hadoop.sqoop.util.StreamHandlerFactory;
import org.apache.hadoop.util.Shell;

/**
 * Manages direct connections to MySQL databases
 * so we can use mysqldump to get really fast dumps.
 */
public class LocalMySQLManager extends MySQLManager {

  public static final Log LOG = LogFactory.getLog(LocalMySQLManager.class.getName());

  // StreamHandlers used to import data from mysqldump directly into HDFS.

  /**
   * Copies data directly from mysqldump into HDFS, after stripping some
   * header and footer characters that are attached to each line in mysqldump.
   */
  static class CopyingStreamHandlerFactory implements StreamHandlerFactory {
    private final BufferedWriter writer;
    private final PerfCounters counters;

    CopyingStreamHandlerFactory(final BufferedWriter w, final PerfCounters ctrs) {
      this.writer = w;
      this.counters = ctrs;
    }

    private CopyingStreamThread child;

    public void processStream(InputStream is) {
      child = new CopyingStreamThread(is, writer, counters);
      child.start();
    }

    public int join() throws InterruptedException {
      child.join();
      if (child.isErrored()) {
        return 1;
      } else {
        return 0;
      }
    }

    private static class CopyingStreamThread extends Thread {
      public static final Log LOG = LogFactory.getLog(CopyingStreamThread.class.getName());

      private final BufferedWriter writer;
      private final InputStream stream;
      private final PerfCounters counters;

      private boolean error;

      CopyingStreamThread(final InputStream is, final BufferedWriter w, final PerfCounters ctrs) {
        this.writer = w;
        this.stream = is;
        this.counters = ctrs;
      }

      public boolean isErrored() {
        return error;
      }

      public void run() {
        BufferedReader r = null;
        BufferedWriter w = this.writer;

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
          error = true;
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
   * The ReparsingStreamHandler will instantiate a RecordParser to read mysqldump's
   * output, and re-emit the text in the user's specified output format.
   */
  static class ReparsingStreamHandlerFactory implements StreamHandlerFactory {
    private final BufferedWriter writer;
    private final ImportOptions options;
    private final PerfCounters counters;

    ReparsingStreamHandlerFactory(final BufferedWriter w, final ImportOptions opts, 
        final PerfCounters ctrs) {
      this.writer = w;
      this.options = opts;
      this.counters = ctrs;
    }

    private ReparsingStreamThread child;

    public void processStream(InputStream is) {
      child = new ReparsingStreamThread(is, writer, options, counters);
      child.start();
    }

    public int join() throws InterruptedException {
      child.join();
      if (child.isErrored()) {
        return 1;
      } else {
        return 0;
      }
    }

    private static class ReparsingStreamThread extends Thread {
      public static final Log LOG = LogFactory.getLog(ReparsingStreamThread.class.getName());

      private final BufferedWriter writer;
      private final ImportOptions options;
      private final InputStream stream;
      private final PerfCounters counters;

      private boolean error;

      ReparsingStreamThread(final InputStream is, final BufferedWriter w,
          final ImportOptions opts, final PerfCounters ctrs) {
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
        MYSQLDUMP_PARSER = new RecordParser(MYSQL_FIELD_DELIM, MYSQL_RECORD_DELIM,
            MYSQL_ENCLOSE_CHAR, MYSQL_ESCAPE_CHAR, MYSQL_ENCLOSE_REQUIRED);
      }

      public boolean isErrored() {
        return error;
      }

      public void run() {
        BufferedReader r = null;
        BufferedWriter w = this.writer;

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
            counters.addBytes(recordLen);
          }
        } catch (IOException ioe) {
          LOG.error("IOException reading from mysqldump: " + ioe.toString());
          // flag this error so the parent can handle it appropriately.
          error = true;
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
  public void importTable(String tableName, String jarFile, Configuration conf)
      throws IOException, ImportError {

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

    if (null == databaseName) {
      throw new ImportError("Could not determine database name");
    }

    LOG.info("Performing import of table " + tableName + " from database " + databaseName);
    args.add(MYSQL_DUMP_CMD); // requires that this is on the path.

    String password = options.getPassword();
    String passwordFile = null;

    Process p = null;
    StreamHandlerFactory streamHandler = null;
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

      args.add("--skip-opt");
      args.add("--compact");
      args.add("--no-create-db");
      args.add("--no-create-info");
      args.add("--quick"); // no buffering
      // TODO(aaron): Add a flag to allow --lock-tables instead for MyISAM data
      args.add("--single-transaction"); 
      // TODO(aaron): Add --host and --port arguments to support remote direct connects.

      String username = options.getUsername();
      if (null != username) {
        args.add("--user=" + username);
      }

      args.add(databaseName);
      args.add(tableName);

      // begin the import in an external process.
      LOG.debug("Starting mysqldump with arguments:");
      for (String arg : args) {
        LOG.debug("  " + arg);
      }

      // This writer will be closed by StreamHandlerFactory.
      OutputStream os = DirectImportUtils.createHdfsSink(conf, options, tableName);
      BufferedWriter w = new BufferedWriter(new OutputStreamWriter(os));

      // Actually start the mysqldump.
      p = Runtime.getRuntime().exec(args.toArray(new String[0]));

      // read from the stdout pipe into the HDFS writer.
      InputStream is = p.getInputStream();

      if (outputDelimsAreMySQL()) {
        LOG.debug("Output delimiters conform to mysqldump; using straight copy"); 
        streamHandler = new CopyingStreamHandlerFactory(w, counters);
      } else {
        LOG.debug("User-specified delimiters; using reparsing import");
        LOG.info("Converting data to use specified delimiters.");
        LOG.info("(For the fastest possible import, use");
        LOG.info("--mysql-delimiters to specify the same field");
        LOG.info("delimiters as are used by mysqldump.)");
        streamHandler = new ReparsingStreamHandlerFactory(w, options, counters);
      }

      // Start an async thread to read and upload the whole stream.
      counters.startClock();
      streamHandler.processStream(is);
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

      // block until the stream handler is done too.
      int streamResult = 0;
      if (null != streamHandler) {
        while (true) {
          try {
            streamResult = streamHandler.join();
          } catch (InterruptedException ie) {
            // interrupted; loop around.
            continue;
          }

          break;
        }
      }

      LOG.info("Transfer loop complete.");

      if (0 != result) {
        throw new IOException("mysqldump terminated with status "
            + Integer.toString(result));
      }

      if (0 != streamResult) {
        throw new IOException("Encountered exception in stream handler");
      }

      counters.stopClock();
      LOG.info("Transferred " + counters.toString());
    }
  }
}

