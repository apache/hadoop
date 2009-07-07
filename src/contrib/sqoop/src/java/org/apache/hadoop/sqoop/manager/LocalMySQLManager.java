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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.sqoop.ImportOptions;
import org.apache.hadoop.sqoop.util.ImportError;

/**
 * Manages local connections to MySQL databases
 * that are local to this machine -- so we can use mysqldump to get
 * really fast dumps.
 */
public class LocalMySQLManager extends MySQLManager {

  public static final Log LOG = LogFactory.getLog(LocalMySQLManager.class.getName());

  public LocalMySQLManager(final ImportOptions options) {
    super(options, false);
  }

  private static final String MYSQL_DUMP_CMD = "mysqldump";
  
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
      LOG.warn("MySQL local import; import will proceed as text files.");
    }

    ArrayList<String> args = new ArrayList<String>();

    // We need to parse the connect string URI to determine the database
    // name. Using java.net.URL directly on the connect string will fail because
    // Java doesn't respect arbitrary JDBC-based schemes. So we chop off the scheme
    // (everything before '://') and replace it with 'http', which we know will work.
    String connectString = options.getConnectString();
    String databaseName = null;
    try {
      String sanitizedString = null;
      int schemeEndOffset = connectString.indexOf("://");
      if (-1 == schemeEndOffset) {
        // couldn't find one? try our best here.
        sanitizedString = "http://" + connectString;
        LOG.warn("Could not find database access scheme in connect string " + connectString);
      } else {
        sanitizedString = "http" + connectString.substring(schemeEndOffset);
      }

      URL connectUrl = new URL(sanitizedString);
      databaseName = connectUrl.getPath();
    } catch (MalformedURLException mue) {
      LOG.error("Malformed connect string URL: " + connectString
          + "; reason is " + mue.toString());
    }

    if (null == databaseName) {
      throw new ImportError("Could not determine database name");
    }

    // database name was found from the 'path' part of the URL; trim leading '/'
    while (databaseName.startsWith("/")) {
      databaseName = databaseName.substring(1);
    }

    LOG.info("Performing import of table " + tableName + " from database " + databaseName);

    args.add(MYSQL_DUMP_CMD); // requires that this is on the path.
    args.add("--skip-opt");
    args.add("--compact");
    args.add("--no-create-db");
    args.add("--no-create-info");

    String username = options.getUsername();
    if (null != username) {
      args.add("--user=" + username);
    }

    String password = options.getPassword();
    if (null != password) {
      // TODO(aaron): This is really insecure.
      args.add("--password=" + password);
    }
    
    String whereClause = options.getWhereClause();
    if (null != whereClause) {
      // Don't use the --where="<whereClause>" version because spaces in it can confuse
      // Java, and adding in surrounding quotes confuses Java as well.
      args.add("-w");
      args.add(whereClause);
    }

    args.add("--quick"); // no buffering
    // TODO(aaron): Add a flag to allow --lock-tables instead for MyISAM data
    args.add("--single-transaction"); 

    args.add(databaseName);
    args.add(tableName);
    
    Process p = null;
    try {
      // begin the import in an external process.
      LOG.debug("Starting mysqldump with arguments:");
      for (String arg : args) {
        LOG.debug("  " + arg);
      }
      
      p = Runtime.getRuntime().exec(args.toArray(new String[0]));
      
      // read from the pipe, into HDFS.
      InputStream is = p.getInputStream();
      OutputStream os = null;

      BufferedReader r = null;
      BufferedWriter w = null;

      try {
        r = new BufferedReader(new InputStreamReader(is));

        // create the paths/files in HDFS 
        FileSystem fs = FileSystem.get(conf);
        String warehouseDir = options.getWarehouseDir();
        Path destDir = null;
        if (null != warehouseDir) {
          destDir = new Path(new Path(warehouseDir), tableName);
        } else {
          destDir = new Path(tableName);
        }

        LOG.debug("Writing to filesystem: " + conf.get("fs.default.name"));
        LOG.debug("Creating destination directory " + destDir);
        fs.mkdirs(destDir);
        Path destFile = new Path(destDir, "data-00000");
        LOG.debug("Opening output file: " + destFile);
        if (fs.exists(destFile)) {
          Path canonicalDest = destFile.makeQualified(fs);
          throw new IOException("Destination file " + canonicalDest + " already exists");
        }

        os = fs.create(destFile);
        w = new BufferedWriter(new OutputStreamWriter(os));

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
          w.write(inLine, preambleLen, inLine.length() - 2 - preambleLen);
          w.newLine();
        }
      } finally {
        LOG.info("Transfer loop complete.");
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
    } finally {
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

      if (0 != result) {
        throw new IOException("mysqldump terminated with status "
            + Integer.toString(result));
      }
    }
  }
}

