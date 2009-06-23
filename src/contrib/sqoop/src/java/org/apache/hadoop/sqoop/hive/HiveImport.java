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

package org.apache.hadoop.sqoop.hive;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.sqoop.ImportOptions;
import org.apache.hadoop.sqoop.manager.ConnManager;
import org.apache.hadoop.sqoop.util.Executor;
import org.apache.hadoop.sqoop.util.LoggingStreamHandlerFactory;

/**
 * Utility to import a table into the Hive metastore. Manages the connection
 * to Hive itself as well as orchestrating the use of the other classes in this
 * package.
 */
public class HiveImport {

  public static final Log LOG = LogFactory.getLog(HiveImport.class.getName());

  private ImportOptions options;
  private ConnManager connManager;
  private Configuration configuration;

  public HiveImport(final ImportOptions opts, final ConnManager connMgr, final Configuration conf) {
    this.options = opts;
    this.connManager = connMgr;
    this.configuration = conf;
  }


  /** 
   * @return the filename of the hive executable to run to do the import
   */
  private String getHiveBinPath() {
    // If the user has $HIVE_HOME set, then use $HIVE_HOME/bin/hive if it
    // exists.
    // Fall back to just plain 'hive' and hope it's in the path.

    String hiveHome = options.getHiveHome();
    if (null == hiveHome) {
      return "hive";
    }

    Path p = new Path(hiveHome);
    p = new Path(p, "bin");
    p = new Path(p, "hive");
    String hiveBinStr = p.toString();
    if (new File(hiveBinStr).exists()) {
      return hiveBinStr;
    } else {
      return "hive";
    }
  }

  /**
   * If we used a MapReduce-based upload of the data, remove the _logs dir
   * from where we put it, before running Hive LOAD DATA INPATH
   */
  private void removeTempLogs(String tableName) throws IOException {
    FileSystem fs = FileSystem.get(configuration);
    String warehouseDir = options.getWarehouseDir();
    Path tablePath; 
    if (warehouseDir != null) {
      tablePath = new Path(new Path(warehouseDir), tableName);
    } else {
      tablePath = new Path(tableName);
    }

    Path logsPath = new Path(tablePath, "_logs");
    if (fs.exists(logsPath)) {
      LOG.info("Removing temporary files from import process: " + logsPath);
      if (!fs.delete(logsPath, true)) {
        LOG.warn("Could not delete temporary files; continuing with import, but it may fail.");
      }
    }
  }

  public void importTable(String tableName) throws IOException {
    removeTempLogs(tableName);

    LOG.info("Loading uploaded data into Hive");

    // For testing purposes against our mock hive implementation, 
    // if the sysproperty "expected.script" is set, we set the EXPECTED_SCRIPT
    // environment variable for the child hive process. We also disable
    // timestamp comments so that we have deterministic table creation scripts.
    String expectedScript = System.getProperty("expected.script");
    List<String> env = Executor.getCurEnvpStrings();
    boolean debugMode = expectedScript != null;
    if (debugMode) {
      env.add("EXPECTED_SCRIPT=" + expectedScript);
      env.add("TMPDIR=" + options.getTempDir());
    }

    // generate the HQL statements to run.
    TableDefWriter tableWriter = new TableDefWriter(options, connManager, tableName,
        configuration, !debugMode);
    String createTableStr = tableWriter.getCreateTableStmt() + ";\n";
    String loadDataStmtStr = tableWriter.getLoadDataStmt() + ";\n";

    // write them to a script file.
    File tempFile = File.createTempFile("hive-script-",".txt", new File(options.getTempDir()));
    try {
      String tmpFilename = tempFile.toString();
      BufferedWriter w = null;
      try {
        FileOutputStream fos = new FileOutputStream(tempFile);
        w = new BufferedWriter(new OutputStreamWriter(fos));
        w.write(createTableStr, 0, createTableStr.length());
        w.write(loadDataStmtStr, 0, loadDataStmtStr.length());
      } catch (IOException ioe) {
        LOG.error("Error writing Hive load-in script: " + ioe.toString());
        ioe.printStackTrace();
        throw ioe;
      } finally {
        if (null != w) {
          try {
            w.close();
          } catch (IOException ioe) {
            LOG.warn("IOException closing stream to Hive script: " + ioe.toString());
          }
        }
      }

      // run Hive on the script and note the return code.
      String hiveExec = getHiveBinPath();
      ArrayList<String> args = new ArrayList<String>();
      args.add(hiveExec);
      args.add("-f");
      args.add(tmpFilename);

      LoggingStreamHandlerFactory lshf = new LoggingStreamHandlerFactory(LOG);
      int ret = Executor.exec(args.toArray(new String[0]), env.toArray(new String[0]), lshf, lshf);
      if (0 != ret) {
        throw new IOException("Hive exited with status " + ret);
      }

      LOG.info("Hive import complete.");
    } finally {
      if (!tempFile.delete()) {
        LOG.warn("Could not remove temporary file: " + tempFile.toString());
        // try to delete the file later.
        tempFile.deleteOnExit();
      }
    }
  }
}

