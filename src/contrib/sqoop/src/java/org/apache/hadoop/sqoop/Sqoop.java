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

package org.apache.hadoop.sqoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.sqoop.hive.HiveImport;
import org.apache.hadoop.sqoop.manager.ConnManager;
import org.apache.hadoop.sqoop.manager.ExportJobContext;
import org.apache.hadoop.sqoop.manager.ImportJobContext;
import org.apache.hadoop.sqoop.orm.ClassWriter;
import org.apache.hadoop.sqoop.orm.CompilationManager;
import org.apache.hadoop.sqoop.util.ExportException;
import org.apache.hadoop.sqoop.util.ImportException;

/**
 * Main entry-point for Sqoop
 * Usage: hadoop jar (this_jar_name) org.apache.hadoop.sqoop.Sqoop (options)
 * See the SqoopOptions class for options.
 */
public class Sqoop extends Configured implements Tool {

  public static final Log LOG = LogFactory.getLog(Sqoop.class.getName());

  /** If this System property is set, always throw an exception, do not just
      exit with status 1.
    */
  public static final String SQOOP_RETHROW_PROPERTY = "sqoop.throwOnError";

  static {
    Configuration.addDefaultResource("sqoop-default.xml");
    Configuration.addDefaultResource("sqoop-site.xml");
  }

  private SqoopOptions options;
  private ConnManager manager;
  private HiveImport hiveImport;
  private List<String> generatedJarFiles;

  public Sqoop() {
    generatedJarFiles = new ArrayList<String>();
  }

  public SqoopOptions getOptions() {
    return options;
  }

  /**
   * @return a list of jar files generated as part of this im/export process
   */
  public List<String> getGeneratedJarFiles() {
    ArrayList<String> out = new ArrayList<String>(generatedJarFiles);
    return out;
  }

  /**
   * Generate the .class and .jar files
   * @return the filename of the emitted jar file.
   * @throws IOException
   */
  private String generateORM(String tableName) throws IOException {
    String existingJar = options.getExistingJarName();
    if (existingJar != null) {
      // The user has pre-specified a jar and class to use. Don't generate.
      LOG.info("Using existing jar: " + existingJar);
      return existingJar;
    }

    LOG.info("Beginning code generation");
    CompilationManager compileMgr = new CompilationManager(options);
    ClassWriter classWriter = new ClassWriter(options, manager, tableName, compileMgr);
    classWriter.generate();
    compileMgr.compile();
    compileMgr.jar();
    String jarFile = compileMgr.getJarFilename();
    this.generatedJarFiles.add(jarFile);
    return jarFile;
  }

  private void importTable(String tableName) throws IOException, ImportException {
    String jarFile = null;

    // Generate the ORM code for the tables.
    jarFile = generateORM(tableName);

    if (options.getAction() == SqoopOptions.ControlAction.FullImport) {
      // Proceed onward to do the import.
      ImportJobContext context = new ImportJobContext(tableName, jarFile, options);
      manager.importTable(context);

      // If the user wants this table to be in Hive, perform that post-load.
      if (options.doHiveImport()) {
        hiveImport.importTable(tableName);
      }
    }
  }

  private void exportTable(String tableName) throws ExportException, IOException {
    String jarFile = null;

    // Generate the ORM code for the tables.
    jarFile = generateORM(tableName);

    ExportJobContext context = new ExportJobContext(tableName, jarFile, options);
    manager.exportTable(context);
  }

  /**
   * Actual main entry-point for the program
   */
  public int run(String [] args) {
    options = new SqoopOptions();
    options.setConf(getConf());
    try {
      options.parse(args);
      options.validate();
    } catch (SqoopOptions.InvalidOptionsException e) {
      // display the error msg
      System.err.println(e.getMessage());
      return 1; // exit on exception here
    }

    // Get the connection to the database
    try {
      manager = new ConnFactory(getConf()).getManager(options);
    } catch (Exception e) {
      LOG.error("Got error creating database manager: " + e.toString());
      if (System.getProperty(SQOOP_RETHROW_PROPERTY) != null) {
        throw new RuntimeException(e);
      } else {
        return 1;
      }
    }

    if (options.doHiveImport()) {
      hiveImport = new HiveImport(options, manager, getConf());
    }

    SqoopOptions.ControlAction action = options.getAction();
    if (action == SqoopOptions.ControlAction.ListTables) {
      String [] tables = manager.listTables();
      if (null == tables) {
        System.err.println("Could not retrieve tables list from server");
        LOG.error("manager.listTables() returned null");
        return 1;
      } else {
        for (String tbl : tables) {
          System.out.println(tbl);
        }
      }
    } else if (action == SqoopOptions.ControlAction.ListDatabases) {
      String [] databases = manager.listDatabases();
      if (null == databases) {
        System.err.println("Could not retrieve database list from server");
        LOG.error("manager.listDatabases() returned null");
        return 1;
      } else {
        for (String db : databases) {
          System.out.println(db);
        }
      }
    } else if (action == SqoopOptions.ControlAction.DebugExec) {
      // just run a SQL statement for debugging purposes.
      manager.execAndPrint(options.getDebugSqlCmd());
      return 0;
    } else if (action == SqoopOptions.ControlAction.Export) {
      // Export a table.
      try {
        exportTable(options.getTableName());
      } catch (IOException ioe) {
        LOG.error("Encountered IOException running export job: " + ioe.toString());
        if (System.getProperty(SQOOP_RETHROW_PROPERTY) != null) {
          throw new RuntimeException(ioe);
        } else {
          return 1;
        }
      } catch (ExportException ee) {
        LOG.error("Error during export: " + ee.toString());
        if (System.getProperty(SQOOP_RETHROW_PROPERTY) != null) {
          throw new RuntimeException(ee);
        } else {
          return 1;
        }
      }
    } else {
      // This is either FullImport or GenerateOnly.

      try {
        if (options.isAllTables()) {
          String [] tables = manager.listTables();
          if (null == tables) {
            System.err.println("Could not retrieve tables list from server");
            LOG.error("manager.listTables() returned null");
            return 1;
          } else {
            for (String tableName : tables) {
              importTable(tableName);
            }
          }
        } else {
          // just import a single table the user specified.
          importTable(options.getTableName());
        }
      } catch (IOException ioe) {
        LOG.error("Encountered IOException running import job: " + ioe.toString());
        if (System.getProperty(SQOOP_RETHROW_PROPERTY) != null) {
          throw new RuntimeException(ioe);
        } else {
          return 1;
        }
      } catch (ImportException ie) {
        LOG.error("Error during import: " + ie.toString());
        if (System.getProperty(SQOOP_RETHROW_PROPERTY) != null) {
          throw new RuntimeException(ie);
        } else {
          return 1;
        }
      }
    }

    return 0;
  }

  public static void main(String [] args) {
    int ret;
    try {
      Sqoop importer = new Sqoop();
      ret = ToolRunner.run(importer, args);
    } catch (Exception e) {
      LOG.error("Got exception running Sqoop: " + e.toString());
      e.printStackTrace();
      ret = 1;
    }

    System.exit(ret);
  }
}
