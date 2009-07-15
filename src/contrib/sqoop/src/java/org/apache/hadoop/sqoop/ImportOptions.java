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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ToolRunner;

/**
 * Command-line arguments used by Sqoop
 */
public class ImportOptions {

  public static final Log LOG = LogFactory.getLog(ImportOptions.class.getName());

  /**
   * Thrown when invalid cmdline options are given
   */
  @SuppressWarnings("serial")
  public static class InvalidOptionsException extends Exception {

    private String message;

    public InvalidOptionsException(final String msg) {
      this.message = msg;
    }

    public String getMessage() {
      return message;
    }

    public String toString() {
      return getMessage();
    }
  }

  // control-flow selector based on command-line switches.
  public enum ControlAction {
    ListDatabases,  // list available databases and exit.
    ListTables,     // list available tables and exit.
    GenerateOnly,   // generate ORM code but do not import.
    FullImport,     // generate code (as needed) and import.
    DebugExec       // just execute a single sql command and print its results.
  }

  // selects in-HDFS destination file format
  public enum FileLayout {
    TextFile,
    SequenceFile
  }


  // TODO(aaron): Adding something here? Add a getter, a cmdline switch, and a properties file
  // entry in loadFromProperties(). Add a default value in initDefaults() if you need one.
  // Make sure you add the stub to the testdata/sqoop.properties.template file.
  private String connectString;
  private String tableName;
  private String [] columns;
  private boolean allTables;
  private String username;
  private String password;
  private String codeOutputDir;
  private String jarOutputDir;
  private ControlAction action;
  private String hadoopHome;
  private String orderByCol;
  private String whereClause;
  private String debugSqlCmd;
  private String driverClassName;
  private String warehouseDir;
  private FileLayout layout;
  private boolean local; // if true and conn is mysql, use mysqldump.
  private String tmpDir; // where temp data goes; usually /tmp
  private String hiveHome;
  private boolean hiveImport;
  private String packageName; // package to prepend to auto-named classes.
  private String className; // package+class to apply to individual table import.

  private static final String DEFAULT_CONFIG_FILE = "sqoop.properties";

  public ImportOptions() {
    initDefaults();
  }

  /**
   * Alternate ImportOptions interface used mostly for unit testing
   * @param connect JDBC connect string to use
   * @param database Database to read
   * @param table Table to read
   */
  public ImportOptions(final String connect, final String table) {
    initDefaults();

    this.connectString = connect;
    this.tableName = table;
  }

  private void loadFromProperties() {
    File configFile = new File(DEFAULT_CONFIG_FILE);
    if (!configFile.canRead()) {
      return; //can't do this.
    }

    Properties props = new Properties();
    InputStream istream = null;
    try {
      LOG.info("Loading properties from " + configFile.getAbsolutePath());
      istream = new FileInputStream(configFile);
      props.load(istream);

      this.hadoopHome = props.getProperty("hadoop.home", this.hadoopHome);
      this.codeOutputDir = props.getProperty("out.dir", this.codeOutputDir);
      this.jarOutputDir = props.getProperty("bin.dir", this.jarOutputDir);
      this.username = props.getProperty("db.username", this.username);
      this.password = props.getProperty("db.password", this.password);
      this.tableName = props.getProperty("db.table", this.tableName);
      this.connectString = props.getProperty("db.connect.url", this.connectString);
      this.orderByCol = props.getProperty("db.sort.column", this.orderByCol);
      this.whereClause = props.getProperty("db.where.clause", this.whereClause);
      this.driverClassName = props.getProperty("jdbc.driver", this.driverClassName);
      this.warehouseDir = props.getProperty("hdfs.warehouse.dir", this.warehouseDir);
      this.hiveHome = props.getProperty("hive.home", this.hiveHome);
      this.className = props.getProperty("java.classname", this.className);
      this.packageName = props.getProperty("java.packagename", this.packageName);

      String localImport = props.getProperty("local.import",
          Boolean.toString(this.local)).toLowerCase();
      this.local = "true".equals(localImport) || "yes".equals(localImport)
          || "1".equals(localImport);

      String hiveImportStr = props.getProperty("hive.import",
          Boolean.toString(this.hiveImport)).toLowerCase();
      this.hiveImport = "true".equals(hiveImportStr) || "yes".equals(hiveImportStr)
          || "1".equals(hiveImportStr);
    } catch (IOException ioe) {
      LOG.error("Could not read properties file " + DEFAULT_CONFIG_FILE + ": " + ioe.toString());
    } finally {
      if (null != istream) {
        try {
          istream.close();
        } catch (IOException ioe) {
          // ignore this; we're closing.
        }
      }
    }
  }

  /**
   * @return the temp directory to use; this is guaranteed to end with
   * the file separator character (e.g., '/')
   */
  public String getTempDir() {
    return this.tmpDir;
  }

  private void initDefaults() {
    // first, set the true defaults if nothing else happens.
    // default action is to run the full pipeline.
    this.action = ControlAction.FullImport;
    this.hadoopHome = System.getenv("HADOOP_HOME");

    // Set this with $HIVE_HOME, but -Dhive.home can override.
    this.hiveHome = System.getenv("HIVE_HOME");
    this.hiveHome = System.getProperty("hive.home", this.hiveHome);

    // Set this to cwd, but -Dsqoop.src.dir can override.
    this.codeOutputDir = System.getProperty("sqoop.src.dir", ".");

    String myTmpDir = System.getProperty("test.build.data", "/tmp/");
    if (!myTmpDir.endsWith(File.separator)) {
      myTmpDir = myTmpDir + File.separator;
    }

    this.tmpDir = myTmpDir;
    this.jarOutputDir = tmpDir + "sqoop/compile";
    this.layout = FileLayout.TextFile;

    loadFromProperties();
  }

  /**
   * Allow the user to enter his password on the console without printing characters.
   * @return the password as a string
   */
  private String securePasswordEntry() {
    return new String(System.console().readPassword("Enter password: "));
  }

  /**
   * Print usage strings for the program's arguments.
   */
  public static void printUsage() {
    System.out.println("Usage: hadoop sqoop.jar org.apache.hadoop.sqoop.Sqoop (options)");
    System.out.println("");
    System.out.println("Database connection options:");
    System.out.println("--connect (jdbc-uri)         Specify JDBC connect string");
    System.out.println("--driver (class-name)        Manually specify JDBC driver class to use");
    System.out.println("--username (username)        Set authentication username");
    System.out.println("--password (password)        Set authentication password");
    System.out.println("-P                           Read password from console");
    System.out.println("--local                      Use local import fast path (mysql only)");
    System.out.println("");
    System.out.println("Import control options:");
    System.out.println("--table (tablename)          Table to read");
    System.out.println("--columns (col,col,col...)   Columns to export from table");
    System.out.println("--order-by (column-name)     Column of the table used to order results");
    System.out.println("--where (where clause)       Where clause to use during export");
    System.out.println("--hadoop-home (dir)          Override $HADOOP_HOME");
    System.out.println("--hive-home (dir)            Override $HIVE_HOME");
    System.out.println("--warehouse-dir (dir)        HDFS path for table destination");
    System.out.println("--as-sequencefile            Imports data to SequenceFiles");
    System.out.println("--as-textfile                Imports data as plain text (default)");
    System.out.println("--all-tables                 Import all tables in database");
    System.out.println("                             (Ignores --table, --columns and --order-by)");
    System.out.println("--hive-import                If set, then import the table into Hive");
    System.out.println("");
    System.out.println("Code generation options:");
    System.out.println("--outdir (dir)               Output directory for generated code");
    System.out.println("--bindir (dir)               Output directory for compiled objects");
    System.out.println("--generate-only              Stop after code generation; do not import");
    System.out.println("--package-name (name)        Put auto-generated classes in this package");
    System.out.println("--class-name (name)          When generating one class, use this name.");
    System.out.println("                             This overrides --package-name.");
    System.out.println("");
    System.out.println("Additional commands:");
    System.out.println("--list-tables                List tables in database and exit");
    System.out.println("--list-databases             List all databases available and exit");
    System.out.println("--debug-sql (statement)      Execute 'statement' in SQL and exit");
    System.out.println("");
    System.out.println("Generic Hadoop command-line options:");
    ToolRunner.printGenericCommandUsage(System.out);
    System.out.println("");
    System.out.println("At minimum, you must specify --connect "
        + "and either --table or --all-tables.");
    System.out.println("Alternatively, you can specify --generate-only or one of the additional");
    System.out.println("commands.");
  }

  /**
   * Read args from the command-line into member fields.
   * @throws Exception if there's a problem parsing arguments.
   */
  public void parse(String [] args) throws InvalidOptionsException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Parsing sqoop arguments:");
      for (String arg : args) {
        LOG.debug("  " + arg);
      }
    }

    int i = 0;
    try {
      for (i = 0; i < args.length; i++) {
        if (args[i].equals("--connect")) {
          this.connectString = args[++i];
        } else if (args[i].equals("--driver")) {
          this.driverClassName = args[++i];
        } else if (args[i].equals("--table")) {
          this.tableName = args[++i];
        } else if (args[i].equals("--columns")) {
          String columnString = args[++i];
          this.columns = columnString.split(",");
        } else if (args[i].equals("--order-by")) {
          this.orderByCol = args[++i];
        } else if (args[i].equals("--where")) {
          this.whereClause = args[++i];
        } else if (args[i].equals("--list-tables")) {
          this.action = ControlAction.ListTables;
        } else if (args[i].equals("--all-tables")) {
          this.allTables = true;
        } else if (args[i].equals("--local")) {
          this.local = true;
        } else if (args[i].equals("--username")) {
          this.username = args[++i];
          if (null == this.password) {
            // Set password to empty if the username is set first,
            // to ensure that they're either both null or neither.
            this.password = "";
          }
        } else if (args[i].equals("--password")) {
          LOG.warn("Setting your password on the command-line is insecure. Consider using -P instead.");
          this.password = args[++i];
        } else if (args[i].equals("-P")) {
          this.password = securePasswordEntry();
        } else if (args[i].equals("--hadoop-home")) {
          this.hadoopHome = args[++i];
        } else if (args[i].equals("--hive-home")) {
          this.hiveHome = args[++i];
        } else if (args[i].equals("--hive-import")) {
          this.hiveImport = true;
        } else if (args[i].equals("--outdir")) {
          this.codeOutputDir = args[++i];
        } else if (args[i].equals("--as-sequencefile")) {
          this.layout = FileLayout.SequenceFile;
        } else if (args[i].equals("--as-textfile")) {
          this.layout = FileLayout.TextFile;
        } else if (args[i].equals("--bindir")) {
          this.jarOutputDir = args[++i];
        } else if (args[i].equals("--warehouse-dir")) {
          this.warehouseDir = args[++i];
        } else if (args[i].equals("--package-name")) {
          this.packageName = args[++i];
        } else if (args[i].equals("--class-name")) {
          this.className = args[++i];
        } else if (args[i].equals("--list-databases")) {
          this.action = ControlAction.ListDatabases;
        } else if (args[i].equals("--generate-only")) {
          this.action = ControlAction.GenerateOnly;
        } else if (args[i].equals("--debug-sql")) {
          this.action = ControlAction.DebugExec;
          // read the entire remainder of the commandline into the debug sql statement.
          if (null == this.debugSqlCmd) {
            this.debugSqlCmd = "";
          }
          for (i++; i < args.length; i++) {
            this.debugSqlCmd = this.debugSqlCmd + args[i] + " ";
          }
        } else if (args[i].equals("--help")) {
          printUsage();
          throw new InvalidOptionsException("");
        } else {
          throw new InvalidOptionsException("Invalid argument: " + args[i] + ".\n"
              + "Try --help for usage.");
        }
      }
    } catch (ArrayIndexOutOfBoundsException oob) {
      throw new InvalidOptionsException("Error: " + args[--i] + " expected argument.\n"
          + "Try --help for usage.");
    }
  }

  private static final String HELP_STR = "\nTry --help for usage instructions.";

  /**
   * Validates options and ensures that any required options are
   * present and that any mutually-exclusive options are not selected.
   * @throws Exception if there's a problem.
   */
  public void validate() throws InvalidOptionsException {
    if (this.allTables && this.columns != null) {
      // If we're reading all tables in a database, can't filter column names.
      throw new InvalidOptionsException("--columns and --all-tables are incompatible options."
          + HELP_STR);
    } else if (this.allTables && this.orderByCol != null) {
      // If we're reading all tables in a database, can't set pkey
      throw new InvalidOptionsException("--order-by and --all-tables are incompatible options."
          + HELP_STR);
    } else if (this.allTables && this.className != null) {
      // If we're reading all tables, can't set individual class name
      throw new InvalidOptionsException("--class-name and --all-tables are incompatible options."
          + HELP_STR);
    } else if (this.connectString == null) {
      throw new InvalidOptionsException("Error: Required argument --connect is missing."
          + HELP_STR);
    } else if (this.className != null && this.packageName != null) {
      throw new InvalidOptionsException(
          "--class-name overrides --package-name. You cannot use both." + HELP_STR);
    }
  }

  /** get the temporary directory; guaranteed to end in File.separator
   * (e.g., '/')
   */
  public String getTmpDir() {
    return tmpDir;
  }

  public String getConnectString() {
    return connectString;
  }

  public String getTableName() {
    return tableName;
  }

  public String[] getColumns() {
    if (null == columns) {
      return null;
    } else {
      return Arrays.copyOf(columns, columns.length);
    }
  }

  public String getOrderByCol() {
    return orderByCol;
  }
  
  public String getWhereClause() {
    return whereClause;
  }

  public ControlAction getAction() {
    return action;
  }

  public boolean isAllTables() {
    return allTables;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public boolean isLocal() {
    return local;
  }

  /**
   * @return the user-specified absolute class name for the table
   */
  public String getClassName() {
    return className;
  }

  /**
   * @return the user-specified package to prepend to table names via --package-name.
   */
  public String getPackageName() {
    return packageName;
  }

  public String getHiveHome() {
    return hiveHome;
  }

  /** @return true if we should import the table into Hive */
  public boolean doHiveImport() {
    return hiveImport;
  }

  /**
   * @return location where .java files go; guaranteed to end with '/'
   */
  public String getCodeOutputDir() {
    if (codeOutputDir.endsWith(File.separator)) {
      return codeOutputDir;
    } else {
      return codeOutputDir + File.separator;
    }
  }

  /**
   * @return location where .jar and .class files go; guaranteed to end with '/'
   */
  public String getJarOutputDir() {
    if (jarOutputDir.endsWith(File.separator)) {
      return jarOutputDir;
    } else {
      return jarOutputDir + File.separator;
    }
  }

  /**
   * Return the value of $HADOOP_HOME
   * @return $HADOOP_HOME, or null if it's not set.
   */
  public String getHadoopHome() {
    return hadoopHome;
  }

  /**
   * @return a sql command to execute and exit with.
   */
  public String getDebugSqlCmd() {
    return debugSqlCmd;
  }

  /**
   * @return The JDBC driver class name specified with --driver
   */
  public String getDriverClassName() {
    return driverClassName;
  }

  /**
   * @return the base destination path for table uploads.
   */
  public String getWarehouseDir() {
    return warehouseDir;
  }

  /**
   * @return the destination file format
   */
  public FileLayout getFileLayout() {
    return this.layout;
  }

  public void setUsername(String name) {
    this.username = name;
  }

  public void setPassword(String pass) {
    this.password = pass;
  }
}
