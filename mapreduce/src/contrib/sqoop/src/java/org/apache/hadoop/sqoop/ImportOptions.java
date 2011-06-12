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
  private String splitByCol;
  private String whereClause;
  private String debugSqlCmd;
  private String driverClassName;
  private String warehouseDir;
  private FileLayout layout;
  private boolean direct; // if true and conn is mysql, use mysqldump.
  private String tmpDir; // where temp data goes; usually /tmp
  private String hiveHome;
  private boolean hiveImport;
  private String packageName; // package to prepend to auto-named classes.
  private String className; // package+class to apply to individual table import.
  private int numMappers;

  private char inputFieldDelim;
  private char inputRecordDelim;
  private char inputEnclosedBy;
  private char inputEscapedBy;
  private boolean inputMustBeEnclosed;

  private char outputFieldDelim;
  private char outputRecordDelim;
  private char outputEnclosedBy;
  private char outputEscapedBy;
  private boolean outputMustBeEnclosed;

  private boolean areDelimsManuallySet;

  public static final int DEFAULT_NUM_MAPPERS = 4;

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
      this.splitByCol = props.getProperty("db.split.column", this.splitByCol);
      this.whereClause = props.getProperty("db.where.clause", this.whereClause);
      this.driverClassName = props.getProperty("jdbc.driver", this.driverClassName);
      this.warehouseDir = props.getProperty("hdfs.warehouse.dir", this.warehouseDir);
      this.hiveHome = props.getProperty("hive.home", this.hiveHome);
      this.className = props.getProperty("java.classname", this.className);
      this.packageName = props.getProperty("java.packagename", this.packageName);

      String directImport = props.getProperty("direct.import",
          Boolean.toString(this.direct)).toLowerCase();
      this.direct = "true".equals(directImport) || "yes".equals(directImport)
          || "1".equals(directImport);

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

    this.inputFieldDelim = '\000';
    this.inputRecordDelim = '\000';
    this.inputEnclosedBy = '\000';
    this.inputEscapedBy = '\000';
    this.inputMustBeEnclosed = false;

    this.outputFieldDelim = ',';
    this.outputRecordDelim = '\n';
    this.outputEnclosedBy = '\000';
    this.outputEscapedBy = '\000';
    this.outputMustBeEnclosed = false;

    this.areDelimsManuallySet = false;

    this.numMappers = DEFAULT_NUM_MAPPERS;

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
    System.out.println("--direct                     Use direct import fast path (mysql only)");
    System.out.println("");
    System.out.println("Import control options:");
    System.out.println("--table (tablename)          Table to read");
    System.out.println("--columns (col,col,col...)   Columns to export from table");
    System.out.println("--split-by (column-name)     Column of the table used to split work units");
    System.out.println("--where (where clause)       Where clause to use during export");
    System.out.println("--hadoop-home (dir)          Override $HADOOP_HOME");
    System.out.println("--hive-home (dir)            Override $HIVE_HOME");
    System.out.println("--warehouse-dir (dir)        HDFS path for table destination");
    System.out.println("--as-sequencefile            Imports data to SequenceFiles");
    System.out.println("--as-textfile                Imports data as plain text (default)");
    System.out.println("--all-tables                 Import all tables in database");
    System.out.println("                             (Ignores --table, --columns and --split-by)");
    System.out.println("--hive-import                If set, then import the table into Hive.");
    System.out.println("                    (Uses Hive's default delimiters if none are set.)");
    System.out.println("-m, --num-mappers (n)        Use 'n' map tasks to import in parallel");
    System.out.println("");
    System.out.println("Output line formatting options:");
    System.out.println("--fields-terminated-by (char)    Sets the field separator character");
    System.out.println("--lines-terminated-by (char)     Sets the end-of-line character");
    System.out.println("--optionally-enclosed-by (char)  Sets a field enclosing character");
    System.out.println("--enclosed-by (char)             Sets a required field enclosing char");
    System.out.println("--escaped-by (char)              Sets the escape character");
    System.out.println("--mysql-delimiters               Uses MySQL's default delimiter set");
    System.out.println("  fields: ,  lines: \\n  escaped-by: \\  optionally-enclosed-by: '");
    System.out.println("");
    System.out.println("Input parsing options:");
    System.out.println("--input-fields-terminated-by (char)    Sets the input field separator");
    System.out.println("--input-lines-terminated-by (char)     Sets the input end-of-line char");
    System.out.println("--input-optionally-enclosed-by (char)  Sets a field enclosing character");
    System.out.println("--input-enclosed-by (char)             Sets a required field encloser");
    System.out.println("--input-escaped-by (char)              Sets the input escape character");
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
   * Given a string containing a single character or an escape sequence representing
   * a char, return that char itself.
   *
   * Normal literal characters return themselves: "x" -&gt; 'x', etc.
   * Strings containing a '\' followed by one of t, r, n, or b escape to the usual
   * character as seen in Java: "\n" -&gt; (newline), etc.
   *
   * Strings like "\0ooo" return the character specified by the octal sequence 'ooo'
   * Strings like "\0xhhh" or "\0Xhhh" return the character specified by the hex sequence 'hhh'
   */
  static char toChar(String charish) throws InvalidOptionsException {
    if (null == charish) {
      throw new InvalidOptionsException("Character argument expected." 
          + "\nTry --help for usage instructions.");
    } else if (charish.startsWith("\\0x") || charish.startsWith("\\0X")) {
      if (charish.length() == 3) {
        throw new InvalidOptionsException("Base-16 value expected for character argument."
          + "\nTry --help for usage instructions.");
      } else {
        String valStr = charish.substring(3);
        int val = Integer.parseInt(valStr, 16);
        return (char) val;
      }
    } else if (charish.startsWith("\\0")) {
      if (charish.equals("\\0")) {
        // it's just '\0', which we can take as shorthand for nul.
        return '\000';
      } else {
        // it's an octal value.
        String valStr = charish.substring(2);
        int val = Integer.parseInt(valStr, 8);
        return (char) val;
      }
    } else if (charish.startsWith("\\")) {
      if (charish.length() == 1) {
        // it's just a '\'. Keep it literal.
        return '\\';
      } else if (charish.length() > 2) {
        // we don't have any 3+ char escape strings. 
        throw new InvalidOptionsException("Cannot understand character argument: " + charish
            + "\nTry --help for usage instructions.");
      } else {
        // this is some sort of normal 1-character escape sequence.
        char escapeWhat = charish.charAt(1);
        switch(escapeWhat) {
        case 'b':
          return '\b';
        case 'n':
          return '\n';
        case 'r':
          return '\r';
        case 't':
          return '\t';
        case '\"':
          return '\"';
        case '\'':
          return '\'';
        case '\\':
          return '\\';
        default:
          throw new InvalidOptionsException("Cannot understand character argument: " + charish
              + "\nTry --help for usage instructions.");
        }
      }
    } else if (charish.length() == 0) {
      throw new InvalidOptionsException("Character argument expected." 
          + "\nTry --help for usage instructions.");
    } else {
      // it's a normal character.
      if (charish.length() > 1) {
        LOG.warn("Character argument " + charish + " has multiple characters; "
            + "only the first will be used.");
      }

      return charish.charAt(0);
    }
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
        } else if (args[i].equals("--split-by")) {
          this.splitByCol = args[++i];
        } else if (args[i].equals("--where")) {
          this.whereClause = args[++i];
        } else if (args[i].equals("--list-tables")) {
          this.action = ControlAction.ListTables;
        } else if (args[i].equals("--all-tables")) {
          this.allTables = true;
        } else if (args[i].equals("--local")) {
          // TODO(aaron): Remove this after suitable deprecation time period.
          LOG.warn("--local is deprecated; use --direct instead.");
          this.direct = true;
        } else if (args[i].equals("--direct")) {
          this.direct = true;
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
        } else if (args[i].equals("--num-mappers") || args[i].equals("-m")) {
          String numMappersStr = args[++i];
          try {
            this.numMappers = Integer.valueOf(numMappersStr);
          } catch (NumberFormatException nfe) {
            throw new InvalidOptionsException("Invalid argument; expected "
                + args[i - 1] + " (number).");
          }
        } else if (args[i].equals("--fields-terminated-by")) {
          this.outputFieldDelim = ImportOptions.toChar(args[++i]);
          this.areDelimsManuallySet = true;
        } else if (args[i].equals("--lines-terminated-by")) {
          this.outputRecordDelim = ImportOptions.toChar(args[++i]);
          this.areDelimsManuallySet = true;
        } else if (args[i].equals("--optionally-enclosed-by")) {
          this.outputEnclosedBy = ImportOptions.toChar(args[++i]);
          this.outputMustBeEnclosed = false;
          this.areDelimsManuallySet = true;
        } else if (args[i].equals("--enclosed-by")) {
          this.outputEnclosedBy = ImportOptions.toChar(args[++i]);
          this.outputMustBeEnclosed = true;
          this.areDelimsManuallySet = true;
        } else if (args[i].equals("--escaped-by")) {
          this.outputEscapedBy = ImportOptions.toChar(args[++i]);
          this.areDelimsManuallySet = true;
        } else if (args[i].equals("--mysql-delimiters")) {
          this.outputFieldDelim = ',';
          this.outputRecordDelim = '\n';
          this.outputEnclosedBy = '\'';
          this.outputEscapedBy = '\\';
          this.outputMustBeEnclosed = false;
          this.areDelimsManuallySet = true;
        } else if (args[i].equals("--input-fields-terminated-by")) {
          this.inputFieldDelim = ImportOptions.toChar(args[++i]);
        } else if (args[i].equals("--input-lines-terminated-by")) {
          this.inputRecordDelim = ImportOptions.toChar(args[++i]);
        } else if (args[i].equals("--input-optionally-enclosed-by")) {
          this.inputEnclosedBy = ImportOptions.toChar(args[++i]);
          this.inputMustBeEnclosed = false;
        } else if (args[i].equals("--input-enclosed-by")) {
          this.inputEnclosedBy = ImportOptions.toChar(args[++i]);
          this.inputMustBeEnclosed = true;
        } else if (args[i].equals("--input-escaped-by")) {
          this.inputEscapedBy = ImportOptions.toChar(args[++i]);
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
    } else if (this.allTables && this.splitByCol != null) {
      // If we're reading all tables in a database, can't set pkey
      throw new InvalidOptionsException("--split-by and --all-tables are incompatible options."
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
    } else if (this.action == ControlAction.FullImport && !this.allTables
        && this.tableName == null) {
      throw new InvalidOptionsException(
          "One of --table or --all-tables is required for import." + HELP_STR);
    }

    if (this.hiveImport) {
      if (!areDelimsManuallySet) {
        // user hasn't manually specified delimiters, and wants to import straight to Hive.
        // Use Hive-style delimiters.
        LOG.info("Using Hive-specific delimiters for output. You can override");
        LOG.info("delimiters with --fields-terminated-by, etc.");
        this.outputFieldDelim = (char)0x1; // ^A
        this.outputRecordDelim = '\n';
        this.outputEnclosedBy = '\000'; // no enclosing in Hive.
        this.outputEscapedBy = '\000'; // no escaping in Hive
        this.outputMustBeEnclosed = false;
      }

      if (this.getOutputEscapedBy() != '\000') {
        LOG.warn("Hive does not support escape characters in fields;");
        LOG.warn("parse errors in Hive may result from using --escaped-by.");
      }

      if (this.getOutputEnclosedBy() != '\000') {
        LOG.warn("Hive does not support quoted strings; parse errors");
        LOG.warn("in Hive may result from using --enclosed-by.");
      }
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

  public String getSplitByCol() {
    return splitByCol;
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

  public boolean isDirect() {
    return direct;
  }

  /**
   * @return the number of map tasks to use for import
   */
  public int getNumMappers() {
    return this.numMappers;
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

  /**
   * @return the field delimiter to use when parsing lines. Defaults to the field delim
   * to use when printing lines
   */
  public char getInputFieldDelim() {
    if (inputFieldDelim == '\000') {
      return this.outputFieldDelim;
    } else {
      return this.inputFieldDelim;
    }
  }

  /**
   * @return the record delimiter to use when parsing lines. Defaults to the record delim
   * to use when printing lines.
   */
  public char getInputRecordDelim() {
    if (inputRecordDelim == '\000') {
      return this.outputRecordDelim;
    } else {
      return this.inputRecordDelim;
    }
  }

  /**
   * @return the character that may enclose fields when parsing lines. Defaults to the
   * enclosing-char to use when printing lines.
   */
  public char getInputEnclosedBy() {
    if (inputEnclosedBy == '\000') {
      return this.outputEnclosedBy;
    } else {
      return this.inputEnclosedBy;
    }
  }

  /**
   * @return the escape character to use when parsing lines. Defaults to the escape
   * character used when printing lines.
   */
  public char getInputEscapedBy() {
    if (inputEscapedBy == '\000') {
      return this.outputEscapedBy;
    } else {
      return this.inputEscapedBy;
    }
  }

  /**
   * @return true if fields must be enclosed by the --enclosed-by character when parsing.
   * Defaults to false. Set true when --input-enclosed-by is used.
   */
  public boolean isInputEncloseRequired() {
    if (inputEnclosedBy == '\000') {
      return this.outputMustBeEnclosed;
    } else {
      return this.inputMustBeEnclosed;
    }
  }

  /**
   * @return the character to print between fields when importing them to text.
   */
  public char getOutputFieldDelim() {
    return this.outputFieldDelim;
  }


  /**
   * @return the character to print between records when importing them to text.
   */
  public char getOutputRecordDelim() {
    return this.outputRecordDelim;
  }

  /**
   * @return a character which may enclose the contents of fields when imported to text.
   */
  public char getOutputEnclosedBy() {
    return this.outputEnclosedBy;
  }

  /**
   * @return a character which signifies an escape sequence when importing to text.
   */
  public char getOutputEscapedBy() {
    return this.outputEscapedBy;
  }

  /**
   * @return true if fields imported to text must be enclosed by the EnclosedBy char.
   * default is false; set to true if --enclosed-by is used instead of --optionally-enclosed-by.
   */
  public boolean isOutputEncloseRequired() {
    return this.outputMustBeEnclosed;
  }
}
