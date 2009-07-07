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

package org.apache.hadoop.sqoop.orm;

import org.apache.hadoop.sqoop.ImportOptions;
import org.apache.hadoop.sqoop.manager.ConnManager;
import org.apache.hadoop.sqoop.manager.SqlManager;
import org.apache.hadoop.sqoop.lib.BigDecimalSerializer;
import org.apache.hadoop.sqoop.lib.JdbcWritableBridge;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Creates an ORM class to represent a table from a database
 *
 * 
 *
 */
public class ClassWriter {

  public static final Log LOG = LogFactory.getLog(ClassWriter.class.getName());

  /**
   * This version number is injected into all generated Java classes to denote
   * which version of the ClassWriter's output format was used to generate the
   * class.
   *
   *  If the way that we generate classes, bump this number.
   */
  public static final int CLASS_WRITER_VERSION = 1;

  private ImportOptions options;
  private ConnManager connManager;
  private String tableName;
  private CompilationManager compileManager;

  /**
   * Creates a new ClassWriter to generate an ORM class for a table.
   * @param opts program-wide options
   * @param connMgr the connection manager used to describe the table.
   * @param table the name of the table to read.
   */
  public ClassWriter(final ImportOptions opts, final ConnManager connMgr,
      final String table, final CompilationManager compMgr) {
    this.options = opts;
    this.connManager = connMgr;
    this.tableName = table;
    this.compileManager = compMgr;
  }


  /**
   * @param javaType
   * @return the name of the method of JdbcWritableBridge to read an entry with a given java type.
   */
  private String dbGetterForType(String javaType) {
    // All Class-based types (e.g., java.math.BigDecimal) are handled with
    // "readBar" where some.package.foo.Bar is the canonical class name.
    // Turn the javaType string into the getter type string.

    String [] parts = javaType.split("\\.");
    if (parts.length == 0) {
      LOG.error("No ResultSet method for Java type " + javaType);
      return null;
    }

    String lastPart = parts[parts.length - 1];
    try {
      String getter = "read" + Character.toUpperCase(lastPart.charAt(0)) + lastPart.substring(1);
      return getter;
    } catch (StringIndexOutOfBoundsException oob) {
      // lastPart.*() doesn't work on empty strings.
      LOG.error("Could not infer JdbcWritableBridge getter for Java type " + javaType);
      return null;
    }
  }

  /**
   * @param javaType
   * @return the name of the method of JdbcWritableBridge to write an entry with a given java type.
   */
  private String dbSetterForType(String javaType) {
    // TODO(aaron): Lots of unit tests needed here.
    // See dbGetterForType() for the logic used here; it's basically the same.

    String [] parts = javaType.split("\\.");
    if (parts.length == 0) {
      LOG.error("No PreparedStatement Set method for Java type " + javaType);
      return null;
    }

    String lastPart = parts[parts.length - 1];
    try {
      String setter = "write" + Character.toUpperCase(lastPart.charAt(0)) + lastPart.substring(1);
      return setter;
    } catch (StringIndexOutOfBoundsException oob) {
      // lastPart.*() doesn't work on empty strings.
      LOG.error("Could not infer PreparedStatement setter for Java type " + javaType);
      return null;
    }
  }

  private String stringifierForType(String javaType, String colName) {
    if (javaType.equals("String")) {
      return colName;
    } else {
      // this is an object type -- just call its toString() in a null-safe way.
      return "\"\" + " + colName;
    }
  }

  /**
   * @param javaType the type to read
   * @param inputObj the name of the DataInput to read from
   * @param colName the column name to read
   * @return the line of code involving a DataInput object to read an entry with a given java type.
   */
  private String rpcGetterForType(String javaType, String inputObj, String colName) {
    if (javaType.equals("Integer")) {
      return "    this." + colName + " = Integer.valueOf(" + inputObj + ".readInt());\n";
    } else if (javaType.equals("Long")) {
      return "    this." + colName + " = Long.valueOf(" + inputObj + ".readLong());\n";
    } else if (javaType.equals("Float")) {
      return "    this." + colName + " = Float.valueOf(" + inputObj + ".readFloat());\n";
    } else if (javaType.equals("Double")) {
      return "    this." + colName + " = Double.valueOf(" + inputObj + ".readDouble());\n";
    } else if (javaType.equals("Boolean")) {
      return "    this." + colName + " = Boolean.valueOf(" + inputObj + ".readBoolean());\n";
    } else if (javaType.equals("String")) {
      return "    this." + colName + " = Text.readString(" + inputObj + ");\n";
    } else if (javaType.equals("java.sql.Date")) {
      return "    this." + colName + " = new Date(" + inputObj + ".readLong());\n";
    } else if (javaType.equals("java.sql.Time")) {
      return "    this." + colName + " = new Time(" + inputObj + ".readLong());\n";
    } else if (javaType.equals("java.sql.Timestamp")) {
      return "    this." + colName + " = new Timestamp(" + inputObj + ".readLong());\n"
          + "    this." + colName + ".setNanos(" + inputObj + ".readInt());\n";
    } else if (javaType.equals("java.math.BigDecimal")) {
      return "    this." + colName + " = " + BigDecimalSerializer.class.getCanonicalName()
          + ".readFields(" + inputObj + ");\n";
    } else {
      LOG.error("No ResultSet method for Java type " + javaType);
      return null;
    }
  }

  /**
   * Deserialize a possibly-null value from the DataInput stream
   * @param javaType name of the type to deserialize if it's not null.
   * @param inputObj name of the DataInput to read from
   * @param colName the column name to read.
   * @return
   */
  private String rpcGetterForMaybeNull(String javaType, String inputObj, String colName) {
    return "    if (" + inputObj + ".readBoolean()) { \n"
        + "        this." + colName + " = null;\n"
        + "    } else {\n"
        + rpcGetterForType(javaType, inputObj, colName)
        + "    }\n";
  }

  /**
   * @param javaType the type to write
   * @param inputObj the name of the DataOutput to write to
   * @param colName the column name to write
   * @return the line of code involving a DataOutput object to write an entry with
   *         a given java type.
   */
  private String rpcSetterForType(String javaType, String outputObj, String colName) {
    if (javaType.equals("Integer")) {
      return "    " + outputObj + ".writeInt(this." + colName + ");\n";
    } else if (javaType.equals("Long")) {
      return "    " + outputObj + ".writeLong(this." + colName + ");\n";
    } else if (javaType.equals("Boolean")) {
      return "    " + outputObj + ".writeBoolean(this." + colName + ");\n";
    } else if (javaType.equals("Float")) {
      return "    " + outputObj + ".writeFloat(this." + colName + ");\n";
    } else if (javaType.equals("Double")) {
      return "    " + outputObj + ".writeDouble(this." + colName + ");\n";
    } else if (javaType.equals("String")) {
      return "    Text.writeString(" + outputObj + ", " + colName + ");\n";
    } else if (javaType.equals("java.sql.Date")) {
      return "    " + outputObj + ".writeLong(this." + colName + ".getTime());\n";
    } else if (javaType.equals("java.sql.Time")) {
      return "    " + outputObj + ".writeLong(this." + colName + ".getTime());\n";
    } else if (javaType.equals("java.sql.Timestamp")) {
      return "    " + outputObj + ".writeLong(this." + colName + ".getTime());\n"
          + "    " + outputObj + ".writeInt(this." + colName + ".getNanos());\n";
    } else if (javaType.equals("java.math.BigDecimal")) {
      return "    " + BigDecimalSerializer.class.getCanonicalName()
          + ".write(this." + colName + ", " + outputObj + ");\n";
    } else {
      LOG.error("No ResultSet method for Java type " + javaType);
      return null;
    }
  }

  /**
   * Serialize a possibly-null value to the DataOutput stream. First a boolean
   * isNull is written, followed by the contents itself (if not null).
   * @param javaType name of the type to deserialize if it's not null.
   * @param inputObj name of the DataInput to read from
   * @param colName the column name to read.
   * @return
   */
  private String rpcSetterForMaybeNull(String javaType, String outputObj, String colName) {
    return "    if (null == this." + colName + ") { \n"
        + "        " + outputObj + ".writeBoolean(true);\n"
        + "    } else {\n"
        + "        " + outputObj + ".writeBoolean(false);\n"
        + rpcSetterForType(javaType, outputObj, colName)
        + "    }\n";
  }

  /**
   * Generate a member field and getter method for each column
   * @param columnTypes - mapping from column names to sql types
   * @param colNames - ordered list of column names for table.
   * @param sb - StringBuilder to append code to
   */
  private void generateFields(Map<String, Integer> columnTypes, String [] colNames,
      StringBuilder sb) {

    for (String col : colNames) {
      int sqlType = columnTypes.get(col);
      String javaType = SqlManager.toJavaType(sqlType);
      if (null == javaType) {
        LOG.error("Cannot resolve SQL type " + sqlType);
        continue;
      }

      sb.append("  private " + javaType + " " + col + ";\n");
      sb.append("  public " + javaType + " get_" + col + "() {\n");
      sb.append("    return " + col + ";\n");
      sb.append("  }\n");
    }
  }

  /**
   * Generate the readFields() method used by the database
   * @param columnTypes - mapping from column names to sql types
   * @param colNames - ordered list of column names for table.
   * @param sb - StringBuilder to append code to
   */
  private void generateDbRead(Map<String, Integer> columnTypes, String [] colNames,
      StringBuilder sb) {

    sb.append("  public void readFields(ResultSet __dbResults) throws SQLException {\n");

    int fieldNum = 0;

    for (String col : colNames) {
      fieldNum++;

      int sqlType = columnTypes.get(col);
      String javaType = SqlManager.toJavaType(sqlType);
      if (null == javaType) {
        LOG.error("No Java type for SQL type " + sqlType);
        continue;
      }

      String getterMethod = dbGetterForType(javaType);
      if (null == getterMethod) {
        LOG.error("No db getter method for Java type " + javaType);
        continue;
      }

      sb.append("    this." + col + " = JdbcWritableBridge." +  getterMethod
          + "(" + fieldNum + ", __dbResults);\n");
    }

    sb.append("  }\n");
  }


  /**
   * Generate the write() method used by the database
   * @param columnTypes - mapping from column names to sql types
   * @param colNames - ordered list of column names for table.
   * @param sb - StringBuilder to append code to
   */
  private void generateDbWrite(Map<String, Integer> columnTypes, String [] colNames,
      StringBuilder sb) {

    sb.append("  public void write(PreparedStatement __dbStmt) throws SQLException {\n");

    int fieldNum = 0;

    for (String col : colNames) {
      fieldNum++;

      int sqlType = columnTypes.get(col);
      String javaType = SqlManager.toJavaType(sqlType);
      if (null == javaType) {
        LOG.error("No Java type for SQL type " + sqlType);
        continue;
      }

      String setterMethod = dbSetterForType(javaType);
      if (null == setterMethod) {
        LOG.error("No db setter method for Java type " + javaType);
        continue;
      }

      sb.append("    JdbcWritableBridge." + setterMethod + "(" + col + ", "
          + fieldNum + ", " + sqlType + ", __dbStmt);\n");
    }

    sb.append("  }\n");
  }


  /**
   * Generate the readFields() method used by the Hadoop RPC system
   * @param columnTypes - mapping from column names to sql types
   * @param colNames - ordered list of column names for table.
   * @param sb - StringBuilder to append code to
   */
  private void generateHadoopRead(Map<String, Integer> columnTypes, String [] colNames,
      StringBuilder sb) {

    sb.append("  public void readFields(DataInput __dataIn) throws IOException {\n");

    for (String col : colNames) {
      int sqlType = columnTypes.get(col);
      String javaType = SqlManager.toJavaType(sqlType);
      if (null == javaType) {
        LOG.error("No Java type for SQL type " + sqlType);
        continue;
      }

      String getterMethod = rpcGetterForMaybeNull(javaType, "__dataIn", col);
      if (null == getterMethod) {
        LOG.error("No RPC getter method for Java type " + javaType);
        continue;
      }

      sb.append(getterMethod);
    }

    sb.append("  }\n");
  }

  /**
   * Generate the toString() method
   * @param columnTypes - mapping from column names to sql types
   * @param colNames - ordered list of column names for table.
   * @param sb - StringBuilder to append code to
   */
  private void generateToString(Map<String, Integer> columnTypes, String [] colNames,
      StringBuilder sb) {

    sb.append("  public String toString() {\n");
    sb.append("    StringBuilder sb = new StringBuilder();\n");

    boolean first = true;
    for (String col : colNames) {
      int sqlType = columnTypes.get(col);
      String javaType = SqlManager.toJavaType(sqlType);
      if (null == javaType) {
        LOG.error("No Java type for SQL type " + sqlType);
        continue;
      }

      if (!first) {
        // TODO(aaron): Support arbitrary record delimiters
        sb.append("    sb.append(\",\");\n");
      }

      first = false;

      String stringExpr = stringifierForType(javaType, col);
      if (null == stringExpr) {
        LOG.error("No toString method for Java type " + javaType);
        continue;
      }

      sb.append("    sb.append(" + stringExpr + ");\n");

    }

    sb.append("    return sb.toString();\n");
    sb.append("  }\n");
  }

  /**
   * Generate the write() method used by the Hadoop RPC system
   * @param columnTypes - mapping from column names to sql types
   * @param colNames - ordered list of column names for table.
   * @param sb - StringBuilder to append code to
   */
  private void generateHadoopWrite(Map<String, Integer> columnTypes, String [] colNames,
      StringBuilder sb) {

    sb.append("  public void write(DataOutput __dataOut) throws IOException {\n");

    for (String col : colNames) {
      int sqlType = columnTypes.get(col);
      String javaType = SqlManager.toJavaType(sqlType);
      if (null == javaType) {
        LOG.error("No Java type for SQL type " + sqlType);
        continue;
      }

      String setterMethod = rpcSetterForMaybeNull(javaType, "__dataOut", col);
      if (null == setterMethod) {
        LOG.error("No RPC setter method for Java type " + javaType);
        continue;
      }

      sb.append(setterMethod);
    }

    sb.append("  }\n");
  }
  /**
   * Generate the ORM code for the class.
   */
  public void generate() throws IOException {
    Map<String, Integer> columnTypes = connManager.getColumnTypes(tableName);

    String [] colNames = options.getColumns();
    if (null == colNames) {
      colNames = connManager.getColumnNames(tableName);
    }

    // Generate the Java code
    StringBuilder sb = generateClassForColumns(columnTypes, colNames);

    // Write this out to a file.
    String codeOutDir = options.getCodeOutputDir();

    // Get the class name to generate, which includes package components
    String className = new TableClassName(options).getClassForTable(tableName);
    // convert the '.' characters to '/' characters
    String sourceFilename = className.replace('.', File.separatorChar) + ".java";
    String filename = codeOutDir + sourceFilename;

    LOG.debug("Writing source file: " + filename);
    LOG.debug("Table name: " + tableName);
    StringBuilder sbColTypes = new StringBuilder();
    for (String col : colNames) {
      Integer colType = columnTypes.get(col);
      sbColTypes.append(col + ":" + colType + ", ");
    }
    String colTypeStr = sbColTypes.toString();
    LOG.debug("Columns: " + colTypeStr);
    LOG.debug("sourceFilename is " + sourceFilename);

    compileManager.addSourceFile(sourceFilename);

    // Create any missing parent directories.
    File file = new File(filename);
    String dirname = file.getParent();
    if (null != dirname) {
      boolean mkdirSuccess = new File(dirname).mkdirs();
      if (!mkdirSuccess) {
        LOG.debug("Could not create directory tree for " + dirname);
      }
    }

    OutputStream ostream = null;
    Writer writer = null;
    try {
      ostream = new FileOutputStream(filename);
      writer = new OutputStreamWriter(ostream);
      writer.append(sb.toString());
    } finally {
      if (null != writer) {
        try {
          writer.close();
        } catch (IOException ioe) {
          // ignored because we're closing.
        }
      }

      if (null != ostream) {
        try {
          ostream.close();
        } catch (IOException ioe) {
          // ignored because we're closing.
        }
      }
    }
  }

  /**
   * Generate the ORM code for a table object containing the named columns
   * @param columnTypes - mapping from column names to sql types
   * @param colNames - ordered list of column names for table.
   * @return - A StringBuilder that contains the text of the class code.
   */
  public StringBuilder generateClassForColumns(Map<String, Integer> columnTypes,
      String [] colNames) {
    StringBuilder sb = new StringBuilder();
    sb.append("// ORM class for " + tableName + "\n");
    sb.append("// WARNING: This class is AUTO-GENERATED. Modify at your own risk.\n");

    TableClassName tableNameInfo = new TableClassName(options);

    String packageName = tableNameInfo.getPackageForTable();
    if (null != packageName) {
      sb.append("package ");
      sb.append(packageName);
      sb.append(";\n");
    }

    sb.append("import org.apache.hadoop.io.Text;\n");
    sb.append("import org.apache.hadoop.io.Writable;\n");
    sb.append("import org.apache.hadoop.mapred.lib.db.DBWritable;\n");
    sb.append("import " + JdbcWritableBridge.class.getCanonicalName() + ";\n");
    sb.append("import java.sql.PreparedStatement;\n");
    sb.append("import java.sql.ResultSet;\n");
    sb.append("import java.sql.SQLException;\n");
    sb.append("import java.io.DataInput;\n");
    sb.append("import java.io.DataOutput;\n");
    sb.append("import java.io.IOException;\n");
    sb.append("import java.sql.Date;\n");
    sb.append("import java.sql.Time;\n");
    sb.append("import java.sql.Timestamp;\n");

    String className = tableNameInfo.getShortClassForTable(tableName);
    sb.append("public class " + className + " implements DBWritable, Writable {\n");
    sb.append("  public static final int PROTOCOL_VERSION = " + CLASS_WRITER_VERSION + ";\n");
    generateFields(columnTypes, colNames, sb);
    generateDbRead(columnTypes, colNames, sb);
    generateDbWrite(columnTypes, colNames, sb);
    generateHadoopRead(columnTypes, colNames, sb);
    generateHadoopWrite(columnTypes, colNames, sb);
    generateToString(columnTypes, colNames, sb);
    // TODO(aaron): Generate hashCode(), compareTo(), equals() so it can be a WritableComparable

    sb.append("}\n");

    return sb;
  }
}
