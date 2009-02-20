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

package org.apache.hadoop.hive.metastore;

// hadoop stuff
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.util.StringUtils;

public class FileStore implements RawStore {

  protected static final Log LOG = LogFactory.getLog(MetaStore.LogKey);

  private File msRoot_;

  private Configuration conf;

  @Override
  public Configuration getConf() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    String msRootPath = HiveConf.getVar(conf, HiveConf.ConfVars.METASTOREDIRECTORY);

    if(msRootPath == null) {
      LOG.fatal("metastore.dir is empty in hadoop config");
      throw new RuntimeException("metastore.dir is empty in hadoop config");
    }
    try {
      msRoot_ = new File(new URI(msRootPath).getPath());
    } catch(URISyntaxException e) {
      throw new RuntimeException(msRootPath + " is not a valid URI");
    }
  }

  public FileStore(Configuration conf) {
    this.conf = conf;
    String msRootPath = HiveConf.getVar(conf, HiveConf.ConfVars.METASTOREDIRECTORY);

    if(msRootPath == null) {
      LOG.fatal("metastore.dir is empty in hadoop config");
      throw new RuntimeException("metastore.dir is empty in hadoop config");
    }
    try {
      msRoot_ = new File(new URI(msRootPath).getPath());
    } catch(URISyntaxException e) {
      throw new RuntimeException(msRootPath + " is not a valid URI");
    }
  }

  private File getSchemaFile(String db, String table) {
    return new File(getSchemaDir(db, table), "schema");
  }

  private File getSchemaDir(String db, String table) {
    return new File(getDBDir(db), table + ".dir");
  }

  private File getDBDir(String dbName) {
    File f = dbName.equals("default") ? msRoot_ : new File(msRoot_, dbName + ".db");
    return f;
  }

  @Override
  public boolean createDatabase(Database db) throws MetaException {
    // ignores the location param
    boolean mkdir = getDBDir(db.getName()).mkdir();
    if(!mkdir) {
      throw new MetaException("Unable to create directory for database " + db.getName());
    }
    return mkdir;
  }

  @Override
  public boolean createDatabase(String name) throws MetaException {
    return this.createDatabase(new Database(name, "ignored param"));
  }

  protected Properties load(DB parent, String tableName) throws MetaException, UnknownTableException {

    Properties schema = new Properties();

    File schemaFile = getSchemaFile(parent.getName(), tableName);

    if(schemaFile.exists() == false) {
      throw new UnknownTableException();
    }

    try {
      FileInputStream fis = new FileInputStream(schemaFile);
      schema.load(fis);
      fis.close();
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new MetaException("got IOException trying to get table schema for : " + tableName + " - " + e);
    }

    return schema;
  }

  protected void store(DB parent, String tableName, Properties schema, boolean overwrite) throws MetaException {

    File schemaFile = getSchemaFile(parent.getName(), tableName);

    if(overwrite == false && schemaFile.exists() == true) {
      // IGNORE FOR NOW            throw new MetaException("trying to overwrite table " + tableName + " already exists");
    }

    File dir = getSchemaDir(parent.getName(), tableName);
    if(!  dir.mkdirs() && !overwrite) {
      throw new MetaException("could not create: " + dir);
    }
    try {
      FileOutputStream fos = new FileOutputStream(schemaFile);
      schema.store(fos,"meta data");
      fos.close();
    } catch(IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new MetaException(e.getMessage());
    }
  }

  protected void rename(DB parent, String tableName, DB newParent, String newTableName) throws MetaException {
    File currentF = getSchemaDir(parent.getName(), tableName);
    File newF = getSchemaDir(newParent.getName(), newTableName);
    boolean renameTo = currentF.renameTo(newF);
    if(!renameTo) {
      throw new MetaException("Rename failed");
    }
  }

  public void drop(DB db, String tableName) throws IOException {
    MetaStoreUtils.recursiveDelete(getSchemaDir(db.getName(), tableName));
  }

  /**
   * getTablesByPattern
   *
   * Looks at metastore directories
   *
   * @param db
   * @param tablePattern
   * @return the list of tables
   * @exception MetaException
   *
   */
  public ArrayList<String> getTablesByPattern(DB parent, String tablePattern) throws MetaException {

    ArrayList<String> names = new ArrayList<String> ();

    if(tablePattern == null || tablePattern.length() == 0) {
      throw new MetaException("Empty table name");
    }

    tablePattern += ".dir$";
    Pattern tpat = Pattern.compile(tablePattern);

    File msPath = getDBDir(parent.getName());

    String tpaths[] = msPath.list();

    if(tpaths == null) {
      LOG.fatal("Internal fatal err - bad db dir: " + msPath);
      throw new MetaException("Internal fatal err - bad db dir: " + msPath);
    }

    for(int i = 0; tpaths != null && i < tpaths.length; i++) {
      Matcher m = tpat.matcher(tpaths[i]);
      if(m.matches()) {
        String name = tpaths[i];
        // strip off .dir in new hiding place for meta data
        name = name.replaceAll(".dir$","");
        names.add(name);
      }
    }
    return names;
  }

  @Override
  public List<String> getTables(String dbName, String pattern) throws MetaException {
    try {
      return this.getTablesByPattern(new DB(dbName, conf), pattern);
    } catch (UnknownDBException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new MetaException("Unknown database " + dbName);
    }
  }

  /**
   * dbExists
   *
   * @return boolean whether the db exists
   * @exception MetaException (for future use possibly)
   * NOTE - there is no strict version of this now. Strict meaning no extraneous .table.field1.field11...
   *
   */
  public boolean dbExists(String dbName) throws MetaException {
    File f =  getDBDir(dbName);
    return dbName.equals(MetaStore.DefaultDB) || f.isDirectory();
  }

  @Override
  public boolean dropDatabase(String dbname) {
    return true; // no-op
  }

  @Override
  public Database getDatabase(String name) throws NoSuchObjectException {
    try {
      DB db = new DB(name, conf);
      return new Database(db.getName(), db.whRoot_.getName());
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new NoSuchObjectException(e.getMessage());
    }
  }

  @Override
  /**
   * getDatabases
   *
   * @return the list of dbs. is never null.
   * @exception MetaException
   * Assuming 1 level here - bugbug
   *
   */
  public List<String> getDatabases() throws MetaException {
    ArrayList<String> names = new ArrayList<String> () ;
    String dbPattern = ".+\\.db$";
    Pattern tpat = Pattern.compile(dbPattern);

    String tpaths[] = msRoot_.list();

    for(int i = 0; i < tpaths.length; i++) {
      Matcher m = tpat.matcher(tpaths[i]);
      if(m.matches()) {
        String name = tpaths[i];
        // strip off .db in new hiding place for meta data
        name = name.replaceAll(".db$","");
        names.add(name);
      }
    }
    names.add("default");
    return names;
  }

  public boolean tableExists(String dbName, String tableName) {
    return getSchemaFile(dbName, tableName).exists();
  }

  @Override
  public void alterTable(String dbname, String name, Table newTable) throws InvalidObjectException,
      MetaException {
    // TODO Auto-generated method stub
    throw new MetaException("Not yet implemented");
  }

  @Override
  public void createTable(Table tbl) throws InvalidObjectException, MetaException {
    Properties p = MetaStoreUtils.getSchema(tbl);
    try {
      DB db = new DB(tbl.getDbName(), conf);
      RWTable.create(db, tbl.getTableName(), p, conf);
    } catch (UnknownDBException e) {
      throw new InvalidObjectException(e.getMessage());
    }
  }

  @Override
  public boolean dropTable(String dbName, String tableName) throws MetaException {
    try {
      new DB(dbName, conf).getTable(tableName, false).drop();
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new MetaException(e.getMessage());
    }
    return true;
  }

  @Override
  public Table getTable(String dbName, String tableName) throws MetaException {
    try {
      Properties p = new DB(dbName, conf).getTable(tableName, true).getSchema();
      return MetaStoreUtils.getTable(conf, p);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new MetaException(e.getMessage());
    }
  }

  @Override
  public boolean createType(Type type) {
    return true; // no-op
  }

  @Override
  public boolean dropType(String typeName) {
    return true; // no-op
  }

  @Override
  public boolean addPartition(Partition part) throws InvalidObjectException, MetaException {
    return true; // no-op as there is no metadata 
  }

  @Override
  public boolean dropPartition(String dbName, String tableName, List<String> part_vals)
      throws MetaException {
    return true; // no-op
  }

  @Override
  public Partition getPartition(String dbName, String tableName, List<String> part_vals)
      throws MetaException {
    // TODO Auto-generated method stub
    throw new MetaException("Not yet implemented");
  }

  @Override
  public List<Partition> getPartitions(String dbName, String tableName, int max)
      throws MetaException {
    // TODO Auto-generated method stub
    throw new MetaException("Not yet implemented");
  }

  @Override
  public Type getType(String typeName) {
    return null; // no-op
  }

  @Override
  public boolean openTransaction() {
    return true;
  }

  @Override
  public void rollbackTransaction() {
    // no-op
  }

  @Override
  public boolean commitTransaction() {
    return true; // no-op
  }

  @Override
  public void shutdown() {
    // no-op
  }

  @Override
  public List<String> listPartitionNames(String db_name, String tbl_name, short max_parts)
      throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

};
