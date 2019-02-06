/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.utils.db;

import com.google.common.base.Preconditions;
import org.eclipse.jetty.util.StringUtil;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.OptionsUtil;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * A Class that controls the standard config options of RocksDB.
 * <p>
 * Important : Some of the functions in this file are magic functions designed
 * for the use of OZONE developers only. Due to that this information is
 * documented in this files only and is *not* intended for end user consumption.
 * Please do not use this information to tune your production environments.
 * Please remember the SpiderMan principal; with great power comes great
 * responsibility.
 */
public final class DBConfigFromFile {
  private static final Logger LOG =
      LoggerFactory.getLogger(DBConfigFromFile.class);

  public static final String CONFIG_DIR = "HADOOP_CONF_DIR";

  private DBConfigFromFile() {
  }

  public static File getConfigLocation() throws IOException {
    String path = System.getenv(CONFIG_DIR);

    // Make testing easy.
    // If there is No Env. defined, let us try to read the JVM property
    if (StringUtil.isBlank(path)) {
      path = System.getProperty(CONFIG_DIR);
    }

    if (StringUtil.isBlank(path)) {
      LOG.debug("Unable to find the configuration directory. "
          + "Please make sure that HADOOP_CONF_DIR is setup correctly.");
    }
    if(StringUtil.isBlank(path)){
      return null;
    }
    return new File(path);

  }

  /**
   * This class establishes a magic pattern where we look for DBFile.ini as the
   * options for RocksDB.
   *
   * @param dbFileName - The DBFile Name. For example, OzoneManager.db
   * @return Name of the DB File options
   */
  public static String getOptionsFileNameFromDB(String dbFileName) {
    Preconditions.checkNotNull(dbFileName);
    return dbFileName + ".ini";
  }

  /**
   * One of the Magic functions designed for the use of Ozone Developers *ONLY*.
   * This function takes the name of DB file and looks up the a .ini file that
   * follows the ROCKSDB config format and uses that file for DBOptions and
   * Column family Options. The Format for this file is specified by RockDB.
   * <p>
   * Here is a sample config from RocksDB sample Repo.
   * <p>
   * https://github.com/facebook/rocksdb/blob/master/examples
   * /rocksdb_option_file_example.ini
   * <p>
   * We look for a specific pattern, say OzoneManager.db will have its configs
   * specified in OzoneManager.db.ini. This option is used only by the
   * performance testing group to allow tuning of all parameters freely.
   * <p>
   * For the end users we offer a set of Predefined options that is easy to use
   * and the user does not need to become an expert in RockDB config.
   * <p>
   * This code assumes the .ini file is placed in the same directory as normal
   * config files. That is in $HADOOP_DIR/etc/hadoop. For example, if we want to
   * control OzoneManager.db configs from a file, we need to create a file
   * called OzoneManager.db.ini and place that file in $HADOOP_DIR/etc/hadoop.
   *
   * @param dbFileName - The DB File Name, for example, OzoneManager.db.
   * @param cfDescs - ColumnFamily Handles.
   * @return DBOptions, Options to be used for opening/creating the DB.
   * @throws IOException
   */
  public static DBOptions readFromFile(String dbFileName,
      List<ColumnFamilyDescriptor> cfDescs) throws IOException {
    Preconditions.checkNotNull(dbFileName);
    Preconditions.checkNotNull(cfDescs);
    Preconditions.checkArgument(cfDescs.size() > 0);

    //TODO: Add Documentation on how to support RocksDB Mem Env.
    Env env = Env.getDefault();
    DBOptions options = null;
    File configLocation = getConfigLocation();
    if(configLocation != null &&
        StringUtil.isNotBlank(configLocation.toString())){
      Path optionsFile = Paths.get(configLocation.toString(),
          getOptionsFileNameFromDB(dbFileName));

      if (optionsFile.toFile().exists()) {
        options = new DBOptions();
        try {
          OptionsUtil.loadOptionsFromFile(optionsFile.toString(),
              env, options, cfDescs, true);

        } catch (RocksDBException rdEx) {
          RDBTable.toIOException("Unable to find/open Options file.", rdEx);
        }
      }
    }
    return options;
  }

}
