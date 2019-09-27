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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.utils.db;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;

/**
 * Holds configuration items for OM RocksDB.
 */
@ConfigGroup(prefix = "hadoop.hdds.db")
public class RocksDBConfiguration {

  private boolean rocksdbLogEnabled;

  @Config(key = "rocksdb.logging.enabled",
      type = ConfigType.BOOLEAN,
      defaultValue = "false",
      tags = {ConfigTag.OM},
      description = "Enable/Disable RocksDB logging for OM.")
  public void setRocksdbLoggingEnabled(boolean enabled) {
    this.rocksdbLogEnabled = enabled;
  }

  public boolean isRocksdbLoggingEnabled() {
    return rocksdbLogEnabled;
  }

  private String rocksdbLogLevel;

  @Config(key = "rocksdb.logging.level",
      type = ConfigType.STRING,
      defaultValue = "INFO",
      tags = {ConfigTag.OM},
      description = "OM RocksDB logging level (INFO/DEBUG/WARN/ERROR/FATAL)")
  public void setRocksdbLogLevel(String level) {
    this.rocksdbLogLevel = level;
  }

  public String getRocksdbLogLevel() {
    return rocksdbLogLevel;
  }

}
