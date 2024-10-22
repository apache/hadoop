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

package org.apache.hadoop.mapreduce.lib.db;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/**
 * A InputFormat that reads input data from an SQL table in an Oracle db.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class OracleDataDrivenDBInputFormat<T extends DBWritable>
    extends DataDrivenDBInputFormat<T> implements Configurable {

  /**
   * @return the DBSplitter implementation to use to divide the table/query into InputSplits.
   */
  @Override
  protected DBSplitter getSplitter(int sqlDataType) {
    switch (sqlDataType) {
    case Types.DATE:
    case Types.TIME:
    case Types.TIMESTAMP:
      return new OracleDateSplitter();

    default:
      return super.getSplitter(sqlDataType);
    }
  }

  @Override
  protected RecordReader<LongWritable, T> createDBRecordReader(DBInputSplit split,
      Configuration conf) throws IOException {

    DBConfiguration dbConf = getDBConf();
    @SuppressWarnings("unchecked")
    Class<T> inputClass = (Class<T>) (dbConf.getInputClass());

    try {
      // Use Oracle-specific db reader
      return new OracleDataDrivenDBRecordReader<T>(split, inputClass,
          conf, createConnection(), dbConf, dbConf.getInputConditions(),
          dbConf.getInputFieldNames(), dbConf.getInputTableName());
    } catch (SQLException ex) {
      throw new IOException(ex);
    }
  }
}
