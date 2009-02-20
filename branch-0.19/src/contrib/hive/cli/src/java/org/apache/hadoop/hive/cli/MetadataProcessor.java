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

package org.apache.hadoop.hive.cli;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.CommandProcessor;


public class MetadataProcessor implements CommandProcessor {

  public int run(String command) {
    SessionState ss = SessionState.get();
    String table_name = command.trim();

    if(table_name.equals("")) {
      return 0;
    }

    try {
      MetaStoreClient msc = new MetaStoreClient(ss.getConf());
      
      if(!msc.tableExists(table_name)) {
        ss.err.println("table does not exist: " + table_name);
        return 1;
      } else {
        List<FieldSchema> fields = msc.get_fields(table_name);

        for(FieldSchema f: fields) {
          ss.out.println(f.getName() + ": " + f.getType());
        }
      }
    } catch (MetaException err) {
      ss.err.println("Got meta exception: " + err.getMessage());
      return 1;
    } catch (Exception err) {
      ss.err.println("Got exception: " + err.getMessage());
      return 1;
    }
    return 0;
  }

}
