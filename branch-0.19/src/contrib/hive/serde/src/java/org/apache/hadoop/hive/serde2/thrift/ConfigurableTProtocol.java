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

package org.apache.hadoop.hive.serde2.thrift;

import org.apache.hadoop.conf.Configuration;
import java.util.Properties;
import com.facebook.thrift.TException;

/**
 * An interface for TProtocols that need to have properties passed in to 
 *  initialize them. e.g., separators for TCTLSeparatedProtocol.
 *  If there was a regex like deserializer, the regex could be passed in
 *  in this manner.
 */
public interface ConfigurableTProtocol {
  /**
   * Initialize the TProtocol
   * @param conf System properties
   * @param tbl  table properties
   * @throws TException
   */
  public void initialize(Configuration conf, Properties tbl) throws TException;

}
