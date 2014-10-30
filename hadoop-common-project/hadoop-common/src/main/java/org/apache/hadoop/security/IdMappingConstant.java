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
package org.apache.hadoop.security;

/**
 * Some constants for IdMapping
 */
public class IdMappingConstant {

  /** Do user/group update every 15 minutes by default, minimum 1 minute */
  public final static String USERGROUPID_UPDATE_MILLIS_KEY = "usergroupid.update.millis";
  public final static long USERGROUPID_UPDATE_MILLIS_DEFAULT = 15 * 60 * 1000; // ms
  public final static long USERGROUPID_UPDATE_MILLIS_MIN = 1 * 60 * 1000; // ms

  public final static String UNKNOWN_USER = "nobody";
  public final static String UNKNOWN_GROUP = "nobody";
  
  // Used for finding the configured static mapping file.
  public static final String STATIC_ID_MAPPING_FILE_KEY = "static.id.mapping.file";
  public static final String STATIC_ID_MAPPING_FILE_DEFAULT = "/etc/nfs.map";
}
