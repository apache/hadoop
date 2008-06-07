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
package org.apache.hadoop.mapred;

/*******************************
 * Some handy constants
 * 
 *******************************/
interface MRConstants {
  //
  // Timeouts, constants
  //
  public static final int HEARTBEAT_INTERVAL_MIN = 5 * 1000;
  
  public static final int CLUSTER_INCREMENT = 50;

  public static final long COUNTER_UPDATE_INTERVAL = 60 * 1000;

  //for the inmemory filesystem (to do in-memory merge)
  /**
   * Constant denoting when a merge of in memory files will be triggered 
   */
  public static final float MAX_INMEM_FILESYS_USE = 0.66f;
  
  /**
   * Constant denoting the max size (in terms of the fraction of the total 
   * size of the filesys) of a map output file that we will try
   * to keep in mem. Ideally, this should be a factor of MAX_INMEM_FILESYS_USE
   */
  public static final float MAX_INMEM_FILESIZE_FRACTION =
    MAX_INMEM_FILESYS_USE/2;
    
  //
  // Result codes
  //
  public static int SUCCESS = 0;
  public static int FILE_NOT_FOUND = -1;
  
  /**
   * The custom http header used for the map output length.
   */
  public static final String MAP_OUTPUT_LENGTH = "Map-Output-Length";

  /**
   * The custom http header used for the "raw" map output length.
   */
  public static final String RAW_MAP_OUTPUT_LENGTH = "Raw-Map-Output-Length";

  /**
   * Temporary directory name 
   */
  public static final String TEMP_DIR_NAME = "_temporary";
  
  public static final String WORKDIR = "work";
}
