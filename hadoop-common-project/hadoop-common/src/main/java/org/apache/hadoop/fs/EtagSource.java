/*
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

package org.apache.hadoop.fs;

/**
 * An optional interface for {@link FileStatus} subclasses to implement
 * to provide access to etags.
 * If available FS SHOULD also implement the matching PathCapabilities
 *   -- etag supported: {@link CommonPathCapabilities#ETAGS_AVAILABLE}.
 *   -- etag consistent over rename:
 *      {@link CommonPathCapabilities#ETAGS_PRESERVED_IN_RENAME}.
 */
public interface EtagSource {

  /**
   * Return an etag of this file status.
   * A return value of null or "" means "no etag"
   * @return a possibly null or empty etag.
   */
  String getEtag();

}
