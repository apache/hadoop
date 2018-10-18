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

package org.apache.hadoop.ozone.web.interfaces;

/**
 * This in the accounting interface, Ozone Rest interface will call into this
 * interface whenever a put or delete key happens.
 * <p>
 * TODO : Technically we need to report bucket creation and deletion too
 * since the bucket names and metadata consume storage.
 * <p>
 * TODO : We should separate out reporting metadata &amp; data --
 * <p>
 * In some cases end users will only want to account for the data they are
 * storing since metadata is mostly a cost of business.
 */
public interface Accounting {
  /**
   * This call is made when ever a put key call is made.
   * <p>
   * In case of a Put which causes a over write of a key accounting system will
   * see two calls, a removeByte call followed by an addByte call.
   *
   * @param owner  - Volume Owner
   * @param volume - Name of the Volume
   * @param bucket - Name of the bucket
   * @param bytes  - How many bytes are put
   */
  void addBytes(String owner, String volume, String bucket, int bytes);

  /**
   * This call is made whenever a delete call is made.
   *
   * @param owner  - Volume Owner
   * @param volume - Name of the Volume
   * @param bucket - Name of the bucket
   * @param bytes  - How many bytes are deleted
   */
  void removeBytes(String owner, String volume, String bucket, int bytes);

}
