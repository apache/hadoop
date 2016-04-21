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

package org.apache.hadoop.security.token;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;

/**
 *  DtFetcher is an interface which permits the abstraction and separation of
 *  delegation token fetch implementaions across different packages and
 *  compilation units.  Resolution of fetcher impl will be done at runtime.
 */
public interface DtFetcher {
  /** Return a key used to identify the object/service implementation. */
  Text getServiceName();

  /** Used to allow the service API to indicate whether a token is required. */
  boolean isTokenRequired();

  /** Add any number of delegation tokens to Credentials object and return
   *  a token instance that is appropriate for aliasing, or null if none. */
  Token<?> addDelegationTokens(Configuration conf, Credentials creds,
                               String renewer, String url) throws Exception;
}
