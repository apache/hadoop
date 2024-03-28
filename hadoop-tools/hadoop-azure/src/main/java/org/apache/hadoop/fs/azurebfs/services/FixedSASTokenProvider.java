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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;

public class FixedSASTokenProvider implements SASTokenProvider {
  private String fixedSASToken;

  public FixedSASTokenProvider(final String fixedSASToken) {
    this.fixedSASToken = fixedSASToken;
  }

  @Override
  public void initialize(final Configuration configuration,
      final String accountName)
      throws IOException {
  }

  @Override
  public String getSASToken(final String account,
      final String fileSystem,
      final String path,
      final String operation) throws IOException {
    return fixedSASToken;
  }
}
