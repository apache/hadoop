/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.adl.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.adl.oauth2.AzureADTokenProvider;

import java.io.IOException;
import java.util.Date;
import java.util.Random;

/**
 * Custom token management without cache enabled.
 */
public class CustomMockTokenProvider extends AzureADTokenProvider {
  private Random random;
  private long expiryTime;
  private int accessTokenRequestCount = 0;

  @Override
  public void initialize(Configuration configuration) throws IOException {
    random = new Random();
  }

  @Override
  public String getAccessToken() throws IOException {
    accessTokenRequestCount++;
    return String.valueOf(random.nextInt());
  }

  @Override
  public Date getExpiryTime() {
    Date before10Min = new Date();
    before10Min.setTime(expiryTime);
    return before10Min;
  }

  public void setExpiryTimeInMillisAfter(long timeInMillis) {
    expiryTime = System.currentTimeMillis() + timeInMillis;
  }

  public int getAccessTokenRequestCount() {
    return accessTokenRequestCount;
  }
}
