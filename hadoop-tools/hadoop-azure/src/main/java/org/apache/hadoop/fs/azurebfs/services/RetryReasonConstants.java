/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

public final class RetryReasonConstants {

  private RetryReasonConstants() {

  }
  public static final String CONNECTION_TIMEOUT_JDK_MESSAGE = "connect timed out";
  public static final String READ_TIMEOUT_JDK_MESSAGE = "Read timed out";
  public static final String CONNECTION_RESET_MESSAGE = "Connection reset";
  public static final String OPERATION_BREACH_MESSAGE = "Operations per second is over the account limit.";
  public static final String CONNECTION_RESET_ABBREVIATION = "CR";
  public static final String CONNECTION_TIMEOUT_ABBREVIATION = "CT";
  public static final String READ_TIMEOUT_ABBREVIATION = "RT";
  public static final String INGRESS_LIMIT_BREACH_ABBREVIATION = "ING";
  public static final String EGRESS_LIMIT_BREACH_ABBREVIATION = "EGR";
  public static final String OPERATION_LIMIT_BREACH_ABBREVIATION = "OPR";
  public static final String UNKNOWN_HOST_EXCEPTION_ABBREVIATION = "UH";
  public static final String IO_EXCEPTION_ABBREVIATION = "IOE";
  public static final String SOCKET_EXCEPTION_ABBREVIATION = "SE";
}
