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
package org.apache.hadoop.security;

/**
 * Standard strings to use in exception messages
 * in {@link KerberosAuthException} when throwing.
 */
final class UGIExceptionMessages {

  public static final String FAILURE_TO_LOGIN = "failure to login:";
  public static final String FOR_USER = " for user: ";
  public static final String FOR_PRINCIPAL = " for principal: ";
  public static final String FROM_KEYTAB = " from keytab ";
  public static final String LOGIN_FAILURE = "Login failure";
  public static final String LOGOUT_FAILURE = "Logout failure";
  public static final String MUST_FIRST_LOGIN =
      "login must be done first";
  public static final String MUST_FIRST_LOGIN_FROM_KEYTAB =
      "loginUserFromKeyTab must be done first";
  public static final String SUBJECT_MUST_CONTAIN_PRINCIPAL =
      "Provided Subject must contain a KerberosPrincipal";
  public static final String SUBJECT_MUST_NOT_BE_NULL =
      "Subject must not be null";
  public static final String USING_TICKET_CACHE_FILE =
      " using ticket cache file: ";

  //checkstyle: Utility classes should not have a public or default constructor.
  private UGIExceptionMessages() {
  }
}
