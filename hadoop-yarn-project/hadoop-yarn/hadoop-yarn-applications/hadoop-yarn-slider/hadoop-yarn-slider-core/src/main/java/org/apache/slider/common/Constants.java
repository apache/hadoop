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

package org.apache.slider.common;

public class Constants {
  public static final int CONNECT_TIMEOUT = 10000;
  public static final int RPC_TIMEOUT = 15000;

  public static final String HADOOP_JAAS_DEBUG = "HADOOP_JAAS_DEBUG";
  public static final String KRB5_CCNAME = "KRB5CCNAME";
  public static final String JAVA_SECURITY_KRB5_CONF
    = "java.security.krb5.conf";
  public static final String JAVA_SECURITY_KRB5_REALM
    = "java.security.krb5.realm";
  public static final String SUN_SECURITY_KRB5_DEBUG
    = "sun.security.krb5.debug";
  public static final String SUN_SECURITY_SPNEGO_DEBUG
    = "sun.security.spnego.debug";
}
