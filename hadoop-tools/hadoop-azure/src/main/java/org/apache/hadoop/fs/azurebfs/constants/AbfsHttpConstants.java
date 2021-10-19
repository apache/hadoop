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

package org.apache.hadoop.fs.azurebfs.constants;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.VersionInfo;

/**
 * Responsible to keep all constant keys used in abfs rest client here.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class AbfsHttpConstants {
  // Abfs Http client constants
  public static final String FILESYSTEM = "filesystem";
  public static final String FILE = "file";
  public static final String DIRECTORY = "directory";
  public static final String APPEND_ACTION = "append";
  public static final String FLUSH_ACTION = "flush";
  public static final String SET_PROPERTIES_ACTION = "setProperties";
  public static final String SET_ACCESS_CONTROL = "setAccessControl";
  public static final String GET_ACCESS_CONTROL = "getAccessControl";
  public static final String CHECK_ACCESS = "checkAccess";
  public static final String GET_STATUS = "getStatus";
  public static final String ACQUIRE_LEASE_ACTION = "acquire";
  public static final String BREAK_LEASE_ACTION = "break";
  public static final String RELEASE_LEASE_ACTION = "release";
  public static final String RENEW_LEASE_ACTION = "renew";
  public static final String DEFAULT_LEASE_BREAK_PERIOD = "0";
  public static final String DEFAULT_TIMEOUT = "90";
  public static final String APPEND_BLOB_TYPE = "appendblob";
  public static final String TOKEN_VERSION = "2";

  public static final String JAVA_VENDOR = "java.vendor";
  public static final String JAVA_VERSION = "java.version";
  public static final String OS_NAME = "os.name";
  public static final String OS_VERSION = "os.version";
  public static final String OS_ARCH = "os.arch";

  public static final String APN_VERSION = "APN/1.0";
  public static final String CLIENT_VERSION = "Azure Blob FS/" + VersionInfo.getVersion();

  // Abfs Http Verb
  public static final String HTTP_METHOD_DELETE = "DELETE";
  public static final String HTTP_METHOD_GET = "GET";
  public static final String HTTP_METHOD_HEAD = "HEAD";
  public static final String HTTP_METHOD_PATCH = "PATCH";
  public static final String HTTP_METHOD_POST = "POST";
  public static final String HTTP_METHOD_PUT = "PUT";

  // Abfs generic constants
  public static final String SINGLE_WHITE_SPACE = " ";
  public static final String EMPTY_STRING = "";
  public static final String FORWARD_SLASH = "/";
  public static final String DOT = ".";
  public static final String PLUS = "+";
  public static final String STAR = "*";
  public static final String COMMA = ",";
  public static final String COLON = ":";
  public static final String EQUAL = "=";
  public static final String QUESTION_MARK = "?";
  public static final String AND_MARK = "&";
  public static final String SEMICOLON = ";";
  public static final String AT = "@";
  public static final String HTTP_HEADER_PREFIX = "x-ms-";
  public static final String HASH = "#";
  public static final String TRUE = "true";

  public static final String PLUS_ENCODE = "%20";
  public static final String FORWARD_SLASH_ENCODE = "%2F";
  public static final String AZURE_DISTRIBUTED_FILE_SYSTEM_AUTHORITY_DELIMITER = "@";
  public static final String UTF_8 = "utf-8";
  public static final String GMT_TIMEZONE = "GMT";
  public static final String APPLICATION_JSON = "application/json";
  public static final String APPLICATION_OCTET_STREAM = "application/octet-stream";

  public static final String ROOT_PATH = "/";
  public static final String ACCESS_MASK = "mask:";
  public static final String ACCESS_USER = "user:";
  public static final String ACCESS_GROUP = "group:";
  public static final String ACCESS_OTHER = "other:";
  public static final String DEFAULT_MASK = "default:mask:";
  public static final String DEFAULT_USER = "default:user:";
  public static final String DEFAULT_GROUP = "default:group:";
  public static final String DEFAULT_OTHER = "default:other:";
  public static final String DEFAULT_SCOPE = "default:";
  public static final String PERMISSION_FORMAT = "%04d";
  public static final String SUPER_USER = "$superuser";

  public static final char CHAR_FORWARD_SLASH = '/';
  public static final char CHAR_EXCLAMATION_POINT = '!';
  public static final char CHAR_UNDERSCORE = '_';
  public static final char CHAR_HYPHEN = '-';
  public static final char CHAR_EQUALS = '=';
  public static final char CHAR_STAR = '*';
  public static final char CHAR_PLUS = '+';

  private AbfsHttpConstants() {}
}
