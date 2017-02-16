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
 *
 */

package org.apache.hadoop.fs.adl;

import org.apache.hadoop.fs.FileStatus;

import java.util.Random;

/**
 * Mock up response data returned from Adl storage account.
 */
public final class TestADLResponseData {

  private TestADLResponseData() {

  }

  public static String getGetFileStatusJSONResponse(FileStatus status) {
    return "{\"FileStatus\":{\"length\":" + status.getLen() + "," +
        "\"pathSuffix\":\"\",\"type\":\"" + (status.isDirectory() ?
        "DIRECTORY" :
        "FILE") + "\"" +
        ",\"blockSize\":" + status.getBlockSize() + ",\"accessTime\":" +
        status.getAccessTime() + ",\"modificationTime\":" + status
        .getModificationTime() + "" +
        ",\"replication\":" + status.getReplication() + ",\"permission\":\""
        + status.getPermission() + "\",\"owner\":\"" + status.getOwner()
        + "\",\"group\":\"" + status.getGroup() + "\"}}";
  }

  public static String getGetFileStatusJSONResponse() {
    return getGetFileStatusJSONResponse(4194304);
  }

  public static String getGetAclStatusJSONResponse() {
    return "{\n" + "    \"AclStatus\": {\n" + "        \"entries\": [\n"
        + "            \"user:carla:rw-\", \n" + "            \"group::r-x\"\n"
        + "        ], \n" + "        \"group\": \"supergroup\", \n"
        + "        \"owner\": \"hadoop\", \n"
        + "        \"permission\":\"775\",\n" + "        \"stickyBit\": false\n"
        + "    }\n" + "}";
  }

  public static String getGetFileStatusJSONResponse(long length) {
    return "{\"FileStatus\":{\"length\":" + length + "," +
        "\"pathSuffix\":\"\",\"type\":\"FILE\",\"blockSize\":268435456," +
        "\"accessTime\":1452103827023,\"modificationTime\":1452103827023," +
        "\"replication\":0,\"permission\":\"777\"," +
        "\"owner\":\"NotSupportYet\",\"group\":\"NotSupportYet\"}}";
  }

  public static String getGetFileStatusJSONResponse(boolean aclBit) {
    return "{\"FileStatus\":{\"length\":1024," +
        "\"pathSuffix\":\"\",\"type\":\"FILE\",\"blockSize\":268435456," +
        "\"accessTime\":1452103827023,\"modificationTime\":1452103827023," +
        "\"replication\":0,\"permission\":\"777\"," +
        "\"owner\":\"NotSupportYet\",\"group\":\"NotSupportYet\",\"aclBit\":\""
        + aclBit + "\"}}";
  }

  public static String getListFileStatusJSONResponse(int dirSize) {
    String list = "";
    for (int i = 0; i < dirSize; ++i) {
      list += "{\"length\":1024,\"pathSuffix\":\"" + java.util.UUID.randomUUID()
          + "\",\"type\":\"FILE\",\"blockSize\":268435456," +
          "\"accessTime\":1452103878833," +
          "\"modificationTime\":1452103879190,\"replication\":0," +
          "\"permission\":\"777\",\"owner\":\"NotSupportYet\"," +
          "\"group\":\"NotSupportYet\"},";
    }

    list = list.substring(0, list.length() - 1);
    return "{\"FileStatuses\":{\"FileStatus\":[" + list + "]}}";
  }

  public static String getListFileStatusJSONResponse(boolean aclBit) {
    return "{\"FileStatuses\":{\"FileStatus\":[{\"length\":0,\"pathSuffix\":\""
        + java.util.UUID.randomUUID()
        + "\",\"type\":\"DIRECTORY\",\"blockSize\":0,"
        + "\"accessTime\":1481184513488,"
        + "\"modificationTime\":1481184513488,\"replication\":0,"
        + "\"permission\":\"770\","
        + "\"owner\":\"4b27fe1a-d9ab-4a04-ad7a-4bba72cd9e6c\","
        + "\"group\":\"4b27fe1a-d9ab-4a04-ad7a-4bba72cd9e6c\",\"aclBit\":\""
        + aclBit + "\"}]}}";
  }

  public static String getJSONResponse(boolean status) {
    return "{\"boolean\":" + status + "}";
  }

  public static String getErrorIllegalArgumentExceptionJSONResponse() {
    return "{\n" +
        "  \"RemoteException\":\n" +
        "  {\n" +
        "    \"exception\"    : \"IllegalArgumentException\",\n" +
        "    \"javaClassName\": \"java.lang.IllegalArgumentException\",\n" +
        "    \"message\"      : \"Invalid\"" +
        "  }\n" +
        "}";
  }

  public static String getErrorBadOffsetExceptionJSONResponse() {
    return "{\n" +
        "  \"RemoteException\":\n" +
        "  {\n" +
        "    \"exception\"    : \"BadOffsetException\",\n" +
        "    \"javaClassName\": \"org.apache.hadoop.fs.adl"
        + ".BadOffsetException\",\n" +
        "    \"message\"      : \"Invalid\"" +
        "  }\n" +
        "}";
  }

  public static String getErrorInternalServerExceptionJSONResponse() {
    return "{\n" +
        "  \"RemoteException\":\n" +
        "  {\n" +
        "    \"exception\"    : \"RuntimeException\",\n" +
        "    \"javaClassName\": \"java.lang.RuntimeException\",\n" +
        "    \"message\"      : \"Internal Server Error\"" +
        "  }\n" +
        "}";
  }

  public static String getAccessControlException() {
    return "{\n" + "  \"RemoteException\":\n" + "  {\n"
        + "    \"exception\"    : \"AccessControlException\",\n"
        + "    \"javaClassName\": \"org.apache.hadoop.security"
        + ".AccessControlException\",\n"
        + "    \"message\"      : \"Permission denied: ...\"\n" + "  }\n" + "}";
  }

  public static String getFileNotFoundException() {
    return "{\n" + "  \"RemoteException\":\n" + "  {\n"
        + "    \"exception\"    : \"FileNotFoundException\",\n"
        + "    \"javaClassName\": \"java.io.FileNotFoundException\",\n"
        + "    \"message\"      : \"File does not exist\"\n" + "  }\n" + "}";
  }

  public static byte[] getRandomByteArrayData() {
    return getRandomByteArrayData(4 * 1024 * 1024);
  }

  public static byte[] getRandomByteArrayData(int size) {
    byte[] b = new byte[size];
    Random rand = new Random();
    rand.nextBytes(b);
    return b;
  }
}
