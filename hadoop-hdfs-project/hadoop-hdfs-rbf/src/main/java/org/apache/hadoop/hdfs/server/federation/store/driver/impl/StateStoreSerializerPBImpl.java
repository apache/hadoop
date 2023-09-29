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
package org.apache.hadoop.hdfs.server.federation.store.driver.impl;

import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreSerializer;
import org.apache.hadoop.hdfs.server.federation.store.records.BaseRecord;
import org.apache.hadoop.hdfs.server.federation.store.records.impl.pb.PBRecord;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.hadoop.thirdparty.protobuf.Message;

/**
 * Protobuf implementation of the State Store serializer.
 */
public final class StateStoreSerializerPBImpl extends StateStoreSerializer {

  private static final String PB_IMPL_PACKAGE_SUFFIX = "impl.pb";
  private static final String PB_IMPL_CLASS_SUFFIX = "PBImpl";

  private Configuration localConf = new Configuration();


  private StateStoreSerializerPBImpl() {
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T newRecordInstance(Class<T> clazz) {
    try {
      String clazzPBImpl = getPBImplClassName(clazz);
      Class<?> pbClazz = localConf.getClassByName(clazzPBImpl);
      Object retObject = ReflectionUtils.newInstance(pbClazz, localConf);
      return (T)retObject;
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private String getPBImplClassName(Class<?> clazz) {
    String srcPackagePart = getPackageName(clazz);
    String srcClassName = getClassName(clazz);
    String destPackagePart = srcPackagePart + "." + PB_IMPL_PACKAGE_SUFFIX;
    String destClassPart = srcClassName + PB_IMPL_CLASS_SUFFIX;
    return destPackagePart + "." + destClassPart;
  }

  private String getClassName(Class<?> clazz) {
    String fqName = clazz.getName();
    return (fqName.substring(fqName.lastIndexOf(".") + 1, fqName.length()));
  }

  private String getPackageName(Class<?> clazz) {
    return clazz.getPackage().getName();
  }

  @Override
  public byte[] serialize(BaseRecord record) {
    byte[] byteArray64 = null;
    if (record instanceof PBRecord) {
      PBRecord recordPB = (PBRecord) record;
      Message msg = recordPB.getProto();
      byte[] byteArray = msg.toByteArray();
      byteArray64 = Base64.encodeBase64(byteArray, false);
    }
    return byteArray64;
  }

  @Override
  public String serializeString(BaseRecord record) {
    byte[] byteArray64 = serialize(record);
    String base64Encoded = StringUtils.newStringUtf8(byteArray64);
    return base64Encoded;
  }

  @Override
  public <T extends BaseRecord> T deserialize(
      byte[] byteArray, Class<T> clazz) throws IOException {

    T record = newRecord(clazz);
    if (record instanceof PBRecord) {
      PBRecord pbRecord = (PBRecord) record;
      byte[] byteArray64 = Base64.encodeBase64(byteArray, false);
      String base64Encoded = StringUtils.newStringUtf8(byteArray64);
      pbRecord.readInstance(base64Encoded);
    }
    return record;
  }

  @Override
  public <T extends BaseRecord> T deserialize(String data, Class<T> clazz)
      throws IOException {
    byte[] byteArray64 = Base64.decodeBase64(data);
    return deserialize(byteArray64, clazz);
  }
}