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
package org.apache.hadoop.ozone.web.response;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.ozone.web.handlers.BucketArgs;
import org.apache.hadoop.ozone.web.handlers.ListArgs;
import org.apache.hadoop.ozone.web.utils.JsonUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.google.common.base.Preconditions;

/**
 * This class the represents the list of keys (Objects) in a bucket.
 */
public class ListKeys {
  static final String OBJECT_LIST = "OBJECT_LIST_FILTER";

  private static final ObjectReader READER =
      new ObjectMapper().readerFor(ListKeys.class);
  private static final ObjectWriter WRITER;

  static {
    ObjectMapper mapper = new ObjectMapper();
    String[] ignorableFieldNames = {"dataFileName"};

    FilterProvider filters = new SimpleFilterProvider()
        .addFilter(OBJECT_LIST, SimpleBeanPropertyFilter
            .serializeAllExcept(ignorableFieldNames));
    mapper.setVisibility(PropertyAccessor.FIELD,
        JsonAutoDetect.Visibility.ANY);
    mapper.addMixIn(Object.class, MixIn.class);

    mapper.setFilterProvider(filters);
    WRITER = mapper.writerWithDefaultPrettyPrinter();
  }

  private String name;
  private String prefix;
  private long maxKeys;
  private boolean truncated;
  private List<KeyInfo> keyList;

  /**
   * Default constructor needed for json serialization.
   */
  public ListKeys() {
    this.keyList = new LinkedList<>();
  }

  /**
   * Constructor for ListKeys.
   *
   * @param args      ListArgs
   * @param truncated is truncated
   */
  public ListKeys(ListArgs args, boolean truncated) {
    Preconditions.checkState(args.getArgs() instanceof  BucketArgs);
    this.name = ((BucketArgs) args.getArgs()).getBucketName();
    this.prefix = args.getPrefix();
    this.maxKeys = args.getMaxKeys();
    this.truncated = truncated;
  }

  /**
   * Converts a Json string to POJO.
   * @param jsonString - json string.
   * @return ListObject
   * @throws IOException - Json conversion error.
   */
  public static ListKeys parse(String jsonString) throws IOException {
    return READER.readValue(jsonString);
  }

  /**
   * Returns a list of Objects.
   *
   * @return List of KeyInfo Objects.
   */
  public List<KeyInfo> getKeyList() {
    return keyList;
  }

  /**
   * Sets the list of Objects.
   *
   * @param objectList - List of Keys
   */
  public void setKeyList(List<KeyInfo> objectList) {
    this.keyList = objectList;
  }

  /**
   * Gets the Max Key Count.
   *
   * @return long
   */
  public long getMaxKeys() {
    return maxKeys;
  }

  /**
   * Gets bucket Name.
   *
   * @return String
   */
  public String getName() {
    return name;
  }

  /**
   * Gets Prefix.
   *
   * @return String
   */
  public String getPrefix() {
    return prefix;
  }

  /**
   * Gets truncated Status.
   *
   * @return Boolean
   */
  public boolean isTruncated() {
    return truncated;
  }

  /**
   * Sets the value of truncated.
   *
   * @param value - Boolean
   */
  public void setTruncated(boolean value) {
    this.truncated = value;
  }

  /**
   * Returns a JSON string of this object. After stripping out bytesUsed and
   * keyCount.
   *
   * @return String
   * @throws  IOException - On json Errors.
   */
  public String toJsonString() throws IOException {
    return WRITER.writeValueAsString(this);
  }

  /**
   * Returns the Object as a Json String.
   *
   * @return String
   * @throws IOException - on json errors.
   */
  public String toDBString() throws IOException {
    return JsonUtils.toJsonString(this);
  }

  /**
   * Sorts the keys based on name and version. This is useful when we return the
   * list of keys.
   */
  public void sort() {
    Collections.sort(keyList);
  }

  /**
   * Add a new key to the list of keys.
   * @param keyInfo - key Info
   */
  public void addKey(KeyInfo keyInfo){
    this.keyList.add(keyInfo);
  }

  /**
   * This class allows us to create custom filters for the Json serialization.
   */
  @JsonFilter(OBJECT_LIST)
  class MixIn {

  }
}
