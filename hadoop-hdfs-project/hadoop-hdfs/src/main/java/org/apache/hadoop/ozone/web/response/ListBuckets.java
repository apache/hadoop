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

/**
 * List Bucket is the response for the ListBucket Query.
 */
public class ListBuckets {
  static final String BUCKET_LIST = "BUCKET_LIST_FILTER";
  private static final ObjectReader READER =
      new ObjectMapper().readerFor(ListBuckets.class);
  private static final ObjectWriter WRITER;

  static {
    ObjectMapper mapper = new ObjectMapper();
    String[] ignorableFieldNames = {"dataFileName"};

    FilterProvider filters = new SimpleFilterProvider()
        .addFilter(BUCKET_LIST, SimpleBeanPropertyFilter
            .serializeAllExcept(ignorableFieldNames));
    mapper.setVisibility(PropertyAccessor.FIELD,
        JsonAutoDetect.Visibility.ANY);
    mapper.addMixIn(Object.class, MixIn.class);

    mapper.setFilterProvider(filters);
    WRITER = mapper.writerWithDefaultPrettyPrinter();
  }

  private List<BucketInfo> buckets;

  /**
   * Constructor for ListBuckets.
   * @param buckets - List of buckets owned by this user
   */
  public ListBuckets(List<BucketInfo> buckets) {
    this.buckets = buckets;

  }

  /**
   * Constructor for ListBuckets.
  */
  public ListBuckets() {
    this.buckets = new LinkedList<BucketInfo>();
  }

  /**
   * Parses a String to return ListBuckets object.
   *
   * @param data - Json String
   *
   * @return - ListBuckets
   *
   * @throws IOException
   */
  public static ListBuckets parse(String data) throws IOException {
    return READER.readValue(data);
  }

  /**
   * Returns a list of Buckets.
   *
   * @return Bucket list
   */
  public List<BucketInfo> getBuckets() {
    return buckets;
  }

  /**
   * Sets the list of buckets owned by this user.
   *
   * @param buckets - List of Buckets
   */
  public void setBuckets(List<BucketInfo> buckets) {
    this.buckets = buckets;
  }


  /**
   * Returns a JSON string of this object.
   * After stripping out bytesUsed and keyCount
   *
   * @return String
   */
  public String toJsonString() throws IOException {
    return WRITER.writeValueAsString(this);
  }

  /**
   * Returns the Object as a Json String.
   */
  public String toDBString() throws IOException {
    return JsonUtils.toJsonString(this);
  }

  /**
   * Sorts the buckets based on bucketName.
   * This is useful when we return the list of buckets
   */
  public void sort() {
    Collections.sort(buckets);
  }

  /**
   * Add a new bucket to the list of buckets.
   * @param bucketInfo - bucket Info
   */
  public void addBucket(BucketInfo bucketInfo){
    this.buckets.add(bucketInfo);
  }

  /**
   * This class allows us to create custom filters
   * for the Json serialization.
   */
  @JsonFilter(BUCKET_LIST)
  class MixIn {

  }

}
