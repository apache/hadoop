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


import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonMethod;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.map.annotate.JsonFilter;
import org.codehaus.jackson.map.ser.FilterProvider;
import org.codehaus.jackson.map.ser.impl.SimpleBeanPropertyFilter;
import org.codehaus.jackson.map.ser.impl.SimpleFilterProvider;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * List Bucket is the response for the ListBucket Query.
 */
public class ListBuckets {
  static final String BUCKET_LIST = "BUCKET_LIST_FILTER";
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
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(data, ListBuckets.class);
  }

  /**
   * Returns a list of Buckets.
   *
   * @return Bucket list
   */
  public List<BucketInfo> getBuckets() {
    return Collections.unmodifiableList(buckets);
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
    ObjectMapper mapper = new ObjectMapper();
    String[] ignorableFieldNames = {"bytesUsed", "keyCount"};

    FilterProvider filters = new SimpleFilterProvider()
        .addFilter(BUCKET_LIST, SimpleBeanPropertyFilter
        .serializeAllExcept(ignorableFieldNames));

    mapper.setVisibility(JsonMethod.FIELD, JsonAutoDetect.Visibility.ANY);
    mapper.getSerializationConfig()
        .addMixInAnnotations(Object.class, MixIn.class);
    ObjectWriter writer = mapper.writer(filters);

    return writer.writeValueAsString(this);
  }

  /**
   * Returns the Object as a Json String.
   */
  public String toDBString() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.writeValueAsString(this);
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
