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
 * List Volume Class is the class that is returned in JSON format to
 * users when they call ListVolumes.
 */
public class ListVolumes {
  private List<VolumeInfo> volumes;

  static final String VOLUME_LIST = "VOLUME_LIST_FILTER";

  /**
   * Used for json filtering.
   */
  @JsonFilter(VOLUME_LIST)
  class MixIn {
  }

  /**
   * Constructs ListVolume objects.
   */
  public ListVolumes() {
    this.volumes = new LinkedList<VolumeInfo>();
  }

  /**
   * Gets the list of volumes.
   *
   * @return List of VolumeInfo Objects
   */
  public List<VolumeInfo> getVolumes() {
    return volumes;
  }


  /**
   * Sets volume info.
   *
   * @param volumes - List of Volumes
   */
  public void setVolumes(List<VolumeInfo> volumes) {
    this.volumes = volumes;
  }

  /**
   * Returns a JSON string of this object.
   * After stripping out bytesUsed and bucketCount
   *
   * @return String
   */
  public String toJsonString() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    String[] ignorableFieldNames = {"bytesUsed", "bucketCount"};

    FilterProvider filters = new SimpleFilterProvider()
        .addFilter(VOLUME_LIST,
            SimpleBeanPropertyFilter.serializeAllExcept(ignorableFieldNames));

    mapper.setVisibility(JsonMethod.FIELD, JsonAutoDetect.Visibility.ANY);
    mapper.getSerializationConfig()
        .addMixInAnnotations(Object.class, MixIn.class);
    ObjectWriter writer = mapper.writer(filters);

    return writer.writeValueAsString(this);
  }

  /**
   * When we serialize a volumeInfo to our database
   * we will use all fields. However the toJsonString
   * will strip out bytesUsed and bucketCount from the
   * volume Info
   *
   * @return Json String
   *
   * @throws IOException
   */
  public String toDBString() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.writeValueAsString(this);
  }

  /**
   * Parses a String to return ListVolumes object.
   *
   * @param data - Json String
   *
   * @return - ListVolumes
   *
   * @throws IOException
   */
  public static ListVolumes parse(String data) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(data, ListVolumes.class);
  }

  /**
   * Adds a new volume info to the List.
   *
   * @param info - VolumeInfo
   */
  public void addVolume(VolumeInfo info) {
    this.volumes.add(info);
  }

  /**
   * Sorts the volume names based on volume name.
   * This is useful when we return the list of volume names
   */
  public void sort() {
    Collections.sort(volumes);
  }
}
