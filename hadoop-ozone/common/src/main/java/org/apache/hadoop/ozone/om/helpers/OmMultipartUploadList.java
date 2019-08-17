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

package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Part;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * This class represents multipart list, which is required for
 * CompleteMultipart upload request.
 */
public class OmMultipartUploadList {

  private final TreeMap<Integer, String> multipartMap;

  /**
   * Construct OmMultipartUploadList which holds multipart map which contains
   * part number and part name.
   * @param partMap
   */
  public OmMultipartUploadList(Map<Integer, String> partMap) {
    this.multipartMap = new TreeMap<>(partMap);
  }

  /**
   * Return multipartMap which is a map of part number and part name.
   * @return multipartMap
   */
  public TreeMap<Integer, String> getMultipartMap() {
    return multipartMap;
  }

  /**
   * Construct Part list from the multipartMap.
   * @return List<Part>
   */
  public List<Part> getPartsList() {
    List<Part> partList = new ArrayList<>();
    multipartMap.forEach((partNumber, partName) -> partList.add(Part
        .newBuilder().setPartName(partName).setPartNumber(partNumber).build()));
    return partList;
  }
}
