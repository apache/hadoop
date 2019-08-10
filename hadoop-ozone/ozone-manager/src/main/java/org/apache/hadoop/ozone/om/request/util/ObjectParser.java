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

package org.apache.hadoop.ozone.om.request.util;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OzoneObj.ObjectType;
import org.apache.hadoop.ozone.security.acl.OzoneObj;

import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;

/**
 * Utility class to parse {@link OzoneObj#getPath()}.
 */
public class ObjectParser {

  private String volume;
  private String bucket;
  private String key;

  /**
   * Parse the path and extract volume, bucket and key names.
   * @param path
   */
  public ObjectParser(String path, ObjectType objectType) throws OMException {
    Preconditions.checkNotNull(path);
    String[] tokens = StringUtils.split(path, OZONE_URI_DELIMITER, 3);

    if (objectType == ObjectType.VOLUME && tokens.length == 1) {
      volume = tokens[0];
    } else if (objectType == ObjectType.BUCKET && tokens.length == 2) {
      volume = tokens[0];
      bucket = tokens[1];
    } else if (objectType == ObjectType.KEY && tokens.length == 3) {
      volume = tokens[0];
      bucket = tokens[1];
      key = tokens[2];
    } else {
      throw new OMException("Illegal path " + path,
          OMException.ResultCodes.INVALID_PATH_IN_ACL_REQUEST);
    }
  }

  public String getVolume() {
    return volume;
  }

  public String getBucket() {
    return bucket;
  }

  public String getKey() {
    return key;
  }
}

