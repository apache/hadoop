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
package org.apache.hadoop.ozone.s3.object;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;

import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.s3.EndpointBase;
import org.apache.hadoop.ozone.s3.commontypes.KeyMetadata;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;

/**
 * List Object Rest endpoint.
 */
@Path("/{volume}/{bucket}")
public class ListObject extends EndpointBase {


  @GET
  @Produces(MediaType.APPLICATION_XML)
  public ListObjectResponse get(
      @PathParam("volume") String volumeName,
      @PathParam("bucket") String bucketName,
      @QueryParam("delimiter") String delimiter,
      @QueryParam("encoding-type") String encodingType,
      @QueryParam("marker") String marker,
      @DefaultValue("1000") @QueryParam("max-keys") int maxKeys,
      @QueryParam("prefix") String prefix,
      @Context HttpHeaders hh) throws OS3Exception, IOException {

    if (delimiter == null) {
      delimiter = "/";
    }
    if (prefix == null) {
      prefix = "";
    }

    OzoneVolume volume = getVolume(volumeName);
    OzoneBucket bucket = getBucket(volume, bucketName);

    Iterator<? extends OzoneKey> ozoneKeyIterator = bucket.listKeys(prefix);

    ListObjectResponse response = new ListObjectResponse();
    response.setDelimiter(delimiter);
    response.setName(bucketName);
    response.setPrefix(prefix);
    response.setMarker("");
    response.setMaxKeys(1000);
    response.setEncodingType("url");
    response.setTruncated(false);

    String prevDir = null;
    while (ozoneKeyIterator.hasNext()) {
      OzoneKey next = ozoneKeyIterator.next();
      String relativeKeyName = next.getName().substring(prefix.length());

      int depth =
          StringUtils.countMatches(relativeKeyName, delimiter);

      if (prefix.length() > 0 && !prefix.endsWith(delimiter)
          && relativeKeyName.length() > 0) {
        response.addPrefix(prefix + "/");
        break;
      }
      if (depth > 0) {
        String dirName = relativeKeyName
            .substring(0, relativeKeyName.indexOf(delimiter));
        if (!dirName.equals(prevDir)) {
          response.addPrefix(
              prefix + dirName + delimiter);
          prevDir = dirName;
        }
      } else if (relativeKeyName.endsWith(delimiter)) {
        response.addPrefix(relativeKeyName);
      } else if (relativeKeyName.length() > 0) {
        KeyMetadata keyMetadata = new KeyMetadata();
        keyMetadata.setKey(next.getName());
        keyMetadata.setSize(next.getDataSize());
        keyMetadata.setETag("" + next.getModificationTime());
        keyMetadata.setStorageClass("STANDARD");
        keyMetadata
            .setLastModified(Instant.ofEpochMilli(next.getModificationTime()));
        response.addKey(keyMetadata);
      }
    }
    return response;
  }

}
