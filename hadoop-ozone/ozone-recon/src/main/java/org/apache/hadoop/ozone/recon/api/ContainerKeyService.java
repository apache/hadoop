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
package org.apache.hadoop.ozone.recon.api;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.recon.api.types.ContainerKeyPrefix;
import org.apache.hadoop.ozone.recon.api.types.ContainerMetadata;
import org.apache.hadoop.ozone.recon.api.types.KeyMetadata;
import org.apache.hadoop.ozone.recon.api.types.KeyMetadata.ContainerBlockMetadata;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ContainerDBServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Endpoint for querying keys that belong to a container.
 */
@Path("/containers")
@Produces(MediaType.APPLICATION_JSON)
public class ContainerKeyService {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerKeyService.class);

  @Inject
  private ContainerDBServiceProvider containerDBServiceProvider;

  @Inject
  private ReconOMMetadataManager omMetadataManager;

  /**
   * Return @{@link org.apache.hadoop.ozone.recon.api.types.ContainerMetadata}
   * for all the containers.
   *
   * @return {@link Response}
   */
  @GET
  public Response getContainers() {
    Map<Long, ContainerMetadata> containersMap;
    try {
      containersMap = containerDBServiceProvider.getContainers();
    } catch (IOException ioEx) {
      throw new WebApplicationException(ioEx,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    return Response.ok(containersMap.values()).build();
  }

  /**
   * Return @{@link org.apache.hadoop.ozone.recon.api.types.KeyMetadata} for
   * all keys that belong to the container identified by the id param.
   *
   * @param containerId Container Id
   * @return {@link Response}
   */
  @GET
  @Path("/{id}")
  public Response getKeysForContainer(@PathParam("id") Long containerId) {
    Map<String, KeyMetadata> keyMetadataMap = new HashMap<>();
    try {
      Map<ContainerKeyPrefix, Integer> containerKeyPrefixMap =
          containerDBServiceProvider.getKeyPrefixesForContainer(containerId);

      // Get set of Container-Key mappings for given containerId.
      for (ContainerKeyPrefix containerKeyPrefix : containerKeyPrefixMap
          .keySet()) {

        // Directly calling get() on the Key table instead of iterating since
        // only full keys are supported now. When we change to using a prefix
        // of the key, this needs to change to prefix seek (TODO).
        OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable().get(
            containerKeyPrefix.getKeyPrefix());
        if (null == omKeyInfo) {
          continue;
        }

        // Filter keys by version.
        List<OmKeyLocationInfoGroup> matchedKeys = omKeyInfo
            .getKeyLocationVersions()
            .stream()
            .filter(k -> (k.getVersion() == containerKeyPrefix.getKeyVersion()))
            .collect(Collectors.toList());

        List<ContainerBlockMetadata> blockIds = new ArrayList<>();
        for (OmKeyLocationInfoGroup omKeyLocationInfoGroup : matchedKeys) {
          List<OmKeyLocationInfo> omKeyLocationInfos = omKeyLocationInfoGroup
              .getLocationList()
              .stream()
              .filter(c -> c.getContainerID() == containerId)
              .collect(Collectors.toList());
          for (OmKeyLocationInfo omKeyLocationInfo : omKeyLocationInfos) {
            blockIds.add(new ContainerBlockMetadata(omKeyLocationInfo
                .getContainerID(), omKeyLocationInfo.getLocalID()));
          }
        }

        String ozoneKey = omMetadataManager.getOzoneKey(
            omKeyInfo.getVolumeName(),
            omKeyInfo.getBucketName(),
            omKeyInfo.getKeyName());
        if (keyMetadataMap.containsKey(ozoneKey)) {
          keyMetadataMap.get(ozoneKey).getVersions()
              .add(containerKeyPrefix.getKeyVersion());

          keyMetadataMap.get(ozoneKey).getBlockIds().putAll(
              Collections.singletonMap(containerKeyPrefix.getKeyVersion(),
                  blockIds));
        } else {
          KeyMetadata keyMetadata = new KeyMetadata();
          keyMetadata.setBucket(omKeyInfo.getBucketName());
          keyMetadata.setVolume(omKeyInfo.getVolumeName());
          keyMetadata.setKey(omKeyInfo.getKeyName());
          keyMetadata.setCreationTime(
              Instant.ofEpochMilli(omKeyInfo.getCreationTime()));
          keyMetadata.setModificationTime(
              Instant.ofEpochMilli(omKeyInfo.getModificationTime()));
          keyMetadata.setDataSize(omKeyInfo.getDataSize());
          keyMetadata.setVersions(new ArrayList<Long>() {{
              add(containerKeyPrefix.getKeyVersion());
            }});
          keyMetadataMap.put(ozoneKey, keyMetadata);
          keyMetadata.setBlockIds(new TreeMap<Long,
              List<ContainerBlockMetadata>>() {{
              put(containerKeyPrefix.getKeyVersion(), blockIds);
            }});
        }
      }
    } catch (IOException ioEx) {
      throw new WebApplicationException(ioEx,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    return Response.ok(keyMetadataMap.values()).build();
  }
}
