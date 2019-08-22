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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import javax.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.recon.api.types.ContainerKeyPrefix;
import org.apache.hadoop.ozone.recon.api.types.ContainerMetadata;
import org.apache.hadoop.ozone.recon.api.types.ContainersResponse;
import org.apache.hadoop.ozone.recon.api.types.KeyMetadata;
import org.apache.hadoop.ozone.recon.api.types.KeyMetadata.ContainerBlockMetadata;
import org.apache.hadoop.ozone.recon.api.types.KeysResponse;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ContainerDBServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.ozone.recon.ReconConstants.FETCH_ALL;
import static org.apache.hadoop.ozone.recon.ReconConstants.PREV_CONTAINER_ID_DEFAULT_VALUE;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_LIMIT;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_QUERY_PREVKEY;


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
   * for the containers starting from the given "prev-key" query param for the
   * given "limit". The given "prev-key" is skipped from the results returned.
   *
   * @param limit max no. of containers to get.
   * @param prevKey the containerID after which results are returned.
   * @return {@link Response}
   */
  @GET
  public Response getContainers(
      @DefaultValue(FETCH_ALL) @QueryParam(RECON_QUERY_LIMIT) int limit,
      @DefaultValue(PREV_CONTAINER_ID_DEFAULT_VALUE)
      @QueryParam(RECON_QUERY_PREVKEY) long prevKey) {
    Map<Long, ContainerMetadata> containersMap;
    long containersCount;
    try {
      containersMap = containerDBServiceProvider.getContainers(limit, prevKey);
      containersCount = containerDBServiceProvider.getCountForContainers();
    } catch (IOException ioEx) {
      throw new WebApplicationException(ioEx,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    ContainersResponse containersResponse =
        new ContainersResponse(containersCount, containersMap.values());
    return Response.ok(containersResponse).build();
  }

  /**
   * Return @{@link org.apache.hadoop.ozone.recon.api.types.KeyMetadata} for
   * all keys that belong to the container identified by the id param
   * starting from the given "prev-key" query param for the given "limit".
   * The given prevKeyPrefix is skipped from the results returned.
   *
   * @param containerID the given containerID.
   * @param limit max no. of keys to get.
   * @param prevKeyPrefix the key prefix after which results are returned.
   * @return {@link Response}
   */
  @GET
  @Path("/{id}/keys")
  public Response getKeysForContainer(
      @PathParam("id") Long containerID,
      @DefaultValue(FETCH_ALL) @QueryParam(RECON_QUERY_LIMIT) int limit,
      @DefaultValue(StringUtils.EMPTY) @QueryParam(RECON_QUERY_PREVKEY)
          String prevKeyPrefix) {
    Map<String, KeyMetadata> keyMetadataMap = new LinkedHashMap<>();
    long totalCount;
    try {
      Map<ContainerKeyPrefix, Integer> containerKeyPrefixMap =
          containerDBServiceProvider.getKeyPrefixesForContainer(containerID,
              prevKeyPrefix);

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
              .filter(c -> c.getContainerID() == containerID)
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
          // break the for loop if limit has been reached
          if (keyMetadataMap.size() == limit) {
            break;
          }
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

      totalCount =
          containerDBServiceProvider.getKeyCountForContainer(containerID);
    } catch (IOException ioEx) {
      throw new WebApplicationException(ioEx,
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    KeysResponse keysResponse =
        new KeysResponse(totalCount, keyMetadataMap.values());
    return Response.ok(keysResponse).build();
  }
}
