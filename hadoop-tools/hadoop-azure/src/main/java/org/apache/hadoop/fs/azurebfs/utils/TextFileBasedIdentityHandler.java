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
package org.apache.hadoop.fs.azurebfs.utils;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COLON;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HASH;


/**
 * {@code TextFileBasedIdentityHandler} is a {@link IdentityHandler} implements
 * translation operation which returns identity mapped to AAD identity by
 * loading the mapping file from the configured location. Location of the
 * mapping file should be configured in {@code core-site.xml}.
 * <p>
 * User identity file should be delimited by colon in below format.
 * <pre>
 * # OBJ_ID:USER_NAME:USER_ID:GROUP_ID:SPI_NAME:APP_ID
 * </pre>
 *
 * Example:
 * <pre>
 * a2b27aec-77bd-46dd-8c8c-39611a333331:user1:11000:21000:spi-user1:abcf86e9-5a5b-49e2-a253-f5c9e2afd4ec
 * </pre>
 *
 * Group identity file should be delimited by colon in below format.
 * <pre>
 * # OBJ_ID:GROUP_NAME:GROUP_ID:SGP_NAME
 * </pre>
 *
 * Example:
 * <pre>
 * 1d23024d-957c-4456-aac1-a57f9e2de914:group1:21000:sgp-group1
 * </pre>
 */
public class TextFileBasedIdentityHandler implements IdentityHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TextFileBasedIdentityHandler.class);

  /**
   * Expected no of fields in the user mapping file.
   */
  private static final int NO_OF_FIELDS_USER_MAPPING = 6;
  /**
   * Expected no of fields in the group mapping file.
   */
  private static final int NO_OF_FIELDS_GROUP_MAPPING = 4;
  /**
   * Array index for the local username.
   * Example:
   *  a2b27aec-77bd-46dd-8c8c-39611a333331:user1:11000:21000:spi-user1:abcf86e9-5a5b-49e2-a253-f5c9e2afd4ec
   */
  private static final int ARRAY_INDEX_FOR_LOCAL_USER_NAME = 1;
  /**
   * Array index for the security group name.
   * Example:
   *  1d23024d-957c-4456-aac1-a57f9e2de914:group1:21000:sgp-group1
   */
  private static final int ARRAY_INDEX_FOR_LOCAL_GROUP_NAME = 1;
  /**
   * Array index for the AAD Service Principal's Object ID.
   */
  private static final int ARRAY_INDEX_FOR_AAD_SP_OBJECT_ID = 0;
  /**
   * Array index for the AAD Security Group's Object ID.
   */
  private static final int ARRAY_INDEX_FOR_AAD_SG_OBJECT_ID = 0;
  private String userMappingFileLocation;
  private String groupMappingFileLocation;
  private HashMap<String, String> userMap;
  private HashMap<String, String> groupMap;

  public TextFileBasedIdentityHandler(String userMappingFilePath, String groupMappingFilePath) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(userMappingFilePath),
        "Local User to Service Principal mapping filePath cannot by Null or Empty");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(groupMappingFilePath),
        "Local Group to Security Group mapping filePath cannot by Null or Empty");
    this.userMappingFileLocation = userMappingFilePath;
    this.groupMappingFileLocation = groupMappingFilePath;
    //Lazy Loading
    this.userMap = new HashMap<>();
    this.groupMap = new HashMap<>();
  }

  /**
   * Perform lookup from Service Principal's Object ID to Local Username.
   * @param originalIdentity AAD object ID.
   * @return Local User name, if no name found or on exception, returns empty string.
   * */
  public synchronized String lookupForLocalUserIdentity(String originalIdentity) throws IOException {
    if(Strings.isNullOrEmpty(originalIdentity)) {
      return EMPTY_STRING;
    }

    if (userMap.size() == 0) {
      loadMap(userMap, userMappingFileLocation, NO_OF_FIELDS_USER_MAPPING, ARRAY_INDEX_FOR_AAD_SP_OBJECT_ID);
    }

    try {
      String username = !Strings.isNullOrEmpty(userMap.get(originalIdentity))
          ? userMap.get(originalIdentity).split(COLON)[ARRAY_INDEX_FOR_LOCAL_USER_NAME] : EMPTY_STRING;

      return username;
    } catch (ArrayIndexOutOfBoundsException e) {
      LOG.error("Error while parsing the line, returning empty string", e);
      return EMPTY_STRING;
    }
  }

  /**
   * Perform lookup from Security Group's Object ID to Local Security Group name.
   * @param originalIdentity AAD object ID.
   * @return Local Security group name, if no name found or on exception, returns empty string.
   * */
  public synchronized String lookupForLocalGroupIdentity(String originalIdentity) throws IOException {
    if(Strings.isNullOrEmpty(originalIdentity)) {
      return EMPTY_STRING;
    }

    if (groupMap.size() == 0) {
      loadMap(groupMap, groupMappingFileLocation, NO_OF_FIELDS_GROUP_MAPPING,
          ARRAY_INDEX_FOR_AAD_SG_OBJECT_ID);
    }

    try {
      String groupname =
          !Strings.isNullOrEmpty(groupMap.get(originalIdentity))
              ? groupMap.get(originalIdentity).split(COLON)[ARRAY_INDEX_FOR_LOCAL_GROUP_NAME] : EMPTY_STRING;

      return groupname;
    } catch (ArrayIndexOutOfBoundsException e) {
      LOG.error("Error while parsing the line, returning empty string", e);
      return EMPTY_STRING;
    }
  }

  /**
   * Creates the map from the file using the key index.
   * @param cache Instance of cache object to store the data.
   * @param fileLocation Location of the file to be loaded.
   * @param keyIndex Index of the key from the data loaded from the key.
   */
  private static void loadMap(HashMap<String, String> cache, String fileLocation, int noOfFields, int keyIndex)
      throws IOException {
    LOG.debug("Loading identity map from file {}", fileLocation);
    int errorRecord = 0;
    File file = new File(fileLocation);
    LineIterator it = null;
    try {
      it = FileUtils.lineIterator(file, "UTF-8");
      while (it.hasNext()) {
        String line = it.nextLine();
        if (!Strings.isNullOrEmpty(line.trim()) && !line.startsWith(HASH)) {
          if (line.split(COLON).length != noOfFields) {
            errorRecord += 1;
            continue;
          }
          cache.put(line.split(COLON)[keyIndex], line);
        }
      }
      LOG.debug("Loaded map stats - File: {}, Loaded: {}, Error: {} ", fileLocation, cache.size(), errorRecord);
    } catch (ArrayIndexOutOfBoundsException e) {
      LOG.error("Error while parsing mapping file", e);
    } finally {
      LineIterator.closeQuietly(it);
    }
  }
}
