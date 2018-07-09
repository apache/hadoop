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

package org.apache.hadoop.ozone.container.ozoneimpl;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerLocationUtil;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;


/**
 * Class used to read .container files from Volume and build container map.
 */
public class ContainerReader implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(
      ContainerReader.class);
  private HddsVolume hddsVolume;
  private final ContainerSet containerSet;
  private final OzoneConfiguration config;
  private final File hddsVolumeDir;

  ContainerReader(HddsVolume volume, ContainerSet cset, OzoneConfiguration
      conf) {
    Preconditions.checkNotNull(volume);
    this.hddsVolume = volume;
    this.hddsVolumeDir = hddsVolume.getHddsRootDir();
    this.containerSet = cset;
    this.config = conf;
  }

  @Override
  public void run() {
    try {
      readVolume(hddsVolumeDir);
    } catch (RuntimeException ex) {
      LOG.info("Caught an Run time exception during reading container files" +
          " from Volume {}", hddsVolumeDir);
    }
  }

  public void readVolume(File hddsVolumeRootDir) {
    Preconditions.checkNotNull(hddsVolumeRootDir, "hddsVolumeRootDir" +
        "cannot be null");


    /**
     *
     * layout of the container directory on the disk.
     * /hdds/<<scmUuid>>/current/<<containerdir>>/</containerID>/metadata
     * /<<containerID>>.container
     * /hdds/<<scmUuid>>/current/<<containerdir>>/<<containerID>>/metadata
     * /<<containerID>>.checksum
     * /hdds/<<scmUuid>>/current/<<containerdir>>/<<containerID>>/metadata
     * /<<containerID>>.db
     * /hdds/<<scmUuid>>/current/<<containerdir>>/<<containerID>>/chunks
     * /<<chunkFile>>
     *
     **/

    //filtering scm directory
    File[] scmDir = hddsVolumeRootDir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        return pathname.isDirectory();
      }
    });

    if (scmDir == null) {
      LOG.error("Volume {} is empty with out metadata and chunks",
          hddsVolumeRootDir);
      return;
    }
    for (File scmLoc : scmDir) {
      File currentDir = null;
      currentDir = new File(scmLoc, Storage.STORAGE_DIR_CURRENT);
      File[] containerTopDirs = currentDir.listFiles();
      if (containerTopDirs != null) {
        for (File containerTopDir : containerTopDirs) {
          if (containerTopDir.isDirectory()) {
            File[] containerDirs = containerTopDir.listFiles();
            if (containerDirs != null) {
              for (File containerDir : containerDirs) {
                File metadataPath = new File(containerDir + File.separator +
                    OzoneConsts.CONTAINER_META_PATH);
                String containerName = containerDir.getName();
                if (metadataPath.exists()) {
                  File containerFile = KeyValueContainerLocationUtil
                      .getContainerFile(metadataPath, containerName);
                  File checksumFile = KeyValueContainerLocationUtil
                      .getContainerCheckSumFile(metadataPath, containerName);
                  if (containerFile.exists() && checksumFile.exists()) {
                    verifyContainerFile(containerName, containerFile,
                        checksumFile);
                  } else {
                    LOG.error(
                        "Missing container metadata files for Container: " +
                            "{}", containerName);
                  }
                } else {
                  LOG.error("Missing container metadata directory for " +
                      "Container: {}", containerName);
                }
              }
            }
          }
        }
      }
    }
  }

  private void verifyContainerFile(String containerName, File containerFile,
                                   File checksumFile) {
    try {
      ContainerData containerData =  ContainerDataYaml.readContainerFile(
          containerFile);

      switch (containerData.getContainerType()) {
      case KeyValueContainer:
        KeyValueContainerData keyValueContainerData = (KeyValueContainerData)
            containerData;
        containerData.setVolume(hddsVolume);
        File dbFile = KeyValueContainerLocationUtil
            .getContainerDBFile(new File(containerFile.getParent()),
                containerName);
        if (!dbFile.exists()) {
          LOG.error("Container DB file is missing for Container {}, skipping " +
                  "this", containerName);
          // Don't further process this container, as it is missing db file.
          return;
        }
        KeyValueContainerUtil.parseKeyValueContainerData(keyValueContainerData,
            containerFile, checksumFile, dbFile, config);
        KeyValueContainer keyValueContainer = new KeyValueContainer(
            keyValueContainerData, config);
        containerSet.addContainer(keyValueContainer);
        break;
      default:
        LOG.error("Unrecognized ContainerType {} format during verify " +
            "ContainerFile", containerData.getContainerType());
      }
    } catch (IOException ex) {
      LOG.error("Error during reading container file {}", containerFile);
    }
  }

}
