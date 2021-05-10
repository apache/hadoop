/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc;

import org.apache.hadoop.classification.InterfaceStability;

import java.util.regex.Pattern;

/**
 * This class stores and validates the runc image metadata.
 */
@InterfaceStability.Unstable
public class ImageMetadata {
  private String image;
  private String tag;
  private String metaNamespace;
  private final String imageCoordinates;
  private static final String DEFAULT_TAG = "latest";
  private static final Pattern VALID_NS_PATTERN =
      Pattern.compile("^[A-Za-z0-9]+$");
  private static final Pattern VALID_NAME_PATTERN =
      Pattern.compile("^[~^+-\\._A-Za-z0-9]+$");

  public ImageMetadata(String imageCoordinates, String metaNamespace) {
    this.imageCoordinates = imageCoordinates;
    this.metaNamespace = metaNamespace;
  }

  /**
   * Validates and returns the image name and tag.
   *
   * @return the image name and tag.
   */
  public String getNameTag() {
    parseImageCoordinates();
    if (!tag.isEmpty()) {
      return image + ":" + tag;
    } else {
      return image;
    }
  }

  /**
   * Validates and returns the image namespace.
   *
   * @return the image namespace.
   */
  public String getMetaNamespace() {
    parseImageCoordinates();
    return metaNamespace;
  }

  /**
   * Parses and validates image coordinates, containing the image namespace,
   * image name and tag.
   */
  public void parseImageCoordinates() {
    if (imageCoordinates == null || imageCoordinates.isEmpty()) {
      throw new IllegalArgumentException("Invalid image coordinates, null" +
          " or empty.");
    }

    String[] nameParts = imageCoordinates.split("/", -1);
    String imageTag;
    if (nameParts.length == 2) {
      metaNamespace = nameParts[0];
      imageTag = nameParts[1];
    } else if (nameParts.length == 1) {
      imageTag = nameParts[0];
    } else {
      throw new IllegalArgumentException("Invalid image coordinates: "
          + imageCoordinates);
    }

    if (!VALID_NS_PATTERN.matcher(metaNamespace).matches()) {
      throw new IllegalArgumentException("Invalid image namespace: "
          + metaNamespace);
    }

    String[] tagParts = imageTag.split(":", -1);
    if (tagParts.length == 2) {
      image = tagParts[0];
      tag = tagParts[1];
    } else if (tagParts.length == 1) {
      image = tagParts[0];
      tag = DEFAULT_TAG;
    } else {
      throw new IllegalArgumentException("Invalid imageTag name: " + imageTag);
    }

    if (!VALID_NAME_PATTERN.matcher(image).matches()) {
      throw new IllegalArgumentException("Invalid image name: " + image);
    }

    if (!VALID_NAME_PATTERN.matcher(tag).matches()) {
      throw new IllegalArgumentException("Invalid tag name: " + tag);
    }
  }

}
