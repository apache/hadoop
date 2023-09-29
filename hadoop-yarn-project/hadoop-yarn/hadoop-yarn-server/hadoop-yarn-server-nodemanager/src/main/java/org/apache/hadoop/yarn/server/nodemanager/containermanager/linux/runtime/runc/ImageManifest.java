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

import java.util.ArrayList;
import java.util.Map;

/**
 * This class is a Java representation of the OCI Image Manifest Specification.
 */
@InterfaceStability.Unstable
public class ImageManifest {
  final private int schemaVersion;
  final private String mediaType;
  final private Blob config;
  final private ArrayList<Blob> layers;
  final private Map<String, String> annotations;

  public ImageManifest() {
    this(0, null, null, null, null);
  }

  public ImageManifest(int schemaVersion, String mediaType, Blob config,
      ArrayList<Blob> layers, Map<String, String> annotations) {
    this.schemaVersion = schemaVersion;
    this.mediaType = mediaType;
    this.config = config;
    this.layers = layers;
    this.annotations = annotations;
  }

  public int getSchemaVersion() {
    return schemaVersion;
  }

  public String getMediaType() {
    return mediaType;
  }

  public Blob getConfig() {
    return config;
  }

  public ArrayList<Blob> getLayers() {
    return layers;
  }

  public Map<String, String> getAnnotations() {
    return annotations;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("schemaVersion: " + schemaVersion + "\n");
    sb.append("mediaType: " + mediaType + "\n");
    sb.append(config.toString());
    for(Blob b : layers) {
      sb.append(b.toString());
    }
    return sb.toString();
  }

  /**
   * This class is a Java representation of an OCI Image Blob.
   */
  @InterfaceStability.Unstable
  public static class Blob {
    final private String mediaType;
    final private String digest;
    final private long size;
    final private ArrayList<String> urls;
    final private Map<String, String> annotations;

    public Blob() {
      this(null, null, 0, null, null);
    }

    public Blob(String mediaType, String digest, long size,
        ArrayList<String> urls, Map<String, String> annotations) {
      this.mediaType = mediaType;
      this.digest = digest;
      this.size = size;
      this.urls = urls;
      this.annotations = annotations;
    }

    public String getMediaType() {
      return mediaType;
    }

    public String getDigest() {
      return digest;
    }

    public long getSize() {
      return size;
    }

    public ArrayList<String> getUrls() {
      return urls;
    }

    public Map<String, String> getAnnotations() {
      return annotations;
    }

    @Override
    public String toString() {
      return "mediaType: " + mediaType + "\n" + "size: " + size + "\n"
          + "digest: " + digest + "\n";
    }
  }
}

