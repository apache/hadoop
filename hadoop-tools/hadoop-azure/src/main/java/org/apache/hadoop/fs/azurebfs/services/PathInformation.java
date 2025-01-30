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

package org.apache.hadoop.fs.azurebfs.services;

public class PathInformation {
    private Boolean pathExists;
    private Boolean isDirectory;
    private String eTag;

    public PathInformation(Boolean pathExists, Boolean isDirectory, String eTag) {
        this.pathExists = pathExists;
        this.isDirectory = isDirectory;
        this.eTag = eTag;
    }

    public PathInformation() {
    }

    public void copy(PathInformation pathInformation) {
        this.pathExists = pathInformation.getPathExists();
        this.isDirectory = pathInformation.getIsDirectory();
        this.eTag = pathInformation.getETag();
    }

    public String getETag() {
        return eTag;
    }

    public Boolean getPathExists() {
        return pathExists;
    }

    public Boolean getIsDirectory() {
        return isDirectory;
    }
}