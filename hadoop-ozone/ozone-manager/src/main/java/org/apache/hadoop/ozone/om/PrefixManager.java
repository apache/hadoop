/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;

import java.util.List;

/**
 * Handles prefix commands.
 * //TODO: support OzoneManagerFS for ozfs optimization using prefix tree.
 */
public interface PrefixManager extends IOzoneAcl {

  /**
   * Returns the metadataManager.
   * @return OMMetadataManager.
   */
  OMMetadataManager getMetadataManager();

  /**
   * Get the list of path components that match with obj's path.
   * longest prefix.
   * Note: the number of the entries include a root "/"
   * so if you have a longtest prefix path /a/b/c/
   * the returned list will be ["/", "a", "b", "c"]
   * @param path ozone object path
   * @return list of longest path components that matches obj's path.
   */
  List<OmPrefixInfo> getLongestPrefixPath(String path);
}
