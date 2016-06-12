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
package org.apache.hadoop.registry.server.dns;

import org.xbill.DNS.Name;
import org.xbill.DNS.Zone;

/**
 * A selector that returns the zone associated with a provided name.
 */
public interface ZoneSelector {
  /**
   * Finds the best matching zone given the provided name.
   * @param name the record name for which a zone is requested.
   * @return the matching zone.
   */
  Zone findBestZone(Name name);
}
