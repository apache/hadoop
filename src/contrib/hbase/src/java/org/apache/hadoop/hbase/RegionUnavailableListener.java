/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase;

import org.apache.hadoop.io.Text;

/**
 * Used as a callback mechanism so that an HRegion can notify the HRegionServer
 * when a region is about to be closed during a split operation. This is done
 * to minimize the amount of time the region is off-line.
 */
public interface RegionUnavailableListener {
  public void regionIsUnavailable(Text regionName);
}
