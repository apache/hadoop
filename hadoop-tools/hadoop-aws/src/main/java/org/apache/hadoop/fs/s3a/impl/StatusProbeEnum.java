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

package org.apache.hadoop.fs.s3a.impl;

import java.util.EnumSet;
import java.util.Set;

/**
 * Enum of probes which can be made of S3.
 */
public enum StatusProbeEnum {

  /** The actual path. */
  Head,
  /** HEAD of the path + /. */
  DirMarker,
  /** LIST under the path. */
  List;

  /** All probes. */
  public static final Set<StatusProbeEnum> ALL = EnumSet.allOf(
      StatusProbeEnum.class);

  /** Skip the HEAD and only look for directories. */
  public static final Set<StatusProbeEnum> DIRECTORIES =
      EnumSet.of(DirMarker, List);

  /** We only want the HEAD or dir marker. */
  public static final Set<StatusProbeEnum> HEAD_OR_DIR_MARKER =
      EnumSet.of(Head, DirMarker);

  /** We only want the HEAD. */
  public static final Set<StatusProbeEnum> HEAD_ONLY =
      EnumSet.of(Head);

  /** We only want the dir marker. */
  public static final Set<StatusProbeEnum> DIR_MARKER_ONLY =
      EnumSet.of(DirMarker);

  /** We only want the dir marker. */
  public static final Set<StatusProbeEnum> LIST_ONLY =
      EnumSet.of(List);

}
