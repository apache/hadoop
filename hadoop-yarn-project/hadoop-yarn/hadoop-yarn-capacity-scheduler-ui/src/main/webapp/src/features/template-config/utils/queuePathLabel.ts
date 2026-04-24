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


const DEFAULT_QUEUE_PATH_LABEL_MAX_LENGTH = 28;

export function formatQueuePathLabel(
  path: string,
  maxLength: number = DEFAULT_QUEUE_PATH_LABEL_MAX_LENGTH,
): string {
  if (path.length <= maxLength) {
    return path;
  }

  const sliceLength = Math.max(0, maxLength - 3);
  const prefixLength = Math.max(1, Math.floor(sliceLength / 2));
  const suffixLength = Math.max(1, sliceLength - prefixLength);

  return `${path.slice(0, prefixLength)}...${path.slice(-suffixLength)}`;
}
