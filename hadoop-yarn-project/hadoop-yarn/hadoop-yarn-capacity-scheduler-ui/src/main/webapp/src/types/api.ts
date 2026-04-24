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


/**
 * API-specific types for YARN REST API responses and requests
 */

import type { SchedulerInfo } from './scheduler';
import type { ConfigProperty } from './config';

// Response types that match the actual YARN API

export type SchedulerResponse = {
  scheduler: {
    schedulerInfo: SchedulerInfo;
  };
};

export type SchedulerConfResponse = {
  property: ConfigProperty[];
};

export type YarnErrorResponse = {
  RemoteException?: {
    exception: string;
    javaClassName: string;
    message: string;
  };
};

export type VersionResponse = {
  versionId: number;
};

export type NodeLabelInfoItem = {
  name: string;
  exclusivity?: boolean | 'true' | 'false';
  partitionName?: string;
  activeNMs?: number;
  partitionInfo?: unknown;
};

export type NodeLabelsResponse = {
  nodeLabelInfo?: NodeLabelInfoItem | NodeLabelInfoItem[];
};

export type NodeToLabelsMapEntry = {
  key: string;
  value?: {
    nodeLabelInfo?:
      | NodeLabelInfoItem
      | NodeLabelInfoItem[]
      | {
          nodeLabelInfo?: NodeLabelInfoItem | NodeLabelInfoItem[];
        };
  };
};

export type NodeToLabelsResponse = {
  nodeToLabels?: {
    entry?: NodeToLabelsMapEntry | NodeToLabelsMapEntry[];
  };
};

export type YarnConfigResponse = {
  property: {
    value: string;
  };
};

// API client configuration
export type ApiClientConfig = {
  timeout?: number;
  headers?: Record<string, string>;
  retryAttempts?: number;
  retryDelay?: number;
  userName?: string;
  detectSecurityMode?: boolean;
  requestInterceptor?: (request: Request) => Request | Promise<Request>;
  responseInterceptor?: (response: Response) => Response | Promise<Response>;
};
