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
 * YARN REST API Client (Simplified for React Query)
 * Provides basic fetch methods for Apache Hadoop YARN's Capacity Scheduler APIs
 * All retry logic, caching, and state management is handled by React Query
 */

import type {
  ApiClientConfig,
  SchedulerResponse,
  SchedulerConfResponse,
  SchedConfUpdateInfo,
  YarnErrorResponse,
  NodeLabelsResponse,
  NodeToLabelsResponse,
  NodesResponse,
  VersionResponse,
  YarnConfigResponse,
  ValidationResponse,
} from '~/types';
import { HTTP_AUTH_PROPERTY, READ_ONLY_PROPERTY } from '~/config';

export class YarnApiClient {
  private readonly baseUrl: string;
  private readonly defaultHeaders: Record<string, string>;
  private readonly timeout: number;
  private readonly userName: string;
  private securityMode: 'simple' | 'kerberos' | null = null;
  private isReadOnly: boolean = false;
  private initPromise: Promise<void> | null = null;

  constructor(baseUrl: string, config: ApiClientConfig = {}) {
    // Remove trailing slash if present
    this.baseUrl = baseUrl.endsWith('/') ? baseUrl.slice(0, -1) : baseUrl;
    this.timeout = config.timeout || 30000;
    this.userName = config.userName || 'yarn';
    this.defaultHeaders = {
      Accept: 'application/json',
      ...config.headers,
    };

    const isTestEnv =
      (typeof process !== 'undefined' && Boolean(process.env?.VITEST)) ||
      (typeof import.meta !== 'undefined' &&
        (import.meta.env?.VITEST || import.meta.env?.MODE === 'test'));
    const shouldDetectSecurityMode = config.detectSecurityMode ?? !isTestEnv;

    if (shouldDetectSecurityMode) {
      // Defer detection until first request to ensure MSW is ready
      // This is initialized lazily in the request() method
      this.initPromise = null;
    } else {
      // Default to simple mode when skipping detection (e.g., unit tests)
      this.securityMode = 'simple';
      this.isReadOnly = false;
      this.initPromise = null;
    }
  }

  /**
   * Get the root URL (origin) from the base URL
   */
  private get rootUrl(): string {
    if (/^https?:\/\//i.test(this.baseUrl)) {
      return new URL(this.baseUrl).origin;
    }

    if (typeof window !== 'undefined') {
      return window.location.origin;
    }

    // Fallback for non-browser environments (tests/SSR)
    return 'http://localhost:8088';
  }

  /**
   * GET /scheduler - Fetch queue hierarchy with live metrics
   */
  async getScheduler(): Promise<SchedulerResponse> {
    return this.request<SchedulerResponse>('GET', '/scheduler');
  }

  /**
   * GET /scheduler-conf - Fetch current configuration properties
   */
  async getSchedulerConf(): Promise<SchedulerConfResponse> {
    return this.request<SchedulerConfResponse>('GET', '/scheduler-conf');
  }

  /**
   * PUT /scheduler-conf - Update configuration
   */
  async updateSchedulerConf(updateInfo: SchedConfUpdateInfo): Promise<void> {
    await this.request('PUT', '/scheduler-conf', {
      body: JSON.stringify(updateInfo),
      headers: {
        'Content-Type': 'application/json',
      },
      expectJson: false,
    });
  }

  /**
   * POST /scheduler-conf/validate - Validate configuration changes
   */
  async validateSchedulerConf(updateInfo: SchedConfUpdateInfo): Promise<ValidationResponse> {
    return this.request<ValidationResponse>('POST', '/scheduler-conf/validate', {
      body: JSON.stringify(updateInfo),
      headers: {
        'Content-Type': 'application/json',
      },
    });
  }

  /**
   * GET /scheduler-conf/version - Get configuration version
   */
  async getSchedulerConfVersion(): Promise<VersionResponse> {
    return this.request<VersionResponse>('GET', '/scheduler-conf/version');
  }

  /**
   * GET /ws/v1/cluster/get-node-labels - List all node labels
   */
  async getNodeLabels(): Promise<NodeLabelsResponse> {
    return this.request<NodeLabelsResponse>('GET', '/get-node-labels');
  }

  /**
   * POST /ws/v1/cluster/add-node-labels - Add new node labels
   */
  async addNodeLabels(labels: { name: string; exclusivity: boolean }[]): Promise<void> {
    await this.request('POST', '/add-node-labels', {
      body: JSON.stringify({
        nodeLabelInfo: labels.map((label) => ({
          name: label.name,
          exclusivity: label.exclusivity,
        })),
      }),
      headers: {
        'Content-Type': 'application/json',
      },
      expectJson: false,
    });
  }

  /**
   * POST /ws/v1/cluster/remove-node-labels - Remove node labels
   */
  async removeNodeLabels(labels: string[]): Promise<void> {
    const query = labels.map((label) => `labels=${encodeURIComponent(label)}`).join('&');
    const path = query ? `/remove-node-labels?${query}` : '/remove-node-labels';

    await this.request('POST', path, {
      expectJson: false,
    });
  }

  /**
   * GET /ws/v1/cluster/get-node-to-labels - Get node to label mappings
   */
  async getNodeToLabels(): Promise<NodeToLabelsResponse> {
    return this.request<NodeToLabelsResponse>('GET', '/get-node-to-labels');
  }

  /**
   * GET /ws/v1/cluster/nodes - Get cluster nodes information
   */
  async getNodes(): Promise<NodesResponse> {
    return this.request<NodesResponse>('GET', '/nodes');
  }

  /**
   * POST /ws/v1/cluster/replace-node-to-labels - Replace node label assignments
   */
  async replaceNodeToLabels(nodeToLabels: { nodeId: string; labels: string[] }[]): Promise<void> {
    await this.request('POST', '/replace-node-to-labels', {
      body: JSON.stringify({
        nodeToLabels: nodeToLabels.map((mapping) => ({
          nodeId: mapping.nodeId,
          labels: mapping.labels,
        })),
      }),
      headers: {
        'Content-Type': 'application/json',
      },
      expectJson: false,
    });
  }

  /**
   * GET /conf?name=<config> - Fetch YARN configuration value
   * Note: This endpoint is at the root level, not under /ws/v1/cluster
   * Uses direct fetch() to avoid circular dependency during initialization
   */
  async getConfiguration(name: string): Promise<string> {
    const url = `${this.rootUrl}/conf?name=${encodeURIComponent(name)}`;

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), this.timeout);

    try {
      const response = await fetch(url, {
        method: 'GET',
        signal: controller.signal,
        credentials: 'include',
        headers: {
          Accept: 'application/json',
        },
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const data = (await response.json()) as YarnConfigResponse;
      return data.property.value;
    } finally {
      clearTimeout(timeoutId);
    }
  }

  /**
   * Detect YARN security mode by checking the HTTP authentication type.
   * This governs the REST API layer, which may differ from the cluster-wide
   * hadoop.security.authentication setting (e.g. Kerberos RPC + simple HTTP).
   */
  private async detectSecurityMode(): Promise<void> {
    try {
      const authMode = await this.getConfiguration(HTTP_AUTH_PROPERTY);
      this.securityMode = authMode.toLowerCase() === 'simple' ? 'simple' : 'kerberos';
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);

      if (
        /user\s+not\s+authenticated/i.test(message) ||
        /unable to obtain user name/i.test(message)
      ) {
        this.securityMode = 'simple';
        return;
      }

      // Fallback to simple mode when detection fails (common in dev proxies)
      this.securityMode = 'simple';

      // Surface unexpected errors for visibility
      console.warn('Proceeding with simple auth mode due to detection failure:', message);
    }
  }

  /**
   * Detect read-only mode by checking the READ_ONLY_PROPERTY configuration
   */
  private async detectReadOnlyMode(): Promise<void> {
    try {
      const readOnlyValue = await this.getConfiguration(READ_ONLY_PROPERTY);
      // Convert string to boolean - treat 'true' (case-insensitive) as true, everything else as false
      this.isReadOnly = readOnlyValue.toLowerCase() === 'true';
    } catch (error) {
      // If the configuration is not found or fails to fetch, default to writable (false)
      this.isReadOnly = false;
      const message = error instanceof Error ? error.message : String(error);
      console.warn('Read-only mode config not found, defaulting to writable:', message);
    }
  }

  /**
   * Get the current read-only mode status
   */
  getIsReadOnly(): boolean {
    return this.isReadOnly;
  }

  /**
   * Simple request method - React Query handles retries and error states
   */
  private async request<T = void>(
    method: string,
    path: string,
    options: RequestInit & { skipAuth?: boolean; expectJson?: boolean } = {},
  ): Promise<T> {
    // Lazy initialization: Start detection on first request (ensures MSW is ready)
    if (this.initPromise === null && this.securityMode === null) {
      this.initPromise = Promise.all([
        this.detectSecurityMode().catch((error) => {
          console.error('Failed to detect YARN security mode:', error);
          // Don't rethrow - allow requests to proceed without auth detection
        }),
        this.detectReadOnlyMode().catch((error) => {
          console.error('Failed to detect YARN read-only mode:', error);
          // Don't rethrow - default to writable mode
        }),
      ])
        .then(() => {})
        .finally(() => {
          // Clear the promise after detection completes
          this.initPromise = null;
        });
    }

    // Wait for security mode detection to complete (if still in progress)
    if (this.initPromise) {
      await this.initPromise;
    }

    // Build URL by appending path to baseUrl
    const { skipAuth, expectJson = true, ...fetchOptions } = options;
    let url = `${this.baseUrl}${path}`;

    // Add user.name parameter for simple auth mode (unless skipAuth is true)
    if (!skipAuth && this.securityMode === 'simple' && this.userName) {
      const separator = url.includes('?') ? '&' : '?';
      url += `${separator}user.name=${encodeURIComponent(this.userName)}`;
    }

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), this.timeout);

    try {
      const response = await fetch(url, {
        method,
        signal: controller.signal,
        credentials: 'include', // Include cookies for cross-origin requests
        ...fetchOptions,
        headers: {
          ...this.defaultHeaders,
          ...fetchOptions.headers,
        },
      });

      if (!response.ok) {
        await this.handleErrorResponse(response);
      }

      // Handle empty responses
      if (response.status === 204 || response.headers.get('content-length') === '0') {
        return undefined as T;
      }

      const contentType = response.headers.get('content-type') || '';
      if (!expectJson) {
        return undefined as T;
      }

      if (contentType.includes('application/json')) {
        try {
          return await response.json();
        } catch (parseError) {
          throw parseError instanceof Error
            ? parseError
            : new Error('Failed to parse JSON response from YARN API.');
        }
      }

      // Return empty for successful non-JSON responses
      return undefined as T;
    } finally {
      clearTimeout(timeoutId);
    }
  }

  /**
   * Handle error responses from YARN API
   */
  private async handleErrorResponse(response: Response): Promise<never> {
    let errorMessage = `HTTP ${response.status}: ${response.statusText}`;

    try {
      const rawBody = await response.text();
      if (rawBody) {
        try {
          const parsed = JSON.parse(rawBody) as
            | YarnErrorResponse
            | { message?: string; errors?: string[] };

          if ('RemoteException' in parsed && parsed.RemoteException?.message) {
            errorMessage = parsed.RemoteException.message;
          } else if (Array.isArray((parsed as { errors?: string[] }).errors)) {
            const combined = (parsed as { errors: string[] }).errors.join('; ');
            if (combined.trim().length > 0) {
              errorMessage = combined;
            }
          } else if (typeof (parsed as { message?: string }).message === 'string') {
            const msg = (parsed as { message: string }).message.trim();
            if (msg.length > 0) {
              errorMessage = msg;
            }
          } else if (rawBody.trim().length > 0) {
            errorMessage = rawBody.trim();
          }
        } catch {
          if (rawBody.trim().length > 0) {
            errorMessage = rawBody.trim();
          }
        }
      }
    } catch {
      // Swallow secondary parsing errors and fall back to default message
    }

    throw new Error(errorMessage);
  }
}
