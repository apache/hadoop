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
 * API configuration
 */

type MockMode = 'static' | 'cluster' | 'off';

const normalizeUrl = (url: string | undefined | null): string | null => {
  if (!url) {
    return null;
  }

  const trimmed = url.trim();

  // Support relative paths so a dev server proxy can share the browser origin
  if (trimmed.startsWith('/')) {
    return trimmed.endsWith('/') ? trimmed.slice(0, -1) : trimmed;
  }

  const protocolNormalized = trimmed.replace(
    /^([a-z]+):?\/\/?/i,
    (_, proto: string) => `${proto}://`,
  );
  const withProtocol = /^[a-z]+:\/\//i.test(protocolNormalized)
    ? protocolNormalized
    : `http://${protocolNormalized}`;

  return withProtocol.endsWith('/') ? withProtocol.slice(0, -1) : withProtocol;
};

const resolveBaseUrl = () => {
  const envBase = import.meta.env.VITE_YARN_API_URL;
  if (envBase) {
    return normalizeUrl(envBase) ?? envBase;
  }

  if (typeof window !== 'undefined') {
    return `${window.location.origin}/ws/v1/cluster`;
  }

  // Fallback for non-browser environments (e.g., SSR) when no env config is provided
  return 'http://localhost:8088/ws/v1/cluster';
};

const resolveMockMode = (): MockMode => {
  const raw = (import.meta.env.VITE_API_MOCK_MODE as string | undefined)?.toLowerCase();

  if (raw === 'off' || raw === 'cluster' || raw === 'static') {
    return raw;
  }

  return import.meta.env.DEV ? 'static' : 'off';
};

const mockMode = resolveMockMode();
const baseUrl = resolveBaseUrl();

export const API_CONFIG = {
  baseUrl,
  mockMode,
  // Username for YARN simple authentication mode
  userName: import.meta.env.VITE_YARN_USER_NAME || 'yarn',
};
