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


import { http, HttpResponse, type HttpHandler } from 'msw';
import { API_CONFIG } from '~/lib/api/config';
import { HTTP_AUTH_PROPERTY, READ_ONLY_PROPERTY } from '~/config';

// Base URL pattern that matches the API configuration

const { baseUrl, mockMode } = API_CONFIG;
const MOCK_ASSET_BASE = `${import.meta.env.BASE_URL}mock/ws/v1/cluster`;

const staticHandlers: HttpHandler[] = [
  // Scheduler endpoints - serve local mock files
  http.get(`${baseUrl}/scheduler`, async () => {
    const response = await fetch(`${MOCK_ASSET_BASE}/scheduler.json`);
    const data = await response.json();
    return HttpResponse.json(data);
  }),

  http.get(`${baseUrl}/scheduler-conf`, async () => {
    const response = await fetch(`${MOCK_ASSET_BASE}/scheduler-conf.json`);
    const data = await response.json();
    return HttpResponse.json(data);
  }),

  http.put(`${baseUrl}/scheduler-conf`, async ({ request }) => {
    // Simulate processing time
    await new Promise((resolve) => setTimeout(resolve, 500));

    const changes = await request.json();
    console.log('Mock: Applying configuration changes:', changes);

    return HttpResponse.json({
      response: 'Configuration updated successfully',
    });
  }),

  http.post(`${baseUrl}/scheduler-conf/validate`, async ({ request }) => {
    // Simulate processing time
    await new Promise((resolve) => setTimeout(resolve, 300));

    const changes = await request.json();
    console.log('Mock: Validating configuration changes:', changes);

    // Always return success for mock mode
    return HttpResponse.json({
      validation: 'success',
    });
  }),

  http.get(`${baseUrl}/scheduler-conf/version`, () => {
    return HttpResponse.json({
      versionId: 15,
    });
  }),

  // Node endpoints
  http.get(`${baseUrl}/nodes`, async () => {
    const response = await fetch(`${MOCK_ASSET_BASE}/nodes.json`);
    const data = await response.json();
    return HttpResponse.json(data);
  }),

  // Node labels endpoints
  http.get(`${baseUrl}/get-node-labels`, async () => {
    const response = await fetch(`${MOCK_ASSET_BASE}/get-node-labels.json`);
    const data = await response.json();
    return HttpResponse.json(data);
  }),

  http.get(`${baseUrl}/get-node-to-labels`, async () => {
    const response = await fetch(`${MOCK_ASSET_BASE}/get-node-to-labels.json`);
    const data = await response.json();
    return HttpResponse.json(data);
  }),

  http.post(`${baseUrl}/add-node-labels`, async ({ request }) => {
    const body = await request.json();
    console.log('Mock: Adding node labels:', body);
    return HttpResponse.json({ message: 'Labels added successfully' });
  }),

  http.post(`${baseUrl}/replace-node-to-labels`, async ({ request }) => {
    const body = await request.json();
    console.log('Mock: Replacing node labels:', body);
    return HttpResponse.json({ message: 'Node labels replaced successfully' });
  }),

  http.post(`${baseUrl}/remove-node-labels`, async ({ request }) => {
    const body = await request.text();
    console.log('Mock: Removing node labels:', body);
    return HttpResponse.json({ message: 'Labels removed successfully' });
  }),

  // Configuration endpoint (at root level, not under baseUrl)
  http.get('/conf', ({ request }) => {
    const url = new URL(request.url);
    const configName = url.searchParams.get('name');

    // Handle HTTP authentication mode query
    if (configName === HTTP_AUTH_PROPERTY) {
      return HttpResponse.json({
        property: {
          name: HTTP_AUTH_PROPERTY,
          value: 'simple',
        },
      });
    }

    // Handle read-only mode query
    if (configName === READ_ONLY_PROPERTY) {
      // Default to false (writable mode) for development
      // Set VITE_READONLY_MODE=true to test read-only mode
      const readOnlyValue = import.meta.env.VITE_READONLY_MODE === 'true' ? 'true' : 'false';
      return HttpResponse.json({
        property: {
          name: READ_ONLY_PROPERTY,
          value: readOnlyValue,
        },
      });
    }

    // Return empty/not found for unknown configs
    return HttpResponse.json(
      {
        property: {
          name: configName || '',
          value: '',
        },
      },
      { status: 404 },
    );
  }),
];

export const handlers: HttpHandler[] = mockMode === 'cluster' ? [] : staticHandlers;
