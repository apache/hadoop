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


import { http, HttpResponse } from 'msw';
import { readFileSync } from 'fs';
import { join } from 'path';
import { HTTP_AUTH_PROPERTY } from '~/config';

// Helper to load JSON files for server-side testing
function loadMockData(filename: string) {
  try {
    const filePath = join(process.cwd(), 'public', 'mock', 'ws', 'v1', 'cluster', filename);
    const data = readFileSync(filePath, 'utf-8');
    return JSON.parse(data);
  } catch (error) {
    console.error(`Failed to load mock data from ${filename}:`, error);
    throw new Error(`Mock data file ${filename} not found`);
  }
}

export const serverHandlers = [
  http.get('*/conf', ({ request }) => {
    const url = new URL(request.url);
    const name = url.searchParams.get('name');

    if (name === HTTP_AUTH_PROPERTY) {
      return HttpResponse.json({ property: { value: 'simple' } });
    }

    return HttpResponse.json({ property: { value: '' } });
  }),

  // Scheduler endpoints - load from static files for testing
  http.get('/ws/v1/cluster/scheduler', () => {
    const data = loadMockData('scheduler.json');
    return HttpResponse.json(data);
  }),

  http.get('/ws/v1/cluster/scheduler-conf', () => {
    const data = loadMockData('scheduler-conf.json');
    return HttpResponse.json(data);
  }),

  http.put('/ws/v1/cluster/scheduler-conf', async ({ request }) => {
    // Simulate processing time
    await new Promise((resolve) => setTimeout(resolve, 100));

    const changes = await request.json();
    console.log('Mock: Applying configuration changes:', changes);

    return HttpResponse.json({
      response: 'Configuration updated successfully',
    });
  }),

  http.get('/ws/v1/cluster/scheduler-conf/version', () => {
    return HttpResponse.json({
      versionId: 1234567890,
    });
  }),

  // Node endpoints
  http.get('/ws/v1/cluster/nodes', () => {
    const data = loadMockData('nodes.json');
    return HttpResponse.json(data);
  }),

  // Node labels endpoints
  http.get('/ws/v1/cluster/get-node-labels', () => {
    const data = loadMockData('get-node-labels.json');
    return HttpResponse.json(data);
  }),

  http.get('/ws/v1/cluster/get-node-to-labels', () => {
    const data = loadMockData('get-node-to-labels.json');
    return HttpResponse.json(data);
  }),

  http.post('/ws/v1/cluster/add-node-labels', async ({ request }) => {
    const body = await request.json();
    console.log('Mock: Adding node labels:', body);
    return HttpResponse.json({ message: 'Labels added successfully' });
  }),

  http.post('/ws/v1/cluster/replace-node-to-labels', async ({ request }) => {
    const body = await request.json();
    console.log('Mock: Replacing node labels:', body);
    return HttpResponse.json({ message: 'Node labels replaced successfully' });
  }),

  http.post('/ws/v1/cluster/remove-node-labels', async ({ request }) => {
    const body = await request.text();
    console.log('Mock: Removing node labels:', body);
    return HttpResponse.json({ message: 'Labels removed successfully' });
  }),
];
