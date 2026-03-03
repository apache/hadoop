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


import { describe, it, expect, beforeAll, afterEach, afterAll } from 'vitest';
import { setupServer } from 'msw/node';
import { http, HttpResponse, delay } from 'msw';
import { YarnApiClient } from './YarnApiClient';
import { serverHandlers } from './mocks/server-handlers';
import type {
  SchedulerResponse,
  SchedConfUpdateInfo,
  YarnErrorResponse,
  NodeLabelsResponse,
  NodeToLabelsResponse,
  VersionResponse,
  QueueInfo,
  ConfigProperty,
} from '~/types';

// Mock data for tests
const mockSchedulerResponse: SchedulerResponse = {
  scheduler: {
    schedulerInfo: {
      type: 'capacityScheduler',
      capacity: 100,
      usedCapacity: 45.5,
      maxCapacity: 100,
      queueName: 'root',
      queues: {
        queue: [
          {
            queueType: 'leaf',
            capacity: 50,
            usedCapacity: 80,
            maxCapacity: 100,
            absoluteCapacity: 50,
            absoluteMaxCapacity: 100,
            absoluteUsedCapacity: 40,
            numApplications: 5,
            numActiveApplications: 3,
            numPendingApplications: 2,
            queueName: 'default',
            queuePath: 'root.default',
            state: 'RUNNING',
          },
          {
            queueType: 'parent',
            capacity: 50,
            usedCapacity: 31,
            maxCapacity: 100,
            absoluteCapacity: 50,
            absoluteMaxCapacity: 100,
            absoluteUsedCapacity: 15.5,
            numApplications: 3,
            numActiveApplications: 2,
            numPendingApplications: 1,
            queueName: 'production',
            queuePath: 'root.production',
            state: 'RUNNING',
            queues: {
              queue: [
                {
                  queueType: 'leaf',
                  capacity: 60,
                  usedCapacity: 50,
                  maxCapacity: 100,
                  absoluteCapacity: 30,
                  absoluteMaxCapacity: 50,
                  absoluteUsedCapacity: 15,
                  numApplications: 2,
                  numActiveApplications: 1,
                  numPendingApplications: 1,
                  queueName: 'batch',
                  queuePath: 'root.production.batch',
                  state: 'RUNNING',
                },
                {
                  queueType: 'leaf',
                  capacity: 40,
                  usedCapacity: 1.25,
                  maxCapacity: 100,
                  absoluteCapacity: 20,
                  absoluteMaxCapacity: 50,
                  absoluteUsedCapacity: 0.5,
                  numApplications: 1,
                  numActiveApplications: 1,
                  numPendingApplications: 0,
                  queueName: 'interactive',
                  queuePath: 'root.production.interactive',
                  state: 'RUNNING',
                },
              ],
            },
          },
        ],
      },
    },
  },
};

const mockVersionResponse: VersionResponse = {
  versionId: 1234567890,
};

// Mock server setup using centralized handlers
const server = setupServer(...serverHandlers);

beforeAll(() => server.listen());
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

describe('YarnApiClient', () => {
  describe('constructor and configuration', () => {
    it('should create instance with default configuration', () => {
      const client = new YarnApiClient('/ws/v1/cluster');
      expect(client).toBeDefined();
    });

    it('should create instance with custom configuration', () => {
      const client = new YarnApiClient('/ws/v1/cluster', {
        timeout: 5000,
        headers: { 'X-Custom': 'header' },
      });
      expect(client).toBeDefined();
    });

    it('should handle authentication headers', () => {
      const client = new YarnApiClient('/ws/v1/cluster', {
        headers: {
          Authorization: 'Bearer token123',
        },
      });
      expect(client).toBeDefined();
    });

    it('should handle baseUrl with trailing slash', () => {
      const client1 = new YarnApiClient('/ws/v1/cluster/');
      const client2 = new YarnApiClient('/ws/v1/cluster');

      expect(client1).toBeDefined();
      expect(client2).toBeDefined();
    });
  });

  describe('getScheduler', () => {
    it('should fetch scheduler data successfully', async () => {
      const client = new YarnApiClient('/ws/v1/cluster');
      const response = await client.getScheduler();

      expect(response.scheduler.schedulerInfo.type).toBe('capacityScheduler');
      expect(response.scheduler.schedulerInfo.queueName).toBe('root');
      expect(response.scheduler.schedulerInfo.queues.queue).toBeDefined();
      expect(Array.isArray(response.scheduler.schedulerInfo.queues.queue)).toBe(true);
    });

    it('should handle nested queue structure', async () => {
      const client = new YarnApiClient('/ws/v1/cluster');
      const response = await client.getScheduler();

      const queues = response.scheduler.schedulerInfo.queues.queue;
      expect(queues.length).toBeGreaterThan(0);

      // Find a parent queue that has children
      const parentQueue = queues.find((q: QueueInfo) => q.queues?.queue);
      if (parentQueue) {
        expect(parentQueue.queues?.queue).toBeDefined();
        expect(Array.isArray(parentQueue.queues?.queue)).toBe(true);
      }
    });

    it('should include custom headers in request', async () => {
      const customHeaders = { 'X-Custom-Header': 'test-value' };
      let capturedHeaders: Headers | undefined;

      server.use(
        http.get('*/ws/v1/cluster/scheduler', ({ request }) => {
          capturedHeaders = request.headers;
          return HttpResponse.json(mockSchedulerResponse);
        }),
      );

      const client = new YarnApiClient('/ws/v1/cluster', {
        headers: customHeaders,
      });
      await client.getScheduler();

      expect(capturedHeaders?.get('X-Custom-Header')).toBe('test-value');
      expect(capturedHeaders?.get('Accept')).toBe('application/json');
    });
  });

  describe('getSchedulerConf', () => {
    it('should fetch configuration data successfully', async () => {
      const client = new YarnApiClient('/ws/v1/cluster');
      const response = await client.getSchedulerConf();

      expect(response.property).toBeDefined();
      expect(Array.isArray(response.property)).toBe(true);
      expect(response.property.length).toBeGreaterThan(0);

      // Check that all properties have name and value
      response.property.forEach((prop: ConfigProperty) => {
        expect(prop.name).toBeDefined();
        expect(prop.value).toBeDefined();
      });
    });

    it('should handle empty configuration', async () => {
      server.use(
        http.get('*/ws/v1/cluster/scheduler-conf', () => {
          return HttpResponse.json({ property: [] });
        }),
      );

      const client = new YarnApiClient('/ws/v1/cluster');
      const response = await client.getSchedulerConf();

      expect(response.property).toEqual([]);
    });
  });

  describe('updateSchedulerConf', () => {
    it('should update configuration successfully', async () => {
      server.use(
        http.put('*/ws/v1/cluster/scheduler-conf', () => {
          return new HttpResponse(null, { status: 200 });
        }),
      );

      const client = new YarnApiClient('/ws/v1/cluster');
      const updateRequest: SchedConfUpdateInfo = {
        'update-queue': [
          {
            'queue-name': 'root.default',
            params: {
              entry: [
                { key: 'capacity', value: '60' },
                { key: 'maximum-capacity', value: '100' },
              ],
            },
          },
        ],
      };

      await expect(client.updateSchedulerConf(updateRequest)).resolves.not.toThrow();
    });

    it('should send correct content type header', async () => {
      let capturedHeaders: Headers | undefined;

      server.use(
        http.put('*/ws/v1/cluster/scheduler-conf', ({ request }) => {
          capturedHeaders = request.headers;
          return new HttpResponse(null, { status: 200 });
        }),
      );

      const client = new YarnApiClient('/ws/v1/cluster');
      await client.updateSchedulerConf({
        'global-updates': [{ entry: [{ key: 'test.property', value: 'value' }] }],
      });

      expect(capturedHeaders?.get('Content-Type')).toBe('application/json');
    });

    it('should handle complex mutation requests', async () => {
      let capturedBody: SchedConfUpdateInfo | undefined;

      server.use(
        http.put('*/ws/v1/cluster/scheduler-conf', async ({ request }) => {
          capturedBody = (await request.json()) as SchedConfUpdateInfo;
          return new HttpResponse(null, { status: 200 });
        }),
      );

      const client = new YarnApiClient('/ws/v1/cluster');
      const complexUpdate: SchedConfUpdateInfo = {
        'add-queue': [
          {
            'queue-name': 'root.test',
            params: {
              entry: [
                { key: 'capacity', value: '10' },
                { key: 'maximum-capacity', value: '50' },
              ],
            },
          },
        ],
        'update-queue': [
          {
            'queue-name': 'root.default',
            params: {
              entry: [{ key: 'capacity', value: '45' }],
            },
          },
        ],
        'remove-queue': 'root.production.interactive',
        'global-updates': [
          {
            entry: [
              {
                key: 'yarn.scheduler.capacity.resource-calculator',
                value: 'org.apache.hadoop.yarn.util.resource.DominantResourceCalculator',
              },
            ],
          },
        ],
      };

      await client.updateSchedulerConf(complexUpdate);

      expect(capturedBody).toEqual(complexUpdate);
      expect(capturedBody!['add-queue']).toHaveLength(1);
      expect(capturedBody!['update-queue']).toHaveLength(1);
      expect(capturedBody!['remove-queue']).toBe('root.production.interactive');
      expect(capturedBody!['global-updates']).toBeDefined();
    });
  });

  describe('validateSchedulerConf', () => {
    it('should validate configuration successfully', async () => {
      server.use(
        http.post('*/ws/v1/cluster/scheduler-conf/validate', () => {
          return HttpResponse.json({ validation: 'success' });
        }),
      );

      const client = new YarnApiClient('/ws/v1/cluster');
      const updateRequest: SchedConfUpdateInfo = {
        'update-queue': [
          {
            'queue-name': 'root.default',
            params: { entry: [{ key: 'capacity', value: '60' }] },
          },
        ],
      };

      await expect(client.validateSchedulerConf(updateRequest)).resolves.toEqual({
        validation: 'success',
      });
    });

    it('should pass through request body correctly', async () => {
      let capturedBody: SchedConfUpdateInfo | undefined;

      server.use(
        http.post('*/ws/v1/cluster/scheduler-conf/validate', async ({ request }) => {
          capturedBody = (await request.json()) as SchedConfUpdateInfo;
          return HttpResponse.json({ validation: 'failed', errors: ['oops'] });
        }),
      );

      const client = new YarnApiClient('/ws/v1/cluster');
      const validationRequest: SchedConfUpdateInfo = {
        'add-queue': [
          {
            'queue-name': 'root.newqueue',
            params: {
              entry: [
                { key: 'capacity', value: '10' },
                { key: 'maximum-capacity', value: '50' },
              ],
            },
          },
        ],
      };

      const response = await client.validateSchedulerConf(validationRequest);
      expect(capturedBody).toEqual(validationRequest);
      expect(response).toEqual({ validation: 'failed', errors: ['oops'] });
    });
  });

  describe('getSchedulerConfVersion', () => {
    it('should fetch configuration version successfully', async () => {
      const client = new YarnApiClient('/ws/v1/cluster');
      const response = await client.getSchedulerConfVersion();

      expect(response).toEqual(mockVersionResponse);
      expect(response.versionId).toBe(1234567890);
    });

    it('should handle version endpoint errors', async () => {
      server.use(
        http.get('*/ws/v1/cluster/scheduler-conf/version', () => {
          return new HttpResponse(null, { status: 404 });
        }),
      );

      const client = new YarnApiClient('/ws/v1/cluster');
      await expect(client.getSchedulerConfVersion()).rejects.toThrow('HTTP 404');
    });
  });

  describe('node label endpoints', () => {
    describe('getNodeLabels', () => {
      it('should fetch node labels successfully', async () => {
        const client = new YarnApiClient('/ws/v1/cluster');
        const response = await client.getNodeLabels();

        const labels = response.nodeLabelInfo;
        expect(labels).toBeDefined();
        if (!labels) {
          throw new Error('Expected node labels to be defined');
        }

        const labelArray = Array.isArray(labels) ? labels : [labels];
        const firstLabel = labelArray[0];
        expect(firstLabel.name).toBeDefined();
        expect(['boolean', 'string']).toContain(typeof firstLabel.exclusivity);
      });

      it('should handle empty node labels', async () => {
        server.use(
          http.get('*/ws/v1/cluster/get-node-labels', () => {
            return HttpResponse.json({ nodeLabelInfo: [] });
          }),
        );

        const client = new YarnApiClient('/ws/v1/cluster');
        const response = await client.getNodeLabels();

        expect(response.nodeLabelInfo).toEqual([]);
      });
    });

    describe('addNodeLabels', () => {
      it('should add node labels successfully', async () => {
        server.use(
          http.post('*/ws/v1/cluster/add-node-labels', () => {
            return new HttpResponse(null, { status: 200 });
          }),
        );

        const client = new YarnApiClient('/ws/v1/cluster');
        const labels = [
          { name: 'gpu', exclusivity: true },
          { name: 'ssd', exclusivity: true },
          { name: 'nvme', exclusivity: true },
        ];
        await expect(client.addNodeLabels(labels)).resolves.not.toThrow();
      });

      it('should send labels in correct format', async () => {
        let capturedBody: any;

        server.use(
          http.post('*/ws/v1/cluster/add-node-labels', async ({ request }) => {
            capturedBody = await request.json();
            return new HttpResponse(null, { status: 200 });
          }),
        );

        const client = new YarnApiClient('/ws/v1/cluster');
        const labels = [
          { name: 'label1', exclusivity: true },
          { name: 'label2', exclusivity: false },
        ];
        await client.addNodeLabels(labels);

        expect(capturedBody).toEqual({
          nodeLabelInfo: [
            { name: 'label1', exclusivity: true },
            { name: 'label2', exclusivity: false },
          ],
        });
      });
    });

    describe('removeNodeLabels', () => {
      it('should remove node labels successfully', async () => {
        let capturedUrl: string | undefined;

        server.use(
          http.post('*/ws/v1/cluster/remove-node-labels', ({ request }) => {
            capturedUrl = request.url;
            return new HttpResponse(null, { status: 200 });
          }),
        );

        const client = new YarnApiClient('/ws/v1/cluster');
        await expect(client.removeNodeLabels(['deprecated-label'])).resolves.not.toThrow();
        expect(capturedUrl).toContain('labels=deprecated-label');
      });

      it('should handle removal of non-existent labels', async () => {
        const errorResponse: YarnErrorResponse = {
          RemoteException: {
            exception: 'IOException',
            javaClassName: 'java.io.IOException',
            message: "Node label 'non-existent' not found",
          },
        };

        server.use(
          http.post('*/ws/v1/cluster/remove-node-labels', () => {
            return HttpResponse.json(errorResponse, { status: 400 });
          }),
        );

        const client = new YarnApiClient('/ws/v1/cluster');
        await expect(client.removeNodeLabels(['non-existent'])).rejects.toThrow(
          "Node label 'non-existent' not found",
        );
      });
    });

    describe('getNodeToLabels', () => {
      it('should fetch node to label mappings', async () => {
        const mockMappings: NodeToLabelsResponse = {
          nodeToLabels: {
            entry: [
              {
                key: 'node1.cluster.com:8041',
                value: {
                  nodeLabelInfo: {
                    name: 'gpu',
                    exclusivity: 'true',
                  },
                },
              },
              {
                key: 'node2.cluster.com:8041',
                value: {
                  nodeLabelInfo: {
                    name: 'ssd',
                    exclusivity: 'false',
                  },
                },
              },
            ],
          },
        };

        server.use(
          http.get('*/ws/v1/cluster/get-node-to-labels', () => {
            return HttpResponse.json(mockMappings);
          }),
        );

        const client = new YarnApiClient('/ws/v1/cluster');
        const response = await client.getNodeToLabels();

        const entries = response.nodeToLabels?.entry;
        expect(entries).toBeDefined();

        const arrayEntries = Array.isArray(entries) ? entries : entries ? [entries] : [];
        expect(arrayEntries).toHaveLength(2);
        expect(arrayEntries[0]?.value?.nodeLabelInfo).toBeDefined();
      });
    });

    describe('replaceNodeToLabels', () => {
      it('should replace node labels successfully', async () => {
        server.use(
          http.post('*/ws/v1/cluster/replace-node-to-labels', () => {
            return new HttpResponse(null, { status: 200 });
          }),
        );

        const client = new YarnApiClient('/ws/v1/cluster');
        const mapping = [
          { nodeId: 'node1.cluster.com:8041', labels: ['gpu'] },
          { nodeId: 'node2.cluster.com:8041', labels: ['ssd'] },
        ];

        await expect(client.replaceNodeToLabels(mapping)).resolves.not.toThrow();
      });

      it('should send mapping in correct format', async () => {
        let capturedBody: any;

        server.use(
          http.post('*/ws/v1/cluster/replace-node-to-labels', async ({ request }) => {
            capturedBody = await request.json();
            return new HttpResponse(null, { status: 200 });
          }),
        );

        const client = new YarnApiClient('/ws/v1/cluster');
        const mapping = [
          { nodeId: 'node1', labels: ['label1'] },
          { nodeId: 'node2', labels: ['label2'] },
        ];

        await client.replaceNodeToLabels(mapping);

        expect(capturedBody).toEqual({
          nodeToLabels: [
            { nodeId: 'node1', labels: ['label1'] },
            { nodeId: 'node2', labels: ['label2'] },
          ],
        });
      });
    });
  });

  describe('error handling', () => {
    it('should handle HTTP 500 errors', async () => {
      server.use(
        http.get('*/ws/v1/cluster/scheduler', () => {
          return new HttpResponse(null, {
            status: 500,
            statusText: 'Internal Server Error',
          });
        }),
      );

      const client = new YarnApiClient('/ws/v1/cluster');
      await expect(client.getScheduler()).rejects.toThrow('HTTP 500: Internal Server Error');
    });

    it('should handle YARN RemoteException errors', async () => {
      const errorResponse: YarnErrorResponse = {
        RemoteException: {
          exception: 'AccessControlException',
          javaClassName: 'org.apache.hadoop.security.AccessControlException',
          message: 'User does not have admin privileges',
        },
      };

      server.use(
        http.put('*/ws/v1/cluster/scheduler-conf', () => {
          return HttpResponse.json(errorResponse, { status: 403 });
        }),
      );

      const client = new YarnApiClient('/ws/v1/cluster');
      await expect(
        client.updateSchedulerConf({
          'global-updates': [{ entry: [{ key: 'test', value: 'value' }] }],
        }),
      ).rejects.toThrow('User does not have admin privileges');
    });

    it('should handle network errors', async () => {
      server.use(
        http.get('*/ws/v1/cluster/scheduler', () => {
          throw new Error('Network error');
        }),
      );

      const client = new YarnApiClient('/ws/v1/cluster');
      await expect(client.getScheduler()).rejects.toThrow();
    });

    it('should handle request timeout', async () => {
      server.use(
        http.get('*/ws/v1/cluster/scheduler', async () => {
          await delay(100);
          return HttpResponse.json(mockSchedulerResponse);
        }),
      );

      const client = new YarnApiClient('/ws/v1/cluster', { timeout: 50 });
      await expect(client.getScheduler()).rejects.toThrow('aborted');
    });

    it('should handle malformed JSON response', async () => {
      server.use(
        http.get('*/ws/v1/cluster/scheduler', () => {
          return new HttpResponse('Invalid JSON {', {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
          });
        }),
      );

      const client = new YarnApiClient('/ws/v1/cluster');
      await expect(client.getScheduler()).rejects.toThrow();
    });
  });

  describe('edge cases and special scenarios', () => {
    it('should handle empty response body for successful mutations', async () => {
      server.use(
        http.put('*/ws/v1/cluster/scheduler-conf', () => {
          return new HttpResponse('', { status: 200 });
        }),
      );

      const client = new YarnApiClient('/ws/v1/cluster');
      await expect(
        client.updateSchedulerConf({
          'global-updates': [{ entry: [{ key: 'test', value: 'value' }] }],
        }),
      ).resolves.not.toThrow();
    });

    it('should preserve queue paths with special characters', async () => {
      const specialQueueResponse: SchedulerResponse = {
        scheduler: {
          schedulerInfo: {
            ...mockSchedulerResponse.scheduler.schedulerInfo,
            queues: {
              queue: [
                {
                  queueType: 'leaf',
                  capacity: 100,
                  usedCapacity: 0,
                  maxCapacity: 100,
                  absoluteCapacity: 100,
                  absoluteMaxCapacity: 100,
                  absoluteUsedCapacity: 0,
                  numApplications: 0,
                  numActiveApplications: 0,
                  numPendingApplications: 0,
                  queueName: 'queue-with-dash',
                  queuePath: 'root.queue-with-dash',
                  state: 'RUNNING',
                },
              ],
            },
          },
        },
      };

      server.use(
        http.get('*/ws/v1/cluster/scheduler', () => {
          return HttpResponse.json(specialQueueResponse);
        }),
      );

      const client = new YarnApiClient('/ws/v1/cluster');
      const response = await client.getScheduler();

      expect(response.scheduler.schedulerInfo.queues.queue[0].queueName).toBe('queue-with-dash');
    });

    it('should handle concurrent requests', async () => {
      const client = new YarnApiClient('/ws/v1/cluster');

      const promises = [
        client.getScheduler(),
        client.getSchedulerConf(),
        client.getNodeLabels(),
        client.getSchedulerConfVersion(),
      ];

      const results = await Promise.all(promises);

      expect(results).toHaveLength(4);
      expect(results[0]).toHaveProperty('scheduler');
      expect(results[1]).toHaveProperty('property');
      const nodeLabelsResponse = results[2] as NodeLabelsResponse;
      expect(nodeLabelsResponse.nodeLabelInfo).toBeDefined();
      expect(results[3]).toHaveProperty('versionId');
    });

    it('should handle partial failure in concurrent requests', async () => {
      server.use(
        http.get('*/ws/v1/cluster/scheduler-conf', () => {
          return new HttpResponse(null, { status: 500 });
        }),
      );

      const client = new YarnApiClient('/ws/v1/cluster');

      const results = await Promise.allSettled([
        client.getScheduler(),
        client.getSchedulerConf(),
        client.getNodeLabels(),
      ]);

      expect(results[0].status).toBe('fulfilled');
      expect(results[1].status).toBe('rejected');
      expect(results[2].status).toBe('fulfilled');
    });
  });
});
