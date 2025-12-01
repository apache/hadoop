import { describe, it, expect, vi } from 'vitest';
import { flattenQueueTree, traverseQueueTree, findQueueByPath } from './treeUtils';
import type { QueueInfo } from '~/types';

describe('treeUtils', () => {
  // Helper to create test queue tree
  const createTestQueueTree = (): QueueInfo => ({
    queueName: 'root',
    queuePath: 'root',
    capacity: 100,
    usedCapacity: 0,
    maxCapacity: 100,
    absoluteCapacity: 100,
    absoluteMaxCapacity: 100,
    absoluteUsedCapacity: 0,
    numApplications: 0,
    numActiveApplications: 0,
    numPendingApplications: 0,
    queueType: 'leaf' as const,
    state: 'RUNNING',
    queues: {
      queue: [
        {
          queueName: 'default',
          queuePath: 'root.default',
          capacity: 40,
          usedCapacity: 0,
          maxCapacity: 60,
          absoluteCapacity: 40,
          absoluteMaxCapacity: 60,
          absoluteUsedCapacity: 0,
          numApplications: 0,
          numActiveApplications: 0,
          numPendingApplications: 0,
          queueType: 'leaf' as const,
          state: 'RUNNING',
        },
        {
          queueName: 'production',
          queuePath: 'root.production',
          capacity: 60,
          usedCapacity: 0,
          maxCapacity: 100,
          absoluteCapacity: 60,
          absoluteMaxCapacity: 100,
          absoluteUsedCapacity: 0,
          numApplications: 0,
          numActiveApplications: 0,
          numPendingApplications: 0,
          queueType: 'leaf' as const,
          state: 'RUNNING',
          queues: {
            queue: [
              {
                queueName: 'team1',
                queuePath: 'root.production.team1',
                capacity: 50,
                usedCapacity: 0,
                maxCapacity: 100,
                absoluteCapacity: 30,
                absoluteMaxCapacity: 60,
                absoluteUsedCapacity: 0,
                numApplications: 0,
                numActiveApplications: 0,
                numPendingApplications: 0,
                queueType: 'leaf' as const,
                state: 'RUNNING',
              },
              {
                queueName: 'team2',
                queuePath: 'root.production.team2',
                capacity: 50,
                usedCapacity: 0,
                maxCapacity: 100,
                absoluteCapacity: 30,
                absoluteMaxCapacity: 60,
                absoluteUsedCapacity: 0,
                numApplications: 0,
                numActiveApplications: 0,
                numPendingApplications: 0,
                queueType: 'leaf' as const,
                state: 'RUNNING',
              },
            ],
          },
        },
      ],
    },
  });

  describe('flattenQueueTree', () => {
    it('should flatten a queue tree into an array', () => {
      const root = createTestQueueTree();
      const flattened = flattenQueueTree(root);

      expect(flattened).toHaveLength(5);
      expect(flattened.map((q) => q.queuePath)).toEqual([
        'root',
        'root.default',
        'root.production',
        'root.production.team1',
        'root.production.team2',
      ]);
    });

    it('should handle a single queue with no children', () => {
      const singleQueue: QueueInfo = {
        queueName: 'root',
        queuePath: 'root',
        capacity: 100,
        usedCapacity: 0,
        maxCapacity: 100,
        absoluteCapacity: 100,
        absoluteMaxCapacity: 100,
        absoluteUsedCapacity: 0,
        numApplications: 0,
        numActiveApplications: 0,
        numPendingApplications: 0,
        queueType: 'leaf' as const,
        state: 'RUNNING',
      };

      const flattened = flattenQueueTree(singleQueue);
      expect(flattened).toHaveLength(1);
      expect(flattened[0]).toBe(singleQueue);
    });

    it('should handle empty queues object', () => {
      const queueWithEmptyQueues: QueueInfo = {
        queueName: 'root',
        queuePath: 'root',
        capacity: 100,
        usedCapacity: 0,
        maxCapacity: 100,
        absoluteCapacity: 100,
        absoluteMaxCapacity: 100,
        absoluteUsedCapacity: 0,
        numApplications: 0,
        numActiveApplications: 0,
        numPendingApplications: 0,
        queueType: 'leaf' as const,
        state: 'RUNNING',
        queues: {
          queue: [],
        },
      };

      const flattened = flattenQueueTree(queueWithEmptyQueues);
      expect(flattened).toHaveLength(1);
    });
  });

  describe('traverseQueueTree', () => {
    it('should call callback for each queue with correct depth', () => {
      const root = createTestQueueTree();
      const callback = vi.fn();

      traverseQueueTree(root, callback);

      expect(callback).toHaveBeenCalledTimes(5);

      // Check calls with correct depths
      expect(callback).toHaveBeenNthCalledWith(1, root, 0, undefined);
      expect(callback).toHaveBeenNthCalledWith(2, root.queues!.queue![0], 1, root); // default
      expect(callback).toHaveBeenNthCalledWith(3, root.queues!.queue![1], 1, root); // production
      expect(callback).toHaveBeenNthCalledWith(
        4,
        root.queues!.queue![1].queues!.queue![0],
        2,
        root.queues!.queue![1],
      ); // team1
      expect(callback).toHaveBeenNthCalledWith(
        5,
        root.queues!.queue![1].queues!.queue![1],
        2,
        root.queues!.queue![1],
      ); // team2
    });

    it('should provide parent reference in callback', () => {
      const root = createTestQueueTree();
      const parents: Array<QueueInfo | undefined> = [];

      traverseQueueTree(root, (_1, _2, parent) => {
        parents.push(parent);
      });

      expect(parents[0]).toBeUndefined(); // root has no parent
      expect(parents[1]).toBe(root); // default's parent is root
      expect(parents[2]).toBe(root); // production's parent is root
      expect(parents[3]).toBe(root.queues!.queue![1]); // team1's parent is production
      expect(parents[4]).toBe(root.queues!.queue![1]); // team2's parent is production
    });
  });

  describe('findQueueByPath', () => {
    it('should find queue by path', () => {
      const root = createTestQueueTree();

      const found = findQueueByPath(root, 'root.production.team1');
      expect(found).not.toBeNull();
      expect(found?.queueName).toBe('team1');
      expect(found?.queuePath).toBe('root.production.team1');
    });

    it('should find root queue', () => {
      const root = createTestQueueTree();

      const found = findQueueByPath(root, 'root');
      expect(found).toBe(root);
    });

    it('should return null for non-existent path', () => {
      const root = createTestQueueTree();

      const found = findQueueByPath(root, 'root.nonexistent');
      expect(found).toBeNull();
    });

    it('should return null for partial path', () => {
      const root = createTestQueueTree();

      const found = findQueueByPath(root, 'production');
      expect(found).toBeNull();
    });

    it('should handle queue with no children', () => {
      const singleQueue: QueueInfo = {
        queueName: 'root',
        queuePath: 'root',
        capacity: 100,
        usedCapacity: 0,
        maxCapacity: 100,
        absoluteCapacity: 100,
        absoluteMaxCapacity: 100,
        absoluteUsedCapacity: 0,
        numApplications: 0,
        numActiveApplications: 0,
        numPendingApplications: 0,
        queueType: 'leaf' as const,
        state: 'RUNNING',
      };

      const found = findQueueByPath(singleQueue, 'root');
      expect(found).toBe(singleQueue);

      const notFound = findQueueByPath(singleQueue, 'root.child');
      expect(notFound).toBeNull();
    });
  });
});
