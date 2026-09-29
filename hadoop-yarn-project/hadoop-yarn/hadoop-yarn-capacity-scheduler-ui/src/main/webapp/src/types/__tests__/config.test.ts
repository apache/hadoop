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


import { describe, it, expect } from 'vitest';
import type {
  ConfigData,
  ConfigProperty,
  SchedConfUpdateInfo,
  QueueMutationParams,
  GlobalUpdateParams,
} from '~/types/config';

describe('ConfigData interface', () => {
  it('should accept valid scheduler configuration response', () => {
    const configData: ConfigData = {
      property: [
        {
          name: 'yarn.scheduler.capacity.root.capacity',
          value: '100',
        },
        {
          name: 'yarn.scheduler.capacity.root.production.capacity',
          value: '70',
        },
        {
          name: 'yarn.scheduler.capacity.root.development.capacity',
          value: '30',
        },
      ],
    };

    expect(configData.property).toHaveLength(3);
    expect(configData.property[0].name).toBe('yarn.scheduler.capacity.root.capacity');
    expect(configData.property[0].value).toBe('100');
  });

  it('should handle empty configuration', () => {
    const emptyConfig: ConfigData = {
      property: [],
    };

    expect(emptyConfig.property).toHaveLength(0);
  });
});

describe('ConfigProperty interface', () => {
  it('should accept standard queue properties', () => {
    const queueProperty: ConfigProperty = {
      name: 'yarn.scheduler.capacity.root.production.maximum-capacity',
      value: '100',
    };

    expect(queueProperty.name).toContain('maximum-capacity');
    expect(queueProperty.value).toBe('100');
  });

  it('should accept node label properties', () => {
    const labelProperty: ConfigProperty = {
      name: 'yarn.scheduler.capacity.root.production.accessible-node-labels.gpu.capacity',
      value: '80',
    };

    expect(labelProperty.name).toContain('accessible-node-labels');
    expect(labelProperty.name).toContain('gpu');
    expect(labelProperty.value).toBe('80');
  });

  it('should accept global scheduler properties', () => {
    const globalProperty: ConfigProperty = {
      name: 'yarn.scheduler.capacity.maximum-applications',
      value: '10000',
    };

    expect(globalProperty.name).toBe('yarn.scheduler.capacity.maximum-applications');
    expect(globalProperty.value).toBe('10000');
  });
});

describe('SchedConfUpdateInfo interface', () => {
  it('should accept add queue mutation', () => {
    const addQueueMutation: SchedConfUpdateInfo = {
      'add-queue': [
        {
          'queue-name': 'root.production.batch',
          params: {
            entry: [
              { key: 'capacity', value: '60' },
              { key: 'maximum-capacity', value: '100' },
            ],
          },
        },
      ],
    };

    expect(addQueueMutation['add-queue']).toHaveLength(1);
    expect(addQueueMutation['add-queue']?.[0]['queue-name']).toBe('root.production.batch');
    expect(addQueueMutation['add-queue']?.[0].params.entry[0]).toEqual({
      key: 'capacity',
      value: '60',
    });
  });

  it('should accept update queue mutation', () => {
    const updateQueueMutation: SchedConfUpdateInfo = {
      'update-queue': [
        {
          'queue-name': 'root.production',
          params: {
            entry: [
              { key: 'capacity', value: '80' },
              { key: 'user-limit-factor', value: '2' },
            ],
          },
        },
      ],
    };

    expect(updateQueueMutation['update-queue']).toHaveLength(1);
    expect(updateQueueMutation['update-queue']?.[0].params.entry[0]).toEqual({
      key: 'capacity',
      value: '80',
    });
  });

  it('should accept remove queue mutation', () => {
    const removeQueueMutation: SchedConfUpdateInfo = {
      'remove-queue': 'root.development.experimental',
    };

    expect(removeQueueMutation['remove-queue']).toBe('root.development.experimental');
  });

  it('should accept global updates', () => {
    const globalUpdateMutation: SchedConfUpdateInfo = {
      'global-updates': [
        {
          entry: [
            {
              key: 'yarn.scheduler.capacity.maximum-applications',
              value: '20000',
            },
            {
              key: 'yarn.scheduler.capacity.maximum-am-resource-percent',
              value: '0.3',
            },
          ],
        },
      ],
    };

    expect(globalUpdateMutation['global-updates']).toBeDefined();
    expect(globalUpdateMutation['global-updates']?.[0].entry[0]).toEqual({
      key: 'yarn.scheduler.capacity.maximum-applications',
      value: '20000',
    });
  });

  it('should accept combined mutations', () => {
    const combinedMutation: SchedConfUpdateInfo = {
      'add-queue': [
        {
          'queue-name': 'root.production.interactive',
          params: {
            entry: [{ key: 'capacity', value: '40' }],
          },
        },
      ],
      'update-queue': [
        {
          'queue-name': 'root.production.batch',
          params: {
            entry: [{ key: 'capacity', value: '60' }],
          },
        },
      ],
      'remove-queue': 'root.production.old-queue',
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

    expect(combinedMutation['add-queue']).toHaveLength(1);
    expect(combinedMutation['update-queue']).toHaveLength(1);
    expect(combinedMutation['remove-queue']).toBe('root.production.old-queue');
    expect(combinedMutation['global-updates']).toBeDefined();
  });
});

describe('QueueMutationParams interface', () => {
  it('should accept queue-specific parameters', () => {
    const params: QueueMutationParams = {
      'queue-name': 'root.production',
      params: {
        entry: [
          { key: 'capacity', value: '70' },
          { key: 'maximum-capacity', value: '100' },
          { key: 'user-limit-factor', value: '1.5' },
          { key: 'state', value: 'RUNNING' },
          { key: 'queues', value: 'batch,interactive' },
          { key: 'accessible-node-labels', value: 'gpu,fpga' },
          { key: 'default-node-label-expression', value: 'gpu' },
          { key: 'maximum-am-resource-percent', value: '0.2' },
          { key: 'minimum-user-limit-percent', value: '10' },
          { key: 'maximum-applications', value: '1000' },
          { key: 'acl-submit-applications', value: 'user1,user2' },
          { key: 'acl-administer-queue', value: 'admin1,admin2' },
        ],
      },
    };

    expect(params['queue-name']).toBe('root.production');
    const paramMap = Object.fromEntries(params.params.entry.map(({ key, value }) => [key, value]));
    expect(paramMap.capacity).toBe('70');
    expect(paramMap.queues).toBe('batch,interactive');
    expect(paramMap['accessible-node-labels']).toBe('gpu,fpga');
  });

  it('should accept node label specific capacity', () => {
    const labelParams: QueueMutationParams = {
      'queue-name': 'root.production',
      params: {
        entry: [
          { key: 'accessible-node-labels.gpu.capacity', value: '80' },
          { key: 'accessible-node-labels.gpu.maximum-capacity', value: '100' },
          { key: 'accessible-node-labels.fpga.capacity', value: '50' },
        ],
      },
    };

    const paramMap = Object.fromEntries(
      labelParams.params.entry.map(({ key, value }) => [key, value]),
    );
    expect(paramMap['accessible-node-labels.gpu.capacity']).toBe('80');
    expect(paramMap['accessible-node-labels.fpga.capacity']).toBe('50');
  });
});

describe('GlobalUpdateParams interface', () => {
  it('should accept global scheduler parameters', () => {
    const globalParams: GlobalUpdateParams = {
      entry: [
        { key: 'yarn.scheduler.capacity.maximum-applications', value: '10000' },
        { key: 'yarn.scheduler.capacity.maximum-am-resource-percent', value: '0.1' },
        {
          key: 'yarn.scheduler.capacity.resource-calculator',
          value: 'org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator',
        },
        { key: 'yarn.scheduler.capacity.root.queues', value: 'production,development,marketing' },
        { key: 'yarn.scheduler.capacity.queue-mappings-override.enable', value: 'true' },
        {
          key: 'yarn.scheduler.capacity.per-node-heartbeat.maximum-offswitch-assignments',
          value: '1',
        },
      ],
    };

    const paramMap = Object.fromEntries(globalParams.entry.map(({ key, value }) => [key, value]));
    expect(paramMap['yarn.scheduler.capacity.maximum-applications']).toBe('10000');
    expect(paramMap['yarn.scheduler.capacity.root.queues']).toBe(
      'production,development,marketing',
    );
  });
});
