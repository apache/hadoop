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


import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { DiagnosticsDialog } from './DiagnosticsDialog';
import { useSchedulerStore } from '~/stores/schedulerStore';

vi.mock('~/stores/schedulerStore');

// Mock UI primitives that rely on portals or complex behaviors
vi.mock('~/components/ui/dialog', () => ({
  Dialog: ({ children }: any) => <div>{children}</div>,
  DialogTrigger: ({ children }: any) => <>{children}</>,
  DialogContent: ({ children }: any) => <div>{children}</div>,
  DialogHeader: ({ children }: any) => <div>{children}</div>,
  DialogTitle: ({ children }: any) => <h2>{children}</h2>,
  DialogDescription: ({ children }: any) => <p>{children}</p>,
  DialogFooter: ({ children }: any) => <div>{children}</div>,
}));

vi.mock('~/components/ui/tooltip', () => ({
  TooltipProvider: ({ children }: any) => <>{children}</>,
  TooltipTrigger: ({ children }: any) => <>{children}</>,
  TooltipContent: ({ children }: any) => <>{children}</>,
  Tooltip: ({ children }: any) => <>{children}</>,
}));

vi.mock('~/components/ui/checkbox', () => ({
  Checkbox: ({ id, checked, onCheckedChange }: any) => (
    <input
      id={id}
      type="checkbox"
      role="checkbox"
      aria-checked={checked}
      checked={checked}
      onChange={(event) => onCheckedChange?.(event.currentTarget.checked)}
    />
  ),
}));

describe('DiagnosticsDialog', () => {
  const createStoreState = () => ({
    configData: new Map([
      ['yarn.scheduler.capacity.root.capacity', '100'],
      ['yarn.scheduler.capacity.root.default.capacity', '50'],
    ]),
    configVersion: 7,
    schedulerData: { queueName: 'root', queuePath: 'root', queues: { queue: [] } } as any,
    nodeLabels: [{ name: 'x', exclusivity: true }],
    nodeToLabels: [{ nodeId: 'node-1', nodeLabels: ['x'] }],
    nodes: [],
  });

  let storeState: ReturnType<typeof createStoreState>;

  beforeEach(() => {
    vi.clearAllMocks();
    storeState = createStoreState();
    vi.mocked(useSchedulerStore).mockImplementation((selector: any) => selector(storeState));
  });

  it('disables download when no datasets are selected', async () => {
    const user = userEvent.setup();
    render(<DiagnosticsDialog />);

    const downloadButton = screen.getByRole('button', { name: /^download$/i });
    expect(downloadButton).not.toBeDisabled();

    await user.click(screen.getByLabelText('Scheduler Configuration'));
    await user.click(screen.getByLabelText('Scheduler Info'));

    expect(downloadButton).toBeDisabled();

    await user.click(screen.getByLabelText('Node Labels'));
    expect(downloadButton).not.toBeDisabled();
  });

  it('creates a downloadable diagnostics bundle with selected datasets', async () => {
    const user = userEvent.setup();
    render(<DiagnosticsDialog />);

    const originalCreateObjectURL = URL.createObjectURL;
    const originalRevokeObjectURL = URL.revokeObjectURL;
    const createObjectURLMock = vi.fn(() => 'blob:url');
    const revokeObjectURLMock = vi.fn();

    Object.assign(URL, {
      createObjectURL: createObjectURLMock,
      revokeObjectURL: revokeObjectURLMock,
    });

    const originalCreateElement = document.createElement.bind(document);
    const anchorElement = originalCreateElement('a');
    const clickSpy = vi.spyOn(anchorElement, 'click').mockImplementation(() => {});
    const createElementMock = vi
      .spyOn(document, 'createElement')
      .mockImplementation((tagName: string) => {
        if (tagName === 'a') {
          return anchorElement;
        }
        return originalCreateElement(tagName);
      });

    const downloadButton = screen.getByRole('button', { name: /^download$/i });

    try {
      await user.click(downloadButton);

      expect(createObjectURLMock).toHaveBeenCalledTimes(1);
      const firstCall = createObjectURLMock.mock.calls[0] as unknown[] | undefined;
      expect(firstCall).toBeDefined();
      const blobArg = firstCall?.[0];
      expect(blobArg).toBeInstanceOf(Blob);
      const payloadText = await (blobArg as Blob).text();
      const payload = JSON.parse(payloadText);

      expect(revokeObjectURLMock).toHaveBeenCalledWith('blob:url');
      expect(clickSpy).toHaveBeenCalledTimes(1);
      expect(anchorElement.href).toBe('blob:url');
      expect(anchorElement.download).toMatch(/^yarn-diagnostics-.*\.json$/);
      expect(Object.keys(payload.datasets)).toEqual(['schedulerConf', 'schedulerInfo']);
      const expectedProperties = Object.fromEntries(
        Array.from(storeState.configData.entries()).sort(([a], [b]) => a.localeCompare(b)),
      );
      expect(payload.datasets.schedulerConf).toEqual({
        version: storeState.configVersion,
        properties: expectedProperties,
      });
      expect(payload.datasets.schedulerInfo).toEqual(storeState.schedulerData);
    } finally {
      createElementMock.mockRestore();
      clickSpy.mockRestore();
      Object.assign(URL, {
        createObjectURL: originalCreateObjectURL,
        revokeObjectURL: originalRevokeObjectURL,
      });
    }
  });
});
