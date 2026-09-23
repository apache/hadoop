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


import { useState } from 'react';
import { FileDown } from 'lucide-react';
import { toast } from 'sonner';

import { Button } from '~/components/ui/button';
import { Checkbox } from '~/components/ui/checkbox';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '~/components/ui/dialog';
import { Input } from '~/components/ui/input';
import { Label } from '~/components/ui/label';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '~/components/ui/tooltip';
import {
  DEFAULT_DIAGNOSTIC_BULK_ACTIVITIES_COUNT,
  DEFAULT_DIAGNOSTIC_RM_JSTACK_COUNT,
  MAX_DIAGNOSTIC_BULK_ACTIVITIES_COUNT,
  MAX_DIAGNOSTIC_RM_JSTACK_COUNT,
} from '~/lib/api/YarnApiClient';
import { useSchedulerStore } from '~/stores/schedulerStore';

type DiagnosticDatasetId =
  | 'schedulerConf'
  | 'schedulerInfo'
  | 'nodeLabels'
  | 'nodeToLabels'
  | 'nodes'
  | 'bulkActivities'
  | 'rmJstack';

interface StoreDiagnosticOption {
  id: DiagnosticDatasetId;
  label: string;
  description: string;
  source: 'store';
  data: unknown;
}

interface RemoteDiagnosticOption {
  id: DiagnosticDatasetId;
  label: string;
  description: string;
  source: 'remote';
  countLabel: string;
  countAriaLabel: string;
  requestParamKey: string;
  min: number;
  max: number;
  defaultCount: number;
}

type DiagnosticOption = StoreDiagnosticOption | RemoteDiagnosticOption;

const DEFAULT_SELECTED: DiagnosticDatasetId[] = ['schedulerConf', 'schedulerInfo'];

const REMOTE_DATASET_OPTIONS: RemoteDiagnosticOption[] = [
  {
    id: 'bulkActivities',
    label: 'Scheduler Bulk Activities',
    description: 'Live response from /scheduler/bulk-activities.',
    source: 'remote',
    countLabel: 'Scheduling cycles to record',
    countAriaLabel: 'Bulk activities count',
    requestParamKey: 'activitiesCount',
    min: 1,
    max: MAX_DIAGNOSTIC_BULK_ACTIVITIES_COUNT,
    defaultCount: DEFAULT_DIAGNOSTIC_BULK_ACTIVITIES_COUNT,
  },
  {
    id: 'rmJstack',
    label: 'ResourceManager JStack',
    description: 'Live thread dump from /jstack (ResourceManager JVM).',
    source: 'remote',
    countLabel: 'JStack iterations to collect',
    countAriaLabel: 'ResourceManager jstack count',
    requestParamKey: 'numberOfJStack',
    min: 1,
    max: MAX_DIAGNOSTIC_RM_JSTACK_COUNT,
    defaultCount: DEFAULT_DIAGNOSTIC_RM_JSTACK_COUNT,
  },
];

function parseCountInput(
  rawValue: string,
  bounds: { min: number; max: number; label: string },
): number | null {
  const trimmed = rawValue.trim();
  if (trimmed.length === 0) {
    return null;
  }

  const value = Number(trimmed);
  if (!Number.isInteger(value) || value < bounds.min || value > bounds.max) {
    return null;
  }

  return value;
}

export function DiagnosticsDialog() {
  const configData = useSchedulerStore((state) => state.configData);
  const configVersion = useSchedulerStore((state) => state.configVersion);
  const schedulerData = useSchedulerStore((state) => state.schedulerData);
  const nodeLabels = useSchedulerStore((state) => state.nodeLabels);
  const nodeToLabels = useSchedulerStore((state) => state.nodeToLabels);
  const nodes = useSchedulerStore((state) => state.nodes);
  const apiClient = useSchedulerStore((state) => state.apiClient);

  const [open, setOpen] = useState(false);
  const [selectedDatasets, setSelectedDatasets] = useState<DiagnosticDatasetId[]>(DEFAULT_SELECTED);
  const [isDownloading, setIsDownloading] = useState(false);
  const [remoteCountInputs, setRemoteCountInputs] = useState<
    Record<DiagnosticDatasetId, string>
  >(() =>
    Object.fromEntries(
      REMOTE_DATASET_OPTIONS.map((option) => [option.id, String(option.defaultCount)]),
    ) as Record<DiagnosticDatasetId, string>,
  );

  const entries = Array.from(configData.entries()).sort(([a], [b]) => a.localeCompare(b));
  const schedulerConfiguration = {
    version: configVersion,
    properties: Object.fromEntries(entries),
  };

  const storeDatasetOptions: StoreDiagnosticOption[] = [
    {
      id: 'schedulerConf',
      label: 'Scheduler Configuration',
      description: 'Key/value pairs returned by /scheduler-conf (including version metadata).',
      source: 'store',
      data: schedulerConfiguration,
    },
    {
      id: 'schedulerInfo',
      label: 'Scheduler Info',
      description: 'Current scheduler metrics returned by /scheduler.',
      source: 'store',
      data: schedulerData,
    },
    {
      id: 'nodeLabels',
      label: 'Node Labels',
      description: 'Label definitions from /node-labels.',
      source: 'store',
      data: nodeLabels,
    },
    {
      id: 'nodeToLabels',
      label: 'Node-to-Labels Mapping',
      description: 'Assignments from /node-to-labels.',
      source: 'store',
      data: nodeToLabels,
    },
    {
      id: 'nodes',
      label: 'Nodes',
      description: 'Node metadata returned by /nodes.',
      source: 'store',
      data: nodes,
    },
  ];

  const datasetOptions: DiagnosticOption[] = [...storeDatasetOptions, ...REMOTE_DATASET_OPTIONS];

  const getCountInput = (datasetId: DiagnosticDatasetId): string =>
    remoteCountInputs[datasetId] ?? '';

  const setCountInput = (datasetId: DiagnosticDatasetId, value: string) => {
    setRemoteCountInputs((prev) => ({ ...prev, [datasetId]: value }));
  };

  const toggleDataset = (datasetId: DiagnosticDatasetId, checked: boolean) => {
    setSelectedDatasets((prev) => {
      if (checked) {
        return prev.includes(datasetId) ? prev : [...prev, datasetId];
      }
      return prev.filter((id) => id !== datasetId);
    });
  };

  const resolveRemoteCount = (option: RemoteDiagnosticOption): number | null => {
    return parseCountInput(getCountInput(option.id), {
      min: option.min,
      max: option.max,
      label: option.label,
    });
  };

  const fetchRemoteDataset = async (
    datasetId: DiagnosticDatasetId,
    count: number,
  ): Promise<unknown> => {
    if (datasetId === 'bulkActivities') {
      return apiClient.getBulkSchedulerActivities(count);
    }
    if (datasetId === 'rmJstack') {
      return apiClient.getResourceManagerJstack(count);
    }
    throw new Error(`Unsupported remote diagnostic dataset: ${datasetId}`);
  };

  const hasInvalidSelectedRemoteCounts = REMOTE_DATASET_OPTIONS.some(
    (option) => selectedDatasets.includes(option.id) && resolveRemoteCount(option) === null,
  );

  const handleDownload = async () => {
    if (selectedDatasets.length === 0 || isDownloading || hasInvalidSelectedRemoteCounts) {
      return;
    }

    setIsDownloading(true);

    try {
      const timestamp = new Date().toISOString();
      const payload: Record<string, unknown> = {
        generatedAt: timestamp,
        datasets: {},
        requestParams: {},
      };

      for (const option of storeDatasetOptions) {
        if (selectedDatasets.includes(option.id)) {
          (payload.datasets as Record<string, unknown>)[option.id] = option.data;
        }
      }

      for (const option of REMOTE_DATASET_OPTIONS) {
        if (!selectedDatasets.includes(option.id)) {
          continue;
        }

        const count = resolveRemoteCount(option);
        if (count === null) {
          toast.error(
            `${option.label}: enter a whole number from ${option.min} to ${option.max}.`,
          );
          return;
        }

        (payload.requestParams as Record<string, unknown>)[option.id] = {
          [option.requestParamKey]: count,
        };

        try {
          (payload.datasets as Record<string, unknown>)[option.id] = await fetchRemoteDataset(
            option.id,
            count,
          );
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          toast.error(`Failed to fetch ${option.label}: ${message}`);
          return;
        }
      }

      if (Object.keys(payload.requestParams as Record<string, unknown>).length === 0) {
        delete payload.requestParams;
      }

      const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement('a');
      anchor.href = url;
      anchor.download = `yarn-diagnostics-${timestamp.replace(/[:.]/g, '-')}.json`;
      document.body.appendChild(anchor);
      anchor.click();
      document.body.removeChild(anchor);
      URL.revokeObjectURL(url);
      setOpen(false);
    } finally {
      setIsDownloading(false);
    }
  };

  const isDownloadDisabled =
    selectedDatasets.length === 0 || isDownloading || hasInvalidSelectedRemoteCounts;

  return (
    <TooltipProvider>
      <Dialog open={open} onOpenChange={setOpen}>
        <Tooltip>
          <TooltipTrigger asChild>
            <DialogTrigger asChild>
              <Button variant="ghost" size="icon" aria-label="Download diagnostics">
                <FileDown className="h-[1.2rem] w-[1.2rem]" />
              </Button>
            </DialogTrigger>
          </TooltipTrigger>
          <TooltipContent sideOffset={8}>Download diagnostics</TooltipContent>
        </Tooltip>

        <DialogContent className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle>Download diagnostics</DialogTitle>
            <DialogDescription>
              Choose which YARN API responses to include in the diagnostic bundle.
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-3">
            {datasetOptions.map((option) => {
              const checkboxId = `diagnostic-${option.id}`;
              const isChecked = selectedDatasets.includes(option.id);

              return (
                <div
                  key={option.id}
                  className="flex items-start gap-3 rounded-md border border-border p-3"
                >
                  <Checkbox
                    id={checkboxId}
                    className="mt-1"
                    checked={isChecked}
                    onCheckedChange={(value) => toggleDataset(option.id, value === true)}
                  />
                  <div className="min-w-0 flex-1 space-y-2">
                    <div className="space-y-1">
                      <Label htmlFor={checkboxId} className="text-sm font-medium leading-none">
                        {option.label}
                      </Label>
                      <p className="text-sm text-muted-foreground">{option.description}</p>
                    </div>
                    {option.source === 'remote' && isChecked ? (
                      <div className="space-y-1">
                        <Label htmlFor={`${checkboxId}-count`} className="text-xs font-medium">
                          {option.countLabel} ({option.min}–{option.max})
                        </Label>
                        <Input
                          id={`${checkboxId}-count`}
                          type="number"
                          min={option.min}
                          max={option.max}
                          step={1}
                          inputMode="numeric"
                          aria-label={option.countAriaLabel}
                          value={getCountInput(option.id)}
                          onChange={(event) => setCountInput(option.id, event.target.value)}
                          className="h-8 w-32"
                        />
                      </div>
                    ) : null}
                  </div>
                </div>
              );
            })}
          </div>

          <DialogFooter className="sm:justify-between">
            <p className="text-xs text-muted-foreground">
              Store-backed datasets reflect current in-memory values; bulk activities and RM jstack
              are fetched live when you download.
            </p>
            <Button onClick={() => void handleDownload()} disabled={isDownloadDisabled}>
              {isDownloading ? 'Downloading…' : 'Download'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </TooltipProvider>
  );
}
