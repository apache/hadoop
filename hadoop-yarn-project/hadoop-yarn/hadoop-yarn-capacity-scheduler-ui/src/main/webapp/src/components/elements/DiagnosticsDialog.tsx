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

interface DiagnosticOption {
  id: DiagnosticDatasetId;
  label: string;
  description: string;
  data?: unknown;
  live?: {
    countLabel: string;
    maxCount: number;
    fetch: (count: number) => Promise<unknown>;
  };
}

const DEFAULT_SELECTED: DiagnosticDatasetId[] = ['schedulerConf', 'schedulerInfo'];

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
  const [counts, setCounts] = useState<Partial<Record<DiagnosticDatasetId, string>>>({
    bulkActivities: String(DEFAULT_DIAGNOSTIC_BULK_ACTIVITIES_COUNT),
    rmJstack: String(DEFAULT_DIAGNOSTIC_RM_JSTACK_COUNT),
  });

  const entries = Array.from(configData.entries()).sort(([a], [b]) => a.localeCompare(b));
  const schedulerConfiguration = {
    version: configVersion,
    properties: Object.fromEntries(entries),
  };

  const datasetOptions: DiagnosticOption[] = [
    {
      id: 'schedulerConf',
      label: 'Scheduler Configuration',
      description: 'Key/value pairs returned by /scheduler-conf (including version metadata).',
      data: schedulerConfiguration,
    },
    {
      id: 'schedulerInfo',
      label: 'Scheduler Info',
      description: 'Current scheduler metrics returned by /scheduler.',
      data: schedulerData,
    },
    {
      id: 'nodeLabels',
      label: 'Node Labels',
      description: 'Label definitions from /node-labels.',
      data: nodeLabels,
    },
    {
      id: 'nodeToLabels',
      label: 'Node-to-Labels Mapping',
      description: 'Assignments from /node-to-labels.',
      data: nodeToLabels,
    },
    {
      id: 'nodes',
      label: 'Nodes',
      description: 'Node metadata returned by /nodes.',
      data: nodes,
    },
    {
      id: 'bulkActivities',
      label: 'Scheduler Bulk Activities',
      description: 'Live response from /scheduler/bulk-activities.',
      live: {
        countLabel: 'Bulk activities count',
        maxCount: MAX_DIAGNOSTIC_BULK_ACTIVITIES_COUNT,
        fetch: (count) => apiClient.getBulkSchedulerActivities(count),
      },
    },
    {
      id: 'rmJstack',
      label: 'ResourceManager JStack',
      description: 'Live thread dump from /jstack (ResourceManager JVM).',
      live: {
        countLabel: 'ResourceManager jstack count',
        maxCount: MAX_DIAGNOSTIC_RM_JSTACK_COUNT,
        fetch: (count) => apiClient.getResourceManagerJstack(count),
      },
    },
  ];

  const isValidCount = (option: DiagnosticOption) => {
    const count = Number(counts[option.id]);
    return Number.isInteger(count) && count >= 1 && count <= (option.live?.maxCount ?? 0);
  };

  const toggleDataset = (datasetId: DiagnosticDatasetId, checked: boolean) => {
    setSelectedDatasets((prev) => {
      if (checked) {
        return prev.includes(datasetId) ? prev : [...prev, datasetId];
      }
      return prev.filter((id) => id !== datasetId);
    });
  };

  const handleDownload = async () => {
    if (selectedDatasets.length === 0) {
      return;
    }

    const timestamp = new Date().toISOString();
    const payload: Record<string, unknown> = {
      generatedAt: timestamp,
      datasets: {},
    };

    for (const option of datasetOptions) {
      if (selectedDatasets.includes(option.id)) {
        (payload.datasets as Record<string, unknown>)[option.id] = option.live
          ? await option.live.fetch(Number(counts[option.id]))
          : option.data;
      }
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
  };

  const handleDownloadClick = () => {
    setIsDownloading(true);
    handleDownload()
      .catch((error) => {
        const message = error instanceof Error ? error.message : String(error);
        toast.error(`Failed to download diagnostics: ${message}`);
      })
      .finally(() => setIsDownloading(false));
  };

  const isDownloadDisabled =
    selectedDatasets.length === 0 ||
    isDownloading ||
    datasetOptions.some(
      (option) => option.live && selectedDatasets.includes(option.id) && !isValidCount(option),
    );

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
                  <div className="space-y-1">
                    <Label htmlFor={checkboxId} className="text-sm font-medium leading-none">
                      {option.label}
                    </Label>
                    <p className="text-sm text-muted-foreground">{option.description}</p>
                    {option.live && isChecked ? (
                      <Input
                        type="number"
                        min={1}
                        max={option.live.maxCount}
                        step={1}
                        aria-label={option.live.countLabel}
                        value={counts[option.id] ?? ''}
                        onChange={(event) =>
                          setCounts((prev) => ({ ...prev, [option.id]: event.target.value }))
                        }
                        className="h-8 w-32"
                      />
                    ) : null}
                  </div>
                </div>
              );
            })}
          </div>

          <DialogFooter className="sm:justify-between">
            <p className="text-xs text-muted-foreground">
              Data reflects the current in-memory store values; bulk activities and RM jstack are
              fetched live.
            </p>
            <Button onClick={handleDownloadClick} disabled={isDownloadDisabled}>
              {isDownloading ? 'Downloading…' : 'Download'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </TooltipProvider>
  );
}
