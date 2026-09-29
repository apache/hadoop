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


import React from 'react';
import { Tag } from 'lucide-react';
import { useShallow } from 'zustand/react/shallow';
import { useSchedulerStore } from '~/stores/schedulerStore';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '~/components/ui/select';
import { Badge } from '~/components/ui/badge';

export const NodeLabelSelector: React.FC = () => {
  // State values (trigger re-renders only when these specific values change)
  const { nodeLabels, selectedNodeLabelFilter } = useSchedulerStore(
    useShallow((s) => ({
      nodeLabels: s.nodeLabels,
      selectedNodeLabelFilter: s.selectedNodeLabelFilter,
    })),
  );

  // Actions (stable references, never trigger re-renders)
  const selectNodeLabelFilter = useSchedulerStore((s) => s.selectNodeLabelFilter);

  const handleChange = (value: string) => {
    selectNodeLabelFilter(value === 'DEFAULT' ? '' : value);
  };

  return (
    <div className="flex items-center gap-2">
      <Tag className="h-4 w-4 text-muted-foreground" />
      <Select value={selectedNodeLabelFilter || 'DEFAULT'} onValueChange={handleChange}>
        <SelectTrigger className="w-[200px]">
          <SelectValue placeholder="Select node label" />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="DEFAULT">
            <span>Default Partition</span>
          </SelectItem>
          {nodeLabels.map((label) => (
            <SelectItem key={label.name} value={label.name}>
              <div className="flex items-center gap-2">
                <span>{label.name}</span>
                {label.exclusivity && (
                  <Badge variant="secondary" className="text-xs">
                    Exclusive
                  </Badge>
                )}
              </div>
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
    </div>
  );
};
