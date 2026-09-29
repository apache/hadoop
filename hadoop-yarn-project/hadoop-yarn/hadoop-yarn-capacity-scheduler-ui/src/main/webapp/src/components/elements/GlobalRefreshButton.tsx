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


import { RefreshCw } from 'lucide-react';

import { Button } from '~/components/ui/button';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '~/components/ui/tooltip';
import { useSchedulerStore } from '~/stores/schedulerStore';

export function GlobalRefreshButton() {
  const loadInitialData = useSchedulerStore((state) => state.loadInitialData);
  const isLoading = useSchedulerStore((state) => state.isLoading);

  const handleRefresh = async () => {
    try {
      await loadInitialData();
    } catch (error) {
      console.error('Failed to refresh scheduler data:', error);
    }
  };

  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <Button
            variant="ghost"
            size="icon"
            onClick={handleRefresh}
            disabled={isLoading}
            aria-label="Refresh data"
          >
            <RefreshCw className={`h-[1.2rem] w-[1.2rem] ${isLoading ? 'animate-spin' : ''}`} />
          </Button>
        </TooltipTrigger>
        <TooltipContent sideOffset={8}>Refresh data</TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}
