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


import { PlacementRulesList } from './PlacementRulesList';
import { PlacementRuleDetail } from './PlacementRuleDetail';
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from '~/components/ui/resizable';
import { useSchedulerStore } from '~/stores/schedulerStore';

export function PlacementRules() {
  const selectedRuleIndex = useSchedulerStore((state) => state.selectedRuleIndex);

  return (
    <ResizablePanelGroup direction="horizontal" className="h-full">
      <ResizablePanel defaultSize={50} minSize={30}>
        <div className="h-full overflow-auto p-6">
          <PlacementRulesList />
        </div>
      </ResizablePanel>

      <ResizableHandle withHandle />

      <ResizablePanel defaultSize={50} minSize={30}>
        <div className="h-full overflow-auto">
          {selectedRuleIndex !== null ? (
            <PlacementRuleDetail />
          ) : (
            <div className="flex h-full items-center justify-center">
              <div className="text-center">
                <h3 className="text-lg font-medium text-muted-foreground">No rule selected</h3>
                <p className="text-sm text-muted-foreground mt-2">
                  Select a rule from the list to view details
                </p>
              </div>
            </div>
          )}
        </div>
      </ResizablePanel>
    </ResizablePanelGroup>
  );
}
