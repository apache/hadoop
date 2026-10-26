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


import React, { useState } from 'react';
import { SPECIAL_VALUES } from '~/types';
import { ChevronDown, ChevronUp, Folder, GitBranch, Plus, Edit2, Trash2 } from 'lucide-react';
import { Badge } from '~/components/ui/badge';
import { Button } from '~/components/ui/button';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '~/components/ui/collapsible';
import type { StagedChange, StagedChangeType } from '~/types';
import { DiffView } from './DiffView';

interface QueueChangeGroupProps {
  queuePath: string;
  changes: StagedChange[];
  onRevert: (change: StagedChange) => void;
}

export const QueueChangeGroup: React.FC<QueueChangeGroupProps> = ({
  queuePath,
  changes,
  onRevert,
}) => {
  const [isOpen, setIsOpen] = useState(true);
  const [expandedTypes, setExpandedTypes] = useState<Set<StagedChangeType>>(
    () => new Set(['add', 'update', 'remove']),
  );

  // Group changes by type
  const changesByType = changes.reduce(
    (acc, change) => {
      acc[change.type].push(change);
      return acc;
    },
    { add: [] as StagedChange[], update: [] as StagedChange[], remove: [] as StagedChange[] },
  );

  // Calculate change summary
  const summary = {
    add: changesByType.add.length,
    update: changesByType.update.length,
    remove: changesByType.remove.length,
  };

  const toggleTypeExpanded = (type: StagedChangeType) => {
    setExpandedTypes((prev) => {
      const next = new Set(prev);
      if (next.has(type)) {
        next.delete(type);
      } else {
        next.add(type);
      }
      return next;
    });
  };

  return (
    <div className="border rounded-lg overflow-hidden bg-card">
      <Collapsible open={isOpen} onOpenChange={setIsOpen}>
        <CollapsibleTrigger asChild>
          <Button variant="ghost" className="w-full p-3 justify-between hover:bg-muted/50 h-auto">
            <div className="flex items-center gap-2">
              {queuePath === SPECIAL_VALUES.GLOBAL_QUEUE_PATH ? (
                <Folder className="h-4 w-4 text-muted-foreground" />
              ) : (
                <GitBranch className="h-4 w-4 text-muted-foreground" />
              )}
              <span className="font-medium text-sm">
                {queuePath === SPECIAL_VALUES.GLOBAL_QUEUE_PATH ? 'Global Settings' : queuePath}
              </span>

              {/* Change summary badges */}
              <div className="flex gap-1 ml-2">
                {summary.add > 0 && (
                  <Badge
                    variant="outline"
                    className="h-5 text-xs border-green-600 text-green-700 dark:text-green-400"
                  >
                    +{summary.add}
                  </Badge>
                )}
                {summary.update > 0 && (
                  <Badge
                    variant="outline"
                    className="h-5 text-xs border-blue-600 text-blue-700 dark:text-blue-400"
                  >
                    ~{summary.update}
                  </Badge>
                )}
                {summary.remove > 0 && (
                  <Badge
                    variant="outline"
                    className="h-5 text-xs border-red-600 text-red-700 dark:text-red-400"
                  >
                    -{summary.remove}
                  </Badge>
                )}
              </div>
            </div>

            {isOpen ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
          </Button>
        </CollapsibleTrigger>

        <CollapsibleContent>
          <div className="px-3 pb-3 space-y-3">
            {/* Modified Properties */}
            {summary.update > 0 && (
              <div>
                <Collapsible
                  open={expandedTypes.has('update')}
                  onOpenChange={() => toggleTypeExpanded('update')}
                >
                  <CollapsibleTrigger asChild>
                    <Button
                      variant="ghost"
                      className="w-full px-2 py-1.5 justify-between hover:bg-muted/30 h-auto"
                    >
                      <div className="flex items-center gap-2">
                        <Edit2 className="h-3.5 w-3.5 text-blue-600 dark:text-blue-400" />
                        <span className="text-xs font-medium">Modified ({summary.update})</span>
                      </div>
                      {expandedTypes.has('update') ? (
                        <ChevronUp className="h-3.5 w-3.5" />
                      ) : (
                        <ChevronDown className="h-3.5 w-3.5" />
                      )}
                    </Button>
                  </CollapsibleTrigger>
                  <CollapsibleContent>
                    <div className="pt-2 space-y-2">
                      {changesByType.update.map((change) => (
                        <DiffView
                          key={change.id}
                          change={change}
                          onRevert={() => onRevert(change)}
                          timestamp={new Date(change.timestamp).toLocaleTimeString()}
                        />
                      ))}
                    </div>
                  </CollapsibleContent>
                </Collapsible>
              </div>
            )}

            {/* New Properties */}
            {summary.add > 0 && (
              <div>
                <Collapsible
                  open={expandedTypes.has('add')}
                  onOpenChange={() => toggleTypeExpanded('add')}
                >
                  <CollapsibleTrigger asChild>
                    <Button
                      variant="ghost"
                      className="w-full px-2 py-1.5 justify-between hover:bg-muted/30 h-auto"
                    >
                      <div className="flex items-center gap-2">
                        <Plus className="h-3.5 w-3.5 text-green-600 dark:text-green-400" />
                        <span className="text-xs font-medium">New ({summary.add})</span>
                      </div>
                      {expandedTypes.has('add') ? (
                        <ChevronUp className="h-3.5 w-3.5" />
                      ) : (
                        <ChevronDown className="h-3.5 w-3.5" />
                      )}
                    </Button>
                  </CollapsibleTrigger>
                  <CollapsibleContent>
                    <div className="pt-2 space-y-2">
                      {changesByType.add.map((change) => (
                        <DiffView
                          key={change.id}
                          change={change}
                          onRevert={() => onRevert(change)}
                          timestamp={new Date(change.timestamp).toLocaleTimeString()}
                        />
                      ))}
                    </div>
                  </CollapsibleContent>
                </Collapsible>
              </div>
            )}

            {/* Removed Properties */}
            {summary.remove > 0 && (
              <div>
                <Collapsible
                  open={expandedTypes.has('remove')}
                  onOpenChange={() => toggleTypeExpanded('remove')}
                >
                  <CollapsibleTrigger asChild>
                    <Button
                      variant="ghost"
                      className="w-full px-2 py-1.5 justify-between hover:bg-muted/30 h-auto"
                    >
                      <div className="flex items-center gap-2">
                        <Trash2 className="h-3.5 w-3.5 text-red-600 dark:text-red-400" />
                        <span className="text-xs font-medium">Removed ({summary.remove})</span>
                      </div>
                      {expandedTypes.has('remove') ? (
                        <ChevronUp className="h-3.5 w-3.5" />
                      ) : (
                        <ChevronDown className="h-3.5 w-3.5" />
                      )}
                    </Button>
                  </CollapsibleTrigger>
                  <CollapsibleContent>
                    <div className="pt-2 space-y-2">
                      {changesByType.remove.map((change) => (
                        <DiffView
                          key={change.id}
                          change={change}
                          onRevert={() => onRevert(change)}
                          timestamp={new Date(change.timestamp).toLocaleTimeString()}
                        />
                      ))}
                    </div>
                  </CollapsibleContent>
                </Collapsible>
              </div>
            )}
          </div>
        </CollapsibleContent>
      </Collapsible>
    </div>
  );
};
