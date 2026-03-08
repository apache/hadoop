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


import React, { useState, useRef, useEffect } from 'react';
import {
  draggable,
  dropTargetForElements,
  monitorForElements,
} from '@atlaskit/pragmatic-drag-and-drop/element/adapter';
import { GripVertical, Trash2, Check, X } from 'lucide-react';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '~/components/ui/table';
import { Button } from '~/components/ui/button';
import { Badge } from '~/components/ui/badge';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '~/components/ui/dialog';
import { cn } from '~/utils/cn';
import type { PlacementRule } from '~/types/features/placement-rules';
import { getPolicyDisplayName } from '~/features/placement-rules/constants/policy-descriptions';

interface PlacementRulesTableProps {
  rules: PlacementRule[];
  selectedRuleIndex: number | null;
  onDelete: (index: number) => void;
  onSelect: (index: number) => void;
  onReorder: (sourceIndex: number, targetIndex: number) => void;
}

interface TableRowProps {
  rule: PlacementRule;
  index: number;
  isSelected: boolean;
  onDelete: () => void;
  onSelect: () => void;
}

function DraggableTableRow({ rule, index, isSelected, onDelete, onSelect }: TableRowProps) {
  const [isDragging, setIsDragging] = useState(false);
  const [showDeleteDialog, setShowDeleteDialog] = useState(false);
  const rowRef = useRef<HTMLTableRowElement>(null);
  const dragHandleRef = useRef<HTMLTableCellElement>(null);

  useEffect(() => {
    const rowEl = rowRef.current;
    const dragHandleEl = dragHandleRef.current;
    if (!rowEl || !dragHandleEl) return;

    // Make the row draggable
    const cleanup = draggable({
      element: rowEl,
      dragHandle: dragHandleEl,
      getInitialData: () => ({
        type: 'placement-rule',
        index,
        ruleId: `rule-${index}`,
      }),
      onDragStart: () => {
        setIsDragging(true);
      },
      onDrop: () => {
        setIsDragging(false);
      },
    });

    return cleanup;
  }, [index]);

  const getRuleTypeColor = (type: string) => {
    switch (type) {
      case 'user':
        return 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200';
      case 'group':
        return 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200';
      case 'application':
        return 'bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200';
      default:
        return 'bg-gray-100 text-gray-800 dark:bg-gray-900 dark:text-gray-200';
    }
  };

  const getTargetQueueDisplay = () => {
    if (rule.value) return rule.value;
    if (rule.parentQueue) return rule.parentQueue;
    if (rule.customPlacement) return 'Custom';
    if (rule.policy === 'reject') return '—';
    if (rule.policy === 'applicationName') return rule.matches;
    return 'Dynamic';
  };

  return (
    <>
      <TableRow
        ref={rowRef}
        className={cn(
          'cursor-pointer transition-colors relative',
          'hover:bg-muted/50',
          isDragging && 'opacity-50',
          isSelected && 'bg-primary/5',
        )}
        onClick={onSelect}
      >
        <TableCell ref={dragHandleRef} className="w-12 cursor-grab hover:bg-accent">
          <div className="flex items-center gap-1">
            <GripVertical className="h-3.5 w-3.5 text-muted-foreground" />
            <span className="text-xs text-muted-foreground">{index + 1}</span>
          </div>
        </TableCell>
        <TableCell className="w-24">
          <Badge className={getRuleTypeColor(rule.type)}>{rule.type}</Badge>
        </TableCell>
        <TableCell className="font-mono text-sm">{rule.matches}</TableCell>
        <TableCell>{getPolicyDisplayName(rule.policy)}</TableCell>
        <TableCell className="font-mono text-sm">{getTargetQueueDisplay()}</TableCell>
        <TableCell className="w-16 text-center">
          {rule.create ? (
            <Check className="h-4 w-4 mx-auto text-green-600" />
          ) : (
            <X className="h-4 w-4 mx-auto text-muted-foreground" />
          )}
        </TableCell>
        <TableCell className="w-24">{rule.fallbackResult || 'skip'}</TableCell>
        <TableCell className="w-16">
          <Button
            variant="ghost"
            size="icon"
            className="h-7 w-7"
            onClick={(e) => {
              e.stopPropagation();
              setShowDeleteDialog(true);
            }}
          >
            <Trash2 className="h-3.5 w-3.5" />
          </Button>
        </TableCell>
      </TableRow>

      <Dialog open={showDeleteDialog} onOpenChange={setShowDeleteDialog}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete Placement Rule</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete this placement rule? This action will be staged and
              can be reverted before applying.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowDeleteDialog(false)}>
              Cancel
            </Button>
            <Button
              variant="destructive"
              onClick={() => {
                onDelete();
                setShowDeleteDialog(false);
              }}
            >
              Delete
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}

interface DropZoneRowProps {
  index: number;
  onReorder: (sourceIndex: number, targetIndex: number) => void;
}

function DropZoneRow({ index, onReorder }: DropZoneRowProps) {
  const [isDraggedOver, setIsDraggedOver] = useState(false);
  const rowRef = useRef<HTMLTableRowElement>(null);

  useEffect(() => {
    const rowEl = rowRef.current;
    if (!rowEl) return;

    return dropTargetForElements({
      element: rowEl,
      canDrop: ({ source }) => {
        return source.data.type === 'placement-rule';
      },
      getData: () => ({ index, isDropZone: true }),
      getIsSticky: () => true,
      onDragEnter: () => {
        setIsDraggedOver(true);
      },
      onDragLeave: () => {
        setIsDraggedOver(false);
      },
      onDrop: () => {
        setIsDraggedOver(false);
      },
    });
  }, [index, onReorder]);

  return (
    <tr ref={rowRef} className="h-2">
      <td colSpan={8} className="p-0">
        {isDraggedOver && (
          <div className="h-0.5 bg-blue-500 relative">
            <div className="absolute -left-2 -top-1 w-2.5 h-2.5 rounded-full bg-blue-500 animate-pulse" />
            <div className="absolute -right-2 -top-1 w-2.5 h-2.5 rounded-full bg-blue-500 animate-pulse" />
          </div>
        )}
      </td>
    </tr>
  );
}

export function PlacementRulesTable({
  rules,
  selectedRuleIndex,
  onDelete,
  onSelect,
  onReorder,
}: PlacementRulesTableProps) {
  useEffect(() => {
    const cleanup = monitorForElements({
      onDrop({ source, location }) {
        if (source.data.type !== 'placement-rule') return;

        const dropTarget = location.current.dropTargets.find((target) => target.data?.isDropZone);
        if (!dropTarget) return;

        const sourceIndex = source.data.index as number;
        const targetIndex = dropTarget.data.index as number;

        if (typeof sourceIndex !== 'number' || typeof targetIndex !== 'number') {
          return;
        }

        if (sourceIndex !== targetIndex) {
          onReorder(sourceIndex, targetIndex);
        }
      },
    });

    return cleanup;
  }, [onReorder]);

  return (
    <div className="border rounded-md overflow-auto">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className="w-12">#</TableHead>
            <TableHead className="w-24">Type</TableHead>
            <TableHead>Matches</TableHead>
            <TableHead>Policy</TableHead>
            <TableHead>Target Queue</TableHead>
            <TableHead className="w-16 text-center">Create</TableHead>
            <TableHead className="w-24">Fallback</TableHead>
            <TableHead className="w-16"></TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {rules.length === 0 ? (
            <TableRow>
              <TableCell colSpan={8} className="text-center py-8 text-muted-foreground">
                No placement rules configured
              </TableCell>
            </TableRow>
          ) : (
            <>
              <DropZoneRow index={0} onReorder={onReorder} />
              {rules.map((rule, index) => (
                <React.Fragment key={`rule-${Date.now()}-${Math.random()}`}>
                  <DraggableTableRow
                    rule={rule}
                    index={index}
                    isSelected={selectedRuleIndex === index}
                    onDelete={() => onDelete(index)}
                    onSelect={() => onSelect(index)}
                  />
                  <DropZoneRow index={index + 1} onReorder={onReorder} />
                </React.Fragment>
              ))}
            </>
          )}
        </TableBody>
      </Table>
    </div>
  );
}
