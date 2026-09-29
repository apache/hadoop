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


import React, { useEffect, useState } from 'react';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '~/components/ui/dialog';
import { Button } from '~/components/ui/button';
import { Input } from '~/components/ui/input';
import {
  Field,
  FieldControl,
  FieldDescription,
  FieldLabel,
  FieldMessage,
} from '~/components/ui/field';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '~/components/ui/select';
import type { TemplateScopeType } from '~/features/template-config/types';
import { getSuffixForTemplateType } from '~/features/template-config/utils/scopeUtils';

interface AddTemplateScopeDialogProps {
  open: boolean;
  queuePath: string;
  onClose: () => void;
  onConfirm: (scope: { queuePath: string; type: TemplateScopeType }) => void;
  existingQueuePaths: string[];
}

const TEMPLATE_TYPE_LABELS: Record<TemplateScopeType, string> = {
  legacyLeaf: 'Legacy leaf template',
  flexibleShared: 'Flexible shared template',
  flexibleLeaf: 'Flexible leaf template',
  flexibleParent: 'Flexible parent template',
};

export const AddTemplateScopeDialog: React.FC<AddTemplateScopeDialogProps> = ({
  open,
  queuePath,
  onClose,
  onConfirm,
  existingQueuePaths,
}) => {
  const [targetPath, setTargetPath] = useState(`${queuePath}.*`);
  const [templateType, setTemplateType] = useState<TemplateScopeType>('flexibleShared');
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (open) {
      setTargetPath(`${queuePath}.*`);
      setTemplateType('flexibleShared');
      setError(null);
    }
  }, [open, queuePath]);

  const suffix = getSuffixForTemplateType(templateType);
  const helperText = `Configurations will be written under "${targetPath}.${suffix}"`;

  const validate = () => {
    const trimmed = targetPath.trim();
    if (!trimmed) {
      return 'Queue path is required';
    }
    if (!trimmed.startsWith(queuePath)) {
      return `Queue path must start with "${queuePath}"`;
    }
    if (!trimmed.includes('*')) {
      return 'Queue path must include a "*" wildcard segment';
    }
    if (existingQueuePaths.includes(`${trimmed}.${getSuffixForTemplateType(templateType)}`)) {
      return 'A template scope already exists for this path';
    }
    return null;
  };

  const handleConfirm = () => {
    const validationError = validate();
    if (validationError) {
      setError(validationError);
      return;
    }
    const normalizedPath = targetPath.trim();
    onConfirm({
      queuePath: `${normalizedPath}.${getSuffixForTemplateType(templateType)}`,
      type: templateType,
    });
    onClose();
  };

  return (
    <Dialog open={open} onOpenChange={onClose}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>Add wildcard template scope</DialogTitle>
          <DialogDescription>
            Create a wildcard template scope for flexible auto-created queues.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-5 py-3">
          <Field>
            <FieldLabel>Template type</FieldLabel>
            <FieldControl>
              <Select
                value={templateType}
                onValueChange={(value) => setTemplateType(value as TemplateScopeType)}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="flexibleShared">Flexible shared template</SelectItem>
                  <SelectItem value="flexibleParent">Flexible parent template</SelectItem>
                  <SelectItem value="flexibleLeaf">Flexible leaf template</SelectItem>
                </SelectContent>
              </Select>
            </FieldControl>
            <FieldDescription>{TEMPLATE_TYPE_LABELS[templateType]}</FieldDescription>
          </Field>

          <Field>
            <FieldLabel>Wildcard queue path</FieldLabel>
            <FieldControl>
              <Input
                value={targetPath}
                onChange={(event) => {
                  setTargetPath(event.target.value);
                  setError(null);
                }}
                placeholder={`${queuePath}.*`}
                autoFocus
                aria-invalid={Boolean(error)}
              />
            </FieldControl>
            {error ? (
              <FieldMessage>{error}</FieldMessage>
            ) : (
              <FieldDescription className="text-xs text-muted-foreground">
                {helperText}
              </FieldDescription>
            )}
          </Field>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={onClose}>
            Cancel
          </Button>
          <Button onClick={handleConfirm}>Add scope</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};
