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
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '~/components/ui/dialog';
import { Input } from '~/components/ui/input';
import { FieldSwitch } from '~/components/ui/field-switch';
import { Button } from '~/components/ui/button';
import { Alert, AlertDescription } from '~/components/ui/alert';
import { validateLabelName } from '~/features/node-labels/utils/labelValidation';
import {
  Field,
  FieldControl,
  FieldDescription,
  FieldLabel,
  FieldMessage,
} from '~/components/ui/field';

interface AddLabelDialogProps {
  open: boolean;
  onClose: () => void;
  onConfirm: (name: string, exclusivity: boolean) => void;
  existingLabels: string[];
  isLoading?: boolean;
}

export const AddLabelDialog: React.FC<AddLabelDialogProps> = ({
  open,
  onClose,
  onConfirm,
  existingLabels,
  isLoading = false,
}) => {
  const [name, setName] = useState('');
  const [exclusivity, setExclusivity] = useState(false);
  const [error, setError] = useState('');

  const handleConfirm = () => {
    const validation = validateLabelName(name, existingLabels);

    if (!validation.valid) {
      setError(validation.error || 'Invalid label name');
      return;
    }

    onConfirm(name.trim(), exclusivity);
    handleClose();
  };

  const handleClose = () => {
    setName('');
    setExclusivity(false);
    setError('');
    onClose();
  };

  return (
    <Dialog open={open} onOpenChange={handleClose}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Add New Node Label</DialogTitle>
          <DialogDescription>Create a new node label for resource allocation</DialogDescription>
        </DialogHeader>

        <div className="space-y-4 py-4">
          <Field>
            <FieldLabel htmlFor="label-name">Label Name</FieldLabel>
            <FieldControl>
              <Input
                id="label-name"
                value={name}
                onChange={(e) => {
                  setName(e.target.value);
                  setError('');
                }}
                placeholder="e.g., gpu, highmem, ssd"
                className={error ? 'border-destructive' : ''}
                autoFocus
                aria-invalid={Boolean(error)}
              />
            </FieldControl>
            {error ? (
              <FieldMessage>{error}</FieldMessage>
            ) : (
              <FieldDescription className="text-sm text-muted-foreground">
                Use letters, numbers, hyphens, and underscores only
              </FieldDescription>
            )}
          </Field>

          <FieldSwitch
            id="exclusive"
            label="Exclusive Label"
            checked={exclusivity}
            onCheckedChange={setExclusivity}
          />

          <Alert>
            <AlertDescription>
              <strong>Exclusive labels:</strong> Only containers specifically requesting this label
              can run on nodes with this label.
              <br />
              <strong>Non-exclusive labels:</strong> Any container can run on these nodes.
            </AlertDescription>
          </Alert>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={handleClose} disabled={isLoading}>
            Cancel
          </Button>
          <Button onClick={handleConfirm} disabled={isLoading}>
            Add Label
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};
