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


import { useEffect } from 'react';
import { useForm, type SubmitHandler } from 'react-hook-form';
import { zodResolver } from '@hookform/resolvers/zod';
import { Form, FormField } from '~/components/ui/form';
import {
  Field,
  FieldControl,
  FieldDescription,
  FieldLabel,
  FieldMessage,
} from '~/components/ui/field';
import { Input } from '~/components/ui/input';
import { Button } from '~/components/ui/button';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '~/components/ui/select';
import { FieldSwitch } from '~/components/ui/field-switch';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '~/components/ui/card';
import { Combobox } from '~/components/ui/combobox';
import { useSchedulerStore } from '~/stores/schedulerStore';
import {
  placementRuleFormSchema,
  type PlacementRuleFormData,
  formDataToPlacementRule,
} from '~/features/placement-rules/schemas/placement-rule-schema';
import { CustomPlacementHelpDialog } from './CustomPlacementHelpDialog';
import {
  getPolicyDescription,
  POLICY_DISPLAY_NAMES,
} from '~/features/placement-rules/constants/policy-descriptions';
import { getAllParentQueues, getAllQueues } from '~/features/placement-rules/utils/queueOptions';
import type { PlacementRule } from '~/types/features/placement-rules';

const PARENT_QUEUE_POLICIES = [
  'user',
  'primaryGroup',
  'primaryGroupUser',
  'secondaryGroup',
  'secondaryGroupUser',
] as const;

const MATCH_PLACEHOLDERS: Record<PlacementRule['type'], string> = {
  user: '* for all, or specify usernames',
  group: 'Enter group names (wildcard not supported)',
  application: '* for all apps, or patterns like spark-*',
};

interface PlacementRuleFormProps {
  rule?: PlacementRule;
  ruleIndex?: number;
  onSubmit: (data: PlacementRule, index?: number) => void;
  onCancel: () => void;
}

export function PlacementRuleForm({ rule, ruleIndex, onSubmit, onCancel }: PlacementRuleFormProps) {
  const schedulerData = useSchedulerStore((state) => state.schedulerData);
  const parentQueues = getAllParentQueues(schedulerData);
  const allQueues = getAllQueues(schedulerData);

  const form = useForm<PlacementRuleFormData>({
    resolver: zodResolver(placementRuleFormSchema) as any, // eslint-disable-line @typescript-eslint/no-explicit-any
    defaultValues: rule
      ? {
          type: rule.type,
          matches: rule.matches,
          policy: rule.policy,
          parentQueue: rule.parentQueue,
          value: rule.value,
          customPlacement: rule.customPlacement,
          create: rule.create ?? false,
          fallbackResult: rule.fallbackResult ?? 'skip',
        }
      : {
          type: 'user',
          matches: '*',
          policy: 'user',
          create: false,
        },
  });

  const selectedPolicy = form.watch('policy');
  const selectedType = form.watch('type');
  const showParentQueue = selectedPolicy
    ? PARENT_QUEUE_POLICIES.includes(selectedPolicy as (typeof PARENT_QUEUE_POLICIES)[number])
    : false;
  const requiresValue = selectedPolicy === 'setDefaultQueue';
  const requiresCustomPlacement = selectedPolicy === 'custom';
  const showCreateOption = !['reject', 'defaultQueue', 'setDefaultQueue'].includes(selectedPolicy);

  useEffect(() => {
    const currentValue = form.getValues('matches');
    if (selectedType === 'group' && currentValue.trim() === '*') {
      form.setValue('matches', '');
    } else if (selectedType === 'user' && currentValue.trim() === '') {
      form.setValue('matches', '*');
    }
  }, [selectedType, form]);

  const handleSubmit: SubmitHandler<PlacementRuleFormData> = (data) => {
    const placementRule = formDataToPlacementRule(data);
    onSubmit(placementRule, ruleIndex);
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle>{rule ? 'Edit' : 'Add'} Placement Rule</CardTitle>
        <CardDescription>
          Define how applications are assigned to queues based on user, group, or application
          attributes.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <Form {...form}>
          <form onSubmit={form.handleSubmit(handleSubmit)} className="space-y-6">
            <div className="grid grid-cols-2 gap-4">
              <FormField
                control={form.control as any} // eslint-disable-line @typescript-eslint/no-explicit-any
                name="type"
                render={({ field, fieldState }) => (
                  <Field>
                    <FieldLabel>Rule Type</FieldLabel>
                    <Select onValueChange={field.onChange} value={field.value}>
                      <FieldControl aria-invalid={Boolean(fieldState.error)}>
                        <SelectTrigger>
                          <SelectValue placeholder="Select rule type" />
                        </SelectTrigger>
                      </FieldControl>
                      <SelectContent>
                        <SelectItem value="user">User</SelectItem>
                        <SelectItem value="group">Group</SelectItem>
                        <SelectItem value="application">Application</SelectItem>
                      </SelectContent>
                    </Select>
                    <FieldDescription>
                      Match applications based on submitting user, group, or application name
                    </FieldDescription>
                    {fieldState.error && (
                      <FieldMessage>{String(fieldState.error.message ?? '')}</FieldMessage>
                    )}
                  </Field>
                )}
              />

              <FormField
                control={form.control as any} // eslint-disable-line @typescript-eslint/no-explicit-any
                name="matches"
                render={({ field, fieldState }) => (
                  <Field>
                    <FieldLabel>Match Pattern</FieldLabel>
                    <FieldControl>
                      <Input
                        {...field}
                        placeholder={MATCH_PLACEHOLDERS[selectedType]}
                        aria-invalid={Boolean(fieldState.error)}
                      />
                    </FieldControl>
                    <FieldDescription>
                      {selectedType === 'user' && 'Use * to match all users, or specify usernames'}
                      {selectedType === 'group' &&
                        'Specify explicit group names; * wildcard is not supported'}
                      {selectedType === 'application' &&
                        'Use * to match all apps, or patterns like spark-*'}
                    </FieldDescription>
                    {fieldState.error && (
                      <FieldMessage>{String(fieldState.error.message ?? '')}</FieldMessage>
                    )}
                  </Field>
                )}
              />
            </div>

            <FormField
              control={form.control as any} // eslint-disable-line @typescript-eslint/no-explicit-any
              name="policy"
              render={({ field, fieldState }) => (
                <Field>
                  <FieldLabel>Placement Policy</FieldLabel>
                  <Select onValueChange={field.onChange} value={field.value}>
                    <FieldControl aria-invalid={Boolean(fieldState.error)}>
                      <SelectTrigger>
                        <SelectValue placeholder="Select placement policy" />
                      </SelectTrigger>
                    </FieldControl>
                    <SelectContent>
                      {Object.entries(POLICY_DISPLAY_NAMES).map(([value, label]) => (
                        <SelectItem key={value} value={value}>
                          {label}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  <FieldDescription>
                    {(() => {
                      const policy = getPolicyDescription(field.value);
                      return policy
                        ? policy.description
                        : 'Determines how the queue path is constructed for matching applications';
                    })()}
                  </FieldDescription>
                  {fieldState.error && (
                    <FieldMessage>{String(fieldState.error.message ?? '')}</FieldMessage>
                  )}
                </Field>
              )}
            />

            {requiresValue && (
              <FormField
                control={form.control as any} // eslint-disable-line @typescript-eslint/no-explicit-any
                name="value"
                render={({ field, fieldState }) => (
                  <Field>
                    <FieldLabel>
                      {selectedPolicy === 'setDefaultQueue' ? 'Default Queue' : 'Queue Value'}
                    </FieldLabel>
                    <FieldControl aria-invalid={Boolean(fieldState.error)}>
                      <Combobox
                        value={field.value || ''}
                        onValueChange={field.onChange}
                        items={allQueues}
                        placeholder="Select a queue..."
                        searchPlaceholder="Search queues..."
                        emptyText="No queues found."
                        aria-label={
                          selectedPolicy === 'setDefaultQueue' ? 'Default Queue' : 'Queue Value'
                        }
                      />
                    </FieldControl>
                    <FieldDescription>
                      {selectedPolicy === 'setDefaultQueue'
                        ? 'The new default queue path that will be used by subsequent defaultQueue policies'
                        : 'The specific queue path where matching applications will be placed'}
                    </FieldDescription>
                    {fieldState.error && (
                      <FieldMessage>{String(fieldState.error.message ?? '')}</FieldMessage>
                    )}
                  </Field>
                )}
              />
            )}

            {showParentQueue && (
              <FormField
                control={form.control as any} // eslint-disable-line @typescript-eslint/no-explicit-any
                name="parentQueue"
                render={({ field, fieldState }) => (
                  <Field>
                    <FieldLabel>Parent Queue</FieldLabel>
                    <FieldControl aria-invalid={Boolean(fieldState.error)}>
                      <Combobox
                        value={field.value || ''}
                        onValueChange={field.onChange}
                        items={parentQueues}
                        placeholder="Select a parent queue..."
                        searchPlaceholder="Search queues..."
                        emptyText="No parent queues found."
                        aria-label="Parent Queue"
                      />
                    </FieldControl>
                    <FieldDescription>
                      Optional parent queue under which matching queues will be created
                    </FieldDescription>
                    {fieldState.error && (
                      <FieldMessage>{String(fieldState.error.message ?? '')}</FieldMessage>
                    )}
                  </Field>
                )}
              />
            )}

            {requiresCustomPlacement && (
              <FormField
                control={form.control as any} // eslint-disable-line @typescript-eslint/no-explicit-any
                name="customPlacement"
                render={({ field, fieldState }) => (
                  <Field>
                    <div className="flex items-center justify-between">
                      <FieldLabel>Custom Placement Pattern</FieldLabel>
                      <CustomPlacementHelpDialog triggerText="View Variables" />
                    </div>
                    <FieldControl>
                      <Input
                        {...field}
                        placeholder="e.g., root.%primary_group.%user"
                        aria-invalid={Boolean(fieldState.error)}
                      />
                    </FieldControl>
                    <FieldDescription>
                      Use variables to construct dynamic queue paths based on user attributes
                    </FieldDescription>
                    {fieldState.error && (
                      <FieldMessage>{String(fieldState.error.message ?? '')}</FieldMessage>
                    )}
                  </Field>
                )}
              />
            )}

            <div className="space-y-4">
              {showCreateOption && (
                <FormField
                  control={form.control as any} // eslint-disable-line @typescript-eslint/no-explicit-any
                  name="create"
                  render={({ field }) => (
                    <FieldSwitch
                      id="create"
                      fieldName={field.name}
                      label="Create queue if it doesn't exist"
                      description="Automatically create the target queue with default settings if not found"
                      checked={field.value}
                      onCheckedChange={field.onChange}
                    />
                  )}
                />
              )}

              <FormField
                control={form.control as any} // eslint-disable-line @typescript-eslint/no-explicit-any
                name="fallbackResult"
                render={({ field, fieldState }) => (
                  <Field>
                    <FieldLabel>Fallback Behavior</FieldLabel>
                    <Select onValueChange={field.onChange} value={field.value}>
                      <FieldControl aria-invalid={Boolean(fieldState.error)}>
                        <SelectTrigger>
                          <SelectValue placeholder="Select fallback behavior" />
                        </SelectTrigger>
                      </FieldControl>
                      <SelectContent>
                        <SelectItem value="skip">Skip to next rule</SelectItem>
                        <SelectItem value="placeDefault">Place in default queue</SelectItem>
                        <SelectItem value="reject">Reject application</SelectItem>
                      </SelectContent>
                    </Select>
                    <FieldDescription>
                      What happens if this rule matches but cannot place the application
                    </FieldDescription>
                    {fieldState.error && (
                      <FieldMessage>{String(fieldState.error.message ?? '')}</FieldMessage>
                    )}
                  </Field>
                )}
              />
            </div>

            <div className="flex justify-end gap-2">
              <Button type="button" variant="outline" onClick={onCancel}>
                Cancel
              </Button>
              <Button type="submit">{rule ? 'Update' : 'Add'} Rule</Button>
            </div>
          </form>
        </Form>
      </CardContent>
    </Card>
  );
}
