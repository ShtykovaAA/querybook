import { FieldArray, Form, Formik, useField, useFormikContext } from 'formik';
import React, { useCallback, useMemo } from 'react';
import { useSelector } from 'react-redux';
import styled from 'styled-components';
import * as Yup from 'yup';

import { MultiCreatableUserSelect } from 'components/UserSelect/MultiCreatableUserSelect';
import type { IQueryResultExporter } from 'const/queryExecution';
import {
    IDataDocScheduleKwargs,
    IDataDocScheduleNotification,
    NotifyOn,
} from 'const/schedule';
import { getExporterAuthentication } from 'lib/result-export';
import { getEnumEntries } from 'lib/typescript';
import {
    cronToRecurrence,
    IRecurrence,
    recurrenceOnYup,
    recurrenceToCron,
    recurrenceTypes,
} from 'lib/utils/cron';
import { IOptions } from 'lib/utils/react-select';
import { queryCellSelector } from 'redux/dataDoc/selector';
import { notificationServiceSelector } from 'redux/notificationService/selector';
import { INotifier } from 'redux/notificationService/types';
import { IStoreState } from 'redux/store/types';
import { AsyncButton } from 'ui/AsyncButton/AsyncButton';
import { SoftButton } from 'ui/Button/Button';
import { IconButton } from 'ui/Button/IconButton';
import { DisabledSection } from 'ui/DisabledSection/DisabledSection';
import { FormField, FormSectionHeader } from 'ui/Form/FormField';
import { FormWrapper } from 'ui/Form/FormWrapper';
import { SimpleField } from 'ui/FormikField/SimpleField';
import { Level } from 'ui/Level/Level';
import { RecurrenceEditor } from 'ui/ReccurenceEditor/RecurrenceEditor';
import {
    getDefaultFormValue,
    SmartForm,
    updateValue,
} from 'ui/SmartForm/SmartForm';

interface IDataDocScheduleFormProps {
    isEditable: boolean;
    docId: number;
    cron?: string;
    enabled?: boolean;
    kwargs: IDataDocScheduleKwargs;

    onCreate: (cron: string, kwargs: IDataDocScheduleKwargs) => Promise<any>;
    onUpdate: (
        cron: string,
        enabled: boolean,
        kwargs: IDataDocScheduleKwargs
    ) => Promise<any>;
    onDelete?: () => Promise<void>;
    onRun?: () => Promise<void>;
}

const scheduleFormSchema = Yup.object().shape({
    recurrence: Yup.object().shape({
        hour: Yup.number().min(0).max(23),
        minute: Yup.number().min(0).max(59),
        recurrence: Yup.string().oneOf(recurrenceTypes),
        on: recurrenceOnYup,
    }),
    enabled: Yup.boolean().notRequired(),
    kwargs: Yup.object().shape({
        timeout_minutes: Yup.number()
            .integer('Must be a whole number of minutes')
            .min(1, 'Minimum 1 minute')
            .max(2880, 'Maximum 2880 minutes (2 days)')
            .nullable(),
        max_retries: Yup.number()
            .integer('Must be a whole number')
            .min(0, 'Cannot be negative')
            .max(10, 'Maximum 10 retries')
            .nullable(),
        notifications: Yup.array().of(
            Yup.object().shape({
                with: Yup.string().nullable(),
                on: Yup.string().required(),
                config: Yup.object().shape({
                    to_all: Yup.array()
                        .of(Yup.object())
                        .required()
                        .min(1, 'Must have at least one recipient'),
                }),
            })
        ),
        exports: Yup.array().of(
            Yup.object().shape({
                exporter_cell_id: Yup.number().required(),
                exporter_name: Yup.string().required(),
                exporter_params: Yup.object(),
            })
        ),
    }),
});

function getDistinctExporters(
    values: IScheduleFormValues,
    exporters: IQueryResultExporter[]
) {
    return [
        ...new Set(
            values.kwargs.exports.map((exportConf) => exportConf.exporter_name)
        ),
    ]
        .map((exporterName) =>
            exporters.find((exp) => exp.name === exporterName)
        )
        .filter((exporter) => exporter);
}

interface IScheduleFormValues {
    recurrence: IRecurrence;
    enabled?: boolean;
    kwargs: {
        notifications: IDataDocScheduleNotification[];
        exports: IDataDocScheduleKwargs['exports'];
        timeout_minutes?: number | null;
        max_retries?: number | null;
    };
}

const WrappedFormField = styled(SimpleField)`
    width: 40%;
`;

export const DataDocScheduleForm: React.FunctionComponent<
    IDataDocScheduleFormProps
> = ({
    isEditable,

    docId,
    cron,
    enabled,
    kwargs,

    onCreate,
    onUpdate,
    onDelete,
    onRun,
}) => {
    const exporters = useSelector(
        (state: IStoreState) => state.queryExecutions.statementExporters
    );
    const notifiers = useSelector(notificationServiceSelector);
    const isCreateForm = !Boolean(cron);
    const recurrence = cronToRecurrence(cron || '0 0 * * *');
    const formValues: IScheduleFormValues = isCreateForm
        ? {
              recurrence,
              kwargs: {
                  exports: [],
                  notifications: [],
                  timeout_minutes: null,
                  max_retries: 0,
              },
          }
        : {
              recurrence,
              enabled,
              kwargs: {
                  exports: kwargs.exports,
                  timeout_minutes:
                      kwargs.timeout_seconds != null
                          ? Math.round(kwargs.timeout_seconds / 60)
                          : null,
                  max_retries: kwargs.max_retries ?? 0,
                  // merge notification config from `config.to_user` and `config.to` to `config.to_all`
                  notifications: kwargs.notifications.map((n) => ({
                      ...n,
                      config: {
                          ...n.config,
                          to_all: [
                              ...(n.config.to_user ?? []).map((to) => ({
                                  value: to,
                                  isUser: true,
                              })),
                              ...(n.config.to ?? []).map((to) => ({
                                  value: to,
                              })),
                          ],
                      },
                  })),
              },
          };

    return (
        <Formik
            validateOnMount
            initialValues={formValues}
            validationSchema={scheduleFormSchema}
            onSubmit={async (values) => {
                const cronRepr = recurrenceToCron(values.recurrence);

                // convert notifications back from `to_all` to `to` and `to_user` and remove the `to_all` field
                values.kwargs.notifications = (
                    values.kwargs.notifications ?? []
                ).map((n) => ({
                    ...n,
                    config: {
                        to_user: n.config['to_all']
                            .filter((v) => v.isUser)
                            .map((v) => v.value),
                        to: n.config['to_all']
                            .filter((v) => !v.isUser)
                            .map((v) => v.value),
                    },
                }));

                const exportersInWorkflow = getDistinctExporters(
                    values,
                    exporters
                );
                for (const exporter of exportersInWorkflow) {
                    await getExporterAuthentication(exporter);
                }

                const kwargsToSend: IDataDocScheduleKwargs = {
                    notifications: values.kwargs.notifications,
                    exports: values.kwargs.exports,
                    max_retries: values.kwargs.max_retries ?? 0,
                };
                if (values.kwargs.timeout_minutes != null) {
                    kwargsToSend.timeout_seconds =
                        Number(values.kwargs.timeout_minutes) * 60;
                }

                if (isCreateForm) {
                    await onCreate(cronRepr, kwargsToSend);
                } else {
                    await onUpdate(cronRepr, values.enabled, kwargsToSend);
                }
            }}
        >
            {({
                submitForm,
                values,
                errors,
                setFieldValue,
                isValid,
                dirty,
            }) => {
                const enabledField = !isCreateForm && (
                    <SimpleField label="Enabled" name="enabled" type="toggle" />
                );

                const executionSettingsField = (
                    <>
                        <FormSectionHeader>
                            Execution Settings
                        </FormSectionHeader>
                        <SimpleField
                            label="Timeout (minutes)"
                            name="kwargs.timeout_minutes"
                            type="number"
                            help="Cancel the run if it takes longer than this. Leave empty to use the global default (2 days)."
                            min={1}
                            max={2880}
                        />
                        <SimpleField
                            label="Max retries on failure"
                            name="kwargs.max_retries"
                            type="number"
                            help="How many times to automatically re-run the DataDoc if it fails or times out. 0 = no retry. Delay grows exponentially: 1, 2, 4, 8… min, capped at 30 min."
                            min={0}
                            max={10}
                        />
                    </>
                );

                const notificationField = (
                    <>
                        <FormSectionHeader>Notification</FormSectionHeader>
                        <ScheduleNotifactionsForm notifiers={notifiers} />
                    </>
                );

                const exportField = (
                    <>
                        {exporters && exporters.length > 0 && (
                            <>
                                <FormSectionHeader>Export</FormSectionHeader>
                                <ScheduleExportsForm
                                    docId={docId}
                                    exporters={exporters}
                                />
                            </>
                        )}
                    </>
                );

                // Run is allowed for any reader of the DataDoc (backend
                // dropped the write check on POST schedule/run/). Edit / delete
                // / create still gated by isEditable.
                const controlDOM = (onRun || isEditable) && (
                    <Level>
                        <div>
                            {onRun && (
                                <AsyncButton
                                    disabled={dirty}
                                    title="Manual Run"
                                    onClick={onRun}
                                />
                            )}
                        </div>
                        {isEditable && (
                            <div>
                                {onDelete && (
                                    <AsyncButton
                                        title="Delete"
                                        color="cancel"
                                        onClick={onDelete}
                                    />
                                )}
                                <AsyncButton
                                    disabled={
                                        !isValid || (!dirty && !isCreateForm)
                                    }
                                    onClick={submitForm}
                                    title={isCreateForm ? 'Create' : 'Update'}
                                />
                            </div>
                        )}
                    </Level>
                );

                return (
                    <div className="DataDocScheduleForm">
                        <FormWrapper minLabelWidth="180px" size={7}>
                            <Form>
                                <DisabledSection disabled={!isEditable}>
                                    <RecurrenceEditor
                                        recurrence={values.recurrence}
                                        recurrenceError={errors?.recurrence}
                                        allowCron={false}
                                        setRecurrence={(val) =>
                                            setFieldValue('recurrence', val)
                                        }
                                    />
                                    {enabledField}
                                    {executionSettingsField}
                                    {notificationField}
                                    {exportField}

                                    <br />
                                    {controlDOM}
                                </DisabledSection>
                            </Form>
                        </FormWrapper>
                    </div>
                );
            }}
        </Formik>
    );
};

const NotificationFormRow: React.FC<{
    name: string;
    onRemove: () => void;
    notifierOptions: string[];
    notifyOnOptions: IOptions;
    getHelp: (notifierName: string) => string;
}> = ({ name, onRemove, notifierOptions, notifyOnOptions, getHelp }) => {
    const [{ value: notification }, ,] = useField(name);
    const [, notifyToAllMeta, notifyToAllHelpers] = useField(
        `${name}.config.to_all`
    );

    return (
        <div className="cell-export-field mb24 flex-row">
            <div className="flex1 mr16">
                <div className="horizontal-space-between">
                    <WrappedFormField
                        label="Notify With"
                        name={`${name}.with`}
                        type="react-select"
                        options={notifierOptions}
                        withDeselect
                    />

                    <WrappedFormField
                        label="Notify On"
                        name={`${name}.on`}
                        type="react-select"
                        isDisabled={!notification.with}
                        options={notifyOnOptions}
                    />
                </div>

                <FormField
                    label="Notify To"
                    help={getHelp(notification.with)}
                    error={
                        notifyToAllMeta.touched ? notifyToAllMeta.error : null
                    }
                >
                    <MultiCreatableUserSelect
                        value={
                            notifyToAllMeta.value ??
                            notifyToAllMeta.initialValue
                        }
                        onChange={notifyToAllHelpers.setValue}
                        selectProps={{
                            isClearable: true,
                            placeholder: getHelp(notification.with),
                            onBlur: () => notifyToAllHelpers.setTouched(true),
                        }}
                    />
                </FormField>
            </div>
            <div>
                <IconButton icon="X" onClick={onRemove} />
            </div>
        </div>
    );
};

const NotifactionsFormName = 'kwargs.notifications';
const ScheduleNotifactionsForm: React.FC<{
    notifiers: INotifier[];
}> = ({ notifiers }) => {
    const { values } = useFormikContext<IScheduleFormValues>();

    const notificationValues = values.kwargs.notifications ?? [];

    const notifierOptions = useMemo(
        () => notifiers.map((notifier) => notifier.name),
        [notifiers]
    );

    const notifyOnOptions = useMemo(
        () =>
            getEnumEntries(NotifyOn).map(([key, value]) => ({
                value,
                label: key,
            })),
        []
    );

    const getNotifierHelp = useCallback(
        (notifierName: string) =>
            notifiers.find((n) => n.name === notifierName)?.help ||
            'Add comma(,) separated recepients here',
        [notifiers]
    );

    const handleNewNotification = useCallback(
        (arrayHelpers) => {
            arrayHelpers.push({
                with: notifierOptions[0],
                on: notifyOnOptions[0]?.value,
                config: {
                    to_all: [],
                },
            });
        },
        [notifierOptions, notifyOnOptions]
    );

    return (
        <FieldArray
            name={NotifactionsFormName}
            render={(arrayHelpers) => {
                const notificationFields = notificationValues.map(
                    (_, index) => (
                        <NotificationFormRow
                            key={index}
                            name={`${NotifactionsFormName}[${index}]`}
                            onRemove={() => arrayHelpers.remove(index)}
                            notifierOptions={notifierOptions}
                            notifyOnOptions={notifyOnOptions}
                            getHelp={getNotifierHelp}
                        />
                    )
                );

                return (
                    <>
                        {notificationFields}
                        <div className="center-align mt8">
                            <SoftButton
                                icon="Plus"
                                title="New Notification"
                                onClick={() =>
                                    handleNewNotification(arrayHelpers)
                                }
                            />
                        </div>
                    </>
                );
            }}
        />
    );
};

const ScheduleExportsForm: React.FC<{
    docId: number;
    exporters: IQueryResultExporter[];
}> = ({ docId, exporters }) => {
    const name = 'kwargs.exports';
    const { values, setFieldValue } = useFormikContext<IScheduleFormValues>();
    const queryCellOptions = useSelector((state: IStoreState) =>
        queryCellSelector(state, docId)
    );
    const exportsValues = values.kwargs.exports ?? [];

    return (
        <FieldArray
            name={name}
            render={(arrayHelpers) => {
                const exportFields = exportsValues.map((exportConf, index) => {
                    const exportFormName = `${name}[${index}]`;

                    const cellPickerField = (
                        <SimpleField
                            label="Export Cell"
                            name={`${exportFormName}.exporter_cell_id`}
                            type="react-select"
                            options={queryCellOptions.map((val) => ({
                                value: val.id,
                                label: val.title,
                            }))}
                            withDeselect
                        />
                    );

                    const exporter = exporters.find(
                        (exp) => exp.name === exportConf.exporter_name
                    );
                    const exporterPickerField = (
                        <SimpleField
                            label="Export with"
                            name={`${exportFormName}.exporter_name`}
                            type="react-select"
                            options={exporters.map((exp) => exp.name)}
                            onChange={(v) => {
                                setFieldValue(
                                    `${exportFormName}.exporter_name`,
                                    v
                                );
                                setFieldValue(
                                    `${exportFormName}.exporter_params`,
                                    exporter?.form
                                        ? getDefaultFormValue(exporter.form)
                                        : {}
                                );
                            }}
                        />
                    );
                    const exporterFormField = exporter?.form && (
                        <>
                            <FormSectionHeader>
                                Export Parameters
                            </FormSectionHeader>
                            <SmartForm
                                formField={exporter.form}
                                value={
                                    values.kwargs.exports[index].exporter_params
                                }
                                onChange={(path, value) =>
                                    setFieldValue(
                                        `${exportFormName}.exporter_params`,
                                        updateValue(
                                            values.kwargs.exports[index]
                                                .exporter_params,
                                            path,
                                            value,
                                            [undefined, '']
                                        )
                                    )
                                }
                            />
                        </>
                    );

                    return (
                        <div
                            key={index}
                            className="cell-export-field mb24 flex-row"
                        >
                            <div className="flex1 mr16">
                                {cellPickerField}
                                {exporterPickerField}
                                {exporterFormField}
                            </div>
                            <div>
                                <IconButton
                                    icon="X"
                                    onClick={() => arrayHelpers.remove(index)}
                                />
                            </div>
                        </div>
                    );
                });

                const controlDOM = (
                    <div className="center-align mt8">
                        <SoftButton
                            icon="Plus"
                            title="New Query Cell Result Export"
                            onClick={() =>
                                arrayHelpers.push({
                                    exporter_cell_id: null,
                                    exporter_name: null,
                                    exporter_params: {},
                                })
                            }
                        />
                    </div>
                );
                return (
                    <div className="ScheduleExportsForm">
                        {exportFields}
                        {controlDOM}
                    </div>
                );
            }}
        />
    );
};
