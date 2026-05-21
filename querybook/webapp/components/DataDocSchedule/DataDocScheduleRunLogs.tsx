import React from 'react';

import { TaskStatusIcon } from 'components/Task/TaskStatusIcon';
import {
    ITaskStatusRecord,
    IRunRecordExecution,
    TaskRunStatus,
} from 'const/schedule';
import { STATUS_TO_TEXT_MAPPING } from 'const/queryStatus';
import { useResource } from 'hooks/useResource';
import { generateFormattedDate } from 'lib/utils/datetime';
import { DataDocScheduleResource } from 'resource/dataDoc';
import { StatementResource } from 'resource/queryExecution';
import { IconButton } from 'ui/Button/IconButton';
import { Loading } from 'ui/Loading/Loading';
import { ErrorMessage } from 'ui/Message/ErrorMessage';
import { ShowMoreText } from 'ui/ShowMoreText/ShowMoreText';
import { EmptyText, StyledText } from 'ui/StyledText/StyledText';

const RecordExecutionLogs: React.FC<{ statementExecutionId: number }> = ({
    statementExecutionId,
}) => {
    const { isLoading, isError, data } = useResource(
        React.useCallback(
            () => StatementResource.getLogs(statementExecutionId),
            [statementExecutionId]
        )
    );

    if (isLoading) return <Loading />;
    if (isError) {
        return <ErrorMessage>Error loading logs</ErrorMessage>;
    }
    if (!data || data.length === 0) {
        return <EmptyText className="m12">No logs</EmptyText>;
    }
    return (
        <pre
            style={{
                whiteSpace: 'pre-wrap',
                wordBreak: 'break-all',
                background: 'var(--bg-light)',
                padding: 8,
                borderRadius: 4,
                maxHeight: 320,
                overflow: 'auto',
                fontSize: 12,
            }}
        >
            {data.join('\n')}
        </pre>
    );
};

const RunRecordExecutionItem: React.FC<{
    item: IRunRecordExecution;
    index: number;
}> = ({ item, index }) => {
    const [showQuery, setShowQuery] = React.useState(false);
    const [showLogs, setShowLogs] = React.useState<number | null>(null);

    const { query_execution: qe, cell_id, cell_title } = item;
    const cellLabel =
        cell_id == null
            ? 'Cell removed'
            : cell_title || `Untitled Cell #${cell_id}`;

    return (
        <div
            className="RunRecordExecutionItem"
            style={{
                padding: 8,
                marginBottom: 8,
                border: '1px solid var(--bg-lightest)',
                borderRadius: 4,
            }}
        >
            <div className="horizontal-space-between">
                <StyledText weight="bold">
                    #{index + 1} — {cellLabel}
                </StyledText>
                <StyledText color="light" size="small">
                    {STATUS_TO_TEXT_MAPPING[qe.status]} ·{' '}
                    {generateFormattedDate(qe.created_at, 'X')}
                    {qe.completed_at
                        ? ` → ${generateFormattedDate(qe.completed_at, 'X')}`
                        : ''}
                </StyledText>
            </div>
            <div className="mt8 flex-row" style={{ gap: 8 }}>
                <IconButton
                    icon={showQuery ? 'ChevronDown' : 'ChevronRight'}
                    title={showQuery ? 'Hide query' : 'Show query'}
                    onClick={() => setShowQuery((v) => !v)}
                    noPadding
                />
            </div>
            {showQuery && (
                <pre
                    style={{
                        whiteSpace: 'pre-wrap',
                        wordBreak: 'break-all',
                        background: 'var(--bg-light)',
                        padding: 8,
                        borderRadius: 4,
                        marginTop: 8,
                        fontSize: 12,
                    }}
                >
                    {qe.query}
                </pre>
            )}
            {qe.statement_executions && qe.statement_executions.length > 0 && (
                <div className="mt8">
                    <StyledText color="light" size="small">
                        Statements:
                    </StyledText>
                    {qe.statement_executions.map((s) =>
                        s.has_log ? (
                            <div key={s.id} className="mt4">
                                <IconButton
                                    icon={
                                        showLogs === s.id
                                            ? 'ChevronDown'
                                            : 'ChevronRight'
                                    }
                                    title={`Statement #${s.id} logs`}
                                    onClick={() =>
                                        setShowLogs((cur) =>
                                            cur === s.id ? null : s.id
                                        )
                                    }
                                    noPadding
                                />
                                {showLogs === s.id && (
                                    <RecordExecutionLogs
                                        statementExecutionId={s.id}
                                    />
                                )}
                            </div>
                        ) : (
                            <div key={s.id} className="mt4">
                                <StyledText color="light" size="small">
                                    Statement #{s.id}: no logs
                                </StyledText>
                            </div>
                        )
                    )}
                </div>
            )}
        </div>
    );
};

const RecordExecutions: React.FC<{ docId: number; recordId: number }> = ({
    docId,
    recordId,
}) => {
    const { isLoading, isError, data } = useResource(
        React.useCallback(
            () =>
                DataDocScheduleResource.getRecordExecutions(docId, recordId),
            [docId, recordId]
        )
    );

    if (isLoading) return <Loading />;
    if (isError) {
        return <ErrorMessage>Error loading executions</ErrorMessage>;
    }
    if (!data || data.length === 0) {
        return (
            <EmptyText className="m12">
                No query executions recorded for this run.
            </EmptyText>
        );
    }
    return (
        <div className="m8">
            {data.map((item, idx) => (
                <RunRecordExecutionItem
                    key={item.query_execution.id}
                    item={item}
                    index={idx}
                />
            ))}
        </div>
    );
};

interface IRecordGroup {
    parentId: number;
    attempts: ITaskStatusRecord[];
    finalRecord: ITaskStatusRecord;
}

function groupRecords(records: ITaskStatusRecord[]): IRecordGroup[] {
    const groups = new Map<number, ITaskStatusRecord[]>();
    for (const r of records) {
        const key = r.parent_run_record_id ?? r.id;
        if (!groups.has(key)) {
            groups.set(key, []);
        }
        groups.get(key).push(r);
    }
    return [...groups.entries()]
        .map(([parentId, attempts]) => {
            const sorted = [...attempts].sort(
                (a, b) => (a.attempt ?? 1) - (b.attempt ?? 1)
            );
            return {
                parentId,
                attempts: sorted,
                finalRecord: sorted[sorted.length - 1],
            };
        })
        .sort(
            (a, b) => b.attempts[0].created_at - a.attempts[0].created_at
        );
}

const TIMEOUT_TOOLTIP = 'Cancelled after exceeding the configured timeout';

const COL_CHEVRON_PX = 32;
const COL_ID_PX = 160;
const COL_DATE_PX = 140;
const COL_STATUS_PX = 90;

const RunRecordHeader: React.FC = () => (
    <div
        style={{
            display: 'flex',
            alignItems: 'center',
            gap: 12,
            padding: '8px 0',
            borderBottom: '1px solid var(--bg-lightest)',
            fontWeight: 'bold',
            fontSize: 12,
            color: 'var(--text-light)',
        }}
    >
        <div style={{ width: COL_CHEVRON_PX }} />
        <div style={{ width: COL_ID_PX }}>ID</div>
        <div style={{ width: COL_DATE_PX }}>Started</div>
        <div style={{ width: COL_DATE_PX }}>Updated</div>
        <div style={{ width: COL_STATUS_PX }}>Status</div>
        <div style={{ flex: 1, minWidth: 0 }}>Error</div>
    </div>
);

const RecordRow: React.FC<{
    record: ITaskStatusRecord;
    expanded: boolean;
    onToggle: () => void;
    docId: number;
    indent?: boolean;
    label?: React.ReactNode;
}> = ({ record, expanded, onToggle, docId, indent, label }) => {
    const isTimeout = record.status === TaskRunStatus.TIMEOUT;
    return (
        <div
            style={{
                borderBottom: '1px solid var(--bg-lightest)',
                paddingBottom: 8,
                marginBottom: 8,
                marginLeft: indent ? 24 : 0,
            }}
        >
            <div
                style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: 12,
                    padding: '8px 0',
                }}
                aria-label={isTimeout ? TIMEOUT_TOOLTIP : undefined}
                data-balloon-pos={isTimeout ? 'up' : undefined}
            >
                <div style={{ width: COL_CHEVRON_PX, flexShrink: 0 }}>
                    <IconButton
                        icon={expanded ? 'ChevronDown' : 'ChevronRight'}
                        onClick={onToggle}
                        noPadding
                        tooltip="Expand"
                    />
                </div>
                <div style={{ width: COL_ID_PX, flexShrink: 0 }}>
                    <b>#{record.id}</b>
                    {label ? (
                        <StyledText
                            color="light"
                            size="small"
                            className="ml4"
                        >
                            {label}
                        </StyledText>
                    ) : null}
                </div>
                <div style={{ width: COL_DATE_PX, flexShrink: 0 }}>
                    {generateFormattedDate(record.created_at, 'X')}
                </div>
                <div style={{ width: COL_DATE_PX, flexShrink: 0 }}>
                    {generateFormattedDate(record.updated_at, 'X')}
                </div>
                <div style={{ width: COL_STATUS_PX, flexShrink: 0 }}>
                    <TaskStatusIcon type={record.status} />
                </div>
                <div
                    style={{
                        flex: 1,
                        minWidth: 0,
                        whiteSpace: 'pre-wrap',
                        wordBreak: 'break-word',
                    }}
                >
                    {record.error_message ? (
                        <ShowMoreText text={record.error_message} />
                    ) : null}
                </div>
            </div>
            {expanded && <RecordExecutions docId={docId} recordId={record.id} />}
        </div>
    );
};

const SingleAttemptGroup: React.FC<{
    record: ITaskStatusRecord;
    expanded: boolean;
    onToggle: () => void;
    docId: number;
}> = ({ record, expanded, onToggle, docId }) => (
    <RecordRow
        record={record}
        expanded={expanded}
        onToggle={onToggle}
        docId={docId}
    />
);

const MultiAttemptGroup: React.FC<{
    group: IRecordGroup;
    expanded: boolean;
    onToggle: () => void;
    docId: number;
}> = ({ group, expanded, onToggle, docId }) => {
    const [expandedAttemptId, setExpandedAttemptId] = React.useState<
        number | null
    >(null);
    const total = group.attempts.length;
    const finalStatusText =
        group.finalRecord.status === TaskRunStatus.SUCCESS
            ? 'SUCCESS'
            : group.finalRecord.status === TaskRunStatus.TIMEOUT
            ? 'TIMEOUT'
            : 'FAILURE';
    const groupTooltip = `Final status after ${total} attempts: ${finalStatusText}`;
    const first = group.attempts[0];

    return (
        <div
            style={{
                borderBottom: '1px solid var(--bg-lightest)',
                paddingBottom: 8,
                marginBottom: 8,
            }}
        >
            <div
                style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: 12,
                    padding: '8px 0',
                }}
                aria-label={groupTooltip}
                data-balloon-pos="up"
            >
                <div style={{ width: COL_CHEVRON_PX, flexShrink: 0 }}>
                    <IconButton
                        icon={expanded ? 'ChevronDown' : 'ChevronRight'}
                        onClick={onToggle}
                        noPadding
                        tooltip="Expand attempts"
                    />
                </div>
                <div style={{ width: COL_ID_PX, flexShrink: 0 }}>
                    <b>#{group.parentId}</b>
                    <StyledText color="light" size="small" className="ml4">
                        Attempt {group.finalRecord.attempt ?? 1} of {total}
                    </StyledText>
                </div>
                <div style={{ width: COL_DATE_PX, flexShrink: 0 }}>
                    {generateFormattedDate(first.created_at, 'X')}
                </div>
                <div style={{ width: COL_DATE_PX, flexShrink: 0 }}>
                    {generateFormattedDate(
                        group.finalRecord.updated_at,
                        'X'
                    )}
                </div>
                <div style={{ width: COL_STATUS_PX, flexShrink: 0 }}>
                    <TaskStatusIcon type={group.finalRecord.status} />
                </div>
                <div style={{ flex: 1, minWidth: 0 }} />
            </div>
            {expanded && (
                <div className="mt8">
                    {group.attempts.map((attempt) => (
                        <RecordRow
                            key={attempt.id}
                            record={attempt}
                            docId={docId}
                            indent
                            label={`Attempt ${attempt.attempt ?? 1}`}
                            expanded={expandedAttemptId === attempt.id}
                            onToggle={() =>
                                setExpandedAttemptId((cur) =>
                                    cur === attempt.id ? null : attempt.id
                                )
                            }
                        />
                    ))}
                </div>
            )}
        </div>
    );
};

export const DataDocScheduleRunLogs: React.FunctionComponent<{
    docId: number;
}> = ({ docId }) => {
    const { isLoading, isError, data } = useResource(
        React.useCallback(() => DataDocScheduleResource.getLogs(docId), [docId])
    );
    const [expandedKey, setExpandedKey] = React.useState<number | null>(null);

    const groups = React.useMemo(() => groupRecords(data ?? []), [data]);

    if (isLoading) {
        return <Loading />;
    }

    if (isError) {
        return <ErrorMessage>Error Loading DataDoc Schedule</ErrorMessage>;
    }

    if (!data || data.length === 0) {
        return <EmptyText className="m24">No run records yet</EmptyText>;
    }

    return (
        <div>
            <RunRecordHeader />
            {groups.map((group) =>
                group.attempts.length === 1 ? (
                    <SingleAttemptGroup
                        key={group.parentId}
                        record={group.attempts[0]}
                        docId={docId}
                        expanded={expandedKey === group.parentId}
                        onToggle={() =>
                            setExpandedKey((cur) =>
                                cur === group.parentId ? null : group.parentId
                            )
                        }
                    />
                ) : (
                    <MultiAttemptGroup
                        key={group.parentId}
                        group={group}
                        docId={docId}
                        expanded={expandedKey === group.parentId}
                        onToggle={() =>
                            setExpandedKey((cur) =>
                                cur === group.parentId ? null : group.parentId
                            )
                        }
                    />
                )
            )}
        </div>
    );
};
