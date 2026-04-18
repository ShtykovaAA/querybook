import React from 'react';

import { TaskStatusIcon } from 'components/Task/TaskStatusIcon';
import { ITaskStatusRecord, IRunRecordExecution } from 'const/schedule';
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

const RunRecordRow: React.FC<{
    record: ITaskStatusRecord;
    expanded: boolean;
    onToggle: () => void;
    docId: number;
}> = ({ record, expanded, onToggle, docId }) => (
    <div
        style={{
            borderBottom: '1px solid var(--bg-lightest)',
            paddingBottom: 8,
            marginBottom: 8,
        }}
    >
        <div className="horizontal-space-between" style={{ gap: 12 }}>
            <IconButton
                icon={expanded ? 'ChevronDown' : 'ChevronRight'}
                onClick={onToggle}
                noPadding
                title="Expand"
            />
            <div style={{ flex: 1, minWidth: 80 }}>
                <b>#{record.id}</b>
            </div>
            <div style={{ flex: 2 }}>
                {generateFormattedDate(record.created_at, 'X')}
            </div>
            <div style={{ flex: 2 }}>
                {generateFormattedDate(record.updated_at, 'X')}
            </div>
            <div style={{ flex: 1 }}>
                <TaskStatusIcon type={record.status} />
            </div>
            <div
                style={{
                    flex: 5,
                    whiteSpace: 'pre-wrap',
                    wordBreak: 'break-all',
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

export const DataDocScheduleRunLogs: React.FunctionComponent<{
    docId: number;
}> = ({ docId }) => {
    const { isLoading, isError, data } = useResource(
        React.useCallback(() => DataDocScheduleResource.getLogs(docId), [docId])
    );
    const [expandedId, setExpandedId] = React.useState<number | null>(null);

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
            {data.map((record) => (
                <RunRecordRow
                    key={record.id}
                    record={record}
                    docId={docId}
                    expanded={expandedId === record.id}
                    onToggle={() =>
                        setExpandedId((cur) =>
                            cur === record.id ? null : record.id
                        )
                    }
                />
            ))}
        </div>
    );
};
