import moment from 'moment';
import React from 'react';
import { useSelector } from 'react-redux';

import { StatusTypes } from 'const/schedule';
import { formatDuration, generateFormattedDate } from 'lib/utils/datetime';
import { Icon } from 'ui/Icon/Icon';
import { getWithinEnvUrl } from 'lib/utils/query-string';
import { IScheduledDoc } from 'redux/scheduledDataDoc/types';
import { IStoreState } from 'redux/store/types';
import { Link } from 'ui/Link/Link';
import { StyledText, UntitledText } from 'ui/StyledText/StyledText';
import { Tag } from 'ui/Tag/Tag';

import {
    DataDocScheduleActionEdit,
    DataDocScheduleActionHistory,
} from './DataDocScheduleActionButtons';
import { HumanReadableCronSchedule } from './HumanReadableCronSchedule';
import { NextRun } from './NextRun';
import { COLUMN_STYLES } from './scheduleColumns';

import './DataDocScheduleItem.scss';

interface IDataDocScheduleItemProps {
    docWithSchedule: IScheduledDoc;
}

function getRunTime(startTime: number, endTime: number) {
    const timeDiff = Math.ceil(endTime - startTime);
    if (timeDiff === 0) {
        return 'less than 1s';
    }
    return formatDuration(moment.duration(timeDiff, 'seconds'));
}

function formatLimits(
    timeoutSeconds: number | undefined,
    maxRetries: number | undefined
): string {
    const parts: string[] = [];
    if (timeoutSeconds) {
        parts.push(`Timeout: ${Math.round(timeoutSeconds / 60)}m`);
    }
    if (maxRetries) {
        parts.push(`Retries: ${maxRetries}`);
    }
    return parts.length ? parts.join(' · ') : '—';
}

export const DataDocScheduleItem: React.FC<IDataDocScheduleItemProps> = ({
    docWithSchedule,
}) => {
    const { doc, schedule, last_record: lastRecord } = docWithSchedule;
    const myUid = useSelector(
        (state: IStoreState) => state.user.myUserInfo?.uid
    );
    const isEditable = doc.owner_uid === myUid;
    const isScheduleDisabled = schedule?.enabled === false;

    const titleNode = doc.title ? (
        <StyledText
            size="text"
            color={isScheduleDisabled ? 'lightest' : 'text'}
        >
            {doc.title}
        </StyledText>
    ) : (
        <UntitledText size="text" />
    );

    const lastStatus = lastRecord ? StatusTypes[lastRecord.status] : null;
    const lastRunTooltip =
        lastRecord && lastStatus
            ? `${lastStatus.text} — ran on ${generateFormattedDate(
                  lastRecord.created_at
              )} for ${getRunTime(
                  lastRecord.created_at,
                  lastRecord.updated_at
              )}`
            : '';

    return (
        <div className="DataDocScheduleItem">
            <div style={COLUMN_STYLES.docTitle} title={doc.title || ''}>
                <Link to={getWithinEnvUrl(`/datadoc/${doc.id}/`)}>
                    {titleNode}
                </Link>
            </div>

            <div style={COLUMN_STYLES.status}>
                {schedule ? (
                    <Tag
                        mini
                        highlighted={schedule.enabled}
                        light={!schedule.enabled}
                    >
                        {schedule.enabled ? 'Enabled' : 'Disabled'}
                    </Tag>
                ) : (
                    <StyledText color="lightest">—</StyledText>
                )}
            </div>

            <div style={COLUMN_STYLES.cron}>
                {schedule ? (
                    <StyledText size="text">
                        <HumanReadableCronSchedule cron={schedule.cron} />
                    </StyledText>
                ) : (
                    <StyledText color="lightest">—</StyledText>
                )}
            </div>

            <div style={COLUMN_STYLES.nextRun}>
                {schedule ? (
                    schedule.enabled ? (
                        <NextRun cron={schedule.cron} />
                    ) : (
                        <StyledText color="lightest">Disabled</StyledText>
                    )
                ) : (
                    <StyledText color="lightest">—</StyledText>
                )}
            </div>

            <div style={COLUMN_STYLES.limits}>
                <StyledText color="light" size="small">
                    {schedule
                        ? formatLimits(
                              schedule.kwargs?.timeout_seconds,
                              schedule.kwargs?.max_retries
                          )
                        : '—'}
                </StyledText>
            </div>

            <div
                style={COLUMN_STYLES.lastRun}
                aria-label={lastRunTooltip || undefined}
                data-balloon-pos={lastRunTooltip ? 'left' : undefined}
                className={lastStatus ? lastStatus.class : undefined}
            >
                {lastStatus ? (
                    <Icon name={lastStatus.iconName} size={16} />
                ) : null}
            </div>

            <div style={COLUMN_STYLES.actions}>
                <DataDocScheduleActionEdit
                    docId={doc.id}
                    isPublic={doc.public}
                    isEditable={isEditable}
                    actionText={
                        isEditable
                            ? schedule
                                ? 'Edit Schedule'
                                : 'New Schedule'
                            : 'View Schedule'
                    }
                />
                {schedule && lastRecord && (
                    <DataDocScheduleActionHistory
                        docId={doc.id}
                        actionText="View Run Record"
                        docTitle={doc.title}
                    />
                )}
            </div>
        </div>
    );
};
