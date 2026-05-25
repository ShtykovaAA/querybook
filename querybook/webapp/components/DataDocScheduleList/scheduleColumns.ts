import { CSSProperties } from 'react';

// Single source of truth for table column geometry, shared between
// DataDocScheduleItem (rows) and the header row in DataDocScheduleList.
// Keeping these aligned by hand is fragile; if a width changes here, both
// header and row pick it up automatically.
export const COLUMN_STYLES: Record<
    | 'docTitle'
    | 'status'
    | 'cron'
    | 'nextRun'
    | 'limits'
    | 'lastRun'
    | 'actions',
    CSSProperties
> = {
    docTitle: {
        flex: 2,
        minWidth: 200,
        overflow: 'hidden',
        textOverflow: 'ellipsis',
        whiteSpace: 'nowrap',
    },
    status: { width: 80, flexShrink: 0 },
    cron: { flex: 2, minWidth: 180, overflow: 'hidden' },
    nextRun: { width: 150, flexShrink: 0 },
    limits: { width: 120, flexShrink: 0 },
    lastRun: {
        width: 32,
        flexShrink: 0,
        display: 'flex',
        justifyContent: 'center',
        alignItems: 'center',
    },
    actions: {
        width: 72,
        flexShrink: 0,
        display: 'flex',
        gap: 4,
        justifyContent: 'flex-end',
    },
};
