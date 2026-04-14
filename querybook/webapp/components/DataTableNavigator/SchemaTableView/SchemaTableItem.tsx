import { startCase } from 'lodash';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';

import { SchemaTableSortKey } from 'const/metastore';
import type { ITableSearchResult } from 'redux/dataTableSearch/types';
import { IconButton } from 'ui/Button/IconButton';
import { InfinityScroll } from 'ui/InfinityScroll/InfinityScroll';
import { OrderByButton } from 'ui/OrderByButton/OrderByButton';
import { Title } from 'ui/Title/Title';
import { AccentText } from 'ui/StyledText/StyledText';

import type { ITableResultWithSelection } from '../DataTableNavigator';

import './SchemaTableItem.scss';

const TABLE_ITEM_HEIGHT = 28;
const MAX_VISIBLE_AMOUNT = 10;

function calculateMaxHeight(numberOfItems = 1) {
    return Math.min(numberOfItems, MAX_VISIBLE_AMOUNT) * TABLE_ITEM_HEIGHT;
}

const StyledItem = styled.div`
    height: 32px;
`;

const SchemaIconButton = styled(IconButton)`
    padding: 4px;
`;

const TypeSectionLabel = styled.div`
    padding: 4px 8px 2px;
    display: flex;
    align-items: center;
    gap: 6px;
`;

type TypeGroup = {
    label: string;
    icon: string;
    items: ITableResultWithSelection[];
};

function groupTablesByType(
    tables: ITableSearchResult[],
    selectedTableId: number
): TypeGroup[] {
    if (!tables) {
        return [];
    }

    const typeConfig: Array<{
        type: string | null;
        label: string;
        icon: string;
    }> = [
        { type: null, label: 'Tables', icon: 'Table' },
        { type: 'view', label: 'Views', icon: 'Eye' },
        { type: 'materialized_view', label: 'Materialized Views', icon: 'Database' },
        { type: 'sequence', label: 'Sequences', icon: 'Hash' },
        { type: 'function', label: 'Functions', icon: 'Code' },
        { type: 'procedure', label: 'Procedures', icon: 'Settings' },
        { type: 'index', label: 'Indexes', icon: 'List' },
    ];

    const buckets = new Map<string | null, ITableResultWithSelection[]>();
    for (const table of tables) {
        const item: ITableResultWithSelection = {
            ...table,
            selected: table.id === selectedTableId,
            displayName: table.name,
        };
        const key = table.type || null;
        if (!buckets.has(key)) {
            buckets.set(key, []);
        }
        buckets.get(key).push(item);
    }

    const groups: TypeGroup[] = [];
    for (const { type, label, icon } of typeConfig) {
        const items = buckets.get(type);
        if (items?.length > 0) {
            groups.push({ label, icon, items });
        }
    }

    return groups;
}

function prepareSchemaNames(
    tables: ITableSearchResult[],
    selectedTableId: number
): ITableResultWithSelection[] {
    if (!tables) {
        return [];
    }

    return tables.map((table) => ({
        ...table,
        selected: table.id === selectedTableId,
        displayName: table.name,
    }));
}

export const SchemaTableItem: React.FC<{
    name: string;
    onLoadMore: () => Promise<any>;
    tables: ITableSearchResult[];
    selectedTableId: number;
    total: number;
    tableRowRenderer: (table: ITableSearchResult) => React.ReactNode;
    onSortChanged: (
        sortKey?: SchemaTableSortKey | null,
        sortAsc?: boolean | null
    ) => void;
    sortOrder: {
        asc: boolean;
        key: SchemaTableSortKey;
    };
}> = ({
    name,
    onLoadMore,
    tables,
    selectedTableId,
    total,
    tableRowRenderer,
    onSortChanged,
    sortOrder,
}) => {
    const [isExpanded, setIsExpanded] = useState<boolean>(false);
    const data = useMemo(
        () => prepareSchemaNames(tables, selectedTableId),
        [tables, selectedTableId]
    );
    const typeGroups = useMemo(
        () => groupTablesByType(tables, selectedTableId),
        [tables, selectedTableId]
    );
    const hasMultipleTypes = typeGroups.length > 1;

    return (
        <div className="SchemaTableItem mb12">
            <StyledItem className="horizontal-space-between navigator-header pl8">
                <div
                    className="schema-name flex1 flex-row"
                    onClick={() => setIsExpanded(!isExpanded)}
                >
                    <Title size="small" className="one-line-ellipsis">
                        {name}
                    </Title>
                </div>
                <OrderByButton
                    asc={sortOrder.asc}
                    hideAscToggle={sortOrder.key === 'relevance'}
                    orderByField={startCase(sortOrder.key)}
                    orderByFieldSymbol={sortOrder.key === 'name' ? 'Aa' : 'Rl'}
                    onAscToggle={() => onSortChanged(null, !sortOrder.asc)}
                    onOrderByFieldToggle={() =>
                        onSortChanged(
                            sortOrder.key === 'name' ? 'relevance' : 'name'
                        )
                    }
                />

                <div className="flex-row">
                    <SchemaIconButton
                        onClick={() => setIsExpanded(!isExpanded)}
                        icon={isExpanded ? 'ChevronDown' : 'ChevronRight'}
                    />
                </div>
            </StyledItem>

            {isExpanded && (
                <div className="board-scroll-wrapper">
                    {total === 0 ? (
                        <div className="empty-section-message">
                            No tables in {name}
                        </div>
                    ) : hasMultipleTypes ? (
                        <>
                            {typeGroups.map((group) => (
                                <div key={group.label} className="mb8">
                                    <TypeSectionLabel>
                                        <AccentText
                                            size="xsmall"
                                            weight="bold"
                                            color="lightest"
                                        >
                                            {group.label}
                                        </AccentText>
                                    </TypeSectionLabel>
                                    {group.items.map(tableRowRenderer)}
                                </div>
                            ))}
                            <InfinityScroll
                                elements={[]}
                                onLoadMore={onLoadMore}
                                hasMore={!total || total > data.length}
                                itemRenderer={tableRowRenderer}
                                itemHeight={TABLE_ITEM_HEIGHT}
                                defaultListHeight={0}
                                autoSizerStyles={{ height: '0px' }}
                            />
                        </>
                    ) : (
                        <InfinityScroll
                            elements={data}
                            onLoadMore={onLoadMore}
                            hasMore={!total || total > data.length}
                            itemRenderer={tableRowRenderer}
                            itemHeight={TABLE_ITEM_HEIGHT}
                            defaultListHeight={calculateMaxHeight(total)}
                            autoSizerStyles={{
                                height: `${calculateMaxHeight(total)}px`,
                            }}
                        />
                    )}
                </div>
            )}
        </div>
    );
};
