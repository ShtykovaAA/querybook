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

    const tablesGroup: ITableResultWithSelection[] = [];
    const functionsGroup: ITableResultWithSelection[] = [];
    const proceduresGroup: ITableResultWithSelection[] = [];

    for (const table of tables) {
        const item: ITableResultWithSelection = {
            ...table,
            selected: table.id === selectedTableId,
            displayName: table.name,
        };
        if (table.type === 'function') {
            functionsGroup.push(item);
        } else if (table.type === 'procedure') {
            proceduresGroup.push(item);
        } else {
            tablesGroup.push(item);
        }
    }

    const groups: TypeGroup[] = [];
    if (tablesGroup.length > 0) {
        groups.push({ label: 'Tables', icon: 'Table', items: tablesGroup });
    }
    if (functionsGroup.length > 0) {
        groups.push({
            label: 'Functions',
            icon: 'Code',
            items: functionsGroup,
        });
    }
    if (proceduresGroup.length > 0) {
        groups.push({
            label: 'Procedures',
            icon: 'Settings',
            items: proceduresGroup,
        });
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
