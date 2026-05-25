import React, { useState } from 'react';
import { useDispatch } from 'react-redux';

import { DataDocSchedule } from 'components/DataDocSchedule/DataDocSchedule';
import { getScheduledDocs } from 'redux/scheduledDataDoc/action';
import { Dispatch } from 'redux/store/types';
import { IconButton } from 'ui/Button/IconButton';
import { Modal } from 'ui/Modal/Modal';

export const DataDocScheduleActionEdit: React.FunctionComponent<{
    docId: number;
    isPublic: boolean;
    isEditable: boolean;
    actionText: string;
}> = ({ docId, isPublic, isEditable, actionText }) => {
    const [showModal, setShowModal] = useState(false);
    const dispatch: Dispatch = useDispatch();

    const buttonIcon = !isEditable
        ? 'Eye'
        : actionText === 'Edit Schedule'
        ? 'Edit'
        : 'Plus';

    return (
        <>
            {showModal && (
                <Modal
                    onHide={() => {
                        setShowModal(false);
                        dispatch(getScheduledDocs({}));
                    }}
                >
                    <div className="DataDocSchedule">
                        <DataDocSchedule
                            docId={docId}
                            isEditable={isEditable}
                            isPublic={isPublic}
                            currentTab={'schedule'}
                        />
                    </div>
                </Modal>
            )}
            <IconButton
                onClick={() => setShowModal(true)}
                icon={buttonIcon}
                tooltip={actionText}
                tooltipPos="left"
            />
        </>
    );
};

export const DataDocScheduleActionHistory: React.FunctionComponent<{
    docId: number;
    docTitle: string;
    actionText?: string;
}> = ({ docId, docTitle, actionText = 'View Run Record' }) => {
    const [showModal, setShowModal] = useState(false);

    return (
        <>
            {showModal && (
                <Modal
                    onHide={() => setShowModal(false)}
                    title={docTitle + ' Run Record'}
                >
                    <div className="schedule-options">
                        <DataDocSchedule
                            docId={docId}
                            isEditable={false}
                            isPublic={true}
                            currentTab={'history'}
                        />
                    </div>
                </Modal>
            )}
            <IconButton
                onClick={() => setShowModal(true)}
                icon="List"
                tooltip={actionText}
                tooltipPos="left"
            />
        </>
    );
};
