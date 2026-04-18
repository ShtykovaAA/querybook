import React, { useState } from 'react';
import { useDispatch } from 'react-redux';

import { DataDocSchedule } from 'components/DataDocSchedule/DataDocSchedule';
import { getScheduledDocs } from 'redux/scheduledDataDoc/action';
import { Dispatch } from 'redux/store/types';
import { Button } from 'ui/Button/Button';
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
            <Button
                onClick={() => setShowModal(true)}
                icon={buttonIcon}
                title={actionText}
            />
        </>
    );
};

export const DataDocScheduleActionHistory: React.FunctionComponent<{
    docId: number;
    docTitle: string;
    actionText?: string;
}> = ({ docId, docTitle, actionText = 'History' }) => {
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
            <Button
                onClick={() => setShowModal(true)}
                icon="List"
                title={actionText}
            />
        </>
    );
};
