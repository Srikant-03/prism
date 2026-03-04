/**
 * PrivacyDisclosure.tsx â€” Mandatory disclosure for LLM usage.
 */

import React, { useState } from 'react';
import { Alert, Checkbox } from 'antd';
import { SafetyCertificateOutlined } from '@ant-design/icons';

const PrivacyDisclosure: React.FC = () => {
    const [accepted, setAccepted] = useState(!!localStorage.getItem('privacy_accepted'));

    if (accepted) return null;

    return (
        <div style={{ padding: '8px 16px', borderBottom: '1px solid rgba(255,255,255,0.1)' }}>
            <Alert
                message="AI Privacy Notice"
                description={
                    <div style={{ fontSize: 11 }}>
                        This platform uses Gemini AI to analyze schema and metadata.
                        Your raw data rows are not sent to the model unless explicitly requested.
                        <div style={{ marginTop: 8 }}>
                            <Checkbox onChange={e => {
                                if (e.target.checked) {
                                    localStorage.setItem('privacy_accepted', 'true');
                                    setAccepted(true);
                                }
                            }}>
                                I understand and wish to use AI features.
                            </Checkbox>
                        </div>
                    </div>
                }
                type="info"
                showIcon
                icon={<SafetyCertificateOutlined />}
            />
        </div>
    );
};

export default PrivacyDisclosure;

