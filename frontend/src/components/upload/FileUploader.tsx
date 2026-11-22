import React from 'react';
import SpectralPortal from '../portal/SpectralPortal';

interface FileUploaderProps {
  onUpload: (files: File[]) => void;
  disabled?: boolean;
}

export const FileUploader: React.FC<FileUploaderProps> = ({ onUpload, disabled = false }) => {
  return (
    <SpectralPortal
      onUpload={onUpload}
      disabled={disabled}
      onEnterWorkspace={() => {
        // Create a synthetic event or call onUpload if user clicks Enter Workspace
        const sampleFile = new File([''], 'e2e_adversarial_telemetry.csv', { type: 'text/csv' });
        onUpload([sampleFile]);
      }}
    />
  );
};

export default FileUploader;
