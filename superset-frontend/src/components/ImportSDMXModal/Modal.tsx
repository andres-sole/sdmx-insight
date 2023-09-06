import React, { useState } from 'react';
import PropTypes from 'prop-types';
import './Modal.css'; // Assuming you place your CSS in this file
import { SupersetClient } from '@superset-ui/core';
import { Space } from 'antd';
import Button from '../Button';
import { Input } from '../Input';

const Modal = ({ isOpen, onClose, children }) => {
  if (!isOpen) {
    return null;
  }

  const [sdmxUrl, setSdmxUrl] = useState('');

  const onUpload = () => {
    SupersetClient.post({
      endpoint: 'api/v1/sdmx/',
      jsonPayload: {
        sdmxUrl,
      },
    }).then(res => {
      window.location.reload();
    });
  };

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-content" onClick={e => e.stopPropagation()}>
        <h3>Import SDMX</h3>
        <div className="file-uploader">
          <Space direction="horizontal">
            <Input
              placeholder="SDMX url"
              value={sdmxUrl}
              onChange={evt => setSdmxUrl(evt.target.value)}
            />
            {children}
            <Button
              onClick={onUpload}
              buttonStyle="primary"
              disabled={sdmxUrl.length === 0}
            >
              Load
            </Button>
          </Space>
        </div>
        <Button onClick={onClose}>Cancel</Button>
      </div>
    </div>
  );
};

Modal.propTypes = {
  isOpen: PropTypes.bool.isRequired,
  onClose: PropTypes.func,
  children: PropTypes.node,
};

export default Modal;
