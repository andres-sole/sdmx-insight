import React, { useState } from 'react';
import PropTypes from 'prop-types';
import './Modal.css'; // Assuming you place your CSS in this file
import { SupersetClient } from '@superset-ui/core';
import { Upload, Space } from 'antd';
import { set } from 'lodash';
import { Input } from '../Input';
import Button from '../Button';

const Modal = ({ isOpen, onClose }) => {
  if (!isOpen) {
    return null;
  }

  const [sdmxFile, setSdmxFile] = useState([]);

  const onUpload = () => {
    const formData = new FormData();

    formData.append('file', sdmxFile[0]);

    SupersetClient.post({
      endpoint: 'api/v1/sdmx/dashboard',
      postPayload: formData,
    }).then(res => {
      window.location.reload();
    });
  };

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-content" onClick={e => e.stopPropagation()}>
        <h3>Import SDMX Dashboard YAML definition</h3>
        <div className="file-uploader">
          <Space direction="vertical">
            <Upload
              multiple={false}
              accept=".yaml"
              fileList={sdmxFile}
              beforeUpload={value => {
                setSdmxFile([value]);
                return false;
              }}
            >
              <Button>Select file</Button>
            </Upload>
            <Button
              onClick={onUpload}
              buttonStyle="tertiary"
              disabled={!sdmxFile.length}
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
