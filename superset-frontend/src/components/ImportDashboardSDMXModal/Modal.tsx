import React, { useState } from 'react';
import PropTypes from 'prop-types';
import './Modal.css'; // Assuming you place your CSS in this file
import { SupersetClient } from '@superset-ui/core';
import { Upload, Space } from 'antd';
import { set } from 'lodash';
import { useToasts } from 'src/components/MessageToasts/withToasts';
import { Input } from '../Input';
import Button from '../Button';

const Modal = ({ isOpen, onClose }) => {
  if (!isOpen) {
    return null;
  }

  const [sdmxFile, setSdmxFile] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const { addDangerToast } = useToasts();

  const onUpload = () => {
    const formData = new FormData();
    setIsLoading(true);
    formData.append('file', sdmxFile[0]);

    SupersetClient.post({
      endpoint: 'api/v1/sdmx/dashboard',
      postPayload: formData,
    })
      .then(res => {
        setIsLoading(false);
        window.location.reload();
      })
      .catch(err => {
        setIsLoading(false);
        addDangerToast('There was an error loading the SDMX file');
        setSdmxFile([]);
      });
  };

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-content" onClick={e => e.stopPropagation()}>
        <h3>Import SDMX Dashboard YAML definition</h3>
        <p style={{ color: 'rgb(75, 75, 75)' }}>
          A sample YAML file can be downloaded{' '}
          <a href="https://sdmx.org/wp-content/uploads/ExampleDashboardILOSTAT-SDMXHackathon-v1.1.yaml.7z">
            here
          </a>
          . This is a proof of concept of what a declaration of graphs through
          configuration files may look like.{' '}
          <a href="https://www.sdmx2023.org/hackathon" target="_blank">
            Learn more about this project
          </a>
        </p>

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
              buttonStyle="primary"
              disabled={!sdmxFile.length}
              loading={isLoading}
            >
              {isLoading ? 'Loading...' : 'Upload'}
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
