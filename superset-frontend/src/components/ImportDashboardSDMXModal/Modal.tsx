import React, { useState } from 'react';
import PropTypes from 'prop-types';
import { SupersetClient } from '@superset-ui/core';
import { Upload, Space, Select } from 'antd';
import Modal from 'src/components/Modal';
import { useToasts } from 'src/components/MessageToasts/withToasts';
import { FormLabel } from 'src/components/Form';
import { RcFile } from 'antd/lib/upload';
import Button from '../Button';

const SdmxDashboardModal = ({
  isOpen,
  onClose,
}: {
  isOpen: boolean;
  onClose: () => void;
}) => {
  const [sdmxFile, setSdmxFile] = useState([] as RcFile[]);
  const [isLoading, setIsLoading] = useState(false);
  const [locale, setLocale] = useState('en');
  const { addDangerToast } = useToasts();

  if (!isOpen) {
    return null;
  }

  const onUpload = () => {
    const formData = new FormData();
    setIsLoading(true);
    formData.append('file', sdmxFile[0]);
    formData.append('locale', locale);

    SupersetClient.post({
      endpoint: 'api/v1/sdmx/dashboard',
      postPayload: formData,
    })
      .then(res => {
        setIsLoading(false);
        window.location.pathname = `/superset/dashboard/${res.json.dashboard_id}`;
      })
      .catch(err => {
        setIsLoading(false);
        addDangerToast('There was an error loading the SDMX file');
        setSdmxFile([]);
      });
  };

  return (
    <Modal
      show={isOpen}
      title="Import SDMX Dashboard YAML definition"
      onHide={onClose}
      footer={
        <>
          <Button
            onClick={onUpload}
            buttonStyle="primary"
            disabled={!sdmxFile.length}
            loading={isLoading}
          >
            {isLoading ? 'Loading...' : 'Upload'}
          </Button>
        </>
      }
    >
      <p style={{ color: 'var(--ifm-font-base-color)' }}>
        A sample YAML file can be downloaded{' '}
        <a
          rel="noreferrer"
          href="https://sdmx.org/wp-content/uploads/ExampleDashboardILOSTAT-SDMXHackathon-v1.1.yaml.7z"
        >
          here
        </a>
        . This is a proof of concept of what a declaration of graphs through
        configuration files may look like.{' '}
        <a
          rel="noreferrer"
          href="https://www.sdmx2023.org/hackathon"
          target="_blank"
        >
          Learn more about this project
        </a>
      </p>
      <div>
        <Space direction="vertical">
          <FormLabel>Load YAML Dashboard file</FormLabel>
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

          <FormLabel>Select Locale</FormLabel>
          <Select
            value={locale}
            onChange={value => setLocale(value)}
            options={[
              {
                label: 'English',
                value: 'en',
              },
              {
                label: 'French',
                value: 'fr',
              },
              { label: 'Spanish', value: 'es' },
            ]}
          />
        </Space>
      </div>
    </Modal>
  );
};

SdmxDashboardModal.propTypes = {
  isOpen: PropTypes.bool.isRequired,
  onClose: PropTypes.func,
  children: PropTypes.node,
};

export default SdmxDashboardModal;
