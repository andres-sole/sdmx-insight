import React, { useState } from 'react';
import PropTypes from 'prop-types';
import { SupersetClient } from '@superset-ui/core';
import { Space } from 'antd';
import Button from '../Button';
import { Input } from '../Input';
import { useToasts } from 'src/components/MessageToasts/withToasts';
import Modal from 'src/components/Modal';
import { FormLabel } from '../Form';



const SdmxImportModal = ({ isOpen, onClose }) => {

  if (!isOpen) {
    return null;
  }
  
  const [sdmxUrl, setSdmxUrl] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const { addDangerToast } = useToasts();

  const onUpload = () => {
    setIsLoading(true)

    
    SupersetClient.post({
      endpoint: 'api/v1/sdmx/',
      jsonPayload: {
        sdmxUrl,
      },
    }).then(res => {
      setIsLoading(false);
      window.location.reload();
    }).catch(err => {
      setIsLoading(false);
      console.log(err)

      addDangerToast("There was an error loading the SDMX Url");
      setSdmxUrl('');
    }
  };

  return (
      <Modal show={isOpen} title="Import SDMX Dataset" onHide={onClose} 
      footer={
        <>
            <Button
              onClick={onUpload}
              buttonStyle="primary"
              disabled={sdmxUrl.length === 0}
              loading={isLoading}
            >
              { !isLoading ? "Load" : "Loading..." }
            </Button>
        </>
      }>
        <p style={{color: "rgb(75, 75, 75)"}}>
          SDMX (Statistical Data and Metadata eXchange) is an international standard for exchanging statistical data and metadata. Supported by major international organizations like the IMF, World Bank, and OECD, it is widely used in various domains like agriculture, finance, and social statistics.
          <a target='_blank' href='https://sdmx.org/' style={{'padding': '0 1em'}}>Learn more </a>
        </p>
        <Space direction="vertical" size={12}>
        <FormLabel>SDMX Url</FormLabel>
            <Input
              placeholder="Insert SDMX url..."
              value={sdmxUrl}
              onChange={evt => setSdmxUrl(evt.target.value)}
            />
          <a target='_blank' href='https://sdmxhub.meaningfuldata.eu/' style={{}}>SDMXHub</a>
        </Space>
      </Modal>
  );
};

SdmxImportModal.propTypes = {
  isOpen: PropTypes.bool.isRequired,
  onClose: PropTypes.func,
  children: PropTypes.node,
};

export default SdmxImportModal;
