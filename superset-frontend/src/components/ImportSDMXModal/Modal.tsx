import React, { useState, useEffect } from 'react';
import PropTypes from 'prop-types';
import { SupersetClient } from '@superset-ui/core';
import { Select, Space } from 'antd';
import Button from '../Button';
import { Input } from '../Input';
import { useToasts } from 'src/components/MessageToasts/withToasts';
import Modal from 'src/components/Modal';
import { FormLabel } from '../Form';
import Tabs, { TabsProps } from 'src/components/Tabs';
import { isPlainObject } from 'lodash';
import "./style.css"

const SdmxImportModal = ({ isOpen, onClose }: { isOpen: boolean, onClose: () => {} }) => {

  const { TabPane } = Tabs;

  if (!isOpen) {
    return null;
  }
  const { addDangerToast } = useToasts();

  const [sdmxUrl, setSdmxUrl] = useState('');
  const [isLoadingDataset, setIsLoading] = useState(false);
  const [currentTab, setCurrentTab] = useState('1');
  const [agencies, setAgencies] = useState([] as string[]);
  const [selectedAgency, setSelectedAgency] = useState(null as null | string);
  const [dataflows, setDataflows] = useState({});
  const [isLoadingDataflows, setIsLoadingDataflows] = useState(false);
  const [selectedDataflow, setSelectedDataflow] = useState(null as null | string);
  const [numberOfObservations, setNumberOfObservations] = useState(5);

  const loadDataflows = (agencyId: string) => {
    if (!dataflows[agencyId]) {
      setIsLoadingDataflows(true)
      SupersetClient.get({
        endpoint: `api/v1/sdmx/agency/${agencyId}`
      }).then(res => {
        setDataflows({ ...dataflows, [agencyId]: res.json })
      }).catch(err => {
        addDangerToast("There was an error loading the available dataflows");
      }).finally(() => {
        setIsLoadingDataflows(false)
      })
    }
  }

  const onUpload = () => {
    setIsLoading(true)
    if (currentTab === '1') {
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
        addDangerToast("There was an error loading the SDMX Url");
        setSdmxUrl('');
      })
    } else if (currentTab === '2') {
      SupersetClient.post({
        endpoint: 'api/v1/sdmx/',
        jsonPayload: {
          agencyId: selectedAgency,
          dataflowId: selectedDataflow,
          numberOfObservations
        },
      }).then(res => {
        setIsLoading(false);
        window.location.reload();
      }).catch(err => {
        setIsLoading(false);
        addDangerToast("There was an error loading the SDMX dataflow");
      })
    }
  };

  useEffect(() => {
    SupersetClient.get({
      endpoint: 'api/v1/sdmx/agency'
    }).then(res => {
      setAgencies(res.json as string[])
    }).catch(err => {
      addDangerToast("There was an error loading the SDMX Url");
      setSdmxUrl('');
    })
  }, [])

  const handleAgencyChange = (agencyId: string) => {
    if (!dataflows[agencyId]) {
      setDataflows({ ...dataflows, [agencyId]: [] })
    }
    setSelectedAgency(agencyId)

    loadDataflows(agencyId)
    setSelectedDataflow(null)
    setNumberOfObservations(5)
  }

  return (
    <Modal show={isOpen} title="Import SDMX Dataset" onHide={onClose}
      footer={
        <>
          <Button
            onClick={onUpload}
            buttonStyle="primary"
            disabled={!!(currentTab === '1' && sdmxUrl.length === 0) || !!(currentTab === '2' && !selectedDataflow)}
            loading={isLoadingDataset}
          >
            {!isLoadingDataset ? "Load" : "Loading..."}
          </Button>
        </>
      }>
      <p style={{ color: "rgb(75, 75, 75)" }}>
        SDMX (Statistical Data and Metadata eXchange) is an international standard for exchanging statistical data and metadata. Supported by major international organizations like the IMF, World Bank, and OECD, it is widely used in various domains like agriculture, finance, and social statistics.
        <a target='_blank' href='https://sdmx.org/' style={{ 'padding': '0 1em' }}>Learn more </a>
      </p>
      <Tabs style={{ marginTop: "1em" }} onChange={(tabId: string) => setCurrentTab(tabId)}>
        <TabPane tab="SDMX Url" key="1">
          <Space direction="vertical" size={12} style={{ width: "100%" }}>
            <FormLabel>SDMX Url</FormLabel>
            <Input
              placeholder="Insert SDMX url..."
              value={sdmxUrl}
              onChange={evt => setSdmxUrl(evt.target.value)}
            />
          </Space>
        </TabPane>
        <TabPane tab="Explore" key="2">
          <Space direction="vertical" size={12} style={{ width: "100%" }}>
            <FormLabel>Select Agency</FormLabel>
            <Select onChange={(agencyId) => handleAgencyChange(agencyId)} style={{ width: '100%' }}
              value={selectedAgency as string}
              options={agencies.map((option, index) => {
                return {
                  label: option,
                  value: option,
                }
              })}
              loading={isLoadingDataflows}
              disabled={isLoadingDataflows}
            />
            {selectedAgency &&
              <>
                <FormLabel>Select Dataflow</FormLabel>
                <Select onChange={(dataflowId: string) => setSelectedDataflow(dataflowId)}
                  style={{ width: '100%' }}
                  value={selectedDataflow as string}
                  disabled={isLoadingDataflows}
                  loading={isLoadingDataflows}
                  options={dataflows[selectedAgency].map((option: any) => {
                    if (isPlainObject(option.name))
                      option.name = option.name.en.content
                    return {
                      label: option.name,
                      value: option.unique_id,
                    }
                  })} />
                <FormLabel>Last time observations</FormLabel>
                <Input type='number' placeholder="Number of last time observations" value={numberOfObservations} min={1} onChange={(evt) => setNumberOfObservations(Number(evt.target.value))} />
              </>}
          </Space>
        </TabPane>

      </Tabs>
    </Modal>
  );
};

SdmxImportModal.propTypes = {
  isOpen: PropTypes.bool.isRequired,
  onClose: PropTypes.func,
  children: PropTypes.node,
};

export default SdmxImportModal;
