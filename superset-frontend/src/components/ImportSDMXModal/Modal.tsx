import React, { useState, useEffect } from 'react';
import PropTypes from 'prop-types';
import { SupersetClient } from '@superset-ui/core';
import { Space } from 'antd';
import { useToasts } from 'src/components/MessageToasts/withToasts';
import Modal from 'src/components/Modal';
import Select from 'src/components/Select/Select';
import Tabs from 'src/components/Tabs';
import { isPlainObject } from 'lodash';
import { FormLabel } from '../Form';
import { Input } from '../Input';
import Button from '../Button';
import './style.css';

const { TabPane } = Tabs;

const SdmxImportModal = ({
  isOpen,
  onClose,
}: {
  isOpen: boolean;
  onClose: () => void;
}) => {
  const { addDangerToast } = useToasts();

  const [sdmxUrl, setSdmxUrl] = useState('');
  const [isLoadingDataset, setIsLoading] = useState(false);
  const [currentTab, setCurrentTab] = useState('1');
  const [agencies, setAgencies] = useState([] as string[]);
  const [selectedAgency, setSelectedAgency] = useState(null as null | string);
  const [dataflows, setDataflows] = useState({});
  const [isLoadingDataflows, setIsLoadingDataflows] = useState(false);
  const [selectedDataflow, setSelectedDataflow] = useState(
    null as null | string,
  );
  const [numberOfObservations, setNumberOfObservations] = useState(1);

  useEffect(() => {
    SupersetClient.get({
      endpoint: 'api/v1/sdmx/agency',
    })
      .then(res => {
        setAgencies(res.json as string[]);
      })
      .catch(err => {
        addDangerToast('There was an error loading the SDMX Url');
        setSdmxUrl('');
      });
  }, []);

  if (!isOpen) {
    return null;
  }

  const loadDataflows = (agencyId: string) => {
    if (!dataflows[agencyId]) {
      setIsLoadingDataflows(true);
      SupersetClient.get({
        endpoint: `api/v1/sdmx/agency/${agencyId}`,
      })
        .then(res => {
          setDataflows({ ...dataflows, [agencyId]: res.json });
        })
        .catch(err => {
          addDangerToast('There was an error loading the available dataflows');
        })
        .finally(() => {
          setIsLoadingDataflows(false);
        });
    }
  };

  const onUpload = () => {
    setIsLoading(true);
    if (currentTab === '1') {
      SupersetClient.post({
        endpoint: 'api/v1/sdmx/',
        jsonPayload: {
          sdmxUrl,
        },
      })
        .then(res => {
          setIsLoading(false);
          window.location.reload();
        })
        .catch(err => {
          setIsLoading(false);
          addDangerToast('There was an error loading the SDMX Url');
          setSdmxUrl('');
        });
    } else if (currentTab === '2') {
      SupersetClient.post({
        endpoint: 'api/v1/sdmx/',
        jsonPayload: {
          agencyId: selectedAgency,
          dataflowId: selectedDataflow,
          numberOfObservations,
        },
      })
        .then(res => {
          setIsLoading(false);
          window.location.reload();
        })
        .catch(err => {
          setIsLoading(false);
          addDangerToast('There was an error loading the SDMX dataflow');
        });
    }
  };

  const filterDataflow = (
    input: string,
    option?: { label: string; value: string },
  ) => (option?.label ?? '').toLowerCase().includes(input.toLowerCase());

  const handleAgencyChange = (agencyId: string) => {
    if (!dataflows[agencyId]) {
      setDataflows({ ...dataflows, [agencyId]: [] });
    }
    setSelectedAgency(agencyId);

    loadDataflows(agencyId);
    setSelectedDataflow(null);
    setNumberOfObservations(1);
  };

  return (
    <Modal
      show={isOpen}
      title="Import SDMX Dataset"
      onHide={onClose}
      footer={
        <>
          <Button
            onClick={onUpload}
            buttonStyle="primary"
            disabled={
              !!(currentTab === '1' && sdmxUrl.length === 0) ||
              !!(currentTab === '2' && !selectedDataflow)
            }
            loading={isLoadingDataset}
          >
            {!isLoadingDataset ? 'Load' : 'Loading...'}
          </Button>
        </>
      }
    >
      <p style={{ color: 'var(--ifm-font-base-color)' }}>
        SDMX (Statistical Data and Metadata eXchange) is an international
        standard for exchanging statistical data and metadata. Supported by
        major international organizations like the IMF, World Bank, and OECD, it
        is widely used in various domains like agriculture, finance, and social
        statistics.
        <a
          rel="noreferrer"
          target="_blank"
          href="https://sdmx.org/"
          style={{ padding: '0 1em' }}
        >
          Learn more{' '}
        </a>
      </p>
      <Tabs
        style={{ marginTop: '1em' }}
        onChange={(tabId: string) => setCurrentTab(tabId)}
      >
        <TabPane tab="SDMX Url" key="1">
          <Space direction="vertical" size={12} style={{ width: '100%' }}>
            <FormLabel>SDMX Url</FormLabel>
            <Input
              placeholder="Insert SDMX url..."
              value={sdmxUrl}
              onChange={evt => setSdmxUrl(evt.target.value)}
            />
          </Space>
        </TabPane>
        <TabPane tab="Explore" key="2">
          <Space direction="vertical" size={12} style={{ width: '100%' }}>
            <FormLabel>Select Agency</FormLabel>
            <Select
              onChange={(agencyId: string) => handleAgencyChange(agencyId)}
              showSearch={false}
              value={selectedAgency as string}
              placeholder="Select an agency"
              options={agencies
                .filter(option => {
                  if (option.endsWith('v2')) return false;
                  return true;
                })
                .map(option => ({
                  label: option,
                  value: option,
                }))}
              loading={isLoadingDataflows}
              disabled={isLoadingDataflows}
            />
            {selectedAgency && (
              <>
                <FormLabel>Select Dataflow</FormLabel>
                <Select
                  onChange={(dataflowId: string) =>
                    setSelectedDataflow(dataflowId)
                  }
                  showSearch
                  filterOption={filterDataflow}
                  placeholder="Select a dataflow"
                  notFoundContent="No dataflows found"
                  value={selectedDataflow as string}
                  disabled={isLoadingDataflows}
                  loading={isLoadingDataflows}
                  options={dataflows[selectedAgency].map((option: any) => {
                    let optionName = option.name;

                    if (isPlainObject(option.name))
                      optionName = optionName.en.content;

                    return {
                      label: optionName,
                      value: option.unique_id,
                    };
                  })}
                />
                <FormLabel>Last time observations</FormLabel>
                <Input
                  type="number"
                  placeholder="Number of last time observations"
                  value={numberOfObservations}
                  min={1}
                  onChange={evt =>
                    setNumberOfObservations(Number(evt.target.value))
                  }
                />
              </>
            )}
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
