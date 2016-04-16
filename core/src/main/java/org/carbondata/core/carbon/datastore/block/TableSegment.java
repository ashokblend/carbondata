/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.carbondata.core.carbon.datastore.block;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.datastore.BlocksBuilder;
import org.carbondata.core.carbon.datastore.BlocksBuilderInfos;
import org.carbondata.core.carbon.datastore.DataBlock;
import org.carbondata.core.carbon.datastore.impl.btree.DriverBtreeFormatBuilder;
import org.carbondata.core.carbon.metadata.leafnode.DataFileMetadata;
import org.carbondata.core.util.CarbonCoreLogEvent;
import org.carbondata.query.util.DataFileMetadataConverter;

/**
 * Class which is responsible for loading the b+ tree block. This class will
 * persist all the detail of a table segment
 */
public class TableSegment {

    private static final LogService LOGGER =
            LogServiceFactory.getLogService(TableBlock.class.getName());
    /**
     * vo class which will hold the RS information of the block
     */
    private SegmentProperties segmentProperties;

    /**
     * to store the segment meta data in some data structure
     */
    private DataBlock dataBlock;

    /**
     * Below method is store the blocks in some data structure
     *
     * @param blockInfo block detail
     */
    public void loadCarbonTableBlock(List<TableBlockInfos> blockInfoList) {

        DataFileMetadataConverter converter = new DataFileMetadataConverter();
        // get the data file metadata from thrift
        List<DataFileMetadata> dataMetadataList =
                new ArrayList<DataFileMetadata>(blockInfoList.size());
        DataFileMetadata dataFileMetadata = null;
        try {
            for (TableBlockInfos blockInfo : blockInfoList) {
                dataFileMetadata = converter
                        .readDataFileMetadata(blockInfo.getFilePath(), blockInfo.getBlockOffset());
                dataMetadataList.add(dataFileMetadata);
            }
        } catch (IOException e) {
            LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
                    e + "Unable to read the block metadata from file");
            return;
        }
        // create a metadata details
        // this will be useful in query handling
        // all the data file metadata will have common segment properties we
        // can use first one to get create the segment properties
        segmentProperties = new SegmentProperties(dataMetadataList.get(0).getColumnInTable(),
                dataMetadataList.get(0).getSegmentInfo().getColumnCardinality());
        // create a segment builder info
        BlocksBuilderInfos segmentBuilderInfos = new BlocksBuilderInfos();
        segmentBuilderInfos.setDataFileMetadataList(dataMetadataList);
        BlocksBuilder blocksBuilder = new DriverBtreeFormatBuilder();
        // load the metadata
        blocksBuilder.build(segmentBuilderInfos);
        dataBlock = blocksBuilder.get();
    }

    /**
     * @return the segmentProperties
     */
    public SegmentProperties getSegmentProperties() {
        return segmentProperties;
    }

    /**
     * @return the dataBlock
     */
    public DataBlock getDataBlock() {
        return dataBlock;
    }

}
