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

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.metadata.leafnode.BlockMetadata;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.datastorage.TableDataStore;

/**
 * Class which is responsible for loading the b+ tree block. This class will
 * persist all the detail of the table block
 */
public class CarbonTableBlock {

    private static final LogService LOGGER =
            LogServiceFactory.getLogService(CarbonTableBlock.class.getName());

    /**
     * block info which will have all the detail about the block eg: block
     * offset,length,etc.
     */

    private CarbonTableBlockInfo blockInfo;
    /**
     * vo class which will hold the RS information of the block
     */
    private BlockRSInfo blockRSInfo;

    /**
     * data store block which will hold the b+ tree
     */
    private TableDataStore dataBlockStore;

    CarbonTableBlock(CarbonTableBlockInfo blockInfo) {
        this.blockInfo = blockInfo;
    }

    /**
     * Below method will be used to load the b+ tree block
     *
     * @param blockInfo block detail
     */
    public void loadCarbonTableBlock() {
        BlockMetadata readBlockMetadata = CarbonUtil.readBlockMetadata(blockInfo);
        blockRSInfo = new BlockRSInfo(readBlockMetadata);
    }

}
