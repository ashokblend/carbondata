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

import java.util.List;

import org.carbondata.core.carbon.metadata.leafnode.BlockMetadata;
import org.carbondata.core.carbon.metadata.leafnode.LeafNodeInfo;

/**
 * below class is responsible for loading the btree and it will have all the metadata
 * related to Btree
 */
public class BlockDataStore {

    private long numberOfKeys;

    /**
     * Below method load the btree
     *
     * @param blockMetadata
     */
    public void loadBtree(BlockMetadata blockMetadata) {
        List<LeafNodeInfo> leafNodeList = blockMetadata.getLeafNodeList();
    }
}
