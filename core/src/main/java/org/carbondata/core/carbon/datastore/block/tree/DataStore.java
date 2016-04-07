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
package org.carbondata.core.carbon.datastore.block.tree;

import org.carbondata.core.carbon.metadata.leafnode.BlockMetadata;

public interface DataStore {

    /**
     * Below method will be used to build the btree
     *
     * @param leafNodes       leaf node details
     * @param leafNodeIndexes leaf node indexes
     */
    void buildTree(BlockMetadata blockMetadata, boolean keepDataPageDetail,
            short[] eachDimensionBlockSize, String filePath);

    /**
     * Get the block based on the key
     *
     * @param key
     * @param isFirst as in data block can have duplicate block so to
     *                get the block range we need to get the first entry and last
     *                entry of the key range
     * @return leaf block
     */
    LeafBlock getDataStoreBlock(NodeEntry key, boolean isFirst);

    /**
     * @return the number of tuples in a btree
     */
    long numberOfTuplesInBTree();

}
