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
package org.carbondata.core.carbon.datastore.block.tree.impl;

import org.carbondata.core.carbon.datastore.block.store.dimension.DimensionBlocksReader;
import org.carbondata.core.carbon.datastore.block.store.dimension.DimensionDataChunkHolder;
import org.carbondata.core.carbon.datastore.block.store.dimension.compressed.CompressedDimensionColumnFileStore;
import org.carbondata.core.carbon.datastore.block.store.measure.MeasureBlocksReader;
import org.carbondata.core.carbon.datastore.block.store.measure.MeasureDataChunkHolder;
import org.carbondata.core.carbon.datastore.block.tree.BTreeNode;
import org.carbondata.core.carbon.datastore.block.tree.NodeEntry;
import org.carbondata.core.carbon.metadata.index.LeafNodeMinMaxIndex;
import org.carbondata.core.carbon.metadata.leafnode.BlockMetadata;
import org.carbondata.core.datastorage.store.FileHolder;

public class BTreeLeafNode extends BTreeNode {

    /**
     * number of keys in a btree
     */
    private int numberOfKeys;

    /**
     * node number
     */
    private int nodeNumber;

    /**
     * Next node of the leaf
     */
    private BTreeNode nextNode;

    /**
     * max key of the column this will be used to check
     * whether this leaf will be used for scanning or not
     */
    private byte[][] maxKeyOfColumns;

    /**
     * min key of the column this will be used to check
     * whether this leaf will be used for scanning or not
     */
    private byte[][] minKeyOfColumns;

    /**
     * reader for dimension blocks
     */
    private DimensionBlocksReader dimensionBlocksReader;

    /**
     * reader for measure blocks
     */
    private MeasureBlocksReader measureBlocksReader;

    public BTreeLeafNode(BlockMetadata blockMetadata, FileHolder fileHolder, int leafIndex,
            String filePath, short[] eachBlockSize) {
        LeafNodeMinMaxIndex leafNodeMinMaxIndex =
                blockMetadata.getLeafNodeIndex().getMinMaxIndexList().get(leafIndex);
        maxKeyOfColumns = leafNodeMinMaxIndex.getMaxValues();
        minKeyOfColumns = leafNodeMinMaxIndex.getMinValues();
        numberOfKeys = blockMetadata.getLeafNodeList().get(leafIndex).getNumberOfRows();
        dimensionBlocksReader = new CompressedDimensionColumnFileStore(
                blockMetadata.getLeafNodeList().get(leafIndex).getDimensionColumnChunk(),
                eachBlockSize, filePath);
    }

    @Override public NodeEntry[] getNodeKeys() {
        //as this is a leaf node so this method implementation is not required
        return null;
    }

    @Override public int blockSize() {
        return this.numberOfKeys;
    }

    @Override public long getNodeNumber() {
        return this.nodeNumber;
    }

    @Override public byte[] getColumnMaxValue(int columnOrdinal) {
        return maxKeyOfColumns[columnOrdinal];
    }

    @Override public byte[] getColumnMinValue(int columnOrdinal) {
        return minKeyOfColumns[columnOrdinal];
    }

    @Override public boolean isLeafNode() {
        return true;
    }

    @Override public BTreeNode getNext() {
        return this.nextNode;
    }

    @Override public void setChildren(BTreeNode[] children) {
        // leaf node in btree will not have any children
    }

    @Override public void setNextNode(BTreeNode nextNode) {
        this.nextNode = nextNode;
    }

    @Override public BTreeNode getChild(int index) {
        return null;
    }

    @Override
    public DimensionDataChunkHolder[] getDimensionDataChunks(int[] blockIndexes, FileHolder fileReader,
            boolean[] needCompressedData) {
        return dimensionBlocksReader.readDataChunks(blockIndexes, fileReader, needCompressedData);
    }

    @Override
    public DimensionDataChunkHolder getDimensionDataChunk(int blockIndex, FileHolder fileReader,
            boolean needCompressedData) {
        return dimensionBlocksReader.readerDataChunk(blockIndex, fileReader, needCompressedData);
    }

    @Override
    public MeasureDataChunkHolder getMeasureDataChunk(FileHolder fileReader, int... blockIndexes) {
        return measureBlocksReader.readDataChunks(fileReader, blockIndexes);
    }

    @Override public void setKey(NodeEntry key) {
        // TODO Auto-generated method stub

    }
}
