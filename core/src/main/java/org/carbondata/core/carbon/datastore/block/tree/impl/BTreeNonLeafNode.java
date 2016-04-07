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

import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.carbon.datastore.block.store.dimension.DimensionDataChunkHolder;
import org.carbondata.core.carbon.datastore.block.store.measure.MeasureDataChunkHolder;
import org.carbondata.core.carbon.datastore.block.tree.BTreeNode;
import org.carbondata.core.carbon.datastore.block.tree.NodeEntry;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.FileHolder;

public class BTreeNonLeafNode extends BTreeNode {

    /**
     * Child nodes
     */
    private BTreeNode[] children;

    /**
     * list of keys in non leaf
     */
    private List<NodeEntry> listOfKeys;

    public BTreeNonLeafNode() {
        listOfKeys = new ArrayList<NodeEntry>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    }

    @Override public NodeEntry[] getNodeKeys() {
        return listOfKeys.toArray(new NodeEntry[listOfKeys.size()]);
    }

    @Override public int blockSize() {
        return listOfKeys.size();
    }

    @Override public long getNodeNumber() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override public byte[] getColumnMaxValue(int columnOrdinal) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override public byte[] getColumnMinValue(int columnOrdinal) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override public boolean isLeafNode() {
        return false;
    }

    @Override public BTreeNode getNext() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override public void setChildren(BTreeNode[] children) {
        this.children = children;
    }

    @Override public void setNextNode(BTreeNode nextNode) {
        // TODO Auto-generated method stub

    }

    @Override public BTreeNode getChild(int index) {
        return this.children[index];
    }

    @Override
    public DimensionDataChunkHolder[] getDimensionDataChunks(int[] blockIndexes, FileHolder fileReader,
            boolean[] needCompressedData) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DimensionDataChunkHolder getDimensionDataChunk(int blockIndex, FileHolder fileReader,
            boolean needCompressedData) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public MeasureDataChunkHolder getMeasureDataChunk(FileHolder fileReader, int... blockIndexes) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override public void setKey(NodeEntry key) {
        listOfKeys.add(key);

    }
}
