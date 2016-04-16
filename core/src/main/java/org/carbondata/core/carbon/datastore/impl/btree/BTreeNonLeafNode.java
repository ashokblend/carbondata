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
package org.carbondata.core.carbon.datastore.impl.btree;

import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.carbon.datastore.DataBlock;
import org.carbondata.core.carbon.datastore.IndexKey;
import org.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;
import org.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.FileHolder;

/**
 * No leaf node of a b+tree class which will keep the matadata(start key) of the
 * leaf node
 */
public class BTreeNonLeafNode implements BTreeNode {

    /**
     * Child nodes
     */
    private BTreeNode[] children;

    /**
     * list of keys in non leaf
     */
    private List<IndexKey> listOfKeys;

    public BTreeNonLeafNode() {
        // creating a list which will store all the indexes
        listOfKeys = new ArrayList<IndexKey>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    }

    /**
     * below method will return the one node indexes
     *
     * @return getting a complete leaf ]node keys
     */
    @Override public IndexKey[] getNodeKeys() {
        return listOfKeys.toArray(new IndexKey[listOfKeys.size()]);
    }

    /**
     * number of keys in the blocks
     *
     * @return number of keys in a non leaf node
     */
    @Override public int blockSize() {
        return listOfKeys.size();
    }

    /**
     * as it is a non leaf node it will have the reference of all the leaf node
     * under it, setting all the children
     *
     * @param leaf nodes
     */
    @Override public void setChildren(BTreeNode[] children) {
        this.children = children;
    }

    /**
     * setting the next node
     */
    @Override public void setNextNode(BTreeNode nextNode) {
        // no required in case of non leaf node
    }

    /**
     * get the leaf node based on children
     *
     * @return leaf node
     */
    @Override public BTreeNode getChild(int index) {
        return this.children[index];
    }

    /**
     * add a key of a leaf node
     *
     * @param leaf node start keys
     */
    @Override public void setKey(IndexKey key) {
        listOfKeys.add(key);

    }

    /**
     * @return block number
     */
    @Override public long getBlockNumber() {
        // no required in case of btree
        // non leaf node
        return 0;
    }

    /**
     * This method will be used to get the max value of all the columns this can
     * be used in case of filter query
     *
     * @param max value of all the columns
     */
    @Override public byte[][] getColumnsMaxValue() {
        // no required in case of btree
        // non leaf node
        return null;
    }

    /**
     * This method will be used to get the max value of all the columns this can
     * be used in case of filter query
     *
     * @param max value of all the columns
     */
    @Override public byte[][] getColumnsMinValue() {
        // no required in case of btree
        //	non leaf node
        return null;
    }

    /**
     * Below method will be used to get the dimension chunks
     *
     * @param fileReader   file reader to read the chunks from file
     * @param blockIndexes indexes of the blocks need to be read
     * @return dimension data chunks
     */
    @Override public DimensionColumnDataChunk[] getDimensionChunks(FileHolder fileReader,
            int[] blockIndexes) {
        // no required in case of btree
        //	non leaf node
        return null;
    }

    /**
     * Below method will be used to get the dimension chunk
     *
     * @param fileReader file reader to read the chunk from file
     * @param blockIndex block index to be read
     * @return dimension data chunk
     */
    @Override public DimensionColumnDataChunk getDimensionChunk(FileHolder fileReader,
            int blockIndexes) {
        // no required in case of btree
        //	non leaf node
        return null;
    }

    /**
     * Below method will be used to get the measure chunk
     *
     * @param fileReader   file reader to read the chunk from file
     * @param blockIndexes block indexes to be read from file
     * @return measure column data chunk
     */
    @Override public MeasureColumnDataChunk[] getMeasureChunks(FileHolder fileReader,
            int[] blockIndexes) {
        // // no required in case of btree
        //	non leaf node
        return null;
    }

    /**
     * Below method will be used to read the measure chunk
     *
     * @param fileReader file read to read the file chunk
     * @param blockIndex block index to be read from file
     * @return measure data chunk
     */
    @Override public MeasureColumnDataChunk getMeasureChunk(FileHolder fileReader, int blockIndex) {
        // // no required in case of btree
        //	non leaf node
        return null;
    }

    /**
     * @return whether its a leaf node or not
     */
    @Override public boolean isLeafNode() {
        return false;
    }

    /**
     * to get the next block of the node
     */
    @Override public DataBlock getNextBlock() {
        // return next of the block
        // not required in no leaf node
        return null;
    }
}
