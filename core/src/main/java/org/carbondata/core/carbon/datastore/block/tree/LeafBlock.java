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

import org.carbondata.core.carbon.datastore.block.store.dimension.DimensionDataChunkHolder;
import org.carbondata.core.carbon.datastore.block.store.measure.MeasureDataChunkHolder;
import org.carbondata.core.datastorage.store.FileHolder;

/**
 * Below class represents the one leaf node data
 */
public interface LeafBlock {

    /**
     * below method will return the next data block from leaf as it is a b+ tree
     * all the leaf node will be stored in a linked list
     *
     * @return data block
     */
    LeafBlock getNext();

    /**
     * below method will be used to get the dimension column data blocks from file
     * based on the index passed
     *
     * @param blockIndexes       list of indexes in the file
     * @param fileHolder         file reader
     * @param needCompressedData
     * @return dimension data block from file
     */
    DimensionDataChunkHolder[] getDimensionDataChunks(int[] blockIndexes, FileHolder fileReader,
            boolean[] needCompressedData);

    /**
     * below method will be used to get the dimension column block from file
     *
     * @param blockIndex         list of indexes the file
     * @param fileHolder         file reader
     * @param needCompressedData
     * @return dimension data block from file
     */
    DimensionDataChunkHolder getDimensionDataChunk(int blockIndex, FileHolder fileReader,
            boolean needCompressedData);

    /**
     * Below method will be used to get the measure blocks from file based on
     * the block indexes passed
     *
     * @param blockIndexes list of block indexes
     * @param fileHolder   file reader
     * @return measure blocks
     */
    MeasureDataChunkHolder getMeasureDataChunk(FileHolder fileReader, int... blockIndexes);

    /**
     * Will return the number of keys present in the leaf
     *
     * @return number of keys
     */
    int blockSize();

    /**
     * get the node number
     *
     * @return node number
     */
    long getNodeNumber();

    /**
     * This will give maximum value of given column
     *
     * @return max value of all the columns
     */
    byte[] getColumnMaxValue(int columnOrdinal);

    /**
     * It will give minimum value of given column
     *
     * @return
     */
    byte[] getColumnMinValue(int columnOrdinal);

}
