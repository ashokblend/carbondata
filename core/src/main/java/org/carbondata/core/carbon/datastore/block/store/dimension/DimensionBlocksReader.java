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
package org.carbondata.core.carbon.datastore.block.store.dimension;

import org.carbondata.core.datastorage.store.FileHolder;

/**
 * Interface for getting the dimension data chunk from the data file
 */
public interface DimensionBlocksReader {

    /**
     * Below method will be used to get the dimension columns
     * data chunk based on the block index passed.This method will
     * be useful mostly for non filter query when all the dimension column chunk
     * can be read at once.
     *
     * @param blockIndexes       index of the blocks to be read
     * @param fileReader         file reader
     * @param needCOmpressedData
     * @return dimension column chunk
     */
    DimensionDataChunkHolder[] readDataChunks(int[] blockIndexes, FileHolder fileReader,
            boolean[] needCompressedData);

    /**
     * Below method will be used to get the dimension column chunk.
     * This method will be useful basically for filter query, when one by one
     * column chunk will be read and filter will be applied
     *
     * @param blockIndex         index of the block to be read
     * @param fileReader         file reader
     * @param needCompressedData
     * @return dimension column data chunk
     */
    DimensionDataChunkHolder readerDataChunk(int blockIndex, FileHolder fileReader,
            boolean needCompressedData);

}
