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
package org.carbondata.core.carbon.datastore.block.store.measure;

import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.datastorage.store.dataholder.CarbonWriteDataHolder;

/**
 * Interface for getting the measure data chunk from the data file
 */
public interface MeasureBlocksReader {

    /**
     * Below method will be used to get the compressed measure values.
     *
     * @param measureDataBlocks measure data block
     * @return compressed measure blocks
     */
    byte[][] getComprssedMeasureBlocks(CarbonWriteDataHolder[] measureDataBlocks);

    /**
     * Below method will be used to get the measure data chunk based on block index.
     * This method will be useful for non filter query, when all the blocks can be
     * read at one shot.
     *
     * @param blockIndexes index of blocks to be read
     * @param fileReader   file reader
     * @return measure column data chunk
     */
    MeasureDataChunkHolder readDataChunks(FileHolder fileReader, int... blockIndexes);

}
