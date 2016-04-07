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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.carbondata.core.carbon.metadata.leafnode.datachunk.DataChunk;
import org.carbondata.core.datastorage.store.compression.Compressor;
import org.carbondata.core.datastorage.store.compression.SnappyCompression;
import org.carbondata.core.keygenerator.mdkey.NumberCompressor;

public abstract class AbstractDimensionColumnStore implements DimensionBlocksReader {

    /**
     * compressor will be used to compress the data
     */
    protected static final Compressor<byte[]> COMPRESSOR =
            SnappyCompression.SnappyByteCompression.INSTANCE;

    protected List<DataChunk> dimensionColumnChunk;

    protected short[] eachBlockSize;

    protected String filePath;

    protected NumberCompressor numberComressor;

    protected Map<Integer, Integer> mapOfColumnIndexAndColumnBlockIndex;

    public AbstractDimensionColumnStore(List<DataChunk> dimensionColumnChunk, boolean isInMemory,
            short[] eachBlockSize, String filePath) {
        this.dimensionColumnChunk = dimensionColumnChunk;
        this.eachBlockSize = eachBlockSize;
        this.filePath = filePath;
    }

    protected int[] getColumnIndexForNonFilter(int[] columnIndex) {
        int[] columnIndexTemp = new int[columnIndex.length];

        for (int i = 0; i < columnIndex.length; i++) {
            columnIndexTemp[columnIndex[i]] = i;
        }
        return columnIndexTemp;
    }

    /**
     * The high cardinality dimensions rows will be send in byte array with its data length
     * appended in the ColumnarKeyStoreDataHolder byte array since high
     * cardinality dim data will not be  part of MDKey/Surrogate keys.
     * In this method the byte array will be scanned and the length which is
     * stored in short will be removed.
     */
    protected Map<Integer, byte[]> mapColumnIndexWithKeyColumnarKeyBlockData(
            byte[] columnarKeyBlockData) {
        Map<Integer, byte[]> mapOfColumnarKeyBlockData = new HashMap<Integer, byte[]>(50);
        ByteBuffer directSurrogateKeyStoreDataHolder =
                ByteBuffer.allocate(columnarKeyBlockData.length);
        directSurrogateKeyStoreDataHolder.put(columnarKeyBlockData);
        directSurrogateKeyStoreDataHolder.flip();
        int row = -1;
        while (directSurrogateKeyStoreDataHolder.hasRemaining()) {
            short dataLength = directSurrogateKeyStoreDataHolder.getShort();
            byte[] directSurrKeyData = new byte[dataLength];
            directSurrogateKeyStoreDataHolder.get(directSurrKeyData);
            mapOfColumnarKeyBlockData.put(++row, directSurrKeyData);
        }
        return mapOfColumnarKeyBlockData;
    }
}
