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
package org.carbondata.core.carbon.datastore.block.store.dimension.compressed;

import java.util.List;

import org.carbondata.core.carbon.datastore.block.store.dimension.AbstractDimensionColumnStore;
import org.carbondata.core.carbon.datastore.block.store.dimension.DimensionDataChunkHolder;
import org.carbondata.core.carbon.datastore.block.store.dimension.DimensionDataHolderMetadata;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.carbon.metadata.leafnode.datachunk.DataChunk;
import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.datastorage.store.columnar.UnBlockIndexer;
import org.carbondata.core.util.CarbonUtil;

/**
 * Class will be used to read the blocks from file based on the block index
 */
public class CompressedDimensionColumnFileStore extends AbstractDimensionColumnStore {

    public CompressedDimensionColumnFileStore(List<DataChunk> dimensionColumnChunk,
            short[] eachBlockSize, String filePath) {
        super(dimensionColumnChunk, false, eachBlockSize, filePath);
    }

    @Override public DimensionDataChunkHolder[] readDataChunks(int[] blockIndexes, FileHolder fileReader,
            boolean[] needCompressedData) {

        DimensionDataChunkHolder[] dataHolderArray = new DimensionDataChunkHolder[blockIndexes.length];
        for (int i = 0; i < dataHolderArray.length; i++) {
            dataHolderArray[i] =
                    readerDataChunk(blockIndexes[i], fileReader, needCompressedData[i]);
        }
        return dataHolderArray;
    }

    @Override public DimensionDataChunkHolder readerDataChunk(int blockIndex, FileHolder fileReader,
            boolean needCompressedData) {

        byte[] dataPage = null;
        int[] rowIdPage = null;
        int[] actualPositionInRow = null;
        int[] rlePage = null;
        DimensionDataHolderMetadata dataHolderMetadata = null;
        DimensionDataChunkHolder dimensionDataHolder = null;
        boolean isUnCompressed = true;
        dataPage = COMPRESSOR.unCompress(fileReader
                .readByteArray(filePath, dimensionColumnChunk.get(blockIndex).getDataPageOffset(),
                        dimensionColumnChunk.get(blockIndex).getDataPageLength()));
        if (dimensionColumnChunk.get(blockIndex).getEncodingList()
                .contains(Encoding.INVERTED_INDEX)) {
            rowIdPage = CarbonUtil.getUnCompressColumnIndex(
                    dimensionColumnChunk.get(blockIndex).getRowIdPageLength(), fileReader
                            .readByteArray(filePath,
                                    dimensionColumnChunk.get(blockIndex).getRowIdPageOffset(),
                                    dimensionColumnChunk.get(blockIndex).getRowIdPageLength()),
                    numberComressor);
            actualPositionInRow = getColumnIndexForNonFilter(rowIdPage);
        }
        if (dimensionColumnChunk.get(blockIndex).getEncodingList().contains(Encoding.RLE)) {
            rlePage = numberComressor.unCompress(fileReader.readByteArray(filePath,
                    dimensionColumnChunk.get(blockIndex).getRlePageOffset(),
                    dimensionColumnChunk.get(blockIndex).getRlePageLength()));
            if (!needCompressedData) {
                dataPage =
                        UnBlockIndexer.uncompressData(dataPage, rlePage, eachBlockSize[blockIndex]);
                rlePage = null;
            } else {
                isUnCompressed = false;
            }
        }

        dataHolderMetadata = new DimensionDataHolderMetadata();
        // Since its an high cardinality dimension, For filter queries.
        if (!dimensionColumnChunk.get(blockIndex).getEncodingList().contains(Encoding.DICTIONARY)) {
            dataHolderMetadata.setEachRowSize(eachBlockSize[blockIndex]);
            dataHolderMetadata.setInvertedIndexes(rowIdPage);
            dataHolderMetadata.setInvertedIndexesReverse(actualPositionInRow);
            dataHolderMetadata.setRleIndexes(rlePage);
            dataHolderMetadata.setUnCompressed(true);
            dimensionDataHolder = new DimensionDataChunkHolder();
            dimensionDataHolder.setDataHolderMetadata(dataHolderMetadata);
            dimensionDataHolder.setMapOfColumnarKeyBlockData(
                    mapColumnIndexWithKeyColumnarKeyBlockData(dataPage));
            return dimensionDataHolder;
        }
        dataHolderMetadata = new DimensionDataHolderMetadata();
        dataHolderMetadata.setEachRowSize(eachBlockSize[blockIndex]);
        dataHolderMetadata.setInvertedIndexes(rowIdPage);
        dataHolderMetadata.setInvertedIndexesReverse(actualPositionInRow);
        dataHolderMetadata.setRleIndexes(rlePage);
        dataHolderMetadata.setUnCompressed(isUnCompressed);
        dimensionDataHolder = new DimensionDataChunkHolder();
        dimensionDataHolder.setDataHolderMetadata(dataHolderMetadata);
        dimensionDataHolder.setKeyblockData(dataPage);
        return dimensionDataHolder;
    }
}
