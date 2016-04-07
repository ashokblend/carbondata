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
package org.carbondata.core.carbon.datastore.block.store.measure.compressed.impl;

import java.util.List;

import org.carbondata.core.carbon.datastore.block.store.measure.MeasureDataChunkHolder;
import org.carbondata.core.carbon.datastore.block.store.measure.compressed.AbstractCompressedStore;
import org.carbondata.core.carbon.metadata.leafnode.datachunk.DataChunk;
import org.carbondata.core.carbon.metadata.leafnode.datachunk.PresenceMeta;
import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.datastorage.store.compression.ValueCompressonHolder;
import org.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;

public class CompressedFileStore extends AbstractCompressedStore {

    public CompressedFileStore(List<DataChunk> measureColumnChunk,
            ValueCompressionModel compressionModel, String filePath) {
        super(measureColumnChunk, compressionModel, filePath, false);
    }

    private CarbonReadDataHolder readMeasureDataChunk(FileHolder fileReader, int blockIndex) {
        CarbonReadDataHolder dataHolder = new CarbonReadDataHolder();
        ValueCompressonHolder.UnCompressValue copy = values[blockIndex].getNew();
        copy.setValue(fileReader
                .readByteArray(filePath, measureColumnChunk.get(blockIndex).getDataPageOffset(),
                        measureColumnChunk.get(blockIndex).getDataPageLength()));
        dataHolder = copy.uncompress(compressionModel.getChangedDataType()[blockIndex])
                .getValues(compressionModel.getDecimal()[blockIndex],
                        compressionModel.getMaxValue()[blockIndex]);
        return dataHolder;
    }

    @Override
    public MeasureDataChunkHolder readDataChunks(FileHolder fileReader, int... blockIndexes) {

        CarbonReadDataHolder[] dataHolders = new CarbonReadDataHolder[values.length];
        PresenceMeta[] presenceMeta = new PresenceMeta[values.length];
        for (int i = 0; i < blockIndexes.length; i++) {
            dataHolders[blockIndexes[i]] = readMeasureDataChunk(fileReader, blockIndexes[i]);
            presenceMeta[i] = measureColumnChunk.get(i).getNullValueIndexForColumn();
        }
        MeasureDataChunkHolder measureDataChunkHolder = new MeasureDataChunkHolder();
        measureDataChunkHolder.setMeasureDataChunk(dataHolders);
        return measureDataChunkHolder;
    }
}
