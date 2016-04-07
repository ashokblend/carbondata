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
package org.carbondata.core.carbon.datastore.block.store.measure.compressed;

import java.util.List;

import org.carbondata.core.carbon.datastore.block.store.measure.MeasureBlocksReader;
import org.carbondata.core.carbon.metadata.leafnode.datachunk.DataChunk;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.datastorage.store.compression.ValueCompressonHolder;
import org.carbondata.core.datastorage.store.compression.ValueCompressonHolder.UnCompressValue;
import org.carbondata.core.datastorage.store.dataholder.CarbonWriteDataHolder;
import org.carbondata.core.util.ValueCompressionUtil;

public abstract class AbstractCompressedStore implements MeasureBlocksReader {

    protected ValueCompressionModel compressionModel;

    protected String filePath;

    protected List<DataChunk> measureColumnChunk;

    protected char[] type;

    protected UnCompressValue[] values;

    protected boolean isInMemoryStore;

    /**
     * @param measureColumnChunk
     * @param compressionModel
     * @param filePath
     */
    public AbstractCompressedStore(List<DataChunk> measureColumnChunk,
            ValueCompressionModel compressionModel, String filePath, boolean isInMemoryStore) {
        this.measureColumnChunk = measureColumnChunk;
        this.compressionModel = compressionModel;
        this.filePath = filePath;
        this.isInMemoryStore = isInMemoryStore;
        if (null != compressionModel) {
            this.type = compressionModel.getType();
            values = new ValueCompressonHolder.UnCompressValue[compressionModel
                    .getUnCompressValues().length];
        }
    }

    @Override public byte[][] getComprssedMeasureBlocks(CarbonWriteDataHolder[] measureDataBlocks) {
        for (int i = 0; i < compressionModel.getUnCompressValues().length; i++) {
            values[i] = compressionModel.getUnCompressValues()[i].getNew();
            if (type[i] != CarbonCommonConstants.BYTE_VALUE_MEASURE
                    && type[i] != CarbonCommonConstants.BIG_DECIMAL_MEASURE) {
                if (type[i] == CarbonCommonConstants.BIG_INT_MEASURE) {
                    values[i].setValue(ValueCompressionUtil
                            .getCompressedValues(compressionModel.getCompType()[i],
                                    measureDataBlocks[i].getWritableLongValues(),
                                    compressionModel.getChangedDataType()[i],
                                    (long) compressionModel.getMaxValue()[i],
                                    compressionModel.getDecimal()[i]));
                } else {
                    values[i].setValue(ValueCompressionUtil
                            .getCompressedValues(compressionModel.getCompType()[i],
                                    measureDataBlocks[i].getWritableDoubleValues(),
                                    compressionModel.getChangedDataType()[i],
                                    (double) compressionModel.getMaxValue()[i],
                                    compressionModel.getDecimal()[i]));
                }
            } else {
                values[i].setValue(measureDataBlocks[i].getWritableByteArrayValues());
            }
            values[i] = values[i].compress();
        }
        byte[][] returnValue = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            returnValue[i] = values[i].getBackArrayData();
        }
        return returnValue;
    }
}
