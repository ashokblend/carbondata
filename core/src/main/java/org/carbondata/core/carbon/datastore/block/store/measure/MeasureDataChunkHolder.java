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

import org.carbondata.core.carbon.metadata.leafnode.datachunk.PresenceMeta;
import org.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;

public class MeasureDataChunkHolder {

    /**
     * measure data chunk
     */
    private CarbonReadDataHolder[] measureDataChunk;

    /**
     * present meta to check for null value
     */
    private PresenceMeta[] presenceMeta;

    /**
     * @return the measureDataChunk
     */
    public CarbonReadDataHolder[] getMeasureDataChunk() {
        return measureDataChunk;
    }

    /**
     * @param measureDataChunk the measureDataChunk to set
     */
    public void setMeasureDataChunk(CarbonReadDataHolder[] measureDataChunk) {
        this.measureDataChunk = measureDataChunk;
    }

    /**
     * @return the presenceMeta
     */
    public PresenceMeta[] getPresenceMeta() {
        return presenceMeta;
    }

    /**
     * @param presenceMeta the presenceMeta to set
     */
    public void setPresenceMeta(PresenceMeta[] presenceMeta) {
        this.presenceMeta = presenceMeta;
    }

}
