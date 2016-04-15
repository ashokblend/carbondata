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
package org.carbondata.core.carbon.datastore.chunk;

/**
 * Interface for dimension column chunk. This is not required because dimension
 * chunk can be encoded with different type of encoder so handing will be
 * different
 *
 */
public interface DimensionColumnDataChunk {

	/** 
	 * Below method will be used to fill the data based on offset and row id
	 * @param data
	 * 			data to filed
	 * @param offset
	 * 			offset from which data need to be filed
	 * @param rowId
	 * 			row id of the chunk  
	 * @return how many bytes was copied 
	 * 
	 */
	int fillChunkData(byte[] data, int offset, int rowId);

	/**
	 * Below method to get  the data based in row id
	 * @param row id
	 * 			row id of the data 
	 * @return chunk
	 */
	byte[] getChunkData(int rowId);

	/**
	 * below method will be used to get the surrogate key based on row id
	 * This will be used for dimension  data aggregation 
	 * This can be used only for fixed length dimension column
	 * chunk as key generator was used to generate the key 
	 * @param rowId
	 * @return surrogate key 
	 */
	int getSurrogate(int columnIndex);

	/**
	 * Below method will be used get the chunk attributes
	 * @return chunk attributes
	 */
	DimensionChunkAttributes getAttributes();
	
	/**
	 * Method to get the complete chunk
	 * @return complete chunk
	 */
	byte[] getChunkData();

}
