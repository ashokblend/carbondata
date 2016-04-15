package org.carbondata.core.carbon.datastore.chunk.impl;

import java.util.List;

import org.carbondata.core.carbon.datastore.chunk.DimensionChunkAttributes;
import org.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;

/**
 * This class is holder of the dimension column chunk data 
 * of the variable length key size 
 */
public class VariableLengthDimensionDataChunk implements DimensionColumnDataChunk {

	/**
	 * dimension chunk attributes
	 */
	private DimensionChunkAttributes chunkAttributes;

	/**
	 * data chunk
	 */
	private List<byte[]> dataChunk;

	/**
	 * Constructor for this class
	 * @param dataChunk
	 * 			data chunk 
	 * @param chunkAttributes
	 * 			chunk attributes
	 */
	public VariableLengthDimensionDataChunk(List<byte[]> dataChunk,
			DimensionChunkAttributes chunkAttributes) {
		this.chunkAttributes=chunkAttributes;
		this.dataChunk=dataChunk;
	}

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
	@Override
	public int fillChunkData(byte[] data, int offset,int index) {
		// no required in this case
		return 0;
	}

	/**
	 * Below method to get  the data based in row id
	 * @param row id
	 * 			row id of the data 
	 * @return chunk
	 */
	@Override
	public byte[] getChunkData(int index) {
		if(null!=chunkAttributes.getInvertedIndexes())
		{
			index=chunkAttributes.getInvertedIndexesReverse()[index];
		}
		return dataChunk.get(index);
	}

	/**
	 * below method will be used to get the surrogate key based on row id
	 * This will be used for dimension  data aggregation 
	 * This can be used only for fixed length dimension column
	 * chunk as key generator was used to generate the key 
	 * @param rowId
	 * @return surrogate key 
	 */
	@Override
	public int getSurrogate(int columnIndex) {
		// not required in this case
		return 0;
	}

	/**
	 * Below method will be used get the chunk attributes
	 * @return chunk attributes
	 */
	@Override
	public DimensionChunkAttributes getAttributes() {
		return chunkAttributes;
	}

	/**
	 * Method to get the complete chunk
	 * @return complete chunk
	 */
	@Override
	public byte[] getChunkData() {
		// not required in this case
		return null;
	}

}
