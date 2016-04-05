package org.carbondata.core.carbon.datastore.block;

import java.io.Serializable;

/**
 * class will be used to pass the block detail
 * detail will be passed form driver to all the executor
 * to load the b+ tree 
 *
 */
public class CarbonTableBlockInfo implements Serializable{

	/**
	 * serialization id
	 */
	private static final long serialVersionUID = -6502868998599821172L;

	/**
	 * full qualified file path of the block 
	 */
	private String filePath;
	
	/**
	 * block offset in the file 
	 */
	private long blockOffset;
	
	/**
	 * length of the block 
	 */
	private int blockLength;
	
	/**
	 * name of the table to be loaded
	 */
	private String tableName;
	
	/**
	 * flag to keep data chunk metadata like offset of data page
	 * length of data page in file this will be useful when btree is 
	 * loaded from the driver and in that case not need to keep all the information 
	 * in btree as in driver we requires some information like 
	 * start key end key and max,min of the block.  
	 */
	private boolean keepDataChunkDetailInBtree;

	/**
	 * @return the filePath
	 */
	public String getFilePath() {
		return filePath;
	}

	/**
	 * @param filePath the filePath to set
	 */
	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	/**
	 * @return the blockOffset
	 */
	public long getBlockOffset() {
		return blockOffset;
	}

	/**
	 * @param blockOffset the blockOffset to set
	 */
	public void setBlockOffset(long blockOffset) {
		this.blockOffset = blockOffset;
	}

	/**
	 * @return the blockLength
	 */
	public int getBlockLength() {
		return blockLength;
	}

	/**
	 * @param blockLength the blockLength to set
	 */
	public void setBlockLength(int blockLength) {
		this.blockLength = blockLength;
	}

	/**
	 * @return the keepDataChunkDetailInBtree
	 */
	public boolean isKeepDataChunkDetailInBtree() {
		return keepDataChunkDetailInBtree;
	}

	/**
	 * @param keepDataChunkDetailInBtree the keepDataChunkDetailInBtree to set
	 */
	public void setKeepDataChunkDetailInBtree(boolean keepDataChunkDetailInBtree) {
		this.keepDataChunkDetailInBtree = keepDataChunkDetailInBtree;
	}

	/**
	 * @return the tableName
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * @param tableName the tableName to set
	 */
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + blockLength;
		result = prime * result + (int) (blockOffset ^ (blockOffset >>> 32));
		result = prime * result
				+ ((filePath == null) ? 0 : filePath.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof CarbonTableBlockInfo)) {
			return false;
		}
		CarbonTableBlockInfo other = (CarbonTableBlockInfo) obj;
		if (blockLength != other.blockLength) {
			return false;
		}
		if (blockOffset != other.blockOffset) {
			return false;
		}
		if (filePath == null) {
			if (other.filePath != null) {
				return false;
			}
		} else if (!filePath.equals(other.filePath)) {
			return false;
		}
		return true;
	}
}
