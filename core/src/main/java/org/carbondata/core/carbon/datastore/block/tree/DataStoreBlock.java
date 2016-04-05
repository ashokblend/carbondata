package org.carbondata.core.carbon.datastore.block.tree;

import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.datastorage.store.MeasureDataWrapper;
import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreDataHolder;

/**
 * Below class represents the one leaf node data 
 *
 */
public interface DataStoreBlock {

	/**
	 * below method will return the one node key entries
	 * @return node entry array
	 */
	NodeEntry[] getNodeKeys();
	/**
	 * below method will return the next data block from leaf as it is a b+ tree
	 * all the leaf node will be stored in a linked list
	 * 
	 * @return data block
	 */
	DataStoreBlock getNext();

	/**
	 * below method will be used to get the dimension column data blocks from file
	 * based on the index passed
	 * @param blockIndexes
	 *            list of indexes in the file
	 * @param fileHolder
	 *            file reader
	 * @return dimension data block from file
	 */
	ColumnarKeyStoreDataHolder[] getColumnarKeyStore(int[] blockIndexes,
			FileHolder fileHolder);

	/**
	 * below method will be used to get the dimension column block from file
	 *
	 * @param blockIndex
	 *            list of indexes the file
	 * @param fileHolder
	 *            file reader
	 * @return dimension data block from file
	 */
	ColumnarKeyStoreDataHolder getColumnarKeyStore(FileHolder fileHolder,
			int blockIndex);

	/**
	 * Below method will be used to get the measure blocks from file based on
	 * the block indexes passed
	 * 
	 * @param blockIndexes
	 *            list of block indexes
	 * @param fileHolder
	 *            file reader
	 * @return measure blocks
	 */
	MeasureDataWrapper getNodeMsrDataWrapper(int[] blockIndexes,
			FileHolder fileHolder);

	/**
	 * Below method will be used to get the measure block from file based on the
	 * block index
	 * 
	 * @param blockIndex
	 *            block index in file
	 * @param fileHolder
	 *            file reader
	 * @return measure block
	 */
	MeasureDataWrapper getNodeMsrDataWrapper(int blockIndex,
			FileHolder fileHolder);

	/**
	 * Will return the number of keys present in the leaf
	 * 
	 * @return number of keys
	 */
	int numberOfKeys();

	/**
	 * get the node number
	 * 
	 * @return node number
	 */
	long getNodeNumber();

	/**
	 * This will give maximum value of given column
	 *
	 * @return max value of all the columns
	 */
	byte[][] getBlockMaxData();

	/**
	 * It will give minimum value of given column
	 * 
	 * @return
	 */
	byte[][] getBlockMinData();

}
