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
package org.carbondata.core.carbon.datastore.impl.btree;

import java.nio.ByteBuffer;

import org.carbondata.core.carbon.datastore.DataBlock;
import org.carbondata.core.carbon.datastore.DataBlockFinder;
import org.carbondata.core.carbon.datastore.IndexKey;
import org.carbondata.core.util.ByteUtil;

/**
 * Below class will be used to find a block in a btree
 *
 */
public class BTreeBasedBlockFinder implements DataBlockFinder {

	/**
	 * this will holds the information about the size of each value of a column,
	 * this will be used during Comparison of the btree node value and the
	 * search value if value is more than zero then its a fixed length column
	 * else its variable length column. So as data of both type of column store
	 * separately so this value size array will be used for both purpose
	 * comparison and jumping(which type value we need to compare)
	 */
	private int[] eachColumnValueSize;

	/**
	 * this will be used during search for no dictionary column
	 */
	private int numberOfNoDictionaryColumns;

	public BTreeBasedBlockFinder(int[] eachColumnValueSize) {
		this.eachColumnValueSize = eachColumnValueSize;

		for (int i = 0; i < eachColumnValueSize.length; i++) {
			if (eachColumnValueSize[i] == -1) {
				numberOfNoDictionaryColumns++;
			}
		}
	}

	/**
	 * Below method will be used to get the data block based on search key
	 * 
	 * @param dataBlocks
	 *            complete data blocks present
	 * @param serachKey
	 *            key to be search
	 * @param isFirst
	 *            in block we can have duplicate data if data is sorted then for
	 *            scanning we need to scan first instance of the search key till
	 *            last so for this is user is passing is first true it will
	 *            return the first instance if false then it will return the
	 *            last. In case of unsorted data this parameter does not matter
	 *            implementation is will handle that scenario
	 * @return data block
	 */
	@Override
	public DataBlock findDataBlock(DataBlock builder, IndexKey serachKey,
			boolean isFirst) {

		// as its for btree type case it to btree interface
		BTreeNode rootNode = (BTreeNode) builder;
		// if first is true then get the first tentative block with this key
		// othewise get the last tentative block
		if (isFirst) {
			while (!rootNode.isLeafNode()) {
				rootNode = findFirstLeafNode(serachKey, rootNode);
			}
		} else {
			while (!rootNode.isLeafNode()) {
				rootNode = findLastLeafNode(serachKey, rootNode);
			}
		}
		return rootNode;
	}

	/**
	 * Binary search used to get the first tentative block of the btree based on
	 * search key
	 * 
	 * @param key
	 *            search key
	 * @param node
	 *            root node of btree
	 * @return first tentative block
	 */
	private BTreeNode findFirstLeafNode(IndexKey key, BTreeNode node) {
		int childNodeIndex;
		int low = 0;
		int high = node.blockSize() - 1;
		int mid = 0;
		int compareRes = -1;
		IndexKey[] nodeKeys = node.getNodeKeys();
		//
		while (low <= high) {
			mid = (low + high) >>> 1;
			// compare the entries
			compareRes=compareIndexes(key, nodeKeys[mid]);
			if (compareRes < 0) {
				high = mid - 1;
			} else if (compareRes > 0) {
				low = low + 1;
			} else {
				// if key is matched then get the first entry
				int currentPos = mid;
				while (currentPos - 1 >= 0
						&& compareIndexes(key, nodeKeys[mid]) == 0) {
					currentPos--;
				}
				mid = currentPos;
				break;
			}
		}
		// if compare result is less than zero then we
		// and mid is more than 0 then we need to previous block as duplicates
		// record can be present
		if (compareRes < 0) {
			if (mid > 0) {
				mid--;
			}
			childNodeIndex = mid;
		} else {
			childNodeIndex = mid;
		}
		// get the leaf child
		node = node.getChild(childNodeIndex);
		return node;
	}

	/**
	 * Binary search used to get the last tentative block of the btree based on
	 * search key
	 * 
	 * @param key
	 *            search key
	 * @param node
	 *            root node of btree
	 * @return first tentative block
	 */
	private BTreeNode findLastLeafNode(IndexKey key, BTreeNode node) {
		int childNodeIndex;
		int low = 0;
		int high = node.blockSize() - 1;
		int mid = 0;
		int compareRes = -1;
		IndexKey[] nodeKeys = node.getNodeKeys();
		//
		while (low <= high) {
			mid = (low + high) >>> 1;
			// compare the entries
			compareRes=compareIndexes(key, nodeKeys[mid]);
			if (compareRes < 0) {
				high = mid - 1;
			} else if (compareRes > 0) {
				low = low + 1;
			} else {
				int currentPos = mid;
				// if key is matched then get the first entry
				while (currentPos + 1 < node.blockSize()
						&& compareIndexes(key, nodeKeys[mid]) == 0) {
					currentPos++;
				}
				mid = currentPos;
				break;
			}
		}
		// if compare result is less than zero then we
		// and mid is more than 0 then we need to previous block as duplicates
		// record can be present
		if (compareRes < 0) {
			if (mid > 0) {
				mid--;
			}
			childNodeIndex = mid;
		} else {
			childNodeIndex = mid;
		}
		node = node.getChild(childNodeIndex);
		return node;
	}

	/**
	 * Below method is to compare two indexes
	 * 
	 * @param first
	 *            first index
	 * @param second
	 *            second index
	 * @return return the compare result
	 */
	private int compareIndexes(IndexKey first, IndexKey second) {
		int dictionaryKeyOffset = 0;
		int nonDictionaryKeyOffset = 0;
		int compareResult = 0;
		int processedNoDictionaryColumn = numberOfNoDictionaryColumns;
		// no dictionary column are store in below format
		// <index,index,index,data,data,data>
		// for example for values are present then
		// [4,8,10,11,1,1,1,1,2,2,3,4] where first for values
		// represent the index of the key and then actual data
		// to compare the no dictionary we need the length of actual data
		// for that if substract the index of the next column value to the
		// current then
		// we can get the number of key to be compared for that column
		// but now for last key there wont be any next column, so for that we
		// need to handle
		// Separately in that case we can substract with the actual length to
		// get the length of the last key
		for (int i = 0; i < eachColumnValueSize.length; i++) {
			if (eachColumnValueSize[i] == -1) {
				if (processedNoDictionaryColumn > 1) {
					compareResult = ByteUtil.UnsafeComparer.INSTANCE
							.compareTo(
									first.getNoDictionaryKeys(),
									first.getNoDictionaryKeys()[nonDictionaryKeyOffset],
									first.getNoDictionaryKeys()[nonDictionaryKeyOffset + 1]
											- first.getNoDictionaryKeys()[nonDictionaryKeyOffset],
									second.getNoDictionaryKeys(),
									second.getNoDictionaryKeys()[nonDictionaryKeyOffset],
									second.getNoDictionaryKeys()[nonDictionaryKeyOffset + 1]
											- second.getNoDictionaryKeys()[nonDictionaryKeyOffset]);
				} else {
					compareResult = ByteUtil.UnsafeComparer.INSTANCE
							.compareTo(
									first.getNoDictionaryKeys(),
									first.getNoDictionaryKeys()[nonDictionaryKeyOffset],
									first.getNoDictionaryKeys().length
											- first.getNoDictionaryKeys()[nonDictionaryKeyOffset],
									second.getNoDictionaryKeys(),
									second.getNoDictionaryKeys()[nonDictionaryKeyOffset],
									second.getNoDictionaryKeys().length
											- second.getNoDictionaryKeys()[nonDictionaryKeyOffset]);

				}
				--processedNoDictionaryColumn;
				++nonDictionaryKeyOffset;
			} else {
				compareResult = ByteUtil.UnsafeComparer.INSTANCE.compareTo(
						first.getDictionaryKeys(), dictionaryKeyOffset,
						eachColumnValueSize[i], second.getDictionaryKeys(),
						dictionaryKeyOffset, eachColumnValueSize[i]);
			}
			if (compareResult != 0) {
				return compareResult;
			}
		}
		return compareResult;
	}

}
