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
package org.carbondata.core.carbon.datastore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.datastore.block.TableBlockInfos;
import org.carbondata.core.carbon.datastore.block.TableSegment;
import org.carbondata.core.constants.CarbonCommonConstants;

/**
 * Singleton Class to handle loading, unloading,clearing,storing of the table
 * blocks
 */
public class CarbonTableSegmentStore {

	/**
	 * singleton instance
	 */
	private static final CarbonTableSegmentStore CARBONTABLESEGMENTSTORE = new CarbonTableSegmentStore();

	/**
	 * map to hold the table and its list of blocks
	 */
	private Map<AbsoluteTableIdentifier, Map<Integer, TableSegment>> tableSegmentMap;

	/**
	 * table and its lock object to this will be useful in case of concurrent
	 * query scenario when more than one query comes for same table and in that
	 * case it will ensure that only one query will able to load the blocks
	 */
	private Map<AbsoluteTableIdentifier, Object> tableLockMap;

	public CarbonTableSegmentStore() {
		tableSegmentMap = new ConcurrentHashMap<AbsoluteTableIdentifier, Map<Integer, TableSegment>>(
				CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
		tableLockMap = new ConcurrentHashMap<AbsoluteTableIdentifier, Object>(
				CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
	}

	/**
	 * Return the instance of this class
	 * 
	 * @return singleton instance
	 */
	public static CarbonTableSegmentStore getInstance() {
		return CARBONTABLESEGMENTSTORE;
	}

	/**
	 * below method will be used to load the segment  which are not loaded and to
	 * get the loaded blocks if all the blocks which are passed is loaded then
	 * it will not load , else it will load.
	 * 
	 * @param segmentToTableBlocksInfos
	 *            segment to table block info
	 * @param absoluteTableIdentifier
	 *            absolute Table Identifier to identify the table
	 */
	public List<TableSegment> loadAndGetBlocks(
			Map<Integer, List<TableBlockInfos>> segmentToTableBlocksInfos,
			AbsoluteTableIdentifier absoluteTableIdentifier) {
		List<TableSegment> loadedBlocksList = new ArrayList<TableSegment>(
				CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
		// add the instance to lock map if it is not present
		tableLockMap.putIfAbsent(absoluteTableIdentifier, new Object());
		Iterator<Entry<Integer, List<TableBlockInfos>>> iteratorOverSegmentBlocksInfos = segmentToTableBlocksInfos
				.entrySet().iterator();
		// get the instance of lock object
		Object lockObject = tableLockMap.get(absoluteTableIdentifier);

		// get the instance
		// Acquire the lock to ensure only one query is loading the table blocks
		// if same block is assigned to both the queries
		synchronized (lockObject) {
			// get table segment map 
			Map<Integer, TableSegment> tableSegmentMapTemp = tableSegmentMap
					.get(absoluteTableIdentifier);
			// if for current table segment map is not present then add new mapping for the
			//segment 
			if (null == tableSegmentMapTemp) {
				tableSegmentMapTemp = new HashMap<Integer, TableSegment>();
				tableSegmentMap.put(absoluteTableIdentifier,
						tableSegmentMapTemp);
			}
			while (iteratorOverSegmentBlocksInfos.hasNext()) {
				Entry<Integer, List<TableBlockInfos>> next = iteratorOverSegmentBlocksInfos.next();
				// get table segment for the segment id 
				TableSegment tableSegment = tableSegmentMapTemp.get(next
						.getKey());
				// if table segment is present then add to the loaded segment list
				// if not loaded the segment and add to table segment map 
				if (null != tableSegment) {
					loadedBlocksList.add(tableSegment);
				} else {
					tableSegment = new TableSegment();
					tableSegment.loadCarbonTableBlock(next.getValue());
					tableSegmentMapTemp.put(next.getKey(), tableSegment);
				}
			}
		}
		return loadedBlocksList;
	}
	
	/**
	 * remove all the details of a table
	 * this will be used in case of drop segment 
	 * @param absoluteTableIdentifier
	 * 			absolute table identifier to find the table
	 */
	public void clear(AbsoluteTableIdentifier absoluteTableIdentifier) {
		// removing all the details of table
		tableLockMap.remove(absoluteTableIdentifier);
		tableSegmentMap.remove(absoluteTableIdentifier);
	}
	
	/**
	 * This will be used to remove a particular blocks
	 * useful in case of deletion of some of the blocks
	 * in case of retention or may be some other scenario
	 * @param removeSegmentIds
	 * 			segment to be removed
	 * @param absoluteTableIdentifier
	 * 			absolute table identifier
	 * 
	 */
	public void removeTableBlocks(List<Integer> removeSegmentIds,AbsoluteTableIdentifier absoluteTableIdentifier) {
		// get the lock object if lock object is not present then it is not
		// loaded at all
		// we can return from here
		Object lockObject = tableLockMap.get(absoluteTableIdentifier);
		if (null == lockObject) {
			return;
		}
		// Acquire the lock and remove only those instance which was loaded
		synchronized (lockObject) {
			Map<Integer, TableSegment> map = tableSegmentMap.get(absoluteTableIdentifier);
			// if there is no loaded segment then return
			if (null == map) {
				return;
			}
			for (Integer removeSegmentId : removeSegmentIds) {
				map.remove(removeSegmentId);
			}
		}
	}

}
