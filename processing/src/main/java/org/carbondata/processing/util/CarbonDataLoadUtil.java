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

package org.carbondata.processing.util;

import java.util.List;

import org.carbondata.core.carbon.CarbonDataLoadSchema;
import org.carbondata.core.carbon.CarbonDataLoadSchema.Relation;

/**
 * Utility class to extract data from schema 
 *
 */
public class CarbonDataLoadUtil {

	/**
	 * It will lookup in relations for dimension table unique columnKey
	 * 
	 * @param carbonDataLoadSchema
	 * @param fromTable
	 * @return
	 */
	public static String getDimensionTableRelColName(
			CarbonDataLoadSchema carbonDataLoadSchema, String dimensionTable) {
		List<CarbonDataLoadSchema.Relation> relations = carbonDataLoadSchema
				.getRelations();
		String dimTableKey = null;
		for (CarbonDataLoadSchema.Relation relation : relations) {
			if (relation.getFromTable() == dimensionTable) {
				dimTableKey = relation.getFromTableKeyColumn();
				break;
			}
		}
		return dimTableKey;
	}

	/**
	 * It will get dimension table name from given columnName
	 * @param dimensionColName
	 * @param carbonDataLoadSchema
	 * @return
	 */
	public static String extractDimensionTableName(String dimensionColName,
			CarbonDataLoadSchema carbonDataLoadSchema) {
		// TO-DO Get it reviewed
		List<CarbonDataLoadSchema.Table> tableList = carbonDataLoadSchema
				.getTableList();
		for (CarbonDataLoadSchema.Table table : tableList) {
			if (table.isFact()) {
				continue;
			}
			for (String field : table.getColumns()) {
				if (dimensionColName.equals(field)) {
					return table.getTableName();
				}
			}
		}
		return carbonDataLoadSchema.getCarbonTable().getFactTableName();
	}

	/**
	 * It looks for fact table in schema table list
	 * 
	 * @param carbonDataLoadSchema
	 * @return
	 */
	public static CarbonDataLoadSchema.Table getFactTable(
			CarbonDataLoadSchema carbonDataLoadSchema) {
		CarbonDataLoadSchema.Table factTable = null;
		for (CarbonDataLoadSchema.Table table : carbonDataLoadSchema
				.getTableList()) {
			if (table.isFact()) {
				factTable = table;
				break;
			}
		}
		return factTable;
	}

	/**
	 * It will get fact table clumnname from given dimension column name from fact table
	 * @param carbonDataLoadSchema
	 * @param dimName
	 * @return
	 */
	public static String getFactTableRelColName(
			CarbonDataLoadSchema carbonDataLoadSchema, String dimName) {
		List<CarbonDataLoadSchema.Relation> relations = carbonDataLoadSchema
				.getRelations();
		String dimTableDestKey = null;
		for (CarbonDataLoadSchema.Relation relation : relations) {
			if (dimName.equals(relation.getFromTableKeyColumn())) {
				dimTableDestKey = relation.getDestTableKeyColumn();
				break;
			}
		}
		return dimTableDestKey;
	}

	/**
	 * It will get fact table column name from given relations
	 * @param relations
	 * @param tableName
	 * @return
	 */
	public static String getFactTableRelColName(List<Relation> relations,
			String tableName) {
		String dimTableDestKey=null;
		for(Relation relation : relations){
			if(relation.getFromTable().equals(tableName)){
				dimTableDestKey=relation.getDestTableKeyColumn();
			}
		}
		return dimTableDestKey;
	}

	/**
	 * It will get dimension table colum name from given relations
	 * @param relations
	 * @param tableName
	 * @return
	 */
	public static String getDimensionTableRelColName(List<Relation> relations,
			String tableName) {
		String dimSrcDestKey=null;
		for(Relation relation : relations){
			if(relation.getFromTable().equals(tableName)){
				dimSrcDestKey=relation.getFromTableKeyColumn();
			}
		}
		return dimSrcDestKey;
	}

}
