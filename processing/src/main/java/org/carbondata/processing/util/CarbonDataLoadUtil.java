/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.carbondata.processing.util;

import java.util.List;

import org.carbondata.core.carbon.CarbonDataLoadSchema;
import org.carbondata.core.carbon.CarbonDataLoadSchema.Relation;

/**
 * Utility class to extract data from Carbon table relations
 */
public class CarbonDataLoadUtil {

    /**
     * It will get dimension table name for given column
     * @param dimensionColName
     * @param carbonDataLoadSchema
     * @return table name which has given column name
     */
    public static String extractDimensionTableName(String dimensionColName,
            CarbonDataLoadSchema carbonDataLoadSchema) {
        List<CarbonDataLoadSchema.Table> tableList = carbonDataLoadSchema.getTableList();
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
     * @param carbonDataLoadSchema
     * @return fact table from table list
     */
    public static CarbonDataLoadSchema.Table
            getFactTable(CarbonDataLoadSchema carbonDataLoadSchema) {
        for (CarbonDataLoadSchema.Table table : carbonDataLoadSchema.getTableList()) {
            if (table.isFact()) {
                return table;
            }
        }
        // schema should have fact table
        return null;
    }

    /**
     * It will get fact table column name from given relations
     * @param relations
     * @param tableName
     * @return dimension table column name as referred in fact table
     */
    public static String getFactTableRelColName(List<Relation> relations, String tableName) {
        for (Relation relation : relations) {
            if (relation.getFromTable().equals(tableName)) {
                return relation.getDestTableKeyColumn();
            }
        }
        //given table should be available in Relations
        return null;
    }

    /**
     * It will get dimension table colum name from given relations
     * @param relations
     * @param tableName
     * @return dimension column name as provided in relation
     */
    public static String getDimensionTableRelColName(List<Relation> relations, String tableName) {
        for (Relation relation : relations) {
            if (relation.getFromTable().equals(tableName)) {
                return relation.getFromTableKeyColumn();
            }
        }
        //given tableName should be available in relations
        return null;
    }

}
