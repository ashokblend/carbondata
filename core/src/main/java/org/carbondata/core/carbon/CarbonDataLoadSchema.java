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
package org.carbondata.core.carbon;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.carbon.metadata.schema.table.CarbonTable;

/**
 * Wrapper Data Load Schema object which will be used to 
 * support relation while data loading
 *
 */
public class CarbonDataLoadSchema implements Serializable {

  /**
   * default serializer
   */
  private static final long serialVersionUID = 1L;

  /**
   * CarbonTable info
   */
  private CarbonTable carbonTable;

  /**
   * dimension table 
   */
  private List<Table> tableList;
  
  /**
   * relation between tables
   */
  private List<Relation> relations;
  
  /**
   * CarbonDataLoadSchema constructor which takes CarbonTable
   * 
   * @param carbonTable
   */
  public CarbonDataLoadSchema(CarbonTable carbonTable) {
    this.carbonTable = carbonTable;
    this.tableList = new ArrayList<Table>();
    this.relations= new ArrayList<Relation>();
  }

  /**
   * get table relation list
   * @return tableList
   */
  public List<Table> getTableList() {
    return tableList;
  }
  
  /**
   * 
   * @return relations
   */
  public List<Relation> getRelations(){
    return relations;
  }
  
  /**
   * add relation to list
   * @param relation
   */
  public void addRelation(Relation relation){
    this.relations.add(relation);
  }
  
  /**
   * add table to list
   * @param table
   */
  public void addTable(Table table){
    this.tableList.add(table);
  }

  /**
   * get carbontable
   * @return carbonTable
   */
  public CarbonTable getCarbonTable() {
    return carbonTable;
  }

  /**
   * Dimension Relation object which will be filled from 
   * Load DML Command to support normalized cube data load
   *
   */
  public static class Table implements Serializable {
    /**
     * default serializer
     */
    private static final long serialVersionUID = 1L;

    /**
     * dimension tableName
     */
    private String tableName;

    /**
     * tableSource csv path
     */
    private String tableSource;


    /**
     * Columns to selected from dimension table.
     * Hierarchy in-memory table should be prepared 
     * based on selected columns
     */
    private List<String> columns;
    /**
     * fact table identifier
     */
    private boolean isFact;
    /**
     * constructor
     * 
     * @param tableName - dimension table name
     * @param dimensionSource - source file path
     * @param columns - list of columns to be used from this dimension table
     * @param isFact - true if this table is fact table
     */
    public Table(String tableName, String tableSource,
        List<String> columns,boolean isFact) {
      this.tableName = tableName;
      this.tableSource = tableSource;
      this.columns = columns;
      this.isFact = isFact;
    }

    /**
     * @return tableName
     */
    public String getTableName() {
      return tableName;
    }

    /**
     * @return tableSource
     */
    public String getTableSource() {
      return tableSource;
    }

    /**
     * @return columns
     */
    public List<String> getColumns() {
      return columns;
    }
    /**
     * 
     * @return true if it is fact table
     */
    public boolean isFact() {
      return isFact;
    }
  }

  /**
   * Relation class to specify relation between two table
   *
   */
  public static class Relation implements Serializable
  {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * from table name
     */
    private String fromTable;
    /**
     * destination table name
     */
    private String destTable;
    /**
     * from table key column
     */
    private String fromTableKeyColumn;
    /**
     * destination table key column
     */
    private String destTableKeyColumn;
    
    /**
     * 
     * @param fromTable
     * @param fromTableKeyColumn
     * @param destTable
     * @param destTableKeyColumn
     */
    public Relation(String fromTable,String fromTableKeyColumn,String destTable, String destTableKeyColumn){
      this.fromTable=fromTable;
      this.fromTableKeyColumn = fromTableKeyColumn;
      this.destTable=destTable;
      this.destTableKeyColumn = destTableKeyColumn;
    }

    /**
     * 
     * @return fromTable
     */
    public String getFromTable() {
      return fromTable;
    }

    /**
     * 
     * @return destTable
     */
    public String getDestTable() {
      return destTable;
    }

    /**
     * 
     * @return fromTableKeyColumn
     */
    public String getFromTableKeyColumn() {
      return fromTableKeyColumn;
    }
    /**
     * 
     * @return destTableKeyColumn
     */
    public String getDestTableKeyColumn() {
      return destTableKeyColumn;
    }
  }
}
