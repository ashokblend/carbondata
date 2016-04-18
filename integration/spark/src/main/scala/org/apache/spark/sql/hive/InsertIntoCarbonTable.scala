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
package org.apache.spark.sql.hive

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.carbondata.core.carbon.metadata.CarbonMetadata
import org.carbondata.core.carbon.CarbonDataLoadSchema
import org.carbondata.core.carbon.CarbonDataLoadSchema.Table
import org.carbondata.core.carbon.CarbonDataLoadSchema.Relation
import org.apache.spark.sql.cubemodel.LoadCube
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.EqualTo
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.CarbonDatasourceRelation
import org.carbondata.processing.util.CarbonDataLoadUtil

/**
 * It will analyse InsertIntoTable command and tries to get all tables and relations used in
 * command and fill in CarbonDataLoadSchema.Table and CarbonDataLoadSchema.Relation
 */
private[sql] case class InsertIntoCarbonTable(carbonDatasourceRelation: CarbonDatasourceRelation,
                                              partition: Map[String, Option[String]],
                                              child: LogicalPlan,
                                              overwrite: Boolean, ifNotExists: Boolean,
                                              override val output: Seq[Attribute])
    extends RunnableCommand {
  //fromTable#fromTableKeyColumn:destinationTable#String destionationTableKeyColumn~<begin again>
  var relationBuilder = new StringBuffer
  //alias name to original column name
  var alias2origNameMap: Map[String, String] = Map()
  //partition related detail
  var partitionOption: Map[String, String] = Map()

  val TABLE_SEPARTOR = ":"
  val RELATION_SEPARATOR = "~"
  val VALUE_SEPARATOR = "#"
  val DELIMITER = "delimiter"
  val QUOTECHAR = "quotechar"
  val ESCAPECHAR = "escapechar"
  val MULTILINE = "multiline"
  val FILEHEADER = "fileheader"
  val COMPLEX_DELIMITER_LEVEL1 = "complex_delimiter_level_1"
  val COMPLEX_DELIMITER_LEVEL2 = "complex_delimiter_level_2"

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val tableName = carbonDatasourceRelation.carbonRelation.cubeName;
    val databaseName = carbonDatasourceRelation.carbonRelation.schemaName;

    val carbonTable = CarbonMetadata.getInstance.getCarbonTable(databaseName + "_" + tableName)
    val carbonDataLoadSchema = new CarbonDataLoadSchema(carbonTable)

    //read dimension and fact table from child and fill in schema
    fillTables(carbonDataLoadSchema, child)
    //fill relation between dimension and fact table
    if (relationBuilder.length() >= 0) {
      fillRelations(carbonDataLoadSchema)
    }
    //start data loading
    loadData(databaseName, tableName, carbonDataLoadSchema, sqlContext)
    Seq.empty
  }
  /**
   * start data loading
   */
  def loadData(databaseName: String,
               tableName: String,
               carbonDataLoadSchema: CarbonDataLoadSchema,
               sqlContext: SQLContext) {
    val tableListItr = carbonDataLoadSchema.getTableList.iterator()
    var factTable: CarbonDataLoadSchema.Table = CarbonDataLoadUtil.
                                                getFactTable(carbonDataLoadSchema)
    LoadCube(Some(databaseName), tableName, factTable.getTableSource,
      Seq.empty, partitionOption,
      Option[CarbonDataLoadSchema](carbonDataLoadSchema)).run(sqlContext)

  }
  /**
   * file CarbonDataLoadSchema with all tables used
   */
  def fillTables(carbonDataLoadSchema: CarbonDataLoadSchema, child: LogicalPlan) {
    child match {
      case Project(projectList: Seq[NamedExpression],
        child: LogicalPlan) => fillTables(carbonDataLoadSchema, child)
      case Join(left, right, joinType, condition) =>
        handleJoin(carbonDataLoadSchema, left, right, condition)
      case MetastoreRelation(databaseName: String, tableName: String, alias: Option[String]) =>
        handleMetaStoreRelation(child.asInstanceOf[MetastoreRelation], carbonDataLoadSchema)
      case _ => child.children.foreach { x => fillTables(carbonDataLoadSchema, x) }
    }

  }
  /**
   * fill CarbonDataLoadSchema with all relations used between table
   */
  def fillRelations(carbonDataLoadSchema: CarbonDataLoadSchema) {
    relationBuilder.toString().split(RELATION_SEPARATOR).foreach { x =>
      val rel = x.split(TABLE_SEPARTOR)
      val from = rel(0).split(VALUE_SEPARATOR)
      val dest = rel(1).split(VALUE_SEPARATOR)
      //resolving alias name
      val fromTableName = alias2origNameMap.get(from(0)).getOrElse(from(0))
      val fromColName = from(1)
      val destTableName = alias2origNameMap.get(dest(0)).getOrElse(dest(0))
      val destcolName = dest(1)
      val relation = new Relation(fromTableName, fromColName, destTableName, destcolName)
      addRelationColumnsIntable(fromTableName, fromColName, destTableName,
        destcolName, carbonDataLoadSchema.getTableList)
      carbonDataLoadSchema.addRelation(relation)

    }
  }

  /**
   * This will check if any column used in relation but not added in table coumns
   */
  def addRelationColumnsIntable(fromTableName: String, fromColName: String, destTableName: String,
                                destColName: String, tableList: java.util.List[Table]) {
    //check for fromTable
    addColumnIfNotExist(tableList, fromTableName, fromColName)
    //check for destinationTable
    addColumnIfNotExist(tableList, destTableName, destColName)
  }

  def addColumnIfNotExist(tableList: java.util.List[Table], tableName: String, colName: String) {
    val tableItr = tableList.iterator()
    while (tableItr.hasNext()) {
      val table = tableItr.next()
      if (table.getTableName.equals(tableName)) {
        if (!table.getColumns.contains(colName.toLowerCase())) {
          table.getColumns.add(colName)
        }
      }
    }
  }

  /**
   * This will analyze join query to extract table and relation information
   */
  def handleJoin(carbonDataLoadSchema: CarbonDataLoadSchema, left: LogicalPlan,
                 right: LogicalPlan, condition: Option[Expression]) {
    handleCondition(condition.get)
    right match {
      case MetastoreRelation(databaseName: String, tableName: String, alias: Option[String]) =>
        handleMetaStoreRelation(right.asInstanceOf[MetastoreRelation], carbonDataLoadSchema)
    }
    left match {
      case MetastoreRelation(databaseName: String, tableName: String, alias: Option[String]) =>
        handleMetaStoreRelation(left.asInstanceOf[MetastoreRelation], carbonDataLoadSchema)
      case _ => fillTables(carbonDataLoadSchema, left)
    }
  }
  /**
   * This method actually fills table information
   */
  def handleMetaStoreRelation(metaStoreRelation: MetastoreRelation,
                              carbonDataLoadSchema: CarbonDataLoadSchema) {
    val tableName = metaStoreRelation.tableName
    val tablePath = metaStoreRelation.table.properties.get("path").get
    val alias = metaStoreRelation.alias.getOrElse(tableName)
    val isHeader = metaStoreRelation.table.properties.get("header").getOrElse("true")
    var isFact: Boolean = false
    val columns = new java.util.ArrayList[String]
    metaStoreRelation.attributes.foreach { y => columns.add(y.name) }
    if (alias.equalsIgnoreCase("fact")) {
      isFact = true
      fillPartitionOption(metaStoreRelation, columns)
    }
    alias2origNameMap += (alias -> tableName)
    val table = new Table(tableName, tablePath, columns, isFact)

    carbonDataLoadSchema.addTable(table)
  }
  /**
   * It will fill Partition options
   */
  def fillPartitionOption(metaStoreRelation: MetastoreRelation, columns: java.util.List[String]) {
    val delimiter = metaStoreRelation.table.properties.getOrElse(DELIMITER, ",")
    val quoteChar = metaStoreRelation.table.properties.getOrElse(QUOTECHAR, "\"")
    val escapeChar = metaStoreRelation.table.properties.getOrElse(ESCAPECHAR, "")
    val multiline = metaStoreRelation.table.properties.getOrElse(MULTILINE, "false")
    val complexDelimiterLevel1 = metaStoreRelation.table.properties.
      getOrElse(COMPLEX_DELIMITER_LEVEL1, "\\$")
    val complexDelimiterLevel2 = metaStoreRelation.table.properties.
      getOrElse(COMPLEX_DELIMITER_LEVEL2, "\\:")
    partitionOption += (DELIMITER -> delimiter)
    partitionOption += (QUOTECHAR -> quoteChar)
    val fileHeader: StringBuffer = new StringBuffer()
    for (i <- 0 until columns.size()) {
      fileHeader.append(columns.get(i))
      if (i < columns.size() - 1) {
        fileHeader.append(partitionOption.get(DELIMITER).get)
      }
    }
    partitionOption += (FILEHEADER -> fileHeader.toString())
    partitionOption += (ESCAPECHAR -> escapeChar)
    partitionOption += (MULTILINE -> multiline)
    partitionOption += (COMPLEX_DELIMITER_LEVEL1 -> complexDelimiterLevel1)
    partitionOption += (COMPLEX_DELIMITER_LEVEL2 -> complexDelimiterLevel2)

  }
  /**
   * This method takes care of relation
   */
  def handleCondition(expr: Expression) {
    expr match {
      case EqualTo(left: Expression, right: Expression) =>
        handleEqualToExpression(left.asInstanceOf[AttributeReference],
          right.asInstanceOf[AttributeReference])
      case _ =>
    }
  }
  /**
   * Relation with Equalto conditon
   */
  def handleEqualToExpression(left: AttributeReference, right: AttributeReference) {
    if (relationBuilder.length() > 0) {
      relationBuilder.append(RELATION_SEPARATOR)
    }
    relationBuilder.append(left.qualifiers(0)).append(VALUE_SEPARATOR).append(left.name)
      .append(TABLE_SEPARTOR)
      .append(right.qualifiers(0)).append(VALUE_SEPARATOR).append(right.name)
  }

}
