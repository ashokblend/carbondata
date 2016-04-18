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

  /**
   * It will analye InsertIntoTable command and tries to get all tables and relations used in command and fill in 
   * CarbonDataLoadSchema.Table and CarbonDataLoadSchema.Relation
   */
  private[sql] case class InsertIntoCarbonTable(table: LogicalPlan,partition: Map[String, Option[String]],child:LogicalPlan,overwrite: Boolean,ifNotExists: Boolean,override val output: Seq[Attribute]) extends RunnableCommand {
    //fromTable#fromTableKeyColumn:destinationTable#String destionationTableKeyColumn~<begin again>
    var relationBuilder=new StringBuffer 
    var alias2origNameMap:Map[String,String]=Map()
    override def run(sqlContext: SQLContext): Seq[Row] = {
       val metaStoreRelation = table.asInstanceOf[MetastoreRelation]
       
       val carbonTable=CarbonMetadata.getInstance.getCarbonTable(metaStoreRelation.databaseName+"_"+metaStoreRelation.tableName)
       val carbonDataLoadSchema=new CarbonDataLoadSchema(carbonTable)
        
       fillTables(carbonDataLoadSchema,child)
       fillRelations(carbonDataLoadSchema)
       LoadCube(Some(metaStoreRelation.databaseName), metaStoreRelation.tableName, null, Seq.empty, Map(),carbonDataLoadSchema).run(sqlContext)
       Seq.empty
     }
     /**
      * file CarbonDataLoadSchema with all tables used
      */
     def fillTables(carbonDataLoadSchema:CarbonDataLoadSchema,child:LogicalPlan){
       child match {
         case Project(projectList: Seq[NamedExpression], child: LogicalPlan) =>fillTables(carbonDataLoadSchema,child)
         case Join(left, right, joinType, condition) =>handleJoin(carbonDataLoadSchema,left,right,condition)
       }
       
     }
     /**
      * fill CarbonDataLoadSchema with all relations used between table
      */
     def fillRelations(carbonDataLoadSchema:CarbonDataLoadSchema){
       if(relationBuilder.length()==0){
         return
       }
       relationBuilder.toString().split("~").foreach { x => 
         val rel=x.split(":")
         val from=rel(0).split("#")
         val dest=rel(1).split("#")
         //resolving alias name
         val fromTableName=alias2origNameMap.get(from(0)).getOrElse(from(0))
         val fromColName=from(1)
         val destTableName=alias2origNameMap.get(dest(0)).getOrElse(dest(0))
         val destcolName=dest(1)
         val relation=new Relation(fromTableName,fromColName,destTableName,destcolName)
         addRelationColumnsIntable(fromTableName,fromColName,destTableName,destcolName,carbonDataLoadSchema.getTableList)
         carbonDataLoadSchema.addRelation(relation)
         
       }
     }
     
     /**
      * This will check if any column used in relation but not added in table coumns
      */
     def addRelationColumnsIntable(fromTableName:String,fromColName:String,destTableName:String,destColName:String,tableList:java.util.List[Table]){
       //check for fromTable
       addColumnIfNotExist(tableList,fromTableName,fromColName)
       //check for destinationTable
       addColumnIfNotExist(tableList,destTableName,destColName)
     }
     
     def addColumnIfNotExist(tableList:java.util.List[Table],tableName:String,colName:String){
        val tableItr=tableList.iterator()
        while(tableItr.hasNext()){
         val table=tableItr.next()
         if(table.getTableName.equals(tableName)){
           if(!table.getColumns.contains(colName.toLowerCase())){
             table.getColumns.add(colName)
           }
         }
       }
     }
     
     /**
      * This will analyze join query to extract table and relation information
      */
     def handleJoin(carbonDataLoadSchema:CarbonDataLoadSchema,left:LogicalPlan,right:LogicalPlan,condition: Option[Expression]){
       handleCondition(condition.get)
       right match {
          case MetastoreRelation(databaseName: String, tableName: String, alias: Option[String])=>
              handleMetaStoreRelation(right.asInstanceOf[MetastoreRelation],carbonDataLoadSchema)
        }
       left match{
          case MetastoreRelation(databaseName: String, tableName: String, alias: Option[String])=>
              handleMetaStoreRelation(left.asInstanceOf[MetastoreRelation],carbonDataLoadSchema)
          case _=> fillTables(carbonDataLoadSchema,left)        
       }
     }
     /**
      * This method actually fills table information
      */
     def handleMetaStoreRelation(metaStoreRelation:MetastoreRelation,carbonDataLoadSchema:CarbonDataLoadSchema){
         val tableName=metaStoreRelation.tableName
         val tablePath=metaStoreRelation.table.properties.get("path").get
         val alias = metaStoreRelation.alias.get
         var isFact:Boolean = false
         if(null!=alias){
           if(alias.equalsIgnoreCase("fact")){
             isFact=true
           }
           alias2origNameMap+=(alias ->tableName)
         }
         val columns=new java.util.ArrayList[String]
         metaStoreRelation.attributes.foreach { y => columns.add(y.name)}
         val table=new Table(tableName,tablePath,columns,isFact)
         
         carbonDataLoadSchema.addTable(table)
     }
     /**
      * This method takes care of relation
      */
     def handleCondition(expr:Expression){
       if(null==expr){
         return
       }
       expr match{
         case EqualTo(left: Expression, right: Expression)=>handleEqualToExpression(left.asInstanceOf[AttributeReference],right.asInstanceOf[AttributeReference])
         case _=>
       }
     }
     /**
      * Relation with Equalto conditon
      */
     def handleEqualToExpression(left: AttributeReference, right: AttributeReference){
        if(relationBuilder.length()>0){
          relationBuilder.append("~")
        }
        relationBuilder.append(left.qualifiers(0)).append("#").append(left.name)
                       .append(":")
                       .append(right.qualifiers(0)).append("#").append(right.name)
     }
    
  }
