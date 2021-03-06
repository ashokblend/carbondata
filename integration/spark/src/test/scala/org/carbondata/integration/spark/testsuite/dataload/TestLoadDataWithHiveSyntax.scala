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

package org.carbondata.integration.spark.testsuite.dataload

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for data loading with hive syntax and old syntax
 *
 */
class TestLoadDataWithHiveSyntax extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("CREATE CUBE carboncube DIMENSIONS (empno Integer, empname String, designation String, doj String, workgroupcategory Integer, workgroupcategoryname String, deptno Integer, deptname String, projectcode Integer, projectjoindate String, projectenddate String) MEASURES (attendance Integer,utilization Integer,salary Integer) OPTIONS (PARTITIONER [PARTITION_COUNT=1])")
    sql("create table hivetable(empno int, empname String, designation string, doj String, workgroupcategory int, workgroupcategoryname String,deptno int, deptname String, projectcode int, projectjoindate String,projectenddate String, attendance String,utilization String,salary String)row format delimited fields terminated by ','")
  }

  test("test data loading and validate query output") {
    //Create test cube and hive table
    sql("CREATE CUBE testcube DIMENSIONS (empno Integer, empname String, designation String, doj String, workgroupcategory Integer, workgroupcategoryname String, deptno Integer, deptname String, projectcode Integer, projectjoindate String, projectenddate String) MEASURES (attendance Integer,utilization Integer,salary Integer) OPTIONS (PARTITIONER [PARTITION_COUNT=1])")
    sql("create table testhivetable(empno int, empname String, designation string, doj String, workgroupcategory int, workgroupcategoryname String,deptno int, deptname String, projectcode int, projectjoindate String,projectenddate String, attendance double,utilization double,salary double)row format delimited fields terminated by ','")
    //load data into test cube and hive table and validate query result
    sql("LOAD DATA local inpath './src/test/resources/data.csv' INTO table testcube")
    sql("LOAD DATA local inpath './src/test/resources/datawithoutheader.csv' overwrite INTO table testhivetable")
    checkAnswer(sql("select * from testcube"), sql("select * from testhivetable"))
    //load data incrementally and validate query result
    sql("LOAD DATA fact from './src/test/resources/data.csv' INTO CUBE testcube PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"')")
    sql("LOAD DATA local inpath './src/test/resources/datawithoutheader.csv' INTO table testhivetable")
    checkAnswer(sql("select * from testcube"), sql("select * from testhivetable"))
    //drop test cube and table
    sql("drop cube testcube")
    sql("drop table testhivetable")
  }
  
  test("test data loading with different case file header and validate query output") {
    //Create test cube and hive table
    sql("CREATE CUBE testcube DIMENSIONS (empno Integer, empname String, designation String, doj String, workgroupcategory Integer, workgroupcategoryname String, deptno Integer, deptname String, projectcode Integer, projectjoindate String, projectenddate String) MEASURES (attendance Integer,utilization Integer,salary Integer) OPTIONS (PARTITIONER [PARTITION_COUNT=1])")
    sql("create table testhivetable(empno int, empname String, designation string, doj String, workgroupcategory int, workgroupcategoryname String,deptno int, deptname String, projectcode int, projectjoindate String,projectenddate String, attendance double,utilization double,salary double)row format delimited fields terminated by ','")
    //load data into test cube and hive table and validate query result
    sql("LOAD DATA local inpath './src/test/resources/datawithoutheader.csv' INTO table testcube options('DELIMITER'=',', 'QUOTECHAR'='\"', 'FILEHEADER'='EMPno,empname,designation,doj,workgroupcategory,workgroupcategoryname,deptno,deptname,projectcode,projectjoindate,projectenddate,attendance,utilization,SALARY')")
    sql("LOAD DATA local inpath './src/test/resources/datawithoutheader.csv' overwrite INTO table testhivetable")
    checkAnswer(sql("select * from testcube"), sql("select * from testhivetable"))
    //drop test cube and table
    sql("drop cube testcube")
    sql("drop table testhivetable")
  }
  
  test("test hive table data loading") {
    sql("LOAD DATA local inpath './src/test/resources/datawithoutheader.csv' overwrite INTO table hivetable")
    sql("LOAD DATA local inpath './src/test/resources/datawithoutheader.csv' INTO table hivetable")
  }

  test("test carbon table data loading using old syntax") {
    sql("LOAD DATA fact from './src/test/resources/data.csv' INTO CUBE carboncube PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"')");
  }
  
  test("test carbon table data loading using new syntax compatible with hive") {
    sql("LOAD DATA local inpath './src/test/resources/data.csv' INTO table carboncube");
    sql("LOAD DATA local inpath './src/test/resources/data.csv' INTO table carboncube options('DELIMITER'=',', 'QUOTECHAR'='\"')");
  }
  
  test("test carbon table data loading using new syntax with overwrite option compatible with hive") {
    try {
      sql("LOAD DATA local inpath './src/test/resources/data.csv' overwrite INTO table carboncube");
    } catch {
      case e : Throwable => e.printStackTrace()
    }
  }
  
  override def afterAll {
    sql("drop cube carboncube")
    sql("drop table hivetable")
  }
}