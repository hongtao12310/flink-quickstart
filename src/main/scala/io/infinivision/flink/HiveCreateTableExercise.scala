/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.infinivision.flink

import java.util
import java.util.{HashMap, Map}

import org.apache.flink.api.scala._
import org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, TableEnvironment, TableSchema}
import org.apache.flink.table.catalog.{CatalogTable, CatalogTableImpl, ObjectPath}
import org.apache.flink.table.catalog.hive.HiveCatalog

/**
 * Skeleton for a Flink Batch Job.
 *
 * For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
object HiveCreateTableExercise {

  def main(args: Array[String]): Unit = {

    // create Blink Planner Table Environment
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
    val tEnv = TableEnvironment.create(settings)
    tEnv.getConfig.getConfiguration.setInteger(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM.key, 20)

    //    val tEnv = StreamTableEnvironment.create(env, settings)
    val hiveCatalog = new HiveCatalog("infinivision_hive", "infinivision_cdp",
      "/etc/hadoop/conf", "1.2.1")

    tEnv.registerCatalog("infinivision_hive", hiveCatalog)
    tEnv.useCatalog("infinivision_hive")
    println(s"CataLogs: ${tEnv.listCatalogs()}")
    println(s"Tables: ${tEnv.listTables()}")

    val schema = TableSchema.builder
      .field("cnt", DataTypes.BIGINT())
      .build

    val properties: util.Map[String, String] = new util.HashMap[String, String]
    val path = new ObjectPath("infinivision_cdp", "flink_hive_table_test")
    val table = new CatalogTableImpl(schema, properties, "hive table")
    hiveCatalog.createTable(path, table, true)

    tEnv.sqlUpdate(
      """
        | INSERT INTO flink_hive_table_test
        | SELECT COUNT(*) FROM cdp_campaign_mid_tag_ordered
      """.stripMargin
    )

    tEnv.execute("Flink-1.9 Hive CREATE Table Testing")
  }
}
