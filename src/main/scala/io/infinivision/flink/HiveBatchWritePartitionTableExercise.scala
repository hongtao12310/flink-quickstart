package io.infinivision.flink

import java.util

import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, TableEnvironment, TableSchema}
import org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM
import org.apache.flink.table.catalog.config.CatalogConfig
import org.apache.flink.table.catalog.{CatalogTableImpl, ObjectPath}
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil

object HiveBatchWritePartitionTableExercise {
  def main(args: Array[String]): Unit = {

    // create Blink Planner Table Environment
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
    val tEnv = TableEnvironment.create(settings)
    tEnv.getConfig.getConfiguration.setInteger(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM.key, 2)

    //    val tEnv = StreamTableEnvironment.create(env, settings)
    val hiveCatalog = new HiveCatalog("infinivision_hive", "infinivision_cdp",
      "/etc/hadoop/conf", "1.2.1")

    tEnv.registerCatalog("infinivision_hive", hiveCatalog)
    tEnv.useCatalog("infinivision_hive")

    // create output table
    val schema = TableSchema.builder
      .field("campaign_coupon_code", DataTypes.STRING())
      .field("campaign_redeem_time", DataTypes.STRING())
      .field("member_origin_channel", DataTypes.STRING())
      .field("mobile_province", DataTypes.STRING())
      .field("mobile_city", DataTypes.STRING())
      .field("first_buy_province", DataTypes.STRING())
      .field("first_buy_city", DataTypes.STRING())
      .field("first_buy_market", DataTypes.STRING())
      .field("first_buy_store", DataTypes.STRING())
      .field("first_buy_num", DataTypes.STRING())
      .build


    // create partition table
    val properties: util.Map[String, String] = new util.HashMap[String, String]
    properties.put(CatalogConfig.IS_GENERIC, "false")
    val partitionKeys = Array("mobile_city")
    val path = new ObjectPath("infinivision_cdp", "cdp_campaign_mid_tag_ordered_flink")
    val table = new CatalogTableImpl(schema,
      JavaScalaConversionUtil.toJava(partitionKeys),
      properties,
      "hive partition table")
    hiveCatalog.createTable(path, table, true)
    tEnv.sqlUpdate(
      """
        | INSERT INTO cdp_campaign_mid_tag_ordered_flink
        | SELECT * FROM cdp_campaign_mid_tag_ordered
      """.stripMargin)
    tEnv.execute("Flink-1.9 Hive Table ReadWrite Testing")
  }
}
