// Databricks notebook source
// Acesso direto ao Data Lake com a chave de acesso.

spark.conf.set(
"fs.azure.account.key.zapdatastorage.dfs.core.windows.net",
"bkAuYMC4ORxx180ohs18unbAU7HC2RowKam+bHYIshhgST8IRiUHaZaZUcaOu8lhrx5+fhNSSlW2+AStWTFHlA=="
)

// COMMAND ----------


// Lista os diretórios do container zapdatalake, teste de conexão.

dbutils.fs.ls("abfss://zapdatalake@zapdatastorage.dfs.core.windows.net/")

// COMMAND ----------

import org.apache.spark.sql.functions._

// Camada Row Config
val bronzeDb= "db_zap_project_bronze"
val bronzeTb = "tb_zap_imoveis"
val bronzeId = s"$bronzeDb.$bronzeTb"

// Camada Silver Config
val silverDb= "db_zap_project_silver"
val silverTb = "tb_zap_imoveis"
val silverId = s"$silverDb.$silverTb"

// leitura da tabela bronze
val bronzeDF = spark.read.table(bronzeId)

// COMMAND ----------

// limpeza dos dados
val preSilverDF = spark.read.json(bronzeDF.select($"value").as [String])

// Extrai os valores dos campos selecionados e exclui os campos desnecessários
// Os campos que eram apenas uma coluna com vários elementos,  se transformam em uma coluna para cada elemento em sua arvore.
val silverDF = preSilverDF.select(expr("*"),$"address.*", $"address.geolocation.location.*", $"pricingInfos.*")
           .drop("address", "images", "pricingInfos", "geolocation")

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS db_zap_project_silver

// COMMAND ----------

/* Sobe uma tabela com os dados da variável "bronzeDF2" caso não exista, se existir faz apenas uma atualização dos dados que faltam na tabela
sem a necessidade de atualizr toda a tabela sempre que tiver uma atualização. Também garante que os dados não dupliquem */

if (!spark.catalog.tableExists(silverId)){
    silverDF.write
             .format("delta")
             .mode("append")
             .option("path", "abfss://zapdatalake@zapdatastorage.dfs.core.windows.net/silver/tb_zap_imoveis_silver")
             .saveAsTable(silverId)
} else {
  silverDF.createOrReplaceTempView("vw_source")
  spark.sql(s"""
   MERGE INTO ${silverId} AS target
   USING vw_source AS source ON target.id = source.id
   WHEN MATCHED AND source.updatedAt > target.updatedAt THEN UPDATE SET *
   WHEN NOT MATCHED THEN INSERT *
   """)
}

// COMMAND ----------

display(spark.sql("select * from " + silverId))

// COMMAND ----------


