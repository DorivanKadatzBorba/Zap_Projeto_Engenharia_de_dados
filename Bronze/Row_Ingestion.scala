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

/* Lendo a base de dados, basta copiar a URL do arquivo no Data Lake
 A leitura será feita em .text para que não seja adotado o schema automaticamente, já que não é o objetivo da camada row não é receber os arquivos já tratados */

val inboundFile = "abfss://zapdatalake@zapdatastorage.dfs.core.windows.net/inbound/source-4-ds-train.json"
val bronzeDF = spark.read.text(inboundFile)

// COMMAND ----------

bronzeDF.printSchema()

// COMMAND ----------

display(bronzeDF)

// COMMAND ----------

// Selecionando a coluna id.

import org.apache.spark.sql.functions.{get_json_object, to_timestamp, col}

bronzeDF.select(get_json_object($"value","$.id") as "id").show()


// COMMAND ----------

// Contando a quatidade de cada id para ver se repetem ou são únicos.

bronzeDF.select(get_json_object($"value","$.id")as "id").groupBy($"id").count().orderBy($"count".desc).show()

// COMMAND ----------

/* Adiciona uma novas coluna para comparar a data de atualização, o objetivo é fazer o update apenas nos id que foram atualizados, para que não seja necessário processar todos os dados toda vez.*/

val bronzeDF2 = bronzeDF.withColumn("id", get_json_object($"value","$.id") as "id") 
                    .withColumn("createdAt", to_timestamp(get_json_object($"value","$.createdAt")as "createdAt"))
                    .withColumn("updatedAt", to_timestamp(get_json_object($"value","$.updatedAt")as "updatedAt"))

display(bronzeDF2)

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE IF NOT EXISTS db_zap_project_bronze;

// COMMAND ----------

/* Sobe uma tabela com os dados da variável "bronzeDF2" caso não exista, se existir faz apenas uma atualização dos dados que faltam na tabela
sem a necessidade de atualizr toda a tabela sempre que tiver uma atualização. Também garante que os dados não dupliquem */

if (!spark.catalog.tableExists("db_zap_project_bronze.tb_zap_imoveis")){
    bronzeDF2.write
             .format("delta")
             .mode("append")
             .option("path", "abfss://zapdatalake@zapdatastorage.dfs.core.windows.net/bronze/tb_zap_imoveis")
             .saveAsTable("db_zap_project_bronze.tb_zap_imoveis")
} else {
  bronzeDF2.createOrReplaceTempView("vw_source")
  spark.sql(s"""
   MERGE INTO db_zap_project_bronze.tb_zap_imoveis AS target
   USING vw_source AS source ON target.id = source.id
   WHEN MATCHED AND source.updatedAt > target.updatedAt THEN UPDATE SET *
   WHEN NOT MATCHED THEN INSERT *
   """)
}

// COMMAND ----------


