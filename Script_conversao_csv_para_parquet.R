###################################################
##### Script de conversão de csv para parquet #####
###################################################

# Carregando os pacotes necessarios. 
library(sparklyr)   
library(dplyr)   
library(tidyverse)   

# Criando uma conexao local com o Spark (sc = spark connection).
sc <- spark_connect(master = "local")

# Verificando se a conexao foi estabelecida.
spark_connection_is_open(sc)

# Identificando o diretorio.
getwd()

# Mudando o diretorio.
setwd("C://Users//helle//Desktop//TCC_II//Bases de dados//Pagamentos mensais do Bolsa Familia")

# Nome do arquivo .csv para conversao.
arquivo = "201906_BolsaFamilia_Pagamentos.csv"

# Abrindo poucas linhas do banco de dados para checar cabecalho etc.
dados_checagem <- read.delim2(arquivo, nrows = 10, sep = ";", dec = ",")
View(dados_checagem)

# Leitura do arquivo csv no Spark.
dados_csv <- spark_read_csv(sc,   #Conexao Spark
                    "BF",   #Nome da nova tabela gerada
                    arquivo,   #Caminho do arquivo csv 
                    delim = ";", dec=".",   #Separador e decimal
                    memory = FALSE)   #Para nao ler na memoria RAM 

# Escrevendo os arquivos .csv em .parquet.
spark_write_parquet(dados_csv,   #DataFrame do Spark
            "BF_pagamentos_06_2019_parquet")   #Nome do arquivo gerado

# Desconectando do Spark.
spark_disconnect(sc)
