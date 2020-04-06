#########################################################
##### Script da análise descritiva de junho de 2019 #####
#########################################################

# Carregando os pacotes necessarios. 
library(sparklyr)
library(dplyr)
library(tidyverse)
library(ggplot2)

# Criando uma conexao local com o Spark (sc = spark connection).
sc <- spark_connect(master = "local")

# Verificando se a conexao foi estabelecida.
spark_connection_is_open(sc)

# Identificando o diretorio
getwd()

# Mudando o diretorio.
setwd("C://Users//helle//Desktop//TCC_II//Bases de dados//Pagamentos mensais do Bolsa Familia")

# Abrindo o arquivo parquet.
BF_jun_2019_parquet <- spark_read_parquet(sc, 
            "BF_jun_2019_p", #Nome da tabela no Spark
            "BF_pagamentos_06_2019_parquet", #Nome do arquivo parquet
            memory = FALSE) #Para nao ler na memoria RAM

# Verificando os nomes das variaveis.
BF_jun_2019_parquet %>% colnames()

# Selecionando as variaveis de interesse.
BF_jun_2019 <- BF_jun_2019_parquet %>% 
  select(UF, CiDIGO_MUNICiPIO_SIAFI, NOME_MUNICiPIO, NIS_FAVORECIDO, VALOR_PARCELA)

# Substituindo virgula por ponto (Spark e nativo do ingles).
BF_jun_2019 <- BF_jun_2019 %>% 
  mutate(VALOR_PARCELA = regexp_replace(VALOR_PARCELA, ",", "."))

# Mudando o tipo de variavel.
BF_jun_2019 <- BF_jun_2019 %>% 
  mutate(VALOR_PARCELA = as.numeric(VALOR_PARCELA))

# Verificando o banco de dados. 
BF_jun_2019 %>% head()

# Verificando a dimensao dos dados.
sdf_dim(BF_jun_2019)    

# Verificando a estrutura dos dados.
glimpse(BF_jun_2019)

# Quantitativo total de dinheiro repassado pelo governo
total_pago_jun_2019 <- BF_jun_2019 %>% 
  select(VALOR_PARCELA) %>% 
  summarize(valor_pago_jun_2019 = sum(VALOR_PARCELA, na.rm = TRUE)) %>% 
  collect()
total_pago_jun_2019 

##################################################
##### MONTANDO A BASE DE DADOS POR MUNICIPIO #####
##################################################

# Carregando base de dados com a populacao de cada municipio, de acordo com Censo 2010.
pop_2010 <- read_excel("C:/Users/helle/Desktop/TCC_II/Bases de dados/Populacao_Brasil_2010.xlsx")

# Padronizando a base de populacao.
pop_2010 <- pop_2010 %>% 
  mutate(NOME_MUNIC = abjutils::rm_accent(NOME_MUNIC)) %>% 
  mutate(NOME_MUNIC = str_to_upper(NOME_MUNIC))  

# Quantidade de beneficiarios e total de recebimento ($) por Municipio.
quant_beneficiarios_munic_jun_2019 <- BF_jun_2019 %>%
  group_by(UF, NOME_MUNICiPIO) %>%
  summarise(total_receb = sum(VALOR_PARCELA, na.rm = TRUE), total_benef=n_distinct(NIS_FAVORECIDO)) %>%  
  collect()

# Transformando em data.frame.
quant_beneficiarios_munic_jun_2019 <- data.frame(quant_beneficiarios_munic_jun_2019)

# Renomeando as colunas.
colnames(quant_beneficiarios_munic_jun_2019) <- c("UF", "NOME_MUNIC", "T_RECEBIDO_JUN_2019", "QUANT_BENEF_JUN_2019")

# Juntando as bases de populacao e quantidade de beneficiarios.
base_pop_benef_jun_2019 <- pop_2010 %>% inner_join(quant_beneficiarios_munic_jun_2019, by=c("UF","NOME_MUNIC"))

# Encontrando a taxa de beneficiarios (taxa de utilizacao)
base_pop_benef_jun_2019 <- base_pop_benef_jun_2019 %>% 
  mutate(TAXA_BENEF_JUN_2019 = QUANT_BENEF_JUN_2019/POP_2010)
base_pop_benef_jun_2019 %>% head()

# Salvando a base de dados.
library(xlsx)
write.xlsx(base_pop_benef_jun_2019, "base_pop_benef_jun_2019.xlsx")

# Sumarizacao da taxa de beneficiarios.
summary(base_pop_benef_jun_2019$TAXA_BENEF_JUN_2019)

################################################################
##### GRAFICO DO TOTAL DE BENEFICIARIOS AO LONGO DOS MESES #####
################################################################

# Plotando o grafico.
ggplot(data = dados_resumo, aes(x = ID, y = TOT_BENEFICIARIOS)) + geom_line() + geom_point() + geom_vline(aes(xintercept = 12.5), colour = "red", linetype = "dashed") + geom_vline(aes(xintercept = 24.5), colour = "red", linetype = "dashed") + geom_vline(aes(xintercept = 36.5), colour = "red", linetype = "dashed") + geom_vline(aes(xintercept = 48.5), colour = "red", linetype = "dashed") + geom_vline(aes(xintercept = 60.5), colour = "red", linetype = "dashed") + geom_vline(aes(xintercept = 72.5), colour = "red", linetype = "dashed") + scale_x_discrete(limits = dados_resumo$ID, labels = dados_resumo$N_MES) + xlab("Meses") + ylab("Quantidade de beneficiarios") + ggtitle("Total de beneficiarios ao longo dos meses.") + theme_classic() + annotate("text", x=6.5, y=14500000, label="2013", color="black", size=7) + annotate("text", x=18.5, y=14500000, label="2014", color="black", size=7) + annotate("text", x=30.5, y=14500000, label="2015", color="black", size=7) + annotate("text", x=42.5, y=14500000, label="2016", color="black", size=7) + annotate("text", x=54.5, y=14500000, label="2017", color="black", size=7) + annotate("text", x=66.5, y=14500000, label="2018", color="black", size=7) + annotate("text", x=76.1, y=14500000, label="2019", color="black", size=7) 

##############################################################
##### GRAFICO DA MEDIA DE RECEBIMENTO AO LONGO DOS MESES #####
##############################################################

# Plotando o grafico.
ggplot(data = dados_resumo, aes(x = ID, y = MEDIA_RECEBIMENTO)) + geom_line() + geom_point() + geom_vline(aes(xintercept = 12.5), colour = "red", linetype = "dashed") + geom_vline(aes(xintercept = 24.5), colour = "red", linetype = "dashed") + geom_vline(aes(xintercept = 36.5), colour = "red", linetype = "dashed") + geom_vline(aes(xintercept = 48.5), colour = "red", linetype = "dashed") + geom_vline(aes(xintercept = 60.5), colour = "red", linetype = "dashed") + geom_vline(aes(xintercept = 72.5), colour = "red", linetype = "dashed") + scale_x_discrete(limits = dados_resumo$ID, labels = dados_resumo$N_MES) + xlab("Meses") + ylab("Media de recebimento") + ggtitle("Media de recebimento ao longo dos meses.") + theme_classic() + annotate("text", x=6.5, y=195, label="2013", color="black", size=7) + annotate("text", x=18.5, y=195, label="2014", color="black", size=7) + annotate("text", x=30.5, y=195, label="2015", color="black", size=7) + annotate("text", x=42.5, y=195, label="2016", color="black", size=7) + annotate("text", x=54.5, y=195, label="2017", color="black", size=7) + annotate("text", x=66.5, y=195, label="2018", color="black", size=7) + annotate("text", x=76.1, y=195, label="2019", color="black", size=7) 

# Desconectando do Spark.
spark_disconnect(sc) 


