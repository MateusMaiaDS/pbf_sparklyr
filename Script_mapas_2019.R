######################################################
##### Script dos mapas de 2019 com 4 e 2 classes #####
######################################################

#####################
##### 4 classes #####
#####################

# Carregando os pacotes necessarios.
require(rgdal)
require(ggplot2)
require(dplyr)
require(sf)
require(tidyverse)
library(abjutils)
library(readxl)
library(brazilmaps)

# Verificando o diretorio.
getwd()

# Mudando o diretorio.
setwd("C://Users//helle//Desktop//TCC_II//Bases de dados")

# Importando o shapefile.
mapa <- st_read("Shapefiles\\municipios_2010.shp", stringsAsFactors = FALSE)

# Padronizando a base mapa.
mapa <- mapa %>% 
  mutate(NOME_MUNIC = abjutils::rm_accent(nome)) %>% #retira os acentos
  mutate(NOME_MUNIC = str_to_upper(nome)) %>%  #coloca em caixa alta
  mutate(UF = uf) %>% 
  mutate(CODIGO_MUNIC = codigo_ibg)

# Carregando a base de dados por municipio.
base_bf_munic_2019 <- read_excel("C://Users//helle//Desktop//TCC_II//Bases de dados//BF//Por municipio/base_bf_munic_2019.xlsx")

# Juntando a base de dados por municipio com a base mapa.
mapa_dados_2019 <- inner_join(mapa, base_bf_munic_2019, by="CODIGO_MUNIC")

# Encontrando os quantis da taxa de utilizacao.
quantis <- quantile(base_bf_munic_2019$MEDIA_TX_BENEF_2019)

# Plotando o grafico
options(scipen = 999) #Para evitar notacao cientifica
mapa_municipio_2019 <- mapa_dados_2019 %>% 
  mutate(Classes = cut(MEDIA_TX_BENEF_2019, c(0.0007108697, 0.0417966157, 0.0825497076, 0.1588324488, 0.4075853450), labels = c("Muito baixo", "Baixo", "Alto", "Muito alto"))) %>% 
  ggplot() +
  geom_sf(aes(fill = Classes), 
          colour = NA, size = 0.1) + #Ajusta o tamanho das linhas
  geom_sf(data = get_brmap("State"),
          fill = "transparent",
          colour = "black", size = 0.9) +
  scale_fill_viridis_d(option = 2, begin = 0.8, end = 0.2) + #Muda a escala de cores
  theme(panel.grid = element_line(colour = "transparent"), #Retira o sistema cartesiano
        panel.background = element_blank(),
        axis.text = element_blank(),
        axis.ticks = element_blank()) +
  ggtitle("2019") +
  theme(plot.title = element_text(hjust=0.5))
  
#####################
##### 2 classes #####
#####################

# Limites para corte.
LI <- min(base_bf_munic_2019$MEDIA_TX_BENEF_2019)
media <- mean(base_bf_munic_2019$MEDIA_TX_BENEF_2019)
LS <- max(base_bf_munic_2019$MEDIA_TX_BENEF_2019)

# Plotando o grafico.
options(scipen = 999)
mapa_municipio_2019 <- mapa_dados_2019 %>% 
  mutate(Classes = cut(MEDIA_TX_BENEF_2019, c(0.0007108697, 0.1006869, 0.4075854), labels = c("Baixo", "Alto"))) %>% 
  ggplot() +
  geom_sf(aes(fill = Classes), 
          colour = NA, size = 0.1) +   #Ajusta o tamanho das linhas
  geom_sf(data = get_brmap("State"),
          fill = "transparent",
          colour = "black", size = 0.9) +
  scale_fill_viridis_d(option = 2, begin = 0.6, end = 0.4) + #Muda escala de cores
  theme(panel.grid = element_line(colour = "transparent"), #Retira sistema cartesiano
        panel.background = element_blank(),
        axis.text = element_blank(),
        axis.ticks = element_blank()) +
  ggtitle("2019") +
  theme(plot.title = element_text(hjust=0.5))
