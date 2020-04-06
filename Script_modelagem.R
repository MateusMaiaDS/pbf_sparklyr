#########################################################################
##### Script da modelagem de classificação com 2 classes pela média #####
#########################################################################

# Carregando pacotes necessarios.
require(randomForest)
require(rpart)
require(tidyverse)
require(readxl)

# Abrindo a base de dados.
base_completa_por_munic <- read_excel("C:/Users/helle/Desktop/TCC_II/Bases de dados/BF/Por municipio/base_completa_por_munic.xlsx")

# Definindo os pontos de corte.
pt_corte_2_2 <- c(min(base_completa_por_munic$MEDIA_TX_BENEF_2019),mean(base_completa_por_munic$MEDIA_TX_BENEF_2019), max(base_completa_por_munic$MEDIA_TX_BENEF_2019))

# Criando a variavel categorizada.
base_completa_por_munic <- base_completa_por_munic %>% 
  mutate(Categoria = cut(MEDIA_TX_BENEF_2019, c(0.0007108697, 0.1006868788, 0.4075853450), labels = c("Baixo", "Alto")))

# Retirando as variaveis UF, CODIGO_MUNIC, NOME_MUNIC, MEDIA_TX_BENEF_2013-2019
dados <- base_completa_por_munic[,-c(2:4,92:98)]

# Corrigindo os tipos das variaveis.
dados$COD_UF <- as.factor(dados$COD_UF)
dados$REGIAO <- as.factor(dados$REGIAO)
dados$TIPOLOG_RUR_URB <- as.factor(dados$TIPOLOG_RUR_URB)

# Verificando a estrutura das variaveis.
str(dados)

# Definindo a semente.
set.seed(20)

# Numero de variaveis.
p <- ncol(dados)-1; p

# Parametros RF.
runs <- 50   #numero de replicacoes
nTREE <- 100 #numero de arvores em cada RF
mTRY <- sqrt(p)  #numero de variaveis selecionadas aleatoriamente em cada divisao. 

# Matriz para guardar as importancias das variaveis. 
IMPS.ACC=matrix(0,nrow=p,ncol=runs)
IMPS.GINI <- IMPS.ACC

# Matriz para guardar as medidas de desempenho.
ACC_geral=matrix(0,ncol=runs) #Acuracia
recall_geral=matrix(0,ncol=runs) #Recall
precisao_geral=matrix(0,ncol=runs) #Precisao
f1_score_geral=matrix(0,ncol=runs) #F1 score
mcc_geral=matrix(0,ncol=runs) #MCC

for (k in 1:runs){
  model.rep <- randomForest(Categoria ~ ., data = dados, 
                            ntree = nTREE, mtry = mTRY,
                            importance = TRUE)
  Imp <- importance(model.rep)  
  IMPS.ACC[,k]=Imp[,3] 
  IMPS.GINI[,k]=Imp[,4] 
  VP <- model.rep$confusion[1,1]
  FP <- model.rep$confusion[1,2]
  FN <- model.rep$confusion[2,1]
  VN <- model.rep$confusion[2,2]
  ACC_geral[,k] <- (VP+VN)/(VP+FP+FN+VN)
  recall_geral[,k] <- VP/(VP+FN)
  precisao_geral[,k] <- VP/(VP+FP)
  f1_score_geral[,k] <- (2*recall_geral[,k]*precisao_geral[,k])/(recall_geral[,k]+precisao_geral[,k])
  mcc_geral[,k] <- ((VP*VN)-(FP*FN))/(sqrt((VP+FP)*(VP+FN)*(VN+FP)*(VN*FN)))
  cat("running",k,"... \n")
}

# Media e variancia das importancias das variaveis.
nomes.var <- names(Imp[,2]) 
Imp.geral = apply(IMPS.ACC,1,mean) 
names(Imp.geral) = nomes.var 
Var.Imp.geral = apply(IMPS.ACC,1,sd) 
names(Var.Imp.geral) = nomes.var #colocando o nome das variaveis

# Guardando os valores.
setwd("C:\\Users\\helle\\Desktop\\TCC_II\\Resultados\\RF")
Imp.geral2 <- as.data.frame(Imp.geral)
write_csv(Imp.geral2, "Imp_geral_2cat_media.csv")
Var.Imp.geral2 <- as.data.frame(Var.Imp.geral)
write_csv2(Var.Imp.geral2, "Var_Imp_geral_2cat_media.csv")
IMPS.ACC2 <- as.data.frame(IMPS.ACC)
write_csv(IMPS.ACC2, "IMPS_ACC_geral_2cat_media.csv")
IMPS.GINI2 <- as.data.frame(IMPS.GINI)
write_csv2(IMPS.GINI2, "IMPS_GINI_geral_2cat_media.csv")
ACC_geral2 <- as.data.frame(ACC_geral)
write_csv2(ACC_geral2, "ACC_geral_2cat_media.csv")
recall_geral2 <- as.data.frame(recall_geral)
write_csv2(recall_geral2, "recall_geral_2cat_media.csv")
precisao_geral2 <- as.data.frame(precisao_geral)
write_csv2(precisao_geral2, "precisao_geral_2cat_media.csv")
f1_score_geral2 <- as.data.frame(f1_score_geral)
write_csv2(f1_score_geral2, "f1_score_geral_2cat_media.csv")
mcc_geral2 <- as.data.frame(mcc_geral)
write_csv2(mcc_geral2, "mcc_geral_2cat_media.csv")

# Grafico de importancia das variaveis.
imp.geral <- sort(Imp.geral, decreasing = T) 
barplot(imp.geral, las = 2, space = 0.7, border = F, col = "black", main = "Media da importancia das variaveis - 2 classes (media)", cex.names = 0.7) 

# Definicao do ponto de corte.
id <- 1:length(Var.Imp.geral)
fit <- rpart(Var.Imp.geral ~ id) 
pred <- predict(fit, data = seq(1,p)) 
points(seq(1,p), pred, type = 'l', lwd = 2)
thr = min(pred)   
abline(h = thr, lwd = 2, lty = 2, col = "red")

# Identificando as variaveis de acordo com o ponto de corte.
lv=Var.Imp.geral>=thr 
corte=min(which(lv == FALSE)) 
corte.imp=min(imp.geral[1:corte]) 

# Importancia das variaveis.
imp.geral <- sort(Imp.geral, decreasing = T)
barplot(imp.geral, las = 2, space = 0.7, border = F, col = "black", main = "Media da importancia das variaveis - 2 classes (media)", cex.names = 0.7)
abline(h=corte.imp,lwd=2,lty=6, col="red")

# variaveis selecionadas.
vars=names(imp.geral[1:corte]) 
n.var=length(vars) 

# Matriz para guardar as medidas de desempenho.
ACC_anin = matrix(0, nrow=n.var, ncol=runs) #Acuracia
OOB = matrix(0, nrow=n.var, ncol=runs) #Out off bagging
recall_anin = matrix(0, nrow=n.var, ncol=runs) #Recall
precisao_anin = matrix(0, nrow=n.var, ncol=runs) #Precisao
f1_score_anin = matrix(0 ,nrow=n.var, ncol=runs) #F1 score
mcc_anin = matrix(0, nrow=n.var, ncol=runs) #MCC

# Modelos aninhados com as variaveis importantes.
for (k in 1:runs) {
  for (i in 1:n.var){
    dados.mod <- dados[c("Categoria",vars[1:i])]
    model2 <- randomForest(Categoria ~ ., 
                           data = dados.mod, 
                           ntree = nTREE, mtry = sqrt(n.var),
                           importance = TRUE)
    VP <- model2$confusion[1,1]
    FP <- model2$confusion[1,2]
    FN <- model2$confusion[2,1]
    VN <- model2$confusion[2,2]
    ACC_anin[i,k] <- (VP+VN)/(VP+FP+FN+VN)
    OOB[i,k] <- 1-ACC_anin[i,k]
    recall_anin[i,k] <- VP/(VP+FN)
    precisao_anin[i,k] <- VP/(VP+FP)
    f1_score_anin[i,k] <- (2*recall_anin[i,k]*precisao_anin[i,k])/(recall_anin[i,k]+precisao_anin[i,k])
    mcc_anin[i,k] <- ((VP*VN)-(FP*FN))/(sqrt((VP+FP)*(VP+FN)*(VN+FP)*(VN*FN)))
  }
  cat("running",k,"... \n")
}

# Guardando os valores.
ACC_anin2 <- as.data.frame(ACC_anin)
write_csv(ACC_anin2, "ACC_anin_2cat_media.csv")
OOB2 <- as.data.frame(OOB)
write_csv2(OOB2, "OOB_2cat_media.csv")
recall_anin2 <- as.data.frame(recall_anin)
write_csv2(recall_anin2, "recall_anin_2cat_media.csv")
precisao_anin2 <- as.data.frame(precisao_anin)
write_csv2(precisao_anin2, "precisao_anin_2cat_media.csv")
f1_score_anin2 <- as.data.frame(f1_score_anin)
write_csv2(f1_score_anin2, "f1_score_anin_2cat_media.csv")
mcc_anin2 <- as.data.frame(mcc_anin)
write_csv2(mcc_anin2, "mcc_anin_2cat_media.csv")

# Encontrando OOB.
OOBs <- apply(OOB,1,mean)
plot(OOBs,type='h', main="OOBs")
points(OOBs,type='p',pch=19)
OOBS.min.position=which(OOBs==min(OOBs))

trh2 <- mean(OOB[OOBS.min.position,]) + sd(OOB[OOBS.min.position,])
abline(h=trh2,lwd=2,lty=6, col="red")

ind.thr=trh2>OOBs
corte.final=min(which(ind.thr== TRUE))

#variaveis selecionadas.
vars[1:corte.final] 

# Salvando a selecao final.
A <- as.data.frame(vars[1:corte.final])
write_csv2(A, "selecao_final_2cat_media.csv")

# Importancia das variaveis com ponto de corte.
imp.geral <- sort(Imp.geral,decreasing = T)
barplot(imp.geral,las=2,space=0.7, border=F, col="black", cex.names = 0.7, main = "Importancia das variaveis - 2 classes (media)")
abline(h=imp.geral[corte.final],lwd=2,lty=6, col="red")

# Variaveis selecionadas.
dados2 <- dados %>%
  select(PPOB, COD_UF, RDPC, PPOBCRI, IDHM_R, T_ANALF11A14, PMPOB, T_DENS, RAZDEP, TRABSC, T_NESTUDA_NTRAB_MMEIO, PMPOBCRI, REGIAO, Categoria)

# Numero de variaveis.
p <- ncol(dados2)-1; p

# Parametros RF.
runs <- 50   
nTREE <- 100 
mTRY <- sqrt(p)  

# Matriz para guardar importancias.
IMPS.ACC_selecionadas=matrix(0,nrow=p,ncol=runs)
IMPS.GINI_selecionadas <- IMPS.ACC_selecionadas

# Matriz para guardar as medidas de desempenho.
ACC_selecionadas = matrix(0,ncol=runs) #Acuracia
recall_selecionadas = matrix(0,ncol=runs) #Recall
precisao_selecionadas = matrix(0,ncol=runs) #Precisao
f1_score_selecionadas = matrix(0,ncol=runs) #F1 score
mcc_selecionadas = matrix(0,ncol=runs) #MCC

for (k in 1:runs){
  model.rep <- randomForest(Categoria ~ ., data = dados2, 
                            ntree = nTREE, mtry = mTRY,
                            importance = TRUE)
  Imp <- importance(model.rep)  
  IMPS.ACC_selecionadas[,k]=Imp[,3] 
  IMPS.GINI_selecionadas[,k]=Imp[,4] 
  VP <- model.rep$confusion[1,1]
  FP <- model.rep$confusion[1,2]
  FN <- model.rep$confusion[2,1]
  VN <- model.rep$confusion[2,2]
  ACC_selecionadas[,k] <- (VP+VN)/(VP+FP+FN+VN)
  recall_selecionadas[,k] <- VP/(VP+FN)
  precisao_selecionadas[,k] <- VP/(VP+FP)
  f1_score_selecionadas[,k] <- (2*recall_selecionadas[,k]*precisao_selecionadas[,k])/(recall_selecionadas[,k]+precisao_selecionadas[,k])
  mcc_selecionadas[,k] <- ((VP*VN)-(FP*FN))/(sqrt((VP+FP)*(VP+FN)*(VN+FP)*(VN*FN)))
  cat("running",k,"... \n")
}
 
# Guardando os valores.
ACC_selecionadas2 <- as.data.frame(ACC_selecionadas)
write_csv2(ACC_selecionadas2, "ACC_selecionadas_2cat_media.csv")
recall_selecionadas2 <- as.data.frame(recall_selecionadas)
write_csv2(recall_selecionadas2, "recall_selecionadas_2cat_media.csv")
precisao_selecionadas2 <- as.data.frame(precisao_selecionadas)
write_csv2(precisao_selecionadas2, "precisao_selecionadas_2cat_media.csv")
f1_score_selecionadas2 <- as.data.frame(f1_score_selecionadas)
write_csv2(f1_score_selecionadas2, "f1_score_selecionadas_2cat_media.csv")
mcc_selecionadas2 <- as.data.frame(mcc_selecionadas)
write_csv2(mcc_selecionadas2, "mcc_selecionadas_2cat_media.csv")
