library(sf)
library(RPostgres)
library(tidyverse)


#0. Conectandome a la bbdd y schema: -------------------

conn<-fun_connect()

dbSendQuery(conn, "set search_path to censos;")


#1.Querys

#Censo 1992:
#7.11	VIVEHAB Código de Comuna o País Residencia Habitual
#7.12	VIVIA87	Código Comuna o País Residencia 1987
#7.30	LUGNAC	Lugar o Comuna de Nacimiento
#.32	LUGVIV	Vive Habitualmente en esta Comuna	 
#7.33	LUGRES87	Comuna o Lugar Residencia en 1987
#Algarrobo: 5602
#Cartagena: 5603
#El Quisco:5604
#El Tabo:5605
#San Antonio: 5601
#Santo Domingo:5

#Censo 2002:
#7.12	NACIMIEN	Código de comuna o país de residencia	P22B	 	INTEGER	0	99999
#7.13	LLEGADA	Año de llegada al país de su madre	P22C	 	INTEGER	9999	9999
#7.14	LUGVIV	Lugar de residencia habitual	P23A	 	INTEGER	1	9
#7.15	VIVEHAB	Código de comuna o país de residencia habitual	P23B	 	INTEGER	0	99999
#7.16	LUGRES97	Lugar de residencia en Abril 1997	P24A	 	INTEGER	1	9
#Value	Description
#1	En esta comuna
#2	En otra comuna
#3	En otro país
#9	Ignorado
#10	MISSING
#0	NOTAPPLICABLE

#7.17	VIVIA97	Código de comuna o país de residencia en Abril 1997	P24B	 	INTEGER	0	99999
#SEXO	Sexo	P18
#EDAD	Edad	P19
#comuna
#Algarrobo: 5602
#Cartagena: 5603
#El Quisco:5604
#El Tabo:5605
#San Antonio: 5601
#Santo Domingo:5606

#Censo 2012:
#P22A: Comuna o país en 2007
#1 Aún no nacía 2 En esta comuna 3 En otra comuna 4 En otro país 9 Ignorado
#comuna
#P19 Sexo
#P20C EDAD

#P22B Texto comuna o país P22C Código comuna o país 0 No Aplica (P22A=1)
#1101-16255 Código de comuna o país 99999 Ignorado 

#P23A Comuna o país madre 
#1 En esta comuna 2 En otra comuna 3 En otro pais 9 Ignorado 

#P23B Texto comuna o país madre 

#P23C Código comuna o país madre 
#1101-16255 Código de comuna o país madre 99999 Ignorado

#Algarrobo: 5602
#Cartagena: 5603
#El Quisco:5604
#El Tabo:5605
#San Antonio: 5601
#Santo Domingo:5606

#Censo 2017:

#COMUNA	División Comunal
#IDCOMUNA	Código Comuna
#NCOMUNA	Comunas
#P08	Sexo
#P09	Edad
#P10COMUNA	Comuna de Residencia Habitual	 	 	INTEGER	997	16305
#P10PAIS	Pais de Residencia Habitual	 	 	INTEGER	0	997
#P10PAIS_GRUPO	Pais de Residencia Habitual(grupo)	 	 	INTEGER	0	997
#P11	Comuna de Residencia Anterior	 	 	INTEGER	0	9
#P11COMUNA	Comuna de Residencia Anterior	



# Creando sub tablas ------------------------------------------------------

#1992
colnames(tbl(conn, "censo_1992"))
tbl(conn, "censo_1992") %>% select(.,"comuna")

dbSendQuery(conn, " CREATE TABLE comunas_censo1992 AS
SELECT *
FROM censo_1992
WHERE (comuna='05601' OR comuna='05602' OR comuna='05603' OR comuna='05604' OR comuna='05604' OR comuna='05605' OR comuna='05606')AND  edad>=60   ;")

#https://stackoverflow.com/questions/37351315/count-number-of-rows-when-using-dplyr-to-access-sql-table-query
tbl(conn, "comunas_censo1992") %>% summarize(n())



#2002
colnames(tbl(conn, "censo_2002"))
dbSendQuery(conn, " CREATE TABLE comunas_censo2002 AS
SELECT *
FROM censo_2002
WHERE (comuna='05601' OR comuna='05602' OR comuna='05603' OR comuna='05604' OR comuna='05604' OR comuna='05605' OR comuna='05606')AND  p19c>=60   ;")
tbl(conn, "comunas_censo2002") %>% summarize(n())

#2012
colnames(tbl(conn, "censo_2012"))
tbl(conn, "censo_2012")

dbSendQuery(conn, " CREATE TABLE comunas_censo2012 AS
SELECT *
FROM censo_2012
WHERE (comuna='05601' OR comuna='05602' OR comuna='05603' OR comuna='05604' OR comuna='05604' OR comuna='05605' OR comuna='05606')AND  p20c>=60   ;")

tbl(conn, "comunas_censo2012") %>% summarize(n())

#2017
colnames(tbl(conn, "censo_2017"))

dbSendQuery(conn, " CREATE TABLE comunas_censo2017 AS
SELECT *
            FROM censo_2017
            WHERE (comuna='05601' OR comuna='05602' OR comuna='05603' OR comuna='05604' OR comuna='05604' OR comuna='05605' OR comuna='05606')AND  p09>=60   ;")

tbl(conn, "comunas_censo2017") %>% summarize(n())



# Cantidad total de personas  ---------------------------------------------
colnames(tbl(conn, "comunas_censo1992"))
rbind(tbl(conn, "comunas_censo1992") %>% summarize(n()) %>% collect(),
tbl(conn, "comunas_censo2002") %>% summarize(n()) %>% collect(),
tbl(conn, "comunas_censo2012") %>% summarize(n()) %>% collect(),
tbl(conn, "comunas_censo2017") %>% summarize(n()) %>% collect())

# Censo 1992 vive en otra comuna --------------------------------------------------------

test<-dbSendQuery(conn, "SELECT comuna, comuna_1987_origen3, COUNT(*)
                  FROM comunas_censo1992
                  GROUP BY comuna,comuna_1987_origen3;") 

test<-dbFetch(test)

test$comuna_1987_origen3<-paste0("0",test$comuna_1987_origen3)

test<-split.data.frame(test,test$comuna)

# Cuantas personas fuera de esta comuna hace 5 años --------
fuera1992<-lapply(test, function(x){sum(x$count[x$comuna_1987_origen3!=unique(x$comuna)])/sum(x$count)*100}) %>% bind_rows(.) %>% pivot_longer(colnames(.))

# Cuantas personas vienen de cada comuna -------
comuna1992<-lapply(test, function(x){x[x$comuna_1987_origen3!=unique(x$comuna),] %>% group_by(comuna_1987_origen3) %>% summarise(n=sum(count))})%>% bind_rows(.,.id="Comuna") 

# Censo 2002: conteos 24a por comuna ------------------------------------------------------

library(dbplyr)

test<-dbSendQuery(conn, "SELECT comuna,p24a,p24b, COUNT(*)
            FROM comunas_censo2002
            GROUP BY comuna,p24a,p24b;") 

test<-dbFetch(test) 

#23A LUGAR DE RESIDENCIA HABITUAL
#23B CODIGO DEL PAIS O COMUNA DE RESIDENCIA HABITUAL
#24A LUGAR DE RESIDENCIA EN EL 97
#24B CODIGO DEL PAIS O COMUNA DE RESIDENCIA EN EL 97

test<-split.data.frame(test,test$comuna)

# Cuantas personas fuera de esta comuna hace 5 años --------
fuera2002<-lapply(test, function(x){sum(x$count[x$p24a==2])/sum(x$count)*100}) %>% bind_rows(.) %>% pivot_longer(colnames(.))

# Cuantas personas vienen de cada comuna -------
comuna2002<-lapply(test,function(x){x[x$p24a!=1,] %>% group_by( p24b) %>% summarise(n=sum(count))}) %>% bind_rows(.,.id="Comuna")

#  Censo 2012: conteos por comuna -----------------------------------------

test<-dbSendQuery(conn, "SELECT comuna,p22a,p22b, COUNT(*)
            FROM comunas_censo2012
            GROUP BY comuna,p22a,p22b;") 

test<-dbFetch(test) 

test<-split.data.frame(test,test$comuna)

lapply(test, function(x){sum(x$count[x$p22a==3])/sum(x$count)*100})%>% bind_rows(.) %>% pivot_longer(colnames(.))
lapply(test,function(x){x[x$p22a!=1,] %>% group_by( p22b) %>% summarise(n=sum(count))}) %>% bind_rows(.,.id="Comuna")

# Censo 2017: conteos p11 por comuna  ---------------------------------------------------------------
test<-dbSendQuery(conn, "SELECT comuna,p11,p11comuna, COUNT(*)
            FROM comunas_censo2017
                  GROUP BY comuna,p11,p11comuna;") 

test<-dbFetch(test) 

test<-split.data.frame(test,test$comuna)

# Cuantas personas fuera de esta comuna hace 5 años --------
fuera2017<-lapply(test, function(x){sum(x$count[x$p11!=2])/sum(x$count)*100})%>% bind_rows(.) %>% pivot_longer(colnames(.))

# Cuantas personas vienen de cada comuna -------
comuna2017<-lapply(test,function(x){x[x$p11!=2,] %>% group_by(p11comuna) %>% summarise(n=sum(count))}) %>% bind_rows(.,.id="Comuna")


# Uniendo diccionarios a tablas  ------------------------------------------

#Cargando diccionarios

codecom1992<-readxl::read_xlsx("C:/CEDEUS/2021/abril1_ciudadesCosteras/input/codecomunas.xlsx",col_types = c("text","text"))# 1992

codecom2002<-readxl::read_xlsx("C:/CEDEUS/2021/abril1_ciudadesCosteras/input/codecomunas.xlsx", sheet=2,col_types = c("numeric","text"))#2002

codecom2017<-readxl::read_xlsx("C:/CEDEUS/2021/abril1_ciudadesCosteras/input/codecomunas.xlsx", sheet=3,col_types = c("numeric","text"))#2017

comuna1992$comuna_1987_origen3<-substr(comuna1992$comuna_1987_origen3,2,nchar(comuna1992$comuna_1987_origen3))

comuna1992<-left_join(comuna1992,codecom1992, by=c("comuna_1987_origen3"="Value")) %>% .[!is.na(.$Description),] #%>% split.data.frame(.,.$Comuna)
colnames(comuna1992)<-c("Comuna_actual", "Comuna_5años_code","Cantidad","Comuna_5años")

comuna2002<-left_join(comuna2002,codecom2002, by=c("p24b"="Value")) %>% .[!is.na(.$Description),]#%>% split.data.frame(.,.$Comuna)
colnames(comuna2002)<-c("Comuna_actual", "Comuna_5años_code","Cantidad","Comuna_5años")

comuna2017<-left_join(comuna2017,codecom2017, by=c("p11comuna"="Value"))%>% .[!is.na(.$Description),]#%>% split.data.frame(.,.$Comuna)
colnames(comuna2017)<-c("Comuna_actual", "Comuna_5años_code","Cantidad","Comuna_5años")

test$Description<-as.factor(test$Description)
test$Description<-with(test, factor(Description, levels = rev(levels(Description))))

library(plotly)

p<-ggplot(test, aes(y=Comuna_actual,x=Comuna_5años, fill=as.integer(Cantidad)))+
  geom_tile()+
  theme(axis.text.y = element_text(size=8),axis.text.x = element_text(size=6,angle=90, hjust=1,vjust=0.5))+
  scale_fill_distiller(palette = "YlOrRd", direction=1)+
  labs(x="Comunas de Origen", title = "Cantidad de personas llegadas a comunas de la zona costera", fill= "Cantidad de personas")+
  scale_y_discrete(labels=c("San Antonio", "Algarrobo", "Cartagena","El Quisco","El  Tabo", "Santo Domingo"))

#San Antonio: 5601
#Algarrobo: 5602
#Cartagena: 5603
#El Quisco:5604
#El Tabo:5605
#Santo Domingo:5606

graficOrigen<-function(x,y){ggplot(x, aes(y=Comuna_actual,x=Comuna_5años, fill=as.integer(Cantidad)))+
    
    geom_tile()+
    theme(axis.text.y = element_text(size=8),axis.text.x = element_text(size=6,angle=90, hjust=1,vjust=0.5))+
    scale_fill_distiller(palette = "YlOrRd", direction=1)+
    labs(x="Comunas de Origen", y="Comunas costeras", title = paste0("Comuna donde residían las personas que actualmente viven comunas costeras en el año ", y) , fill= "Cantidad de personas")+
    scale_y_discrete(labels=c("San Antonio", "Algarrobo", "Cartagena","El Quisco","El  Tabo", "Santo Domingo"))
}

g1992<-graficOrigen(comuna1992_ii,"1987") %>% plotly::ggplotly(.,originalData=F)
g2002<-graficOrigen(comuna2002_ii, "1997")%>% plotly::ggplotly(.)
g2017<-graficOrigen(comuna2017_ii, "2012")%>% plotly::ggplotly(.)

tabla<-data.frame(
Comunas=c("San Antonio", "Algarrobo", "Cartagena","El Quisco","El  Tabo", "Santo Domingo"),
Censo1992=round(fuera1992$value,digits=2),
Censo2002=round(fuera2002$value,digits=2),
Censo2017=round(fuera2017$value,digits=2))


t1<-knitr::kable(tabla, col.names = c("Comunas", "Año 1987", "Año 1997", "Año 2017"), caption = "Porcentaje de personas que vivían en una comuna distinta a la actual")%>% 
  kableExtra::kable_styling()

save(g1992,g2002,g2017,t1,file="C:/CEDEUS/2021/abril1_ciudadesCosteras/output/graphicData_v1.RData")
?save
getwd()
dir()