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


rbind(tbl(conn, "comunas_censo1992") %>% summarize(n()) %>% collect(),
tbl(conn, "comunas_censo2002") %>% summarize(n()) %>% collect(),
tbl(conn, "comunas_censo2012") %>% summarize(n()) %>% collect(),
tbl(conn, "comunas_censo2017") %>% summarize(n()) %>% collect())


# conteos 24a por comuna ------------------------------------------------------

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

lapply(test, function(x){sum(x$count[x$p24a==2])/sum(x$count)*100})


# Test 2017 ---------------------------------------------------------------
test<-dbSendQuery(conn, "SELECT comuna,p11, COUNT(*)
            FROM comunas_censo2017
                  GROUP BY comuna,p11;") 

test<-dbFetch(test) 

test<-split.data.frame(test,test$comuna)

lapply(test, function(x){sum(x$count[x$p11==3])/sum(x$count)*100})



# Gente por comuna --------------------------------------------------------


test<-dbSendQuery(conn, "SELECT comuna, COUNT(*)
              FROM comunas_censo2002
                    GROUP BY comuna;") 
test<-dbFetch(test)
  
sum(test$count)