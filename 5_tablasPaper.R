library(sf)
library(RPostgres)
library(tidyverse)


#0. Conectandome a la bbdd y schema: -------------------

conn<-fun_connect()

dbSendQuery(conn, "set search_path to censos;")


rbind(tbl(conn, "com_censo1992_ii") %>% summarize(n()) %>% collect(),
      tbl(conn, "com_censo2002_ii") %>% summarize(n()) %>% collect(),
      tbl(conn, "com_censo2017_ii") %>% summarize(n()) %>% collect())



# Creando tablas con todos los datos regionales ---------------------------

# 1992
dbSendQuery(conn, " CREATE TABLE com_censo1992OG AS
            SELECT *
            FROM censo_1992
            WHERE (comuna='05103' OR comuna='05107' OR comuna='05105' OR comuna='05405' OR comuna='05403');")


# 2002
dbSendQuery(conn, " CREATE TABLE com_censo2002OG AS
            SELECT *
            FROM censo_2002
            WHERE (comuna='05103' OR comuna='05107' OR comuna='05105' OR comuna='05405' OR comuna='05403');")


# 2017
dbSendQuery(conn, " CREATE TABLE com_censo2017OG AS
            SELECT *
            FROM censo_2017
            WHERE (comuna='05103' OR comuna='05107' OR comuna='05105' OR comuna='05405' OR comuna='05403');")

# Consultando por la cantidad total de población ---------------------------

#1992
a<-dbSendQuery(conn, "SELECT comuna, COUNT(*)
                  FROM com_censo1992OG
                  GROUP BY comuna;") 

a<-dbFetch(a) 

#2002 
b<-dbSendQuery(conn, "SELECT comuna, COUNT(*)
                  FROM com_censo2002OG
                  GROUP BY comuna;") 

b<-dbFetch(b) 


#2017 

c<-dbSendQuery(conn, "SELECT comuna, COUNT(*)
                  FROM com_censo2017OG
                  GROUP BY comuna;") 

c<-dbFetch(c) 

#Juntando resultados

ab<-left_join(a,b, by="comuna")
abc<-left_join(ab,c, by="comuna")
colnames(abc)[2:4]<- c("1992","2002","2017")
abc$comuna<-c("Zapallar","Puchuncaví","Concón","Papudo","Quintero")
ni<-abc
#     Concón| Quintero| Puchuncaví | Zapallar | Papudo
#1992: 5103 |   5107  |  5105      |  5405    |   5403

# Consultando por la población adulta mayor -------------------------------

#1992

a<-dbSendQuery(conn, "SELECT comuna, COUNT(*)
                  FROM com_censo1992_ii
                  GROUP BY comuna;") 

a<-dbFetch(a) 

#2002 

b<-dbSendQuery(conn, "SELECT comuna, COUNT(*)
                  FROM com_censo2002_ii
                  GROUP BY comuna;") 

b<-dbFetch(b) 


#2017 

c<-dbSendQuery(conn, "SELECT comuna, COUNT(*)
                  FROM com_censo2017_ii
                  GROUP BY comuna;") 

c<-dbFetch(c) 

#Juntando resultados:

ab<-left_join(a,b, by="comuna")
abc<-left_join(ab,c, by="comuna")
colnames(abc)[2:4]<- c("1992","2002","2017")
abc$comuna<-c("Zapallar","Puchuncaví","Concón","Papudo","Quintero")

n1<-abc
# Creando base de datos de todas las personas del conjunto Santo Domingo-Algarrobo --------


# Creando tablas con todos los datos regionales ---------------------------


#1992
dbSendQuery(conn, " CREATE TABLE comunas_censo1992OG_ii AS
            SELECT *
            FROM censo_1992
            WHERE (comuna='05601' OR comuna='05602' OR comuna='05603' OR comuna='05604' OR comuna='05604' OR comuna='05605' OR comuna='05606');")


#2002

dbSendQuery(conn, " CREATE TABLE comunas_censo2002OG_ii AS
            SELECT *
            FROM censo_2002
            WHERE (comuna='05601' OR comuna='05602' OR comuna='05603' OR comuna='05604' OR comuna='05604' OR comuna='05605' OR comuna='05606');")


#2017

dbSendQuery(conn, " CREATE TABLE comunas_censo2017OG_ii AS
            SELECT *
            FROM censo_2017
            WHERE (comuna='05601' OR comuna='05602' OR comuna='05603' OR comuna='05604' OR comuna='05604' OR comuna='05605' OR comuna='05606');")


# Consultando por la cantidad total de población set S.Domingo-Algarrobo ---------------------------

#1992
a<-dbSendQuery(conn, "SELECT comuna, COUNT(*)
               FROM comunas_censo1992OG_ii
               GROUP BY comuna;") 

a<-dbFetch(a) 

#2002 
b<-dbSendQuery(conn, "SELECT comuna, COUNT(*)
               FROM comunas_censo2002OG_ii
               GROUP BY comuna;") 

b<-dbFetch(b) 


#2017 

c<-dbSendQuery(conn, "SELECT comuna, COUNT(*)
               FROM comunas_censo2017OG_ii
               GROUP BY comuna;") 

c<-dbFetch(c) 

#Juntando resultados

ab<-left_join(a,b, by="comuna")
abc<-left_join(ab,c, by="comuna")
colnames(abc)[2:4]<- c("1992","2002","2017")
abc$comuna<-c("Santo Domingo","Cartagena","San Antonio","El Quisco","Algarrobo","El Tabo")
nii<-abc
#Algarrobo: 5602
#Cartagena: 5603
#El Quisco:5604
#El Tabo:5605
#San Antonio: 5601
#Santo Domingo:5606

# Consultando por la población adulta mayor S.Domingo-Algarrobo -------------------------------

#1992

a<-dbSendQuery(conn, "SELECT comuna, COUNT(*)
               FROM comunas_censo1992
               GROUP BY comuna;") 

a<-dbFetch(a) 

#2002 

b<-dbSendQuery(conn, "SELECT comuna, COUNT(*)
               FROM comunas_censo2002
               GROUP BY comuna;") 

b<-dbFetch(b) 


#2017 

c<-dbSendQuery(conn, "SELECT comuna, COUNT(*)
               FROM comunas_censo2017
               GROUP BY comuna;") 

c<-dbFetch(c) 

#Juntando resultados:

ab<-left_join(a,b, by="comuna")
abc<-left_join(ab,c, by="comuna")
colnames(abc)[2:4]<- c("1992","2002","2017")
abc$comuna<-c("Santo Domingo","Cartagena","San Antonio","El Quisco","Algarrobo","El Tabo")
nii_am<-abc


# Sintetizando tablas y calculando porcentajes ----------------------------

#Cuanto varía la población total de cada comuna respecto años anteriores

tvar<-data.frame(ni,
var92a02=round((ni$`2002`-ni$`1992`)/ni$`2002`*100,2),
var02a17=round((ni$`2017`-ni$`2002`)/ni$`2017`*100,2)) %>% knitr::kable(caption="Cantidad de población total y cambio porcentual entre censo", col.names = c("Comuna","1992","2002","2017","Var % 1992 a 2002","Var % 2002a 2017")) %>% kableExtra::kable_styling()

tvar_ii<-data.frame(nii,
var92a02=round((nii$`2002`-nii$`1992`)/nii$`2002`*100,2),
var02a17=round((nii$`2017`-nii$`2002`)/nii$`2017`*100,2)) %>% knitr::kable(caption="Cantidad de población total y cambio porcentual entre censo", col.names = c("Comuna","1992","2002","2017","Var % 1992 a 2002","Var % 2002a 2017")) %>% kableExtra::kable_styling()


#Cuanto varía la población adulto mayor de cada comuna respecto años anteriores

tvar_am<-data.frame(ni_am,
var92a02=round((ni_am$`2002`-ni_am$`1992`)/ni_am$`2002`*100,2),
var02a17=round((ni_am$`2017`-ni_am$`2002`)/ni_am$`2017`*100,2))%>% knitr::kable(caption="Cantidad de población adulto mayor total y cambio porcentual entre censo", col.names = c("Comuna","1992","2002","2017","Var % 1992 a 2002","Var % 2002a 2017")) %>% kableExtra::kable_styling()


tvar_amii<-data.frame(nii_am,
var92a02=round((nii_am$`2002`-nii_am$`1992`)/nii_am$`2002`*100,2),
var02a17=round((nii_am$`2017`-nii_am$`2002`)/nii_am$`2017`*100,2))%>% knitr::kable(caption="Cantidad de población adulto mayor total y cambio porcentual entre censo", col.names = c("Comuna","1992","2002","2017","Var % 1992 a 2002","Var % 2002a 2017")) %>% kableExtra::kable_styling()

#Cuanto representa la población adulto mayor respecto a la pob total en cada año 

tp_am<-data.frame(
comuna=ni_am$comuna,
porAM1992=round(ni_am$`1992`/ni$`1992`*100,2),
porAM2002=round(ni_am$`2002`/ni$`2002`*100,2),
porAM2017=round(ni_am$`2017`/ni$`2017`*100,2))%>% knitr::kable(caption="Porcentaje de población adulto mayor respecto al total de la población", col.names = c("Comuna","Porcentaje 1992","Porcentaje 2002","Porcentaje 2017")) %>% kableExtra::kable_styling()

tp_am_ii<-data.frame(
comuna=nii_am$comuna,
porAM1992=round(nii_am$`1992`/nii$`1992`*100,2),
porAM2002=round(nii_am$`2002`/nii$`2002`*100,2),
porAM2017=round(nii_am$`2017`/nii$`2017`*100,2))%>% knitr::kable(caption="Porcentaje de población adulto mayor respecto al total de la población", col.names = c("Comuna","Porcentaje 1992","Porcentaje 2002","Porcentaje 2017")) %>% kableExtra::kable_styling()


save(tp_am, tp_am_ii,tvar,tvar_ii, tvar_am, tvar_amii, file="output/tabsMarkdown.RData")


# Provenientes de otras comunas de otros rangos de edades -------------------------------------------
colnames(tbl(conn, "censo_2017"))

#<15

test<-dbSendQuery(conn, "SELECT comuna,p11,p11comuna, P09,  COUNT(*)
                  FROM com_censo2017OG
                  WHERE P09<15 
                  GROUP BY comuna,P09,p11,p11comuna;") 

test<-dbFetch(test)

test<-split.data.frame(test,test$comuna)

test<-lapply(test, function(x){sum(x$count[x$p11!=2])/sum(x$count)*100})%>% bind_rows(.) %>% pivot_longer(colnames(.))
test<-lapply(test,function(x){x[x$p11!=2,] %>% group_by(p11comuna) %>% summarise(n=sum(count))}) %>% bind_rows(.,.id="Comuna")


left_join(test,codecom2017, by=c("p11comuna"="Value"))%>% .[!is.na(.$Description),]#%>% split.data.frame(.,.$Comuna)

colnames(test)<-c("Comuna_actual", "Comuna_5años_code","Cantidad","Comuna_5años")



#>15-50
test<-dbSendQuery(conn, "SELECT comuna,p11,p11comuna, P09,  COUNT(*)
                  FROM com_censo2017OG
                  WHERE P09>15 AND P09<50
                  GROUP BY comuna,P09,p11,p11comuna;") 

test<-dbFetch(test)

test<-split.data.frame(test,test$comuna)

test<-lapply(test, function(x){sum(x$count[x$p11!=2])/sum(x$count)*100})%>% bind_rows(.) %>% pivot_longer(colnames(.))
test<-lapply(test,function(x){x[x$p11!=2,] %>% group_by(p11comuna) %>% summarise(n=sum(count))}) %>% bind_rows(.,.id="Comuna")


left_join(test,codecom2017, by=c("p11comuna"="Value"))%>% .[!is.na(.$Description),]#%>% split.data.frame(.,.$Comuna)

colnames(test)<-c("Comuna_actual", "Comuna_5años_code","Cantidad","Comuna_5años")


#50-55

test<-dbSendQuery(conn, "SELECT comuna,p11,p11comuna, P09,  COUNT(*)
                  FROM com_censo2017OG
                  WHERE P09>50 AND P09<55
                  GROUP BY comuna,P09,p11,p11comuna;") 

test<-dbFetch(test)

test<-split.data.frame(test,test$comuna)

test<-lapply(test, function(x){sum(x$count[x$p11!=2])/sum(x$count)*100})%>% bind_rows(.) %>% pivot_longer(colnames(.))
test<-lapply(test,function(x){x[x$p11!=2,] %>% group_by(p11comuna) %>% summarise(n=sum(count))}) %>% bind_rows(.,.id="Comuna")


left_join(test,codecom2017, by=c("p11comuna"="Value"))%>% .[!is.na(.$Description),]#%>% split.data.frame(.,.$Comuna)

colnames(test)<-c("Comuna_actual", "Comuna_5años_code","Cantidad","Comuna_5años")


#55-60

test<-dbSendQuery(conn, "SELECT comuna,p11,p11comuna, P09,  COUNT(*)
                  FROM com_censo2017OG
                  WHERE P09>55 AND P09<60
                  GROUP BY comuna,P09,p11,p11comuna;") 

test<-dbFetch(test)

test<-split.data.frame(test,test$comuna)

test<-lapply(test, function(x){sum(x$count[x$p11!=2])/sum(x$count)*100})%>% bind_rows(.) %>% pivot_longer(colnames(.))
test<-lapply(test,function(x){x[x$p11!=2,] %>% group_by(p11comuna) %>% summarise(n=sum(count))}) %>% bind_rows(.,.id="Comuna")


left_join(test,codecom2017, by=c("p11comuna"="Value"))%>% .[!is.na(.$Description),]#%>% split.data.frame(.,.$Comuna)

colnames(test)<-c("Comuna_actual", "Comuna_5años_code","Cantidad","Comuna_5años")


# Set de comunas 2 --------------------------------------------------------



test<-dbSendQuery(conn, "SELECT comuna,p11,p11comuna, P09,  COUNT(*)
                  FROM comunas_censo2017OG_ii
                  WHERE P09<15 
                  GROUP BY comuna,P09,p11,p11comuna;") 

test<-dbFetch(test)

test<-split.data.frame(test,test$comuna)

test<-lapply(test, function(x){sum(x$count[x$p11!=2])/sum(x$count)*100})%>% bind_rows(.) %>% pivot_longer(colnames(.))
test<-lapply(test,function(x){x[x$p11!=2,] %>% group_by(p11comuna) %>% summarise(n=sum(count))}) %>% bind_rows(.,.id="Comuna")


left_join(test,codecom2017, by=c("p11comuna"="Value"))%>% .[!is.na(.$Description),]#%>% split.data.frame(.,.$Comuna)

colnames(test)<-c("Comuna_actual", "Comuna_5años_code","Cantidad","Comuna_5años")



#>15-50

test<-dbSendQuery(conn, "SELECT comuna,p11,p11comuna, P09,  COUNT(*)
                  FROM comunas_censo2017OG_ii
                  WHERE P09>15 AND P09<50
                  GROUP BY comuna,P09,p11,p11comuna;") 

test<-dbFetch(test)

test<-split.data.frame(test,test$comuna)

test<-lapply(test, function(x){sum(x$count[x$p11!=2])/sum(x$count)*100})%>% bind_rows(.) %>% pivot_longer(colnames(.))
test<-lapply(test,function(x){x[x$p11!=2,] %>% group_by(p11comuna) %>% summarise(n=sum(count))}) %>% bind_rows(.,.id="Comuna")


left_join(test,codecom2017, by=c("p11comuna"="Value"))%>% .[!is.na(.$Description),]#%>% split.data.frame(.,.$Comuna)

colnames(test)<-c("Comuna_actual", "Comuna_5años_code","Cantidad","Comuna_5años")


#50-55

test<-dbSendQuery(conn, "SELECT comuna,p11,p11comuna, P09,  COUNT(*)
                  FROM comunas_censo2017OG_ii
                  WHERE P09>50 AND P09<55
                  GROUP BY comuna,P09,p11,p11comuna;") 

test<-dbFetch(test)

test<-split.data.frame(test,test$comuna)

test<-lapply(test, function(x){sum(x$count[x$p11!=2])/sum(x$count)*100})%>% bind_rows(.) %>% pivot_longer(colnames(.))
test<-lapply(test,function(x){x[x$p11!=2,] %>% group_by(p11comuna) %>% summarise(n=sum(count))}) %>% bind_rows(.,.id="Comuna")


left_join(test,codecom2017, by=c("p11comuna"="Value"))%>% .[!is.na(.$Description),]#%>% split.data.frame(.,.$Comuna)

colnames(test)<-c("Comuna_actual", "Comuna_5años_code","Cantidad","Comuna_5años")


#55-60

test<-dbSendQuery(conn, "SELECT comuna,p11,p11comuna, P09,  COUNT(*)
                  FROM comunas_censo2017OG_ii
                  WHERE P09>55 AND P09<60
                  GROUP BY comuna,P09,p11,p11comuna;") 

test<-dbFetch(test)

test<-split.data.frame(test,test$comuna)

test<-lapply(test, function(x){sum(x$count[x$p11!=2])/sum(x$count)*100})%>% bind_rows(.) %>% pivot_longer(colnames(.))
test<-lapply(test,function(x){x[x$p11!=2,] %>% group_by(p11comuna) %>% summarise(n=sum(count))}) %>% bind_rows(.,.id="Comuna")


left_join(test,codecom2017, by=c("p11comuna"="Value"))%>% .[!is.na(.$Description),]#%>% split.data.frame(.,.$Comuna)

colnames(test)<-c("Comuna_actual", "Comuna_5años_code","Cantidad","Comuna_5años")

