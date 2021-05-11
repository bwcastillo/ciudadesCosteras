library(sf)
library(RPostgres)
library(tidyverse)


#0. Conectandome a la bbdd y schema: -------------------

conn<-fun_connect()

dbSendQuery(conn, "set search_path to censos;")

# Parte II: Set de comunas a comparar -------------------------------------
#Concón, Quintero, Puchuncaví, Zapallar, Papudo
#1992:5103,5107,5105,5405,5403
#2002:5103,5107,5105,5405,5403
#2017:5103,5107,5105,5405,5403

codecom1992[codecom1992$Description=="Papudo",]
codecom2002[codecom2002$Description=="PAPUDO",]
codecom2017[codecom2017$Description=="PAPUDO",]
# Creando sub tablas ------------------------------------------------------

#1992
colnames(tbl(conn, "censo_1992"))
tbl(conn, "censo_1992") %>% select(.,"comuna")

dbSendQuery(conn, " CREATE TABLE com_censo1992_ii AS
            SELECT *
            FROM censo_1992
            WHERE (comuna='05103' OR comuna='05107' OR comuna='05105' OR comuna='05405' OR comuna='05403')AND  edad>=60   ;")

#https://stackoverflow.com/questions/37351315/count-number-of-rows-when-using-dplyr-to-access-sql-table-query
tbl(conn, "com_censo1992_ii") %>% summarize(n())



#2002
colnames(tbl(conn, "censo_2002"))
dbSendQuery(conn, " CREATE TABLE com_censo2002_ii AS
            SELECT *
            FROM censo_2002
            WHERE (comuna='05103' OR comuna='05107' OR comuna='05105' OR comuna='05405' OR comuna='05403')AND  p19>=60   ;")


#2012
colnames(tbl(conn, "censo_2012"))
tbl(conn, "censo_2012")

dbSendQuery(conn, " CREATE TABLE com_censo2012_ii AS
            SELECT *
            FROM censo_2012
            WHERE (comuna='05103' OR comuna='05107' OR comuna='05105' OR comuna='05405' OR comuna='05403')AND  p20c>=60   ;")

tbl(conn, "com_censo2012_ii") %>% summarize(n())

#2017
colnames(tbl(conn, "censo_2017"))

dbSendQuery(conn, " CREATE TABLE com_censo2017_ii AS
            SELECT *
            FROM censo_2017
            WHERE (comuna='05103' OR comuna='05107' OR comuna='05105' OR comuna='05405' OR comuna='05403')AND  p09>=60   ;")

tbl(conn, "com_censo2017_ii") %>% summarize(n())



# Cantidad total de personas  ---------------------------------------------
colnames(tbl(conn, "comunas_censo1992"))
rbind(tbl(conn, "com_censo1992_ii") %>% summarize(n()) %>% collect(),
      tbl(conn, "com_censo2002_ii") %>% summarize(n()) %>% collect(),
      tbl(conn, "com_censo2017_ii") %>% summarize(n()) %>% collect())

# Censo 1992 vive en otra comuna --------------------------------------------------------

test<-dbSendQuery(conn, "SELECT comuna, comuna_1987_origen3, COUNT(*)
                  FROM com_censo1992_ii
                  GROUP BY comuna,comuna_1987_origen3;") 

test<-dbFetch(test)

test$comuna_1987_origen3<-paste0("0",test$comuna_1987_origen3)

test<-split.data.frame(test,test$comuna)

# Cuantas personas fuera de esta comuna hace 5 años --------
fuera1992_ii<-lapply(test, function(x){sum(x$count[x$comuna_1987_origen3!=unique(x$comuna)])/sum(x$count)*100}) %>% bind_rows(.) %>% pivot_longer(colnames(.))

# Cuantas personas vienen de cada comuna -------
comuna1992_ii<-lapply(test, function(x){x[x$comuna_1987_origen3!=unique(x$comuna),] %>% group_by(comuna_1987_origen3) %>% summarise(n=sum(count))})%>% bind_rows(.,.id="Comuna") 

# Censo 2002: conteos 24a por comuna ------------------------------------------------------

library(dbplyr)

test<-dbSendQuery(conn, "SELECT comuna,p24a,p24b, COUNT(*)
                  FROM com_censo2002_ii
                  GROUP BY comuna,p24a,p24b;") 

test<-dbFetch(test) 

#23A LUGAR DE RESIDENCIA HABITUAL
#23B CODIGO DEL PAIS O COMUNA DE RESIDENCIA HABITUAL
#24A LUGAR DE RESIDENCIA EN EL 97
#24B CODIGO DEL PAIS O COMUNA DE RESIDENCIA EN EL 97

test<-split.data.frame(test,test$comuna)

# Cuantas personas fuera de esta comuna hace 5 años --------
fuera2002_ii<-lapply(test, function(x){sum(x$count[x$p24a==2])/sum(x$count)*100}) %>% bind_rows(.) %>% pivot_longer(colnames(.))

# Cuantas personas vienen de cada comuna -------
comuna2002_ii<-lapply(test,function(x){x[x$p24a!=1,] %>% group_by( p24b) %>% summarise(n=sum(count))}) %>% bind_rows(.,.id="Comuna")

#  Censo 2012: conteos por comuna -----------------------------------------

test<-dbSendQuery(conn, "SELECT comuna,p22a, COUNT(*)
                  FROM comunas_censo2012_ii
                  GROUP BY comuna,p22a;") 

test<-dbFetch(test) 

test<-split.data.frame(test,test$comuna)

lapply(test, function(x){sum(x$count[x$p22a==3])/sum(x$count)*100})%>% bind_rows(.) %>% pivot_longer(colnames(.))

# Censo 2017: conteos p11 por comuna  ---------------------------------------------------------------
test<-dbSendQuery(conn, "SELECT comuna,p11,p11comuna, COUNT(*)
                  FROM com_censo2017_ii
                  GROUP BY comuna,p11,p11comuna;") 

test<-dbFetch(test) 

test<-split.data.frame(test,test$comuna)

# Cuantas personas fuera de esta comuna hace 5 años --------
fuera2017_ii<-lapply(test, function(x){sum(x$count[x$p11!=2])/sum(x$count)*100})%>% bind_rows(.) %>% pivot_longer(colnames(.))

# Cuantas personas vienen de cada comuna -------
comuna2017_ii<-lapply(test,function(x){x[x$p11!=2,] %>% group_by(p11comuna) %>% summarise(n=sum(count))}) %>% bind_rows(.,.id="Comuna")


# Uniendo diccionario a tablas --------------------------------------------

comuna1992_ii$comuna_1987_origen3<-substr(comuna1992_ii$comuna_1987_origen3,2,nchar(comuna1992_ii$comuna_1987_origen3))

comuna1992_ii<-left_join(comuna1992_ii,codecom1992, by=c("comuna_1987_origen3"="Value")) %>% .[!is.na(.$Description),] #%>% split.data.frame(.,.$Comuna)
colnames(comuna1992_ii)<-c("Comuna_actual", "Comuna_5años_code","Cantidad","Comuna_5años")

comuna2002_ii<-left_join(comuna2002_ii,codecom2002, by=c("p24b"="Value")) %>% .[!is.na(.$Description),]#%>% split.data.frame(.,.$Comuna)
colnames(comuna2002_ii)<-c("Comuna_actual", "Comuna_5años_code","Cantidad","Comuna_5años")

comuna2017_ii<-left_join(comuna2017_ii,codecom2017, by=c("p11comuna"="Value"))%>% .[!is.na(.$Description),]#%>% split.data.frame(.,.$Comuna)
colnames(comuna2017_ii)<-c("Comuna_actual", "Comuna_5años_code","Cantidad","Comuna_5años")



# Graficando --------------------------------------------------------------
graficOrigen_ii<-function(x,y){ggplot(x, aes(y=Comuna_actual,x=Comuna_5años, fill=as.integer(Cantidad)))+
    
    geom_tile()+
    theme(axis.text.y = element_text(size=8),axis.text.x = element_text(size=6,angle=90, hjust=1,vjust=0.5))+
    scale_fill_distiller(palette = "YlOrRd", direction=1)+
    labs(x="Comunas de Origen", y="Comunas costeras", title = paste0("Comuna donde residían las personas que actualmente viven comunas costeras en el año ", y) , fill= "Cantidad de personas")+
    scale_y_discrete(labels=c("Concón", "Quintero", "Puchuncaví", "Zapallar", "Papudo"))
}

g1992_ii<-graficOrigen_ii(comuna1992_ii,"1987") %>% plotly::ggplotly(.,originalData=F)
g2002_ii<-graficOrigen_ii(comuna2002_ii, "1997")%>% plotly::ggplotly(.)
g2017_ii<-graficOrigen_ii(comuna2017_ii, "2012")%>% plotly::ggplotly(.)


tabla_ii<-data.frame(
  Comunas=c("Concón", "Quintero", "Puchuncaví", "Papudo","Zapallar"),
  Censo1992=round(fuera1992_ii$value,digits=2),
  Censo2002=round(fuera2002_ii$value,digits=2),
  Censo2017=round(fuera2017_ii$value,digits=2))

t2<-knitr::kable(tabla_ii, col.names = c("Comunas", "Año 1987", "Año 1997", "Año 2017"), caption = "Porcentaje de personas que vivían en una comuna distinta a la actual")%>% 
  kableExtra::kable_styling()

save(g1992_ii,g2002_ii,g2017_ii,t2,file="C:/CEDEUS/2021/abril1_ciudadesCosteras/output/graphicData_v2.RData")

