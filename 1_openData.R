library(sf)
library(RPostgres)
library(tidyverse)

censo1992<-colnames(readRDS("C:/CEDEUS/Censos/Censo1992_Persona_Full.Rds"))
censo2002<-colnames(readRDS("C:/CEDEUS/Censos/Censo2002_Persona_Full.Rds"))
censo2012<-colnames(readRDS("C:/CEDEUS/Censos/Censo2012_Persona_Full.Rds"))
View(test)

test2012<-summarise_all(readRDS("C:/CEDEUS/Censos/Censo2012_Persona_Full.Rds"),funs(
  case_when(is.character(.)~max(nchar(.), na.rm=T),
            is.numeric(.) ~max(nchar(as.character(.)), na.rm=T))))

test2002<-summarise_all(readRDS("C:/CEDEUS/Censos/Censo2002_Persona_Full.Rds"),funs(
  case_when(is.character(.)~max(nchar(.), na.rm=T),
            is.numeric(.) ~max(nchar(as.character(.)), na.rm=T))))

test1992<-summarise_all(readRDS("C:/CEDEUS/Censos/Censo1992_Persona_Full.Rds"),funs(
  case_when(is.character(.)~max(nchar(.), na.rm=T),
            is.numeric(.) ~max(nchar(as.character(.)), na.rm=T))))


(c(1238,43,56,5,76))
#write.csv(readRDS("C:/CEDEUS/Censos/Censo2002_Persona_Full.Rds"),"C:/CEDEUS/2021/abril1_ciudadesCosteras/input/censo2002.csv")
#head(read.csv("C:/CEDEUS/2021/abril1_ciudadesCosteras/input/censo2002.csv"))

colnames(st_read("C:/CEDEUS/2021/abril1_ciudadesCosteras/input/Cartografia censo 2012-20210402T161853Z-001/Cartografia censo 2012/Carto_Region_5.gdb",layer="MANZANA"))

#https://stackoverflow.com/questions/22930292/is-it-possible-to-store-str-output-in-a-r-object
test<-spread(group_by(as.data.frame(summary.default(readRDS("C:/CEDEUS/Censos/Censo1992_Persona_Full.Rds")), by=Var1)),key = Var2, value = Freq)

abreColumnas<-function(x){tidyr::spread(dplyr::group_by(as.data.frame(summary.default(readRDS(x),funs(summarise_all(max(nchar(.))))), by=Var1)),key = Var2, value = Freq)}

censo1992<-abreColumnas("C:/CEDEUS/Censos/Censo1992_Persona_Full.Rds")
censo2002<-abreColumnas("C:/CEDEUS/Censos/Censo2002_Persona_Full.Rds")
censo2012<-abreColumnas("C:/CEDEUS/Censos/Censo2012_Persona_Full.Rds")



# 1. Aprendiendo a crear y  moverme entre BBDD ----------------------------

#Crea función con los parámetros
fun_connect<-function(){dbConnect(RPostgres::Postgres(),
                                  dbname = 'censos', 
                                  host = 'localhost', # i.e. 'ec2-54-83-201-96.compute-1.amazonaws.com'
                                  port = 5432, # or any other port specified by your DBA
                                  user = 'postgres',
                                  password = 'adminpass',
                                  options="-c search_path=censos")}
#Activo función
conn <- fun_connect()

#Creo base de datos
dbSendQuery(conn, "CREATE DATABASE test")

#Creo función para conectarme a la base de datos recién creadas
fun_connect2<-function(){dbConnect(RPostgres::Postgres(),
                                  dbname = 'test', 
                                  host = 'localhost', # i.e. 'ec2-54-83-201-96.compute-1.amazonaws.com'
                                  port = 5432, # or any other port specified by your DBA
                                  user = 'postgres',
                                  password = 'adminpass',
                                  options="-c search_path=censo")}

#Me conecto a esta nueva base de datos
coon <- fun_connect2()

#Me desconecto
dbDisconnect(coon, dbname="test")

#Ahora que estoy desconectado la puedo borrar y la borro
dbSendQuery(conn, "DROP DATABASE test")

#Me desconecto de la primera base de datos también
dbDisconnect(conn)


# Creando esquemas y tablas -----------------------------------------------

#me vuelvo a conectar a la bbdd

conn<-fun_connect()

#dbSendQuery(conn, "CREATE SCHEMA censos")
dbSendQuery(conn, "set search_path to censos;")

#paste(test$Var1, test$Mode)

test<-paste(c(paste(dQuote("ID"), "SERIAL PRIMARY KEY"),
    paste(paste(test$Var1, test$Mode), "NOT NULL")),
    collapse=",")

dbSendQuery(conn,paste0("CREATE TABLE censo1992 (",test, ")")) #Funciona!

x$Var1<- gsub(".x$Var1") 
gsub("\\.","",censo2002$Var1)

#Creo una función que me crea las tablas

creaTabla<-function(x){
  y<-deparse(substitute(x))
  
  x$Var1<-gsub("\\.","",x$Var1)
  x<-paste(c(paste("ID", "SERIAL PRIMARY KEY"),
                                  paste(paste(x$Var1, x$Mode), "NOT NULL")),
                                collapse=",")
  
                     x<-paste0("CREATE TABLE ",y," (",x, ")")
                     return(x)}



deparse(substitute(test))
class(deparse(quote(test)))
test<-creaTabla(censo2002)

dbSendQuery(conn,creaTabla(censo2002))

dbSendQuery(conn, "\copy censos("censo2002") C:/CEDEUS/2021/abril1_ciudadesCosteras/input/censo2002.7z' delimiter ';' csv header;")
View(test)

C:/CEDEUS/2021/abril1_ciudadesCosteras/input

paste(c("(ID",as.vector(censo2002$Var1),")"), collapse=",")

