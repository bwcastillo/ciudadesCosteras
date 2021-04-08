library(sf)
library(RPostgres)

censo1992<-colnames(readRDS("C:/CEDEUS/Censos/Censo1992_Persona_Full.Rds"))
censo2002<-colnames(readRDS("C:/CEDEUS/Censos/Censo2002_Persona_Full.Rds"))
censo2012<-colnames(readRDS("C:/CEDEUS/Censos/Censo2012_Persona_Full.Rds"))

colnames(st_read("C:/CEDEUS/2021/abril1_ciudadesCosteras/input/Cartografia censo 2012-20210402T161853Z-001/Cartografia censo 2012/Carto_Region_5.gdb",layer="MANZANA"))



# 1. Aprendiendo a crear y  moverme entre BBDD ----------------------------

#Crea función con los parámetros
fun_connect<-function(){dbConnect(RPostgres::Postgres(),
                                  dbname = 'censos', 
                                  host = 'localhost', # i.e. 'ec2-54-83-201-96.compute-1.amazonaws.com'
                                  port = 5432, # or any other port specified by your DBA
                                  user = 'postgres',
                                  password = 'adminpass',
                                  options="-c search_path=censo")}
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

dbSendQuery(conn, "CREATE SCHEMA censos")
dbSendQuery(conn, "set search_path to censos;")

dbSendQuery()

?copy_to
?setdiff



test<-paste(c(paste(dQuote("ID"), "SERIAL PRIMARY KEY"),
    paste(dQuote(censo1992), "INT NOT NULL")),
    collapse=",")

dbSendQuery(conn,paste0("CREATE TABLE censo1992 (",test, ")"))