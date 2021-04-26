library(sf)
library(RPostgres)
library(tidyverse)


# 0. Leyendo los datos y creando summaries: Tipo de datos y largo ------------

# Bonus track:Forma hechiza de guardar datos de RDS a csv
write.csv(readRDS("C:/CEDEUS/Censos/Censo2002_Persona_Full.Rds"),"C:/CEDEUS/2021/abril1_ciudadesCosteras/input/censo2002.csv",na="0")
write.csv(readRDS("C:/CEDEUS/Censos/Censo1992_Persona_Full.Rds"),"C:/CEDEUS/2021/abril1_ciudadesCosteras/input/censo1992.csv",na="0")
write.csv(readRDS("C:/CEDEUS/Censos/Censo2012_Persona_Full.Rds"),"C:/CEDEUS/2021/abril1_ciudadesCosteras/input/censo2012.csv",na="0")
write.csv(readRDS("C:/CEDEUS/Censos/Censo2017_Persona_Full.Rds"),"C:/CEDEUS/2021/abril1_ciudadesCosteras/input/censo2017.csv",na="0")


write.csv(as.data.frame(test), "p35.csv", na="0")
# 0.0 sabiendo como se debería llamar las columnas de la tabla

# Formato RDS:
#censo1992<-colnames(readRDS("C:/CEDEUS/Censos/Censo1992_Persona_Full.Rds"))
#censo2002<-colnames(readRDS("C:/CEDEUS/Censos/Censo2002_Persona_Full.Rds"))
#censo2012<-colnames(readRDS("C:/CEDEUS/Censos/Censo2012_Persona_Full.Rds"))

# Formato csv:
#head(read.csv("C:/CEDEUS/2021/abril1_ciudadesCosteras/input/censo2002.csv"))

# Formato shape:
#colnames(st_read("C:/CEDEUS/2021/abril1_ciudadesCosteras/input/Cartografia censo 2012-20210402T161853Z-001/Cartografia censo 2012/Carto_Region_5.gdb",layer="MANZANA"))


# 0.1 Viendo el largo de todos los caracteres
#Ref:grasshoppermouse: https://www.reddit.com/r/rstats/comments/azumgy/is_there_a_summarylike_function_which_gives_nchar/

test2012<-summarise_all(readRDS("C:/CEDEUS/Censos/Censo2012_Persona_Full.Rds"),funs(
  case_when(is.character(.)~max(nchar(.), na.rm=T),
            is.numeric(.) ~max(nchar(as.character(.)), na.rm=T))))

test2002<-summarise_all(readRDS("C:/CEDEUS/Censos/Censo2002_Persona_Full.Rds"),funs(
  case_when(is.character(.)~max(nchar(.), na.rm=T),
            is.numeric(.) ~max(nchar(as.character(.)), na.rm=T))))

test1992<-summarise_all(readRDS("C:/CEDEUS/Censos/Censo1992_Persona_Full.Rds"),funs(
  case_when(is.character(.)~max(nchar(.), na.rm=T),
            is.numeric(.) ~max(nchar(as.character(.)), na.rm=T))))



# 0.2 Summaries de tipo de dato
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


# 2. Creando esquemas y tablas -----------------------------------------------

# 2.1. Creando esquema y tabla a partir de los summaries

tablita<-function(x,y){
  a<-cbind(variables=row.names(as.data.frame(t(x))),as.data.frame(t(x))) 

  rownames(a)<- 1:nrow(a)

  a<-left_join(y, a, by=c("Var1"="variables"))
  a<-a[,c(1,4,5)]
  colnames(a)<-c("variable","clase","largo")
  return(a)
}

censo_1992<-tablita(test1992, censo1992)
censo_2012<-tablita(test2012, censo2012)
censo_2002<-tablita(test2002, censo2002)

#me vuelvo a conectar a la bbdd

conn<-fun_connect()

#dbSendQuery(conn, "CREATE SCHEMA censos")
dbSendQuery(conn, "set search_path to censos;")

#paste(test$Var1, test$Mode)

test<-paste(c(paste(dQuote("ID"), "SERIAL PRIMARY KEY"),
    paste(paste(test$variable, test$clase), "(",test$largo,")", "NOT NULL")),
    collapse=",")

dbSendQuery(conn,paste0("CREATE TABLE censo1992 (",test, ")")) #Funciona!

x$Var1<- gsub(".x$Var1") 
gsub("\\.","",censo2002$Var1)

#Creo una función que me crea las tablas

creaTabla<-function(x){
  y<-deparse(substitute(x))
  
  x$variable<-gsub("\\.","",x$variable)
  x<-paste(c(paste("ID", "SERIAL PRIMARY KEY"),
                                  paste(paste(x$variable, x$clase), "(",x$largo,")", "NOT NULL")),
                                collapse=",")
  
                     x<-paste0("CREATE TABLE ",y," (",x, ")")
                     return(x)}



deparse(substitute(test))
class(deparse(quote(test)))
creaTabla(test)

dbSendQuery(conn,creaTabla(test))
dbSendQuery(conn,creaTabla(censo_1992))
dbSendQuery(conn,creaTabla(censo_2002))
dbSendQuery(conn,creaTabla(censo_2012))


paste(c("(id",as.vector(censo_1992$variable),")"),  collapse=",")

(censo_1992$variable)
#dbSendQuery(conn, "\copy censos('test') C:/CEDEUS/2021/abril1_ciudadesCosteras/input/censo2002.7z' delimiter ';' csv header;")
View(test)

#C:/CEDEUS/2021/abril1_ciudadesCosteras/input#

#paste(c("(ID",as.vector(censo2002$Var1),")"),  collapse=",")

