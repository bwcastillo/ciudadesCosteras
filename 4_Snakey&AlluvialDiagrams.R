

# Ordenando de norte a sur ------------------------------------------------

case_when(comuna2017$Comuna_actual=="05601"~"SAN ANTONIO",
          comuna2017$Comuna_actual=="05602"~"ALGARROBO",
          comuna2017$Comuna_actual=="05603"~"CARTAGENA",
          comuna2017$Comuna_actual=="05604"~"EL QUISCO",
          comuna2017$Comuna_actual=="05605"~"EL TABO",
          comuna2017$Comuna_actual=="05606"~"SANTO DOMINGO")

case_when(comuna2017_ii$Comuna_actual=="05405"~"ZAPALLAR",
          comuna2017_ii$Comuna_actual=="05403"~"PAPUDO",
          comuna2017_ii$Comuna_actual=="05107"~"PUCHUNCAVÍ",
          comuna2017_ii$Comuna_actual=="05103"~"CONCON",
          comuna2017_ii$Comuna_actual=="05105"~"EL QUISCO"
          )


comunas<-st_read("C:/CEDEUS/2021/abril1_ciudadesCosteras/input/division_comunal_geo_ide_1/division_comunal_geo_ide_1.shp")

unique(comunas$NOM_REG)
comunas<-comunas[comunas$NOM_REG=="Región de Valparaíso" & comunas$NOM_REG=="Región Metropolitana de Santiago",]
comunas$NOM_COM<-chartr("ÁÉÍÓÚ", "AEIOU", toupper(comunas$NOM_COM))


tp_am
tp_am_ii
map<-rbind(tp_am,
tp_am_ii)


map$comuna<-chartr("ÁÉÍÓÚ", "AEIOU", toupper(map$comuna))





dbSendQuery(conn, " CREATE TABLE com_censo1992_ii AS
            SELECT *
            FROM censo_1992
            WHERE (comuna='05103' OR comuna='05107' OR comuna='05105' OR comuna='05405' OR comuna='05403' OR omuna='05601' OR comuna='05602' OR comuna='05603' OR comuna='05604' OR comuna='05604' OR comuna='05605' OR comuna='05606')AND  edad>=60   ;")


dbSendQuery(conn, " CREATE TABLE com_censo2002_ii AS
            SELECT *
            FROM censo_2002
            WHERE (comuna='05103' OR comuna='05107' OR comuna='05105' OR comuna='05405' OR comuna='05403' OR comuna='05601' OR comuna='05602' OR comuna='05603' OR comuna='05604' OR comuna='05604' OR comuna='05605' OR comuna='05606')AND  p19>=60  ;")


dbSendQuery(conn, " CREATE TABLE com_censo2017_ii AS
            SELECT *
            FROM censo_2017
            WHERE (comuna='05103' OR comuna='05107' OR comuna='05105' OR comuna='05405' OR comuna='05403')AND  p09>=60   ;")


?lm




dbSendQuery(conn, " CREATE TABLE comunas_censo2017 AS
SELECT *
            FROM censo_2017
            WHERE ()AND  p09>=60   ;")

