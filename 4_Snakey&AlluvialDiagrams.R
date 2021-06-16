

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
comunas<-comunas[comunas$NOM_REG=="Región de Valparaíso",]
comunas$NOM_COM<-chartr("ÁÉÍÓÚ", "AEIOU", toupper(comunas$NOM_COM))

st_centroid(comunas)





# NetworkD3 ---------------------------------------------------------------
install.packages("networkD3")
library(networkD3)

