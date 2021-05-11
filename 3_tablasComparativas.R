
# 1. Conteo total adultos mayores en cada una de las dos zonas cost --------

tbl(conn, "com_censo1992_ii") %>% summarize(n())
tbl(conn, "com_censo2002_ii") %>% summarize(n())
tbl(conn, "com_censo2017_ii") %>% summarize(n())

tbl(conn, "comunas_censo1992") %>% summarize(n())
tbl(conn, "comunas_censo2002") %>% summarize(n())
tbl(conn, "comunas_censo2017") %>% summarize(n())


# 2. Contando total de gente que viene de fuera --------

c1992_i<-lapply(split.data.frame(comuna1992, comuna1992$Comuna), function(x){as.numeric(sum(x$n))}) %>% bind_rows(.) %>% as.data.frame(.) %>% pivot_longer(.,1:6)
c2002_i<-lapply(split.data.frame(comuna2002, comuna2002$Comuna_actual), function(x){as.numeric(sum(x$Cantidad))})%>% bind_rows(.) %>% as.data.frame(.) %>% pivot_longer(.,1:6)
c2017_i<-lapply(split.data.frame(comuna2017, comuna2017$Comuna_actual), function(x){as.numeric(sum(x$Cantidad))})%>% bind_rows(.) %>% as.data.frame(.) %>% pivot_longer(.,1:6)

c1992_ii<-lapply(split.data.frame(comuna1992_ii, comuna1992_ii$Comuna_actual), function(x){as.numeric(sum(x$Cantidad))})%>% bind_rows(.) %>% as.data.frame(.) %>% pivot_longer(.,1:5)
c2002_ii<-lapply(split.data.frame(comuna2002_ii, comuna2002_ii$Comuna_actual), function(x){as.numeric(sum(x$Cantidad))})%>% bind_rows(.) %>% as.data.frame(.) %>% pivot_longer(.,1:5)
c2017_ii<-lapply(split.data.frame(comuna2017_ii, comuna2017_ii$Comuna_actual), function(x){as.numeric(sum(x$Cantidad))})%>% bind_rows(.) %>% as.data.frame(.) %>% pivot_longer(.,1:5)


# Transformando en tablas -------------------------------------------------
ti_c<-data.frame(c1992_i$value, c2002_i$value,c2017_i$value, row.names =c("San Antonio", "Algarrobo", "Cartagena", "El Quisco", "El Tabo", "Santo Domingo")) %>% knitr::kable(col.names = c("Censo 1992", "Censo 2002", "Censo 2017"),caption = "Cantidad de personas que vivía en otras comunas hace 5 años") %>% kableExtra::kable_styling()
tii_c<-data.frame(c1992_ii$value, c2002_ii$value,c2017_ii$value, row.names =c("Concón", "Puchuncaví", "Quintero", "Papudo", "Zapallar")) %>% knitr::kable(col.names = c("Censo 1992", "Censo 2002", "Censo 2017"),caption = "Cantidad de personas que vivía en otras comunas hace 5 años") %>% kableExtra::kable_styling()
#Concón, Quintero, Puchuncaví, Zapallar, Papudo
#1992:5103,5107,5105,5405,5403


save(ti_c, tii_c,file="C:/CEDEUS/2021/abril1_ciudadesCosteras/output/graphicData_counts.RData")
