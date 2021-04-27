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


#Censo 2002:
#7.12	NACIMIEN	Código de comuna o país de residencia	P22B	 	INTEGER	0	99999
#7.13	LLEGADA	Año de llegada al país de su madre	P22C	 	INTEGER	9999	9999
#7.14	LUGVIV	Lugar de residencia habitual	P23A	 	INTEGER	1	9
#7.15	VIVEHAB	Código de comuna o país de residencia habitual	P23B	 	INTEGER	0	99999
#7.16	LUGRES97	Lugar de residencia en Abril 1997	P24A	 	INTEGER	1	9
#7.17	VIVIA97	Código de comuna o país de residencia en Abril 1997	P24B	 	INTEGER	0	99999

#Censo 2012:
#P22A: Comuna o país en 2007
#1 Aún no nacía 2 En esta comuna 3 En otra comuna 4 En otro país 9 Ignorado

#P22B Texto comuna o país P22C Código comuna o país 0 No Aplica (P22A=1)
#1101-16255 Código de comuna o país 99999 Ignorado 

#P23A Comuna o país madre 
#1 En esta comuna 2 En otra comuna 3 En otro pais 9 Ignorado 

#P23B Texto comuna o país madre 

#P23C Código comuna o país madre 
#1101-16255 Código de comuna o país madre 99999 Ignorado