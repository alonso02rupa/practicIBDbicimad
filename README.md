# practica2_IBD
Repositorio para la segunda práctica de IBD del curso 2024/2025

## Objetivo 1:
Tratamiento de trafico_horario.csv con python para analizar los horarios con mayor congestión de tráfico en Madrid y tipo de vehículos.  
Para ello necesitaremos las columnas: fecha_hora, total_vehiculos, coches,	motos,	camiones,	buses y nivel_congestion.

## Objetivo 2:
Para las rutas de bicimad más populares y el uso entre usuarios abonados y ocasionales utilizaremos el bicimad-usos.csv que trataremos con SQL
De bicimad-usos utilizaremos las columnas: tipo_usuario, estacion_origen,	estacion_destino.  

Tratamiento de tablas distritos y estaciones_transporte SQL haciendo join por el distrito_id (id en la tabla distritos) para analizar la densidad de población por distrito en comparación con la presencia de transporte público.  
De la tabla distritos utilizaremos las columnas: id, nombre, densidad_poblacion.  
De la tabla estaciones_transporte utilizaremos las columnas: distrito_id, tipo.  

## Objectivo 3:
Hacer visualizaciones de la variación diaria (por horas) de ocupación de parkings de Madrid a través de  un join de parkings-rotacion.csv y ext_aparcamientos_info.csv por la columna aparcamniento_id. En este último csv hemos añadido a mano en qué distrito se encuentra cada parking para luego analizar los distritos con parkings más llenos.  
De parkings-rotacion.csv utilizaremos las columnas: aparcamiento_id,	hora,	plazas_ocupadas.  
De ext_aparcamientos_info.csv nos quedamos con las columnas: aparcamiento_id, capacidad_total, distrito_id, nombre_distrito.  
