# practica2_IBD
Repositorio para la segunda práctica de IBD del curso 2024/2025

## Objetivo 1:
Tratamiento de trafico_horario.csv con python para analizar los horarios con mayor congestión de tráfico en Madrid y tipo de vehículos.
Para ello necesitaremos las columnas: fecha_hora, total_vehiculos, coches,	motos,	camiones,	buses y nivel_congestion.

## Objetivo 2:
Para las rutas de bicimad más populares y el uso entre usuarios abonados y ocasionales utilizaremos el bicimad-usos.csv que trataremos con SQL
De bicimad-usos utilizaremos las columnas: tipo_usuario, estacion_origen,	estacion_destino, (distancia_km).

Tratamiento de tablas distritos y estaciones_transporte SQL haciendo join por el distrito_id para analizar la densidad de población por distrito en comparación con la presencia de transporte público.
De la tabla distritos utilizaremos las columnas: id, nombre, densidad_poblacion.
De la tabla estaciones_transporte utilizaremos las columnas: distrito_id, tipo.

## Objectivo 3:
Hacer visualizaciones de la variación diaria (por horas) de ocupación de parkings de Madrid a través de parkings-rotacion.csv y ext_aparcamientos_info.csv. Con este último csv también veremos como se correlaciona la ubicación de los parkings más ocupados según su ubicación en la ciudad.
De parkings-rotacion.csv utilizaremos las columnas: aparcamiento_id,	hora,	plazas_ocupadas,	plazas_libres,	porcentaje_ocupacion.
De ext_aparcamientos_info.csv nos quedamos con las columnas: aparcamiento_id, capacidad_total,	plazas_movilidad_reducida, plazas_vehiculos_electricos.
