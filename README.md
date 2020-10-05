# Ingesta Datos Mercado Eléctrico
En este proyecto se trata la ingesta de datos del mercado eléctrico mediante la Api de Esios (Sistema de Información del Operador del Sistema), que contiene todos los KPIs necesarios para entender y analizar el mercado eléctrico. Se parte de un análisis de los indicadores de los que se dispone, para posteriormente elegir los considerados mas importante, eligiendo aquellos que ofrecen información para cumplir el objetivo, por lo que este proyecto está realizado para aquellos indicadores que se han elegido y no es general para la elección del mismo. Aunque del mismo se puede obtener suficiente información como para personalizarlo con cambios mínimos. Los datos cargados serán guardados en una base de datos postgres.

## Ficheros
Para llevar a cabo este proyecto, se requieren diferentes ficheros de los cuales se van sacando información, o se van desarrollando módulos, para finalmente ejecutar las funciones necesarias para ingestar los datos. Para entender mejor la función de cada directorio se explicará uno a uno.

### tables
En esta carpeta se encuentran los ficheros de las tablas que se van a generar en la base de datos. Estos contienen tanto la información con la nomenclatura con la que se disponen en la API como el mapeo con la nomenclatura final. Cabe destacar que la tabla de "indicadores" que ofrece la descipción de cada indicador no es necesaria, pero también será cargada.

| File  | Description |
| ------------- | ------------- | 
| demanda  | Información de la demanda en la pemínsula  | 
| demanda_real  |  Demanda real de la península | 
| desvios  |  Precio de cobro y costes de los desvíos del mercado eléctrico | 
| facturación  |  Términos de Facturación de energía activa en en diferentes tipos de factura | 
| generación libre de CO2  |  Gneración con energías libres de CO2 y porcentaje sobre el total de las mismas | 
| generacion_medida | Información acerca de la generación por cada tipo de tecnología en las diferentes provincias con un desfase de un mes | 
| generación p48  |  Generación en el programa horario operativo | 
| generación prevista  |  Previsión Diaria de las energías eólica y fotovoltaica, además del total generado | 
| generacion_tiemporeal | Información acerca de la generación por cada tipo de tecnología en la península en tiempo real (cada 10 minutos) | 
| precios  | Información tanto del precio del spot diario como del pvpc| 


### variables
Obtiene el fichero de variables del cual se alimenta el script de ejecución, desde los parámetros para las conexiones, como ciertas listas de valores que facilitan un código más limpio y ordenado.

### module
Contiene los módulos de python generados para el funcionamiento de este proyecto.

| File  | Description |
| ------------- | ------------- | 
| esios_hook  | Permite la conexión y la interacción con la API del Esios | 
| postgres_hook  | Permite la conexión e interacción con la base de datos| 
| operators | Se alimenta de los conectores y crea las funciones necesarias para el proceso de ingesta de los datos | 

### script
Contiene el fichero de ejecución del "pipeline" de ingesta, y se utilizan los operados establecidos en el módulo visto anteriormente.



