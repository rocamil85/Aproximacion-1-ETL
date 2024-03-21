# Documentación del Caso de Estudio (Aproximación 1 ETL)
## Problemática

Cada día a las 2:00 am hay que hacer un proceso bash, ¿en qué consiste ese bash?, a través de una petición POST a un endpoint de un "sistema tercerizado" se devuelve un objeto json muy grande anidado con muchos objetos dentro (cada objeto interno es una orden de compra realizada por un cliente), una orden tiene alrededor de 100 campos o atributos, o sea es bastante complejo pero bien estructurado (incluye por ejemplo nombre del comprador, el producto, la dirección de envío, etc.). 

La petición POST devuelve las órdenes de los últimos 5 meses, si tenemos en cuenta que un día cualquiera hay una aproximado de 6000 órdenes, y en un día como navidad pueden haber por alta demanda 20000 órdenes, notarás que ya se trata de un ambiente de Bigdata. Cabe recalcar aquí que esta petición POST no devuelve las órdenes de los últimos 5 meses en una sola petición, en realidad la API devuelve sólo 100 órdenes por un tema de rendimiento, por lo que hay que hacer una especie de paginación de 100 en 100 hasta llegar a todas las órdenes de 5 meses atrás. La API tiene un campo "total de orden" y se sabe hasta cuando habría que iterar.

Sigamos con el escenario general del ejercicio. Se hace la petición POST y cada una de las órdenes deben insertarse en Bigquery. O sea, hay que diseñar una tabla de Bigquery que tenga como columnas los atributos internos de una orden. Una orden tiene un campo que es un identificador, como este proceso se va a hacer todos los días la idea es que las órdenes que sean nuevas o sea que aún no existan en Bigquery hay que insertarlas, pero si ya existen sólo hay que actualizarlas. 

Cuando dicha orden ya está insertada o actualizada (ya sea una u otra, siempre debe ser una u otra) entonces hay que notificarla al "sistema tercerizado" a través de otra petición POST "diciéndole" que ya tengo esa orden en Bigquery. La idea de hacer esta notificación exitosa es asegurarse de que no hubo ningún problema durante un proceso intermedio de Transformación y que efectivamente llegó la orden a Bigquery y está tengo lista para hacer Análisis y Machine Learning en el futuro. 

## ¿Cual podría ser una solución costo efectiva en GCP?

### Análisis de la Propuesta de Solución:

#### 1. Extracción de Datos y Almacenamiento Intermedio
**Cloud Scheduler & Cloud Functions:** Utilizar Cloud Scheduler para activar una Cloud Functions diariamente a las 2:00 am. Esta función ejecutará el script de extracción de datos. La Cloud Functions realizará las peticiones POST paginadas al sistema "sistema tercerizado". Para manejar el volumen de datos, puedes considerar almacenar los resultados intermedios en un formato eficiente como JSON o Avro.

**Google Cloud Storage (GCS):** Almacenar los datos extraídos en GCS. Elige un formato de archivo adecuado (CSV, JSON, Avro, o Parquet) según tus necesidades de procesamiento y análisis posterior.

#### 2. Transformación y Carga en BigQuery
**Apache Beam con Google Cloud Dataflow:** Utilizar Apache Beam para crear un pipeline de procesamiento de datos que se ejecutará en Dataflow. Este pipeline realizará las siguientes operaciones:
* Leer los datos desde GCS.
* Transformar los datos si es necesario (por ejemplo, normalizar campos, calcular agregaciones, etc.).
* Verificar si cada orden ya existe en BigQuery y decidir si insertar o actualizar.
* Cargar los datos en la tabla original de BigQuery.
* Realizar las transformaciones necesarias y cargar los datos en una segunda tabla transformada en BigQuery.

#### 3. Ejecución de Procedimientos Almacenados en Bigquery
Aunque las tablas ya tienen la mayor transformacion realizada previamente por Dataflow todavía es necesario ejecutar varios procedimientos almacenados en BigQuery que ajustan el contenido de las tablas iniciales para crear las tablas finales.
  
#### 4. Notificación de Éxito
**Cloud Functions para Notificación:** Una vez que los datos estén cargados en BigQuery, se usará otra Cloud Functions para notificar al "sistema tercerizado" a través de una petición POST por cada orden procesada.

#### 5. Orquestación de Procesos
**Cloud Composer (Apache Airflow):** Utilizar Cloud Composer para orquestar todo el flujo de trabajo. Esto incluye:
- La activación de la Cloud Functions de extracción de datos e inserción intermedia en GCS.
- La ejecución del pipeline de Dataflow.
- La ejecución de procedimientos almacenados en BigQuery.
- La activación de la Cloud Functions de notificación.

### ---------------------- Razones para Esta Arquitectura ----------------------------------
**Escalabilidad y Flexibilidad:** Esta arquitectura es escalable para manejar el alto volumen de datos y las variaciones en el número de órdenes (como en temporadas altas).

**Costo-Eficiencia:** El uso de servicios gestionados como Cloud Functions, Dataflow y BigQuery optimiza los costos, ya que pagas sólo por los recursos que utilizas.

**Fiabilidad y Mantenibilidad:** Al utilizar servicios de GCP, te beneficias de la alta disponibilidad y la facilidad de mantenimiento.

**Automatización y Orquestación:** Cloud Composer facilita la gestión de dependencias y la secuencia de tareas, asegurando que el flujo de trabajo se ejecute sin problemas.

#### Cloud Run VS Cloud Functions VS App Engine
Para entender por qué Cloud Run puede ser una mejor opción que Cloud Functions o App Engine en este caso, es importante considerar las  características específicas de la tarea y las capacidades de cada plataforma. Vamos a compararlas en términos de flexibilidad, tiempo de ejecución, escalabilidad y costos.

#### Cloud Run 
**Ventajas:**
* **Flexibilidad en Tiempos de Ejecución:** Cloud Run permite tiempos de ejecución más prolongados en comparación con Cloud Functions. Puede manejar tareas que duren hasta 15 minutos o más, dependiendo de la configuración, lo cual es ideal para tu caso de uso.
* **Contenerización:** Cloud Run utiliza contenedores, lo que te ofrece flexibilidad para incluir cualquier biblioteca o dependencia necesaria sin preocuparte por el entorno de ejecución.

* **Escalabilidad Automática:** Escala automáticamente y puede escalar a cero cuando no hay solicitudes, lo que significa que no pagas por el tiempo de inactividad.

* **Costos:** Sólo pagas por el tiempo de ejecución de las solicitudes, lo que puede ser más económico para tareas que se ejecutan una vez al día.

**Desventajas:**
* Requiere un conocimiento básico de contenedores y Docker.

***


#### Cloud Functions
**Ventajas:**
* **Facilidad de Uso:** Es muy sencilla de configurar y usar, ideal para tareas pequeñas y ligeras.

* **Costos:** Sólo pagas por el tiempo de ejecución, lo que es ideal para funciones que se ejecutan en respuesta a eventos.

**Desventajas:**
* **Límite de Tiempo:** Tiene un límite de tiempo de ejecución máximo (9 minutos), lo que no es adecuado para esta tarea que puede durar 15 minutos.

* **Menos Flexibilidad:** Menos flexibilidad en términos de entorno de ejecución en comparación con los contenedores.

***


#### App Engine
**Ventajas:**
* **Fácil Despliegue de Aplicaciones Web:** Excelente para aplicaciones web que requieren escalabilidad automática.

* **Gestión de Infraestructura:** Gestiona la infraestructura por ti, lo que reduce la sobrecarga de mantenimiento.

**Desventajas:**
* **Costo y Recursos:** Aunque escala automáticamente, puede ser más costoso mantener una instancia en ejecución constantemente, especialmente si la tarea sólo necesita ejecutarse una vez al día.

* **Tiempo de Ejecución:** Los tiempos de ejecución prolongados podrían no ser ideales para App Engine, que está diseñado más para responder a solicitudes web rápidas.

***


#### En términos de costos:

* **Cloud Run:** Sólo pagas por el tiempo de ejecución real. Si tu servicio está inactivo, no incurre en costos. Esto lo hace ideal para tareas que se ejecutan en horarios específicos y no constantemente.

* **Cloud Functions:** Similar a Cloud Run en términos de costos, pero con las limitaciones de tiempo ya mencionadas.

* **App Engine:** Puede incurrir en costos más altos, especialmente si necesita estar ejecutándose continuamente o manejar cargas de trabajo impredecibles.


#### Conclusión Parcial:
Para una tarea que se ejecuta diariamente y puede durar hasta 15 minutos, Cloud Run es probablemente la mejor opción debido a su flexibilidad, capacidad para manejar tiempos de ejecución más largos y un modelo de precios que sólo te cobra por el tiempo de ejecución real. Además, te brinda la flexibilidad de trabajar con contenedores, lo que significa que puedes configurar tu entorno exactamente como lo necesitas.



## Propuesta de solución
![Cloud Composer](Composer/composer.png)
1. Una Cloud Run contiene una app Python que se encarga de hacer las peticiones al "sistema tercerizado" de forma paginada y colocar cada objeto json extraído en GCS, (en un archivo .json correspondiente al día solicitado) que tiene todos los objetos json extraídos de las peticiones. (Se genera un archivo por día). (_**Ver App extract-ceo-app-repository**_)
   
1. Se utilizan técnicas de CI/CD para este despliegue. En Editor de Código de la Cloud Shell. Una vez probado el código localmente usando un entorno virtual de Python para aislar las dependencias. Se hace un Push a una rama por ejemplo Staging (repositorio creado previamente en Cloud Sources Repositories).

1. Al hacer este Push se dispara un trigger de Cloud Build (herramienta CI/CD de GCP) que lee el archivo Dockerfile y procede a construir una imagen. Dicha imagen es almacenada en Container Registry automáticamente.

1. Desde Cloud Run se importa esta imagen y se despliega. (Esta última parte puede ser manual o bien automáticamente leyendo alguna actualización de una rama del repositorio). Cabe destacar que esta url proporcionada por Cloud Run puede ser pública o protegida (requerir autenticación, variante escogida en este caso).

1. Se realizan 5 Dataflows. El objetivo de cada uno es desagregar campos json anidados y complejos que están dentro de cada objeto extraído y convertirlo en una tabla distinta de Bigquery (se usan transformaciones FlatMap que permiten a partir de un sólo elemento pueda ser transformado en varios elementos de salida, esto es así porque un subcampo json es un array de varios objetos json a su vez).
    
1. Primero se desarrolla el pipeline de Beam en Jupyter Notebook (Workbench de Dataflow), esto es así para aprovechar las capacidades de exploración de datos, visualización y pruebas intermedias que son útiles durante del desarrollo del pipeline, además se usa DirectRunner para ejecutar este pipeline y ver un resultado progresivo.

1. Una vez que se tiene una línea base del pipeline entonces se procede a traspasarlo (exportando a .py) a un ambiente de desarrollo mas adecuado para continuar con las respectivas pruebas, desarrollo y despliegue. Una vez en el Code Editor de la Shell de GCP se procede a probar el desarrollo en un entorno virtual de Python para asegurar el funcionamiento con las dependencia adecuadas. 

1. Luego se procede al diseño de las Pruebas Unitarias usando el Framework UnitTest de Python. Consiste en el desarrollo de múltiples pruebas unitarias que posteriormente se deben ejecutar cuando se despliega el proyecto (pipeline).

1. Entonces se procede a diseñar un flujo de CI/CD a través del archivo cloudbuild.yaml y la herramienta Cloud Build de GCP. El objetivo es que cuando se haga un push a una rama por ejemplo Staging en Cloud Source Repositories, sea ejecutado el archivo cloudbuild.yaml (se hace configurando un trigger de Cloud Build que lea el .yaml). Este .yaml define una serie de pasos en orden durante el despliegue al repositorio priorizando la ejecución automática de pruebas unitarias y si pasan correctamente continua con un segundo paso que sería copiar este pipeline de beam (fichero .py) al bucket GCS correspondiente al DAG de Airflow (Composer) ya que la tarea programada de Composer siempre estará leyendo de este archivo en esta ubicación para ejecutar la tarea diaria. (_**Ver App alas_dataflow_1_repository**_).

1. En este punto queda desarrollar todo el esquema de las tablas y procedimientos almacenados dentro de Bigquery, (los esquemas de las 5 tablas para probar los 5 dataflows deberían existir). Se procede a hacer una serie de procedimientos almacenados que son transformaciones a partir de las 5 tablas iniciales provenientes de los 5 dataflows. Transformaciones que requieren el uso de Update y Delete continuadamente hasta llegar a las tablas delivery_order_master (histórica) y delivery_order_work (Final).

1. Orquestación a través de Composer, se crea una DAG (_**Ver Composer/DAG_cloudrun_dataflow_sp**_) y se sube al bucket del DAG correspondiente generado por Composer durante la configuración inicial. Este DAG básicamente inicia una serie de tareas a las 2:00 am comenzando por la llamada (autenticada) a la Cloud Run (usando PythonOperator). La misma desencadena la lectura de órdenes y guarda en GCS los archivos json de cada día de los últimos 5 meses. Luego desencadena 5 dataflows (que generan tablas temporales en Bigquery) (usando BeamRunPythonPipelineOperator) en orden y por último todos los procedimientos almacenados (BigQueryExecuteQueryOperator) en el orden correcto. Debajo se muestra el proceso de construcción/ejecución de Cloud Composer (Versión admnistrada de Apache Airflow).
![Cloud Composer ejecución](Composer/airflow.png) 

## Conclusión
Debajo de Cloud Composer hay un clúster de Kubernetes. Cloud Composer utiliza Google Kubernetes Engine (GKE) para orquestar y gestionar los contenedores que ejecutan Apache Airflow y sus componentes relacionados. Este enfoque aprovecha la escalabilidad, la gestión de la infraestructura y las capacidades de auto-curación de Kubernetes, proporcionando una plataforma robusta y escalable para la orquestación de flujos de trabajo.

Por ello y sin embargo el uso de Cloud Composer termina siendo costoso y el paradigma de creación de 5 dataflows seguidos provoca lentitud en el flujo de trabajo ya que Composer tiene que iniciar primeramente la infraestructura de cada Dataflow antes de lanzar un job sobre él. El proceso demora alrededor de 2 horas lo que no cumple con el criterio de eficiencia/rendimiento.

Para este Caso de Estudio particular la aproximación utilizada ETL con Cloud Composer orquestando todos los procesos no es costo/eficiente por lo que se procede a analizar una nueva alternativa arquitectónica de tipo ELT. **(Ver Aproximación 2 ELT)**.
