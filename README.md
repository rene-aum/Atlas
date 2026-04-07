# aum-dashboard
repo colaborativo para dashboard de seguimiento de automarket

## Manual notebook dashboard

Para trabajar sobre el repo:
- Clonar repo
- Cambiar a rama dev, copiar rama dev a otra rama eg… feature/rene
- Hacer cambios, add, commit push a la rama creada , 
- Merge request a dev, añadirme como reviewer. No hacer push directo a dev nunca.

Para ejecutar en colab:
- Abrir pagina inicial de colab colab.research.google.com, introducir usuario
- En la ventana inicial ir a GitHub, dar acceso al repo, abrir notebook etl_looker.ipynb
- Añadir en secrets gitToken el token de GitHub, y en gitUser su usuario de Github
- Ejecutar notebook. Tener cuidado de los parámetros deseados.
    - from_drive en True si se requiere leer o escribir en drive en algun punto

- NO HACER CAMBIOS DESDE COLAB. CUALQUIER CAMBIO COMMIT Y PUSH HACERLO LOCALMENTE.


## Links importantes
Colab: https://colab.research.google.com/  
Folder de drive para insumos de looker: https://drive.google.com/drive/folders/1q3K65aHlO365k9d5ABTLT7DRpcACVTR4?usp=sharing  
repo github: https://github.com/rene-aum/aum-dashboard


## Overview del código

- La clase Loader en el archivo loader.py debe contener métodos que lean de las fuentes raw (dw, reportes etc) más no hagan ninguna transformación o join importante. Una vez ejecutado un método del tipo load, el resultado se guardará en memoria dentro de la misma instancia de la clase Loader. 

- La clase Transformer contiene métodos que transforman a la data raw cargada por la clase Loader. El constructo de la clase Transformer recibe como argumento un objeto de la clase Loader, este objeto ya debería traer cargada toda la data raw necesaria y esta data será accesible dentro de la clase Transformer. Acá podemos poner métodos que tomen en cuenta la lógica de negocio para obtener los kpis o cualquier otra tabla resultante de cruzar, filtrar, o agrupar.

- utils.py contiene cualquier función auxiliar, se puede importar desde cualquier .py.
    - IMPORTANTE: función custom_read, para leer csvs,o tabs de un xlsx. prioriza csv, si el parámetro csv_path no es None se deben especificar los parámetros excel_path y excel_tab_name forzosamente

- constants.py, cualquier constante que desee guardarse. Obviamente no tokens, no contraseñas.

- drive_toolbox.py, funciones para la gestión de archivos de drive en colab.

- toolbox.py es legacy, ignorar por ahora.

## Infra sistema dashboard

![alt text](INFRA_SISTEMA_DASHBOARD.png)