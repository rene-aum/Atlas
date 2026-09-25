# aum-dashboard
repo colaborativo para dashboard de seguimiento de automarket

Atlas guide: see `ATLAS_GUIDE.md` for the current high-level architecture and pipeline flow.

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

