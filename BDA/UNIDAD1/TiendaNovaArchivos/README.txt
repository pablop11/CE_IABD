Tienda Nova — Caso REAL (Día 3)
===============================

Contenido:
- data/ : fuentes de datos
  - clientes.csv
  - ventas_mes.csv
  - logs_web.json
  - facturas/ (PDFs de ejemplo, placeholders)
- src/TiendaNovaETL : proyecto .NET 8 (Consola) con CsvHelper y Newtonsoft.Json
- salidas/ : carpeta para resultados
- ESTE README

Cómo ejecutar (Visual Studio o CLI):
1) Abrir Visual Studio 2022 y cargar src/TiendaNovaETL/TiendaNovaETL.csproj
   - O por CLI: `cd src/TiendaNovaETL` y `dotnet restore && dotnet run`
2) Asegúrate de que la ruta relativa a `data/` sea correcta (el programa la calcula respecto al ejecutable).
3) El programa:
   - Lee clientes y ventas (CSV) y logs (JSON)
   - Normaliza provincias y fechas, deduplica ventas por id_venta
   - Filtra por integridad referencial y coherencia
   - Genera:
       salidas/ventas_mes_limpio.csv
       salidas/ventas_por_dia.csv
       salidas/top5_productos.csv
   - Muestra una validación final por consola (suma ventas vs por día).

Extensiones para el alumnado:
- Añadir encriptación/enmascarado de email/NIF.
- Crear facturas_meta.csv desde los nombres de PDF (parseo de id_cliente y fecha).
- Unir logs_web por producto y día para analizar efecto de campaña.
- Exportar un XML por venta según el mapa de datos.

Notas:
- PDFs son de ejemplo (placeholders). No se requiere OCR en este caso.
- CsvHelper y Newtonsoft.Json se restauran con `dotnet restore`.
