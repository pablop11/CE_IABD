using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using Newtonsoft.Json.Linq;

class Cliente {
    public int id_cliente { get; set; }
    public string? nombre { get; set; }
    public string? fecha_alta { get; set; }
    public string? provincia { get; set; }
    public string? email { get; set; }
}

class Venta {
    public int id_venta { get; set; }
    public string? fecha { get; set; }
    public int id_cliente { get; set; }
    public string? id_producto { get; set; }
    public int unidades { get; set; }
    public decimal importe { get; set; }
}

class VentaLimpia {
    public int id_venta { get; set; }
    public DateTime fecha { get; set; }
    public int id_cliente { get; set; }
    public string id_producto { get; set; } = "";
    public int unidades { get; set; }
    public decimal importe { get; set; }
}

class FacturaMeta
{
    public string id_factura { get; set; } = "";
    public DateTime fecha { get; set; }
    public int id_cliente { get; set; }
    public decimal importe_total { get; set; }
    public string ruta_pdf { get; set; } = "";
}

static class Program {

    static void Main(string[] args) {
        var baseDir = AppContext.BaseDirectory;
        var dataDir = Path.GetFullPath(Path.Combine(baseDir, "..", "..", "..", "..", "data"));
        var outDir = Path.GetFullPath(Path.Combine(baseDir, "..", "..", "..", "..", "salidas"));
        var rootDir = Path.GetFullPath(Path.Combine(baseDir, "..", "..", "..", ".."));
        Directory.CreateDirectory(outDir);

        Console.WriteLine($"Leyendo datos desde: {dataDir}");
        // 1) Leer clientes
        var clientesPath = Path.Combine(dataDir, "clientes.csv");
        var ventasPath = Path.Combine(dataDir, "ventas_mes.csv");
        var logsPath = Path.Combine(dataDir, "logs_web.json");
        var factMetaPath = Path.Combine(dataDir, "facturas_meta.csv");

        var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture) {
            HasHeaderRecord = true,
            Delimiter = ","
        };
        List<Cliente> clientes;
        using (var reader = new StreamReader(clientesPath)) {
            using var csv = new CsvReader(reader, csvConfig);
            clientes = csv.GetRecords<Cliente>().ToList();
        }

        // Normalizar provincia (ejemplo simple)
        foreach (var c in clientes) {
            if (c.provincia != null) {
                c.provincia = c.provincia.Replace("La Coruña","A Coruña").Replace("Araba/Álava","Álava");
            }
        }

        // 2) Leer ventas (posibles duplicados y fechas con dos formatos)
        List<Venta> ventasRaw;
        using (var reader = new StreamReader(ventasPath)) {
            using var csv = new CsvReader(reader, csvConfig);
            ventasRaw = csv.GetRecords<Venta>().ToList();
        }

        // 3) Depuración: deduplicado por id_venta (quedarse con la última ocurrencia)
        var ventasDedup = ventasRaw
            .GroupBy(v => v.id_venta)
            .Select(g => g.Last())
            .ToList();

        // 4) Transformación: fecha → ISO (parseo múltiple)
        string[] formatos = { "yyyy-MM-dd", "dd/MM/yyyy", "yyyy/MM/dd" };
        var ventasParsed = new List<VentaLimpia>();
        foreach (var v in ventasDedup) {
            if (DateTime.TryParseExact(v.fecha, formatos, CultureInfo.InvariantCulture, DateTimeStyles.None, out var f)) {
                ventasParsed.Add(new VentaLimpia {
                    id_venta = v.id_venta,
                    fecha = f,
                    id_cliente = v.id_cliente,
                    id_producto = v.id_producto ?? "",
                    unidades = v.unidades,
                    importe = v.importe
                });
            }
        }

        // 5) Integridad referencial: filtrar ventas con cliente inexistente
        var clientesSet = clientes.Select(c => c.id_cliente).ToHashSet();
        var ventasRefOK = ventasParsed.Where(v => clientesSet.Contains(v.id_cliente)).ToList();

        // 6) Coherencia básica
        ventasRefOK = ventasRefOK.Where(v => v.unidades > 0 && v.importe >= 0).ToList();

        // 7) Guardar ventas limpias
        var ventasLimpioPath = Path.Combine(outDir, "ventas_mes_limpio.csv");
        using (var writer = new StreamWriter(ventasLimpioPath))
        using (var csv = new CsvWriter(writer, csvConfig)) {
            csv.WriteHeader<VentaLimpia>();
            csv.NextRecord();
            foreach (var v in ventasRefOK) {
                csv.WriteRecord(v);
                csv.NextRecord();
            }
        }

        // 8) Resúmenes: ventas por día
        var ventasPorDia = ventasRefOK
            .GroupBy(v => v.fecha.Date)
            .Select(g => new {
                fecha = g.Key.ToString("yyyy-MM-dd"),
                ventas_totales = g.Sum(x => x.importe),
                unidades_totales = g.Sum(x => x.unidades)
            })
            .OrderBy(x => x.fecha)
            .ToList();

        var ventasPorDiaPath = Path.Combine(outDir, "ventas_por_dia.csv");
        using (var writer = new StreamWriter(ventasPorDiaPath)) {
            writer.WriteLine("fecha,ventas_totales,unidades_totales");
            foreach (var r in ventasPorDia) {
                writer.WriteLine($"{r.fecha},{r.ventas_totales:F2},{r.unidades_totales}");
            }
        }

        // 9) Top 5 productos
        var top5 = ventasRefOK
            .GroupBy(v => v.id_producto)
            .Select(g => new {
                id_producto = g.Key,
                unidades_totales = g.Sum(x => x.unidades),
                importe_total = g.Sum(x => x.importe)
            })
            .OrderByDescending(x => x.unidades_totales)
            .Take(5)
            .ToList();

        var top5Path = Path.Combine(outDir, "top5_productos.csv");
        using (var writer = new StreamWriter(top5Path)) {
            writer.WriteLine("id_producto,unidades_totales,importe_total");
            foreach (var r in top5) {
                writer.WriteLine($"{r.id_producto},{r.unidades_totales},{r.importe_total:F2}");
            }
        }

        // 10) Validación final simple
        var sumaDias = ventasPorDia.Sum(x => x.ventas_totales);
        var sumaVentas = ventasRefOK.Sum(x => x.importe);
        Console.WriteLine($"Validación: suma ventas_por_dia = {sumaDias:F2} vs suma ventas_limpio = {sumaVentas:F2}");

        var validPath = Path.Combine(outDir, "validacion_final.txt");
        File.WriteAllText(validPath,
            $"Suma ventas_por_dia: {sumaDias:F2}\n" +
            $"Suma ventas_limpio : {sumaVentas:F2}\n" +
            $"Coinciden          : {(Math.Abs(sumaDias - sumaVentas) < 0.01m ? "SI" : "NO")}\n");

        // ===== 5) Captura de facturas (facturas_meta.csv) =====
        List<FacturaMeta>? facturasMeta = null;
        if (File.Exists(factMetaPath))
        {
            using var reader = new StreamReader(factMetaPath);
            using var csv = new CsvReader(reader, csvConfig);
            facturasMeta = csv.GetRecords<FacturaMeta>().ToList();

            // Conciliación simple: total facturas vs total ventas
            var totalFacturas = facturasMeta.Sum(f => f.importe_total);
            var totalVentas = sumaVentas;

            // Verificación de existencia de PDFs
            int pdfFaltantes = 0;
            foreach (var f in facturasMeta)
            {
                // Si ruta_pdf es relativa a /data, construimos ruta absoluta
                var rutaPdf = f.ruta_pdf.Replace("\\", "/");
                string absPdf = rutaPdf.StartsWith("data/") ?
                    Path.Combine(rootDir, rutaPdf) :
                    Path.Combine(dataDir, rutaPdf);
                if (!File.Exists(absPdf)) pdfFaltantes++;
            }

            var concPath = Path.Combine(outDir, "conciliacion_facturas_vs_ventas.txt");
            File.WriteAllText(concPath,
                $"TOTAL FACTURAS: {totalFacturas:F2}\n" +
                $"TOTAL VENTAS  : {totalVentas:F2}\n" +
                $"COINCIDEN     : {(Math.Abs(totalFacturas - totalVentas) < 0.01m ? "SI" : "NO")}\n" +
                $"PDFs faltantes: {pdfFaltantes}\n");
            Console.WriteLine($"Conciliación generada: {concPath}");
        }
        else
        {
            Console.WriteLine("No se encontró facturas_meta.csv — se omite la conciliación.");
        }

        // ===== 6) JSON de logs (eventos) =====
        if (File.Exists(logsPath))
        {
            var jsonText = File.ReadAllText(logsPath);
            var arr = JArray.Parse(jsonText);

            // Normalizar: fecha, evento, producto
            var logs = arr.Select(x => new {
                fecha = DateTime.Parse(x["ts"]!.ToString(), null, DateTimeStyles.AdjustToUniversal).Date,
                evento = x["evento"]!.ToString(),
                producto = x["producto"]!.ToString()
            }).ToList();

            // a) Eventos por día
            var eventosPorDia = logs
                .GroupBy(l => l.fecha)
                .Select(g => new {
                    fecha = g.Key.ToString("yyyy-MM-dd"),
                    total_eventos = g.Count(),
                    clicks = g.Count(x => x.evento.Equals("click", StringComparison.OrdinalIgnoreCase)),
                    views = g.Count(x => x.evento.Equals("view", StringComparison.OrdinalIgnoreCase))
                })
                .OrderBy(x => x.fecha)
                .ToList();

            var eventosDiaPath = Path.Combine(outDir, "eventos_por_dia.csv");
            using (var w = new StreamWriter(eventosDiaPath))
            {
                w.WriteLine("fecha,total_eventos,clicks,views");
                foreach (var r in eventosPorDia)
                    w.WriteLine($"{r.fecha},{r.total_eventos},{r.clicks},{r.views}");
            }

            // b) Clicks por producto
            var clicksPorProducto = logs
                .Where(l => l.evento.Equals("click", StringComparison.OrdinalIgnoreCase))
                .GroupBy(l => l.producto)
                .Select(g => new { producto = g.Key, clicks = g.Count() })
                .OrderByDescending(x => x.clicks)
                .ToList();

            var clicksProdPath = Path.Combine(outDir, "clicks_por_producto.csv");
            using (var w = new StreamWriter(clicksProdPath))
            {
                w.WriteLine("producto,clicks");
                foreach (var r in clicksPorProducto)
                    w.WriteLine($"{r.producto},{r.clicks}");
            }

            Console.WriteLine("JSON procesado: eventos_por_dia.csv y clicks_por_producto.csv");
        }
        else
        {
            Console.WriteLine("No se encontró logs_web.json — se omite el análisis de eventos.");
        }

        Console.WriteLine("Listo. Revisa la carpeta de salidas.");
    }
}
