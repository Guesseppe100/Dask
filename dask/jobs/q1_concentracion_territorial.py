from pathlib import Path
import re
import unicodedata

import dask.dataframe as dd
import pandas as pd
import matplotlib.pyplot as plt
from dask.distributed import Client


class Q1ConcentracionTerritorial:
    """
    Pipeline Dask para responder la Pregunta 1:

    ¿Los 5 primeros departamentos concentran más del 60% del gasto adjudicado?

    El pipeline:
    1. Lee CSV con Dask.
    2. Normaliza nombres de columnas.
    3. Resuelve columnas semánticas.
    4. Crea un universo analítico con:
       - departamento
       - valor_adjudicado
       - fecha_publicacion
       - estado_adjudicacion
       - anio_publicacion
    5. Limpia texto:
       - minúscula
       - sin tildes
       - sin caracteres especiales
       - sin espacios, usando guion bajo
    6. Convierte valor adjudicado a numérico.
    7. Extrae año de publicación.
    8. Calcula top 10 departamentos.
    9. Calcula porcentaje nacional y porcentaje acumulado.
    10. Genera CSV y gráficas.
    """

    def __init__(
        self,
        data_glob: str,
        output_dir: str | Path,
        blocksize: str = "64MB",
        export_parquet: bool = True,
    ):
        self.data_glob = data_glob
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True, parents=True)
        self.blocksize = blocksize
        self.export_parquet = export_parquet

        self.columnas_q1 = [
            "departamento",
            "valor_adjudicado",
            "fecha_publicacion",
            "estado_adjudicacion",
            "anio_publicacion",
        ]

        self.diccionario_semantico_q1 = {
            "departamento": [
                "departamento_entidad",
                "departamento",
                "departamento_proveedor",
            ],
            "valor_adjudicado": [
                "valor_total_adjudicacion",
                "valor_total_adquisicion",
                "valor_total_del_contrato",
                "valor_adjudicado",
                "valor_del_contrato",
                "precio_base",
            ],
            "fecha_publicacion": [
                "fecha_de_publicacion_del",
                "fecha_de_publicacion_del_proceso",
                "fecha_de_publicacion",
                "fecha_publicacion",
                "fecha_de_publicacion_fase",
                "fecha_de_ultima_publicaci",
            ],
            "estado_adjudicacion": [
                "adjudicado",
                "estado_adjudicacion",
                "estado_del_procedimiento",
                "estado_resumen",
                "fase",
            ],
        }

    # ============================================================
    # 1. Normalización general
    # ============================================================

    @staticmethod
    def quitar_tildes(texto: str) -> str:
        texto = str(texto)
        texto = unicodedata.normalize("NFKD", texto)
        texto = texto.encode("ascii", "ignore").decode("utf-8")
        return texto

    @classmethod
    def normalizar_nombre_columna(cls, col: str) -> str:
        """
        Normaliza nombres de columnas:
        - minúsculas
        - sin tildes
        - sin caracteres especiales
        - espacios convertidos a _
        """
        col = cls.quitar_tildes(col)
        col = col.strip().lower()
        col = re.sub(r"[^a-z0-9]+", "_", col)
        col = re.sub(r"_+", "_", col)
        col = col.strip("_")
        return col

    @classmethod
    def normalizar_texto_valor(cls, serie):
        """
        Normaliza valores de texto:
        - minúsculas
        - sin tildes
        - sin caracteres especiales
        - sin espacios, usando _
        """
        return (
            serie.fillna("no_definido")
            .astype(str)
            .str.strip()
            .str.lower()
            .map(cls.quitar_tildes, meta=("texto", "object"))
            .str.replace(r"[^a-z0-9]+", "_", regex=True)
            .str.replace(r"_+", "_", regex=True)
            .str.strip("_")
        )

    @staticmethod
    def limpiar_valor_monetario(serie):
        """
        Convierte valores monetarios a numérico.

        Soporta textos como:
        - $ 1,234,567
        - COP 1234567
        - 1.234.567
        - 1.234.567,89

        Nota:
        Para este caso se prioriza limpiar separadores de miles.
        """
        serie_limpia = (
            serie.fillna("")
            .astype(str)
            .str.upper()
            .str.replace("COP", "", regex=False)
            .str.replace("$", "", regex=False)
            .str.replace(" ", "", regex=False)
            .str.replace(r"[^0-9,\.]", "", regex=True)
        )

        # Formato colombiano frecuente: 1.234.567,89
        serie_limpia = serie_limpia.str.replace(".", "", regex=False)
        serie_limpia = serie_limpia.str.replace(",", ".", regex=False)

        return dd.to_numeric(serie_limpia, errors="coerce")

    # ============================================================
    # 2. Carga de datos
    # ============================================================

    def cargar_datos(self):
        print("Cargando datos desde:", self.data_glob)

        ddf = dd.read_csv(
            self.data_glob,
            dtype=str,
            assume_missing=True,
            blocksize=self.blocksize,
        )

        print("Normalizando nombres de columnas...")

        ddf.columns = [
            self.normalizar_nombre_columna(col)
            for col in ddf.columns
        ]

        print("Columnas detectadas:", len(ddf.columns))

        return ddf

    # ============================================================
    # 3. Resolución semántica
    # ============================================================

    @staticmethod
    def resolver_columna(ddf, candidatos):
        for col in candidatos:
            if col in ddf.columns:
                return col
        return None

    def resolver_columnas_q1(self, ddf):
        columnas_resueltas = {}

        for nombre_final, candidatos in self.diccionario_semantico_q1.items():
            columnas_resueltas[nombre_final] = self.resolver_columna(
                ddf,
                candidatos,
            )

        print("\nColumnas semánticas resueltas:")
        for nombre_final, columna_origen in columnas_resueltas.items():
            print(f"{nombre_final:<25} <- {columna_origen}")

        faltantes = [
            nombre_final
            for nombre_final, columna_origen in columnas_resueltas.items()
            if columna_origen is None
        ]

        if faltantes:
            raise ValueError(
                f"No se encontraron columnas requeridas para Q1: {faltantes}. "
                f"Columnas disponibles: {list(ddf.columns)}"
            )

        return columnas_resueltas

    # ============================================================
    # 4. Universo analítico Q1
    # ============================================================

    def crear_universo_analitico_q1(self, ddf):
        columnas_resueltas = self.resolver_columnas_q1(ddf)

        columnas_origen = list(columnas_resueltas.values())

        ddf_q1 = ddf[columnas_origen].copy()

        rename_dict = {
            columna_origen: nombre_final
            for nombre_final, columna_origen in columnas_resueltas.items()
        }

        ddf_q1 = ddf_q1.rename(columns=rename_dict)

        print("\nLimpiando departamento...")
        ddf_q1["departamento"] = self.normalizar_texto_valor(
            ddf_q1["departamento"]
        )

        print("Limpiando estado_adjudicacion...")
        ddf_q1["estado_adjudicacion"] = self.normalizar_texto_valor(
            ddf_q1["estado_adjudicacion"]
        )

        print("Convirtiendo valor_adjudicado a numérico...")
        ddf_q1["valor_adjudicado"] = self.limpiar_valor_monetario(
            ddf_q1["valor_adjudicado"]
        )

        print("Convirtiendo fecha_publicacion a fecha...")
        ddf_q1["fecha_publicacion"] = dd.to_datetime(
            ddf_q1["fecha_publicacion"],
            errors="coerce",
            dayfirst=False,
        )

        print("Extrayendo anio_publicacion...")
        ddf_q1["anio_publicacion"] = ddf_q1["fecha_publicacion"].dt.year

        ddf_q1 = ddf_q1[self.columnas_q1]

        return ddf_q1

    # ============================================================
    # 5. Diagnóstico
    # ============================================================

    def diagnosticar_q1(self, ddf_q1):
        rows = int(ddf_q1.shape[0].compute())
        partitions = ddf_q1.npartitions

        nulls = ddf_q1.isna().sum().compute()

        nulls_df = pd.DataFrame({
            "columna": list(nulls.index.astype(str)),
            "nulos": list(nulls.values),
        })

        nulls_df["nulos"] = pd.to_numeric(
            nulls_df["nulos"],
            errors="coerce",
        ).fillna(0).astype(int)

        nulls_df["porcentaje_nulos"] = (
            nulls_df["nulos"] / rows * 100
        ).round(2)

        nulls_df = nulls_df.sort_values(
            by="nulos",
            ascending=False,
            ignore_index=True,
        )

        path = self.output_dir / "q1_diagnostico_nulos.csv"
        nulls_df.to_csv(path, index=False)

        print("\n===================================================")
        print("DIAGNÓSTICO UNIVERSO ANALÍTICO Q1")
        print("===================================================")
        print("Filas:", rows)
        print("Particiones:", partitions)
        print("Columnas:", list(ddf_q1.columns))
        print("\nNulos:")
        print(nulls_df.to_string(index=False))
        print("\nDiagnóstico exportado:", path)

        return nulls_df

    # ============================================================
    # 6. Análisis Pregunta 1
    # ============================================================

    def analizar_concentracion_territorial(self, ddf_q1, top_n: int = 10):
        print("\nCalculando total nacional adjudicado...")

        total_nacional = float(
            ddf_q1["valor_adjudicado"]
            .sum()
            .compute()
        )

        if total_nacional == 0:
            raise ValueError(
                "El total nacional adjudicado es 0. "
                "Revisa la conversión numérica de valor_adjudicado."
            )

        print("Calculando top departamentos...")

        tabla = (
            ddf_q1
            .dropna(subset=["valor_adjudicado"])
            .groupby("departamento")["valor_adjudicado"]
            .sum()
            .nlargest(top_n)
            .compute()
            .reset_index()
        )

        tabla.columns = [
            "departamento",
            "valor_total_adjudicado",
        ]

        tabla["ranking"] = range(1, len(tabla) + 1)

        tabla["porcentaje_total"] = (
            tabla["valor_total_adjudicado"] / total_nacional * 100
        ).round(4)

        tabla["porcentaje_acumulado"] = (
            tabla["porcentaje_total"].cumsum()
        ).round(4)

        promedio_top = tabla["porcentaje_total"].mean()

        tabla["dif_vs_promedio"] = (
            tabla["porcentaje_total"] - promedio_top
        ).round(4)

        tabla["indice_relativo"] = (
            tabla["porcentaje_total"] / promedio_top
        ).round(4)

        # Escala para presentación.
        # 1 billón COP = 1e12 COP
        tabla["valor_billones_cop"] = (
            tabla["valor_total_adjudicado"] / 1_000_000_000_000
        ).round(2)

        tabla["valor_formateado"] = tabla["valor_billones_cop"].map(
            lambda x: f"{x:,.2f} billones COP"
        )

        tabla = tabla[
            [
                "ranking",
                "departamento",
                "valor_total_adjudicado",
                "valor_billones_cop",
                "valor_formateado",
                "porcentaje_total",
                "porcentaje_acumulado",
                "dif_vs_promedio",
                "indice_relativo",
            ]
        ]

        top5_concentracion = float(
            tabla.head(5)["porcentaje_total"].sum()
        )

        supera_60 = top5_concentracion > 60

        conclusion = (
            "los_5_primeros_departamentos_superan_el_60_del_gasto"
            if supera_60
            else "los_5_primeros_departamentos_no_superan_el_60_del_gasto"
        )

        interpretacion = (
            "existe_alta_concentracion_territorial_del_gasto_publico"
            if supera_60
            else "el_gasto_presenta_menor_concentracion_territorial"
        )

        resumen = {
            "total_nacional": total_nacional,
            "total_nacional_billones_cop": total_nacional / 1_000_000_000_000,
            "concentracion_top5": top5_concentracion,
            "supera_60": supera_60,
            "conclusion": conclusion,
            "interpretacion": interpretacion,
        }

        tabla_path = self.output_dir / "q1_top10_concentracion_territorial.csv"
        tabla.to_csv(tabla_path, index=False)

        resumen_path = self.output_dir / "q1_resumen_concentracion_territorial.csv"
        pd.DataFrame([resumen]).to_csv(resumen_path, index=False)

        print("\n===================================================")
        print("ANÁLISIS PREGUNTA 1 — CONCENTRACIÓN TERRITORIAL")
        print("===================================================")
        print(
            "Total nacional adjudicado:",
            f"{resumen['total_nacional_billones_cop']:,.2f}",
            "billones COP",
        )
        print("Concentración Top 5:", f"{top5_concentracion:.2f}%")

        if supera_60:
            print(
                "Conclusión: los 5 primeros departamentos concentran más "
                "del 60% del gasto adjudicado."
            )
            print(
                "Interpretación: existe una alta concentración territorial "
                "del gasto, lo que puede indicar centralización o mayor "
                "actividad contractual en pocos territorios."
            )
        else:
            print(
                "Conclusión: los 5 primeros departamentos no concentran más "
                "del 60% del gasto adjudicado."
            )
            print(
                "Interpretación: el gasto presenta una distribución "
                "territorial menos concentrada."
            )

        print("\nTabla exportada:", tabla_path)
        print("Resumen exportado:", resumen_path)
        print("\nTop 10:")
        print(tabla.to_string(index=False))

        return tabla, resumen

    # ============================================================
    # 7. Análisis por año
    # ============================================================

    def analizar_por_anio(self, ddf_q1):
        print("\nCalculando valor adjudicado por año...")

        tabla_anio = (
            ddf_q1
            .dropna(subset=["valor_adjudicado", "anio_publicacion"])
            .groupby("anio_publicacion")["valor_adjudicado"]
            .sum()
            .compute()
            .reset_index()
        )

        tabla_anio.columns = [
            "anio_publicacion",
            "valor_total_adjudicado",
        ]

        tabla_anio = tabla_anio.sort_values("anio_publicacion")

        tabla_anio["valor_billones_cop"] = (
            tabla_anio["valor_total_adjudicado"] / 1_000_000_000_000
        ).round(2)

        path = self.output_dir / "q1_valor_adjudicado_por_anio.csv"
        tabla_anio.to_csv(path, index=False)

        print("Tabla por año exportada:", path)
        print(tabla_anio.to_string(index=False))

        return tabla_anio

    # ============================================================
    # 8. Gráficas
    # ============================================================

    def graficar_top10_valor(self, tabla):
        df_plot = tabla.sort_values(
            "valor_total_adjudicado",
            ascending=True,
        )

        plt.figure(figsize=(11, 6))

        plt.barh(
            df_plot["departamento"],
            df_plot["valor_billones_cop"],
        )

        plt.title("Top 10 departamentos por valor adjudicado")
        plt.xlabel("Billones COP")
        plt.ylabel("Departamento")
        plt.tight_layout()

        path = self.output_dir / "q1_top10_valor_adjudicado.png"
        plt.savefig(path, dpi=150)
        plt.close()

        print("Gráfica exportada:", path)

        return path

    def graficar_pareto(self, tabla):
        df_plot = tabla.copy()

        fig, ax1 = plt.subplots(figsize=(12, 6))

        ax1.bar(
            df_plot["departamento"],
            df_plot["porcentaje_total"],
        )

        ax1.set_xlabel("Departamento")
        ax1.set_ylabel("% participación")
        ax1.tick_params(axis="x", rotation=45)

        ax2 = ax1.twinx()

        ax2.plot(
            df_plot["departamento"],
            df_plot["porcentaje_acumulado"],
            marker="o",
        )

        ax2.set_ylabel("% acumulado")
        ax2.axhline(60, linestyle="--")
        ax2.axhline(80, linestyle="--")

        plt.title("Pareto de concentración territorial del gasto público")
        plt.tight_layout()

        path = self.output_dir / "q1_pareto_concentracion_territorial.png"
        plt.savefig(path, dpi=150)
        plt.close()

        print("Gráfica exportada:", path)

        return path

    def graficar_valor_por_anio(self, tabla_anio):
        if tabla_anio.empty:
            print("No hay datos para graficar por año.")
            return None

        plt.figure(figsize=(10, 6))

        plt.bar(
            tabla_anio["anio_publicacion"].astype(str),
            tabla_anio["valor_billones_cop"],
        )

        plt.title("Valor adjudicado total por año de publicación")
        plt.xlabel("Año de publicación")
        plt.ylabel("Billones COP")
        plt.tight_layout()

        path = self.output_dir / "q1_valor_adjudicado_por_anio.png"
        plt.savefig(path, dpi=150)
        plt.close()

        print("Gráfica exportada:", path)

        return path

    # ============================================================
    # 9. Exportación del universo analítico
    # ============================================================

    def exportar_universo_q1(self, ddf_q1):
        if not self.export_parquet:
            print("Exportación Parquet desactivada.")
            return None

        print("\nExportando universo analítico Q1 a Parquet...")

        parquet_path = self.output_dir / "q1_universo_analitico_parquet"

        ddf_q1.to_parquet(
            str(parquet_path),
            engine="pyarrow",
            write_index=False,
            overwrite=True,
        )

        print("Parquet exportado:", parquet_path)

        return parquet_path

    # ============================================================
    # 10. Ejecución completa
    # ============================================================

    def ejecutar_pipeline_q1(self):
        print("\n===================================================")
        print("INICIO PIPELINE Q1")
        print("===================================================")

        print("\n1. Cargando datos...")
        ddf = self.cargar_datos()

        print("\n2. Creando universo analítico Q1...")
        ddf_q1 = self.crear_universo_analitico_q1(ddf)

        print("\nUniverso analítico Q1 creado.")

        # Nota:
        # Se omite persist() en esta versión para evitar cancelaciones
        # durante pruebas y ejecución inicial.
        # Si el cluster está estable y hay memoria suficiente, se puede activar:
        # ddf_q1 = ddf_q1.persist()

        print("\n3. Diagnóstico de calidad...")
        self.diagnosticar_q1(ddf_q1)

        print("\n4. Exportación del universo analítico...")
        self.exportar_universo_q1(ddf_q1)

        print("\n5. Análisis de concentración territorial...")
        tabla_top10, resumen = self.analizar_concentracion_territorial(
            ddf_q1,
            top_n=10,
        )

        print("\n6. Análisis por año...")
        tabla_anio = self.analizar_por_anio(ddf_q1)

        print("\n7. Generando gráficas...")
        self.graficar_top10_valor(tabla_top10)
        self.graficar_pareto(tabla_top10)
        self.graficar_valor_por_anio(tabla_anio)

        print("\n===================================================")
        print("PIPELINE Q1 FINALIZADO")
        print("===================================================")

        return ddf_q1, tabla_top10, resumen


# ============================================================
# EJECUCIÓN DESDE TERMINAL
# ============================================================

if __name__ == "__main__":
    """
    Ejecutar desde Windows PowerShell con:

    docker compose -f docker-compose.dask.yml exec jupyter python /home/jovyan/dask_jobs/q1_concentracion_territorial.py

    Para escalar al dataset completo, cambiar DATA_GLOB a:
    /home/jovyan/data/secop_chunks/*.csv
    """

    DATA_GLOB = "/home/jovyan/sample_data_demo/*.csv"
    OUTPUT_DIR = "/home/jovyan/output"

    print("\nConectando al cluster Dask...")
    client = Client("tcp://dask-scheduler:8786")

    print("Dashboard:", client.dashboard_link)
    print("Workers:", client.scheduler_info()["workers"].keys())

    pipeline_q1 = Q1ConcentracionTerritorial(
        data_glob=DATA_GLOB,
        output_dir=OUTPUT_DIR,
        blocksize="64MB",
        export_parquet=True,
    )

    ddf_q1, tabla_top10, resumen_q1 = pipeline_q1.ejecutar_pipeline_q1()

    print("\nTabla Top 10 final:")
    print(tabla_top10.to_string(index=False))

    print("\nResumen final:")
    for key, value in resumen_q1.items():
        print(f"{key}: {value}")