# ============================================================
# contenedor_dask.py
# CONTENEDOR SECOP II - DASK
# ============================================================

import re
import time
import unicodedata
import threading
import concurrent.futures
from io import StringIO
from pathlib import Path

import requests
import pandas as pd
import dask.dataframe as dd
import matplotlib.pyplot as plt


class CONTENEDOR_SECOP:

    def __init__(self):

        self.chunk_default = 100_000
        self.total_filas_default = 8_410_000

        self.columnas_q1 = [
            "departamento",
            "valor_adjudicado",
            "fecha_publicacion",
            "estado_adjudicacion"
        ]

        self.diccionario_semantico = {
            "departamento_entidad": "departamento",
            "valor_total_adjudicacion": "valor_adjudicado",
            "fecha_de_publicacion_del": "fecha_publicacion",
            "adjudicado": "estado_adjudicacion",
            "entidad": "nombre_entidad",
            "nit_entidad": "nit_entidad",
            "ciudad_entidad": "ciudad_entidad",
            "modalidad_de_contratacion": "modalidad_contratacion",
            "estado_del_procedimiento": "estado_procedimiento",
            "tipo_de_contrato": "tipo_contrato",
            "subtipo_de_contrato": "subtipo_contrato",
            "departamento_proveedor": "departamento_proveedor",
            "ciudad_proveedor": "ciudad_proveedor",
            "nombre_del_proveedor": "nombre_proveedor",
            "nit_del_proveedor_adjudicado": "nit_proveedor",
            "fecha_adjudicacion": "fecha_adjudicacion",
            "precio_base": "precio_base",
            "codigo_principal_de_categoria": "codigo_categoria_unspsc",
            "urlproceso": "url_proceso"
        }

    # ========================================================
    # DESCARGA
    # ========================================================

    def descargar_chunk_secop(self, offset, base_url, data_dir, chunk=None, timeout=300):

        if chunk is None:
            chunk = self.chunk_default

        data_dir = Path(data_dir)
        data_dir.mkdir(parents=True, exist_ok=True)

        ruta = data_dir / f"secop_chunk_{offset:07d}.csv"

        if ruta.exists() and ruta.stat().st_size > 0:
            print(f"[SKIP] Ya existe: {ruta.name}")
            return {
                "offset": offset,
                "filas": None,
                "columnas": None,
                "mem_mb": None,
                "ruta": ruta,
                "estado": "existente"
            }

        url = f"{base_url}?$limit={chunk}&$offset={offset}"

        hilo_nombre = threading.current_thread().name
        hilo_num = hilo_nombre.split("_")[-1] if "_" in hilo_nombre else "0"

        print(f"[Hilo-{hilo_num}] Iniciando offset={offset:,}")

        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()

        df = pd.read_csv(StringIO(resp.text), low_memory=False)
        df.to_csv(ruta, index=False)

        mem_mb = df.memory_usage(deep=True).sum() / 1e6
        tam_mb = ruta.stat().st_size / (1024 * 1024)

        print(
            f"[Hilo-{hilo_num}] OK offset={offset:,}: "
            f"{len(df):,} filas | {mem_mb:.1f} MB RAM | "
            f"{tam_mb:.1f} MB disco -> {ruta.name}"
        )

        return {
            "offset": offset,
            "filas": len(df),
            "columnas": df.shape[1],
            "mem_mb": mem_mb,
            "ruta": ruta,
            "estado": "descargado"
        }

    def descargar_dataset_secop(
        self,
        base_url,
        data_dir,
        total_filas=None,
        chunk=None,
        max_workers=2,
        timeout=300
    ):

        if total_filas is None:
            total_filas = self.total_filas_default

        if chunk is None:
            chunk = self.chunk_default

        data_dir = Path(data_dir)
        data_dir.mkdir(parents=True, exist_ok=True)

        offsets = list(range(0, total_filas, chunk))

        print(f"Descargando {total_filas:,} filas en chunks de {chunk:,}")
        print(f"Carpeta destino: {data_dir}")
        print(f"Offsets generados: {len(offsets)}")
        print(f"Workers: {max_workers}")
        print()

        t0 = time.time()

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            resultados = list(
                executor.map(
                    lambda offset: self.descargar_chunk_secop(
                        offset=offset,
                        base_url=base_url,
                        data_dir=data_dir,
                        chunk=chunk,
                        timeout=timeout
                    ),
                    offsets
                )
            )

        t_total = time.time() - t0
        resumen = pd.DataFrame(resultados)
        csvs = sorted(data_dir.glob("secop_chunk_*.csv"))

        print()
        print("=" * 75)
        print("RESUMEN DE DESCARGA")
        print("=" * 75)

        print("\nEstados de ejecución:")
        print(resumen["estado"].value_counts(dropna=False))

        descargados = (resumen["estado"] == "descargado").sum()
        existentes = (resumen["estado"] == "existente").sum()

        print("\nMétricas:")
        print(f"Chunks descargados nueva ejecución : {descargados}")
        print(f"Chunks previamente existentes      : {existentes}")
        print(f"Total archivos detectados          : {len(csvs)}")

        if "filas" in resumen.columns:
            filas_descargadas = resumen["filas"].dropna().sum()
            print(f"Filas descargadas esta ejecución   : {filas_descargadas:,.0f}")

        print()
        print(f"Particiones esperadas              : {len(offsets)}")
        print(f"Particiones reales                 : {len(csvs)}")

        tam_total_gb = sum(f.stat().st_size for f in csvs) / (1024 ** 3)
        print(f"Tamaño total en disco              : {tam_total_gb:.2f} GB")

        print("\nPrimeros archivos detectados:")
        for f in csvs[:10]:
            tam_mb = f.stat().st_size / (1024 ** 2)
            print(f"{f.name} ({tam_mb:.1f} MB)")

        if len(csvs) > 10:
            print(f"... {len(csvs) - 10} archivos adicionales")

        print("\nTiempo total ejecución:")
        print(f"{t_total:.1f} segundos")

        print(f"\n-> Dask leerá {len(csvs)} archivos como particiones.")

        return resumen

    # ========================================================
    # CARGA
    # ========================================================

    def cargar_dask_dataframe(self, data_dir, mostrar_resumen=True, mostrar_head=True):

        data_dir = Path(data_dir)
        patron = str(data_dir / "secop_chunk_*.csv")

        ddf = dd.read_csv(
            patron,
            dtype="object",
            assume_missing=True,
            blocksize=None
        )

        print("Dask DataFrame cargado correctamente.")

        if mostrar_resumen:
            print("\nInformación general")
            print("-" * 60)
            print("Número de particiones:", ddf.npartitions)

            print("\nFilas por partición estimadas:")
            print(ddf.map_partitions(len).compute())

            print("\nTipos de datos:")
            print(ddf.dtypes)

            print("\nColumnas detectadas:")
            print(ddf.columns.tolist())

            n_registros = ddf.shape[0].compute()

            print("\nTotal registros cargados:")
            print(f"{n_registros:,}")

        if mostrar_head:
            print("\nPrimeras observaciones:")
            print(ddf.head(10))

        return ddf

    # ========================================================
    # NORMALIZACIÓN COLUMNAS
    # ========================================================

    def normalizar_nombre_columna(self, col):

        col = str(col).strip().lower()

        col = (
            unicodedata.normalize("NFKD", col)
            .encode("ascii", "ignore")
            .decode("utf-8")
        )

        col = re.sub(r"\s+", "_", col)
        col = re.sub(r"[^a-z0-9_]", "_", col)
        col = re.sub(r"_+", "_", col)
        col = col.strip("_")

        return col

    def normalizar_columnas(self, ddf):

        columnas_limpias = [
            self.normalizar_nombre_columna(col)
            for col in ddf.columns
        ]

        columnas_finales = []
        contador = {}

        for col in columnas_limpias:
            if col in contador:
                contador[col] += 1
                columnas_finales.append(f"{col}_{contador[col]}")
            else:
                contador[col] = 0
                columnas_finales.append(col)

        ddf.columns = columnas_finales

        print("Columnas normalizadas correctamente.")
        print("Columnas únicas:", len(set(ddf.columns)) == len(ddf.columns))

        return ddf

    def renombrar_columnas_semanticas(self, ddf):

        diccionario_existente = {
            origen: destino
            for origen, destino in self.diccionario_semantico.items()
            if origen in ddf.columns
        }

        ddf = ddf.rename(columns=diccionario_existente)

        print("Columnas renombradas semánticamente:")
        for origen, destino in diccionario_existente.items():
            print(f"{origen} -> {destino}")

        return ddf

    # ========================================================
    # LIMPIEZA TEXTO
    # ========================================================

    def quitar_tildes_texto(self, texto):

        if texto is None:
            return texto

        texto = str(texto)

        return (
            unicodedata.normalize("NFKD", texto)
            .encode("ascii", "ignore")
            .decode("utf-8")
        )

    def normalizar_texto(self, serie, valor_nulo="NO DEFINIDO"):

        return (
            serie
            .fillna(valor_nulo)
            .str.strip()
            .str.upper()
            .map_partitions(
                lambda s: s.apply(self.quitar_tildes_texto),
                meta=("texto", "object")
            )
            .str.replace(r"\s+", " ", regex=True)
        )

    def normalizar_departamento_q1(self, ddf_q1):

        mapa_departamentos = {
            "DISTRITO CAPITAL DE BOGOTA": "BOGOTA D.C.",
            "BOGOTA": "BOGOTA D.C.",
            "BOGOTA D C": "BOGOTA D.C.",
            "BOGOTA D.C": "BOGOTA D.C.",
            "BOGOTA D.C.": "BOGOTA D.C.",
            "NO DEFINIDO": "NO DEFINIDO"
        }

        ddf_q1["departamento"] = ddf_q1["departamento"].replace(mapa_departamentos)

        print("Departamentos normalizados semánticamente.")

        return ddf_q1

    # ========================================================
    # PREGUNTA 1 — PREPARACIÓN
    # ========================================================

    def seleccionar_variables_pregunta_1(self, ddf):

        faltantes = [
            col for col in self.columnas_q1
            if col not in ddf.columns
        ]

        if faltantes:
            raise ValueError(
                f"Faltan columnas requeridas para Pregunta 1: {faltantes}"
            )

        ddf_q1_raw = ddf[self.columnas_q1].copy()

        print("DataFrame auxiliar Pregunta 1 creado.")
        print("Columnas:", ddf_q1_raw.columns.tolist())

        return ddf_q1_raw

    def limpiar_pregunta_1(self, ddf_q1_raw):

        ddf_q1 = ddf_q1_raw.copy()

        ddf_q1["departamento"] = self.normalizar_texto(
            ddf_q1["departamento"]
        )

        ddf_q1["estado_adjudicacion"] = self.normalizar_texto(
            ddf_q1["estado_adjudicacion"]
        )

        ddf_q1["valor_adjudicado"] = dd.to_numeric(
            ddf_q1["valor_adjudicado"],
            errors="coerce"
        )

        ddf_q1["fecha_publicacion"] = dd.to_datetime(
            ddf_q1["fecha_publicacion"],
            errors="coerce"
        )

        ddf_q1["anio_publicacion"] = (
            ddf_q1["fecha_publicacion"]
            .dt.year
            .astype("Int64")
        )

        ddf_q1 = self.normalizar_departamento_q1(ddf_q1)

        print("Limpieza y transformación Pregunta 1 finalizada.")

        return ddf_q1

    def diagnosticar_pregunta_1(self, ddf_q1):

        print("\n" + "=" * 70)
        print("DIAGNÓSTICO DE CALIDAD — PREGUNTA 1")
        print("=" * 70)

        diagnostico = {
            "total_registros": ddf_q1.shape[0],
            "nulos_valor_adjudicado": ddf_q1["valor_adjudicado"].isna().sum(),
            "valores_cero": (ddf_q1["valor_adjudicado"] == 0).sum(),
            "valores_negativos": (ddf_q1["valor_adjudicado"] < 0).sum(),
            "fechas_invalidas": ddf_q1["fecha_publicacion"].isna().sum(),
            "anios_invalidos": ddf_q1["anio_publicacion"].isna().sum(),
            "departamento_no_definido": (
                ddf_q1["departamento"] == "NO DEFINIDO"
            ).sum(),
            "procesos_adjudicados_si": (
                ddf_q1["estado_adjudicacion"] == "SI"
            ).sum(),
            "procesos_adjudicados_no": (
                ddf_q1["estado_adjudicacion"] == "NO"
            ).sum()
        }

        diagnostico = dd.compute(diagnostico)[0]

        diagnostico_df = pd.DataFrame(
            list(diagnostico.items()),
            columns=["Indicador", "Valor"]
        )

        return diagnostico_df

    def crear_universo_analitico_q1(self, ddf_q1):

        ddf_q1_analitico = ddf_q1[
            (ddf_q1["estado_adjudicacion"] == "SI") &
            (ddf_q1["valor_adjudicado"] > 0) &
            (ddf_q1["departamento"] != "NO DEFINIDO") &
            (ddf_q1["anio_publicacion"].notnull())
        ]

        print("Universo analítico Pregunta 1 creado.")

        return ddf_q1_analitico

    # ========================================================
    # PREGUNTA 1 — ANÁLISIS
    # ========================================================

    def ejecutar_analisis_pregunta_1(
        self,
        ddf_q1_analitico,
        escala=1e12,
        nombre_escala="Billones COP",
        umbral=60
    ):

        gasto_departamento = (
            ddf_q1_analitico
            .groupby("departamento")["valor_adjudicado"]
            .sum()
            .compute()
            .sort_values(ascending=False)
        )

        total_nacional = gasto_departamento.sum()

        tabla_top10 = gasto_departamento.head(10).reset_index()

        tabla_top10.columns = [
            "departamento",
            "valor_total_adjudicado"
        ]

        tabla_top10.insert(
            0,
            "ranking",
            range(1, len(tabla_top10) + 1)
        )

        tabla_top10["valor_escala"] = (
            tabla_top10["valor_total_adjudicado"] / escala
        )

        tabla_top10["valor_formateado"] = (
            tabla_top10["valor_escala"]
            .map(lambda x: f"{x:,.2f} {nombre_escala}")
        )

        tabla_top10["porcentaje_total"] = (
            tabla_top10["valor_total_adjudicado"] /
            total_nacional * 100
        )

        tabla_top10["porcentaje_acumulado"] = (
            tabla_top10["porcentaje_total"].cumsum()
        )

        promedio_participacion = tabla_top10["porcentaje_total"].mean()

        tabla_top10["dif_vs_promedio"] = (
            tabla_top10["porcentaje_total"] -
            promedio_participacion
        )

        tabla_top10["indice_relativo"] = (
            tabla_top10["porcentaje_total"] /
            promedio_participacion
        )

        columnas_redondeo = [
            "valor_escala",
            "porcentaje_total",
            "porcentaje_acumulado",
            "dif_vs_promedio",
            "indice_relativo"
        ]

        tabla_top10[columnas_redondeo] = (
            tabla_top10[columnas_redondeo]
            .round(4)
        )

        top5_porcentaje = (
            tabla_top10
            .head(5)["porcentaje_total"]
            .sum()
        )

        resultado_top5 = {
            "top5_porcentaje": round(top5_porcentaje, 4),
            "umbral": umbral,
            "supera_umbral": top5_porcentaje > umbral
        }

        print("\n" + "=" * 70)
        print("ANÁLISIS PREGUNTA 1 — CONCENTRACIÓN TERRITORIAL")
        print("=" * 70)
        print(f"Total nacional adjudicado: {total_nacional / escala:,.2f} {nombre_escala}")
        print(f"Concentración Top 5: {top5_porcentaje:.2f}%")

        if top5_porcentaje > umbral:
            print(f"Conclusión: los 5 primeros departamentos superan el {umbral}% del gasto.")
        else:
            print(f"Conclusión: los 5 primeros departamentos no superan el {umbral}% del gasto.")

        return tabla_top10, total_nacional, resultado_top5

    def graficar_pareto_q1(
        self,
        tabla_top10,
        umbral=60
    ):

        fig, ax1 = plt.subplots(figsize=(12, 7))

        ax1.bar(
            tabla_top10["departamento"],
            tabla_top10["porcentaje_total"]
        )

        ax1.set_ylabel("% participación")
        ax1.set_xlabel("Departamento")
        ax1.set_title("Pareto de concentración territorial del gasto público")
        ax1.tick_params(axis="x", rotation=45)

        ax2 = ax1.twinx()

        ax2.plot(
            tabla_top10["departamento"],
            tabla_top10["porcentaje_acumulado"],
            marker="o"
        )

        ax2.axhline(
            umbral,
            linestyle="--"
        )

        ax2.set_ylabel("% acumulado")

        plt.tight_layout()
        plt.show()

    def graficar_top10_valor_q1(
        self,
        tabla_top10,
        columna_valor="valor_escala",
        nombre_escala="Billones COP"
    ):

        tabla_grafica = tabla_top10.sort_values(
            columna_valor,
            ascending=True
        )

        plt.figure(figsize=(11, 6))

        plt.barh(
            tabla_grafica["departamento"],
            tabla_grafica[columna_valor]
        )

        plt.title("Top 10 departamentos por valor adjudicado")
        plt.xlabel(nombre_escala)
        plt.ylabel("Departamento")

        plt.tight_layout()
        plt.show()