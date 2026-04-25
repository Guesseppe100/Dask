# ============================================================
# pipeline_secop.py
# Primera versión: carga SECOP con Dask
# ============================================================

from pathlib import Path
import dask.dataframe as dd


# ============================================================
# CONFIGURACIÓN
# ============================================================

DATA_DIR = Path("app/data/secop_chunks")


# ============================================================
# FUNCIÓN 1: CARGAR DATAFRAME DASK
# ============================================================

def cargar_dask_dataframe(
    data_dir: Path,
    patron_archivos: str = "secop_chunk_*.csv",
    mostrar_resumen: bool = True,
    mostrar_head: bool = True
):
    """
    Carga los chunks CSV de SECOP como un Dask DataFrame.

    No usa pandas directamente.
    Cada archivo CSV será tratado como una partición porque blocksize=None.
    """

    data_dir = Path(data_dir)
    patron = str(data_dir / patron_archivos)

    if not data_dir.exists():
        raise FileNotFoundError(f"No existe la carpeta: {data_dir}")

    archivos = sorted(data_dir.glob(patron_archivos))

    if not archivos:
        raise FileNotFoundError(
            f"No se encontraron archivos con el patrón: {patron_archivos}"
        )

    print("=" * 80)
    print("CARGA DE DATAFRAME SECOP CON DASK")
    print("=" * 80)
    print(f"Carpeta origen: {data_dir}")
    print(f"Patrón archivos: {patron_archivos}")
    print(f"Archivos detectados: {len(archivos)}")
    print(f"Primer archivo: {archivos[0].name}")
    print(f"Último archivo: {archivos[-1].name}")

    ddf = dd.read_csv(
        patron,
        dtype="object",
        assume_missing=True,
        blocksize=None
    )

    print("\nDask DataFrame cargado correctamente.")

    if mostrar_resumen:
        print("\nInformación general")
        print("-" * 80)

        print("Número de particiones:")
        print(ddf.npartitions)

        print("\nFilas por partición:")
        filas_particion = ddf.map_partitions(len).compute()
        print(filas_particion)

        print("\nTipos de datos:")
        print(ddf.dtypes)

        print("\nColumnas detectadas:")
        for col in ddf.columns:
            print(f" - {col}")

        total_registros = ddf.shape[0].compute()

        print("\nTotal registros cargados:")
        print(f"{total_registros:,}")

    if mostrar_head:
        print("\nPrimeras observaciones:")
        print(ddf.head(10))

    return ddf


# ============================================================
# FUNCIÓN 2: EJECUTAR PRUEBA INICIAL
# ============================================================

def ejecutar_carga_inicial_dask(
    data_dir: Path = DATA_DIR,
    mostrar_resumen: bool = True,
    mostrar_head: bool = True
):
    """
    Ejecuta la primera prueba del pipeline:
    cargar todos los chunks con Dask.
    """

    ddf = cargar_dask_dataframe(
        data_dir=data_dir,
        mostrar_resumen=mostrar_resumen,
        mostrar_head=mostrar_head
    )

    return ddf


# ============================================================
# EJECUCIÓN DIRECTA
# ============================================================

if __name__ == "__main__":

    ddf = ejecutar_carga_inicial_dask(
        data_dir=DATA_DIR,
        mostrar_resumen=True,
        mostrar_head=True
    )