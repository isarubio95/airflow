from airflow.sdk import dag, task
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from validadores_patentes import (
    validate_single_patent,
    RE_INTERNACIONAL_ID,
)
import os, json, gzip, shutil, csv, io

# ------------------------------------------------------------
# Rutas y configuración
# ------------------------------------------------------------
DAG_FOLDER = os.path.dirname(__file__)
PATENTES_FILE_PATH = os.path.join(DAG_FOLDER, "patente_invenes.json")

BATCH_SIZE = int(os.getenv("PATENTS_BATCH_SIZE", "500"))          # nº patentes por lote
TMP_DIR = os.getenv("AIRFLOW_TMP_DIR", "/opt/airflow/tmp/patents")
KEEP_TMP = os.getenv("KEEP_TMP", "0") == "1"                      # KEEP_TMP=1 para inspeccionar archivos

# MinIO / S3 (vía conexión Airflow)
MINIO_CONN_ID = os.getenv("MINIO_CONN_ID", "minio")
MINIO_BUCKET  = os.getenv("MINIO_BUCKET", "patentes")             # ej: "patentes"
MINIO_PREFIX  = os.getenv("MINIO_PREFIX", "validation")           # ej: "validation"

default_args = {"retries": 1, "retry_delay": timedelta(seconds=15)}

# ------------------------------------------------------------
# Utilidades
# ------------------------------------------------------------

def _s3_ensure_bucket_and_upload_csv(*, rows: list[dict], header: list[str], key: str):
    """
    Sube un CSV en memoria a MinIO/S3 usando S3Hook y crea el bucket si no existe.
    - rows: lista de dicts con mismas claves que header
    - header: orden de las columnas
    - key: ruta dentro del bucket (sin s3://)
    """
    hook = S3Hook(aws_conn_id=MINIO_CONN_ID)

    # asegurar bucket
    if not hook.check_for_bucket(bucket_name=MINIO_BUCKET):
        hook.create_bucket(bucket_name=MINIO_BUCKET)

    # construir CSV en memoria (utf-8)
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=header, extrasaction="ignore")
    writer.writeheader()
    for r in rows:
        writer.writerow(r)

    hook.load_string(
        string_data=buf.getvalue(),
        key=key,
        bucket_name=MINIO_BUCKET,
        replace=True,
        encoding="utf-8",
    )

def _is_international(pid: str) -> bool:
    return isinstance(pid, str) and RE_INTERNACIONAL_ID.match(pid) is not None

def _s3_concat_and_upload_csv(keys: list[str], dest_key: str, header: list[str]):
    """
    Lee varios CSV (mismo esquema) desde S3 y sube uno único ordenado por 'id'.
    Si no hay filas, sube igualmente un CSV con solo cabecera.
    """
    hook = S3Hook(aws_conn_id=MINIO_CONN_ID)

    # asegurar bucket
    if not hook.check_for_bucket(bucket_name=MINIO_BUCKET):
        hook.create_bucket(bucket_name=MINIO_BUCKET)

    all_rows: list[dict] = []
    for k in keys:
        if not k:
            continue
        try:
            content = hook.read_key(key=k, bucket_name=MINIO_BUCKET)
            if not content:
                continue
            rdr = csv.DictReader(io.StringIO(content))
            for row in rdr:
                # normalizamos columnas esperadas
                all_rows.append({
                    "id": row.get("id", ""),
                    "level": row.get("level", ""),
                    "message": row.get("message", ""),
                })
        except Exception:
            # si un lote no generó fichero o hubo carrera, seguimos
            continue

    # ordenar por id
    all_rows.sort(key=lambda r: r.get("id", ""))

    # subir
    _s3_ensure_bucket_and_upload_csv(rows=all_rows, header=header, key=dest_key)

# ------------------------------------------------------------
# DAG
# ------------------------------------------------------------

@dag(
    dag_id="patent_processing_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["patents", "validation", "check-only"],
)
def patent_processing_dag():
    """Valida por lotes desde disco, sube reportes CSV (errores y warnings) a MinIO y limpia temporales."""

    @task
    def load_and_slice_to_files() -> list[str]:
        # crear carpeta temporal SOLO en ejecución
        os.makedirs(TMP_DIR, exist_ok=True)

        if not os.path.exists(PATENTES_FILE_PATH):
            raise FileNotFoundError(f"No se encontró el archivo: {PATENTES_FILE_PATH}")

        with open(PATENTES_FILE_PATH, "r", encoding="utf-8") as f:
            patentes = json.load(f)

        total = len(patentes)
        if total == 0:
            print("Archivo vacío.")
            return []

        batch_paths: list[str] = []
        for i in range(0, total, BATCH_SIZE):
            batch = patentes[i : i + BATCH_SIZE]
            fname = f"batch_{i//BATCH_SIZE:05d}.json.gz"
            fpath = os.path.join(TMP_DIR, fname)
            with gzip.open(fpath, "wt", encoding="utf-8") as gz:
                json.dump(batch, gz, ensure_ascii=False)
            batch_paths.append(fpath)

        print(f"Lotes creados: {len(batch_paths)} (~{BATCH_SIZE} c/u) de {total} patentes.")
        return batch_paths

    @task
    def validate_batch_from_file(batch_path: str) -> dict:
        """
        Valida lote leyendo el .json.gz, genera:
          - lista de válidas (para potenciales pasos posteriores)
          - filas de warnings
          - filas de errores
        Sube dos CSV (warnings y errores) a MinIO/S3 con claves únicas por run y por lote.
        Devuelve métricas pequeñas para el resumen final.
        """
        with gzip.open(batch_path, "rt", encoding="utf-8") as gz:
            batch = json.load(gz)

        valid = []
        warn_rows: list[dict] = []
        error_rows: list[dict] = []

        for p in batch:
            pid = p.get("_id")
            is_valid, errors, warnings = validate_single_patent(p)

            # acumular filas detalladas
            if warnings:
                for w in warnings:
                    warn_rows.append({
                        "id": pid or "",
                        "level": "WARNING",
                        "message": w,
                    })
            if not is_valid and errors:
                for e in errors:
                    error_rows.append({
                        "id": pid or "",
                        "level": "ERROR",
                        "message": e,
                    })

            if is_valid:
                valid.append(p)

        total = len(batch)
        valid_count = len(valid)
        invalid_count = total - valid_count
        intl_total = sum(1 for p in batch if _is_international(p.get("_id")))
        intl_valid = sum(1 for p in valid if _is_international(p.get("_id")))

        # Guardar válidas del lote (por si hicieras algo después); no subimos a MinIO aquí.
        out_name = f"valid_{os.path.basename(batch_path)}"
        out_path = os.path.join(TMP_DIR, out_name)
        with gzip.open(out_path, "wt", encoding="utf-8") as gz:
            json.dump(valid, gz, ensure_ascii=False)

        # Eliminar lote de entrada para ahorrar disco
        try:
            os.remove(batch_path)
        except OSError:
            pass

        # Subir CSVs de warnings y errores de ESTE lote a MinIO
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        lote_id = os.path.splitext(os.path.basename(batch_path))[0]  # ej: batch_00012.json.gz -> batch_00012.json
        lote_id = lote_id.replace(".json", "")                       # -> batch_00012

        warn_header = ["id", "level", "message"]
        error_header = ["id", "level", "message"]

        warn_key = f"{MINIO_PREFIX}/warnings/{ts}_{lote_id}.csv"
        err_key  = f"{MINIO_PREFIX}/errors/{ts}_{lote_id}.csv"

        if warn_rows:
            _s3_ensure_bucket_and_upload_csv(rows=warn_rows, header=warn_header, key=warn_key)
        if error_rows:
            _s3_ensure_bucket_and_upload_csv(rows=error_rows, header=error_header, key=err_key)

        print(
            f"Lote validado: válidas={valid_count} | inválidas={invalid_count} | "
            f"intl_total={intl_total} | intl_valid={intl_valid} | "
            f"warnings_csv={'yes' if warn_rows else 'no'} | errors_csv={'yes' if error_rows else 'no'} → {out_path}"
        )

        # Devolvemos métricas para el resumen final + las claves donde quedaron los CSV de lote
        return {
            "total": total,
            "valid": valid_count,
            "invalid": invalid_count,
            "intl_total": intl_total,
            "intl_valid": intl_valid,
            "warnings_key": warn_key if warn_rows else None,
            "errors_key": err_key if error_rows else None,
        }

    @task
    def finalize(validated: list[dict]) -> int:
        """
        Imprime el resumen final global y genera además:
          - validation/warnings/all_<ts>.csv  (todos los warnings ordenados por id)
          - validation/errors/all_<ts>.csv    (todos los errores ordenados por id)
        """
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

        if not validated:
            # subir CSVs vacíos (solo cabecera) para dejar la huella de ejecución
            _s3_concat_and_upload_csv(
                keys=[],
                dest_key=f"{MINIO_PREFIX}/warnings/all_{ts}.csv",
                header=["id", "level", "message"],
            )
            _s3_concat_and_upload_csv(
                keys=[],
                dest_key=f"{MINIO_PREFIX}/errors/all_{ts}.csv",
                header=["id", "level", "message"],
            )
            print(
                "\n==== RESUMEN FINAL ====\n"
                "Total: 0\nVálidas: 0\nInválidas: 0\nInternacionales: 0 (0 válidas)\n"
                "=======================\n"
            )
            return 0

        total_all     = sum(v.get("total", 0) for v in validated)
        total_valid   = sum(v.get("valid", 0) for v in validated)
        total_invalid = sum(v.get("invalid", 0) for v in validated)
        intl_total    = sum(v.get("intl_total", 0) for v in validated)
        intl_valid    = sum(v.get("intl_valid", 0) for v in validated)

        # Consolidación global (merge + sort) y subida a MinIO en carpeta validation/
        warnings_keys = [v.get("warnings_key") for v in validated if v.get("warnings_key")]
        errors_keys   = [v.get("errors_key")   for v in validated if v.get("errors_key")]

        _s3_concat_and_upload_csv(
            keys=warnings_keys,
            dest_key=f"{MINIO_PREFIX}/warnings/all_{ts}.csv",
            header=["id", "level", "message"],
        )
        _s3_concat_and_upload_csv(
            keys=errors_keys,
            dest_key=f"{MINIO_PREFIX}/errors/all_{ts}.csv",
            header=["id", "level", "message"],
        )

        print("\n==== RESUMEN FINAL ====")
        print(f"Total:           {total_all}")
        print(f"Válidas:         {total_valid}")
        print(f"Inválidas:       {total_invalid}")
        print(f"Internacionales: {intl_total} ({intl_valid} válidas)")
        print("=======================\n")

        return total_valid

    @task(trigger_rule="all_done")
    def cleanup_tmp_dir():
        if KEEP_TMP:
            print(f"KEEP_TMP=1 → conservo {TMP_DIR}")
            return
        try:
            shutil.rmtree(TMP_DIR, ignore_errors=True)
            print(f"🧹 Carpeta temporal eliminada: {TMP_DIR}")
        except Exception as e:
            print(f"No se pudo eliminar {TMP_DIR}: {e}")

    # Pipeline
    batch_files = load_and_slice_to_files()                               # -> list[str]
    validated   = validate_batch_from_file.expand(batch_path=batch_files) # mapping OK
    finalize(validated) >> cleanup_tmp_dir()

patent_processing_dag()
