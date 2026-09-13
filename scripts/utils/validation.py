from pyspark.sql.functions import (
    col, from_json, explode, when, lit, array, array_compact, size,
    current_timestamp, unix_timestamp, max as smax, explode as ex
)
from pyspark.sql.types import StringType




# ---------- Seuils ----------
# Placeholders. A recalibrer sur la distribution reelle des retards.
DELAY_MIN = -3600
DELAY_MAX = 7200
FRAICHEUR_MAX_S = 600


def parse_tu(raw):
    from google.transit import gtfs_realtime_pb2
    from google.protobuf.json_format import MessageToJson
    tu = gtfs_realtime_pb2.TripUpdate()
    tu.ParseFromString(raw)
    return MessageToJson(tu)


def ajouter_violations(df):
    """Marque chaque ligne avec la liste de ses violations.
    Tableau vide = ligne conforme."""
    return df.withColumn("violations", array_compact(array(

        # Presence: l'arret doit etre identifiable
        when(col("stop_id").isNull() & col("stop_sequence").isNull(),
             lit("arret_non_identifiable")),

        # Presence: un arret SCHEDULED doit porter une prediction
        when(
            (col("stop_schedule_rel").isNull() | (col("stop_schedule_rel") == "SCHEDULED"))
            & col("arrival_delay").isNull() & col("arrival_time").isNull()
            & col("departure_delay").isNull() & col("departure_time").isNull(),
            lit("aucune_prediction")
        ),

        # Domaine: retard plausible
        when((col("arrival_delay") < DELAY_MIN) | (col("arrival_delay") > DELAY_MAX),
             lit("retard_arrivee_aberrant")),
        when((col("departure_delay") < DELAY_MIN) | (col("departure_delay") > DELAY_MAX),
             lit("retard_depart_aberrant")),

    )))


def mesurer(batch_id, total, valides, invalides, fraicheur_s):
    """Metriques de batch. N'ecarte rien, sert a alerter."""
    n_inv = invalides.count()
    print(f"[batch {batch_id}] total={total} valides={total - n_inv} rejets={n_inv}")

    if n_inv > 0:
        (invalides
         .select(ex("violations").alias("regle"))
         .groupBy("regle").count()
         .show(truncate=False))

    if fraicheur_s is None:
        print(f"[batch {batch_id}] ALERTE: aucun tu_timestamp exploitable")
    elif fraicheur_s > FRAICHEUR_MAX_S:
        print(f"[batch {batch_id}] ALERTE: flux amont fige, {fraicheur_s}s de retard")


def traiter_batch(batch_df, batch_id):
    batch_df = batch_df.cache()
    try:
        total = batch_df.count()
        if total == 0:
            return

        marque = ajouter_violations(batch_df).cache()

        valides = marque.filter(size("violations") == 0).drop("violations")
        invalides = marque.filter(size("violations") > 0)

        (valides.write
         .mode("append")
         .partitionBy("start_date")
         .parquet("/opt/jobs/clean"))

        (invalides.write
         .mode("append")
         .parquet("/opt/jobs/quarantine"))

        # Fraicheur: ecart entre le timestamp du flux et maintenant
        ligne = batch_df.select(
            (unix_timestamp(current_timestamp()) - smax("tu_timestamp")).alias("ecart")
        ).collect()[0]

        mesurer(batch_id, total, valides, invalides, ligne["ecart"])
        marque.unpersist()
    finally:
        batch_df.unpersist()