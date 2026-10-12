"""Normalización compartida por el pipeline real y la prueba de escalabilidad."""

import re
import unicodedata

from pyspark.sql import functions as F


def header(s):
    s = unicodedata.normalize("NFKD", str(s or ""))
    s = "".join(x for x in s if not unicodedata.combining(x))
    return re.sub("[^a-z0-9]+", "_", s.lower()).strip("_")


def norm(col):
    return F.regexp_replace(
        F.upper(F.trim(F.translate(F.col(col), "áéíóúüñÁÉÍÓÚÜÑ", "aeiouunAEIOUUN"))), r"\s+", " "
    )


def number(col, kind="double"):
    return F.regexp_replace(F.regexp_replace(F.trim(F.col(col)), r"\s+", ""), ",", ".").cast(kind)
