import settings


# project procedures...
from DataProcessing.Modules import \
    utils, \
    downloads, \
    inflections,\
    InOut  # \




def get_lang_pos(lang: str):
    data = InOut.load_parquet(lang, "has_id_column")
    distinct_pos = data.select("pos").distinct().collect()
    return [row["pos"] for row in distinct_pos]

"""
given: language and language data
return: the grammatical_feature column names which have values in them.
"""