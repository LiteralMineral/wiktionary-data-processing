

import pyspark.sql.functions as funcs


def get_definition_entries(self, dataframe, separator="\n\n"):
    og = dataframe
    print(og.count())
    # separate out the senses, drop the original 'senses' column
    in_progress = dataframe.withColumn(
        "separate_senses", funcs.explode_outer("senses")
    ).drop("senses")
    # detect if the sense is tagged as "form-of"
    in_progress = in_progress.withColumn(
        "not_dict_form", funcs.array_contains("separate_senses.tags", "form-of")
    )
    # Turn individual senses into their own strings
    in_progress = in_progress.withColumn(
        "separate_senses_glosses", funcs.array_join("separate_senses.glosses", "###")
    )
    # TODO: detect if the 'form-of' entries are diminutives, make sure they get included
    # if the sense was not tagged, it was not tagged with "form-of"
    in_progress = in_progress.na.fill({"not_dict_form": False})
    # select the senses that are not an inflection or other form...
    in_progress = in_progress.where(in_progress.not_dict_form == False).drop(
        "not_dict_form"
    )

    grouped_by_id = in_progress.groupBy("entry_id").agg(
        funcs.concat_ws(
            separator, funcs.collect_list(in_progress.separate_senses_glosses)
        ).alias("filtered_glosses")
    )
    print(grouped_by_id.count())
    grouped_by_id = grouped_by_id.withColumnRenamed("entry_id", "entry_id2")
    # grouped_by_id.show(100, truncate=False)

    in_progress = og.join(
        grouped_by_id,
        self.editable_dataset.entry_id == grouped_by_id.entry_id2,
        "right",
    )
    # print(grouped_by_id.count())

    in_progress = in_progress.drop("entry_id2")

    return in_progress

