import csv

import dedupe

from preprocess import dataframe_processing
from settings import settings
from train import train, read_and_process

logger = settings.setup_logging()


def cluster_membership(linked_records):
    """Create a dictionary of cluster membership from the results of the
    Record Linkage step

    Args:
        linked_records: The results of the Record Linkage step

    Returns:
        A dictionary of cluster membership
    """
    _cluster_membership = {}
    for cluster_id, (cluster, score) in enumerate(linked_records):
        logger.info("clustering id %s" % cluster_id)
        for record_id in cluster:
            _cluster_membership[record_id] = {
                "cluster_id": cluster_id,
                "confidence_score": score,
            }
    return _cluster_membership


def get_results(use_static: bool = False):
    logger.info("Clustering...")

    if use_static:
        with open(settings.SETTINGS_FILE, "rb") as learned_settings:
            linker = dedupe.StaticRecordLink(learned_settings)

    else:
        linker = dedupe.RecordLink(settings.FIELDS)

    # read csv to dict format
    left_data = read_and_process(settings.LEFT_DATA_PROCESSED)
    right_data = read_and_process(settings.RIGHT_DATA_PROCESSED)

    linked_records = linker.join(
        left_data, right_data, threshold=settings.THRESHOLD
    )

    _cluster_membership = cluster_membership(linked_records)

    output_file = settings.OUTPUT_FILE

    with open(output_file, "w") as _output_file:
        header_unwritten = True

        left_file = str(settings.LEFT_DATA_PROCESSED)
        right_file = str(settings.RIGHT_DATA_PROCESSED)

        for fileno, filename in enumerate((left_file, right_file)):
            with open(filename) as file_input:
                reader = csv.DictReader(file_input)

                if header_unwritten:
                    # should be a dictionary of the fieldnames with cluster id and confidence and source file
                    fieldnames = reader.fieldnames + [
                        "cluster_id",
                        "confidence_score",
                        "source_file",
                    ]

                    writer = csv.DictWriter(_output_file, fieldnames=fieldnames)
                    writer.writeheader()

                    header_unwritten = False

                logger.info("iterating through rows")
                for row_id, row in enumerate(reader):
                    record_id = filename + str(row_id)
                    row["source_file"] = fileno
                    # _cluster_membership is a dictionary of cluster_id and confidence score
                    row["cluster_id"] = [(key, value["cluster_id"]) for key, value in _cluster_membership.items()]
                    row["confidence_score"] = [(key, value["confidence_score"]) for key, value in _cluster_membership.items()]

                    # for key, value in _cluster_membership.items():
                    #     row["cluster_id"] = value["cluster_id"]
                    #     row["confidence_score"] = value["confidence_score"]

                    writer.writerow(row)



if __name__ == "__main__":

    # if not settings.LEFT_DATA_PROCESSED.exists() and not settings.RIGHT_DATA_PROCESSED.exists():
    # dataframe_processing()
    # logger.info("Dataframe processing complete!")

    # train()
    # logger.info("Training complete!")
    get_results(use_static=True)
    logger.info("Results complete!")
