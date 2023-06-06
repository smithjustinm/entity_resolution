import csv

import dedupe

from src.preprocess import dataframe_processing
from src.settings import settings
from src.train import train, read_and_process

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
                "cluster id": cluster_id,
                "confidence": score,
            }
    return _cluster_membership


def get_results():
    logger.info("Clustering...")

    fields = settings.FIELDS

    linker = dedupe.RecordLink(fields)

    # read csv to dict format
    left_data = read_and_process(settings.LEFT_DATA_PROCESSED)
    right_data = read_and_process(settings.RIGHT_DATA_PROCESSED)

    linked_records = linker.join(
        left_data, right_data, threshold=0.0
    )

    logger.info("# duplicate sets %s" % len(linked_records))

    _cluster_membership = cluster_membership(linked_records)

    output_file = settings.OUTPUT_FILE

    with open(output_file, "w") as _output_file:
        header_unwritten = True

    left_file = settings.LEFT_FILE
    right_file = settings.RIGHT_FILE

    for fileno, filename in enumerate((left_file, right_file)):
        with open(filename) as file_input:
            reader = csv.DictReader(file_input)

            if header_unwritten:
                fieldnames = ([
                    "cluster id",
                    "confidence",
                    "source file",
                ] + reader.fieldnames)

                writer = csv.DictWriter(_output_file, fieldnames)
                writer.writeheader()

                header_unwritten = False

            for row_id, row in enumerate(reader):
                logger.info("writing row %s" % row_id)
                record_id = filename + str(row_id)
                cluster_details = _cluster_membership.get(record_id, {})
                row["source file"] = fileno
                row.update(cluster_details)

                writer.writerow(row)


if __name__ == "__main__":
    # if df_a and df_b exist in data/output then skip the dataframe processing
    # otherwise process the dataframes
    if not settings.DF_A.exists() and not settings.DF_B.exists():
        dataframe_processing()
        logger.info("Dataframe processing complete")

    train()
    logger.info("Training complete")
    get_results()
    logger.info("Results complete!")
