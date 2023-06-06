import csv

import dedupe

from settings import settings

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

    linker = dedupe.RecordLink(fields, in_memory=True)

    linked_records = linker.join(
        settings.LEFT_DATA_PROCESSED, settings.RIGHT_DATA_PROCESSED, threshold=0.0
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
                fieldnames = [
                    "cluster id",
                    "confidence",
                    "source file",
                ] + reader.fieldnames

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
    # dataframe_processing()
    # logger.info("Dataframe processing complete")
    # train()
    # logger.info("Training complete")
    get_results()
    logger.info("Results complete!")
