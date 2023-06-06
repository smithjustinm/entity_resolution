import csv
import os

import dedupe
import pandas as pd

from src.preprocess import format_postal_code, string_manipulations
from src.settings import settings

logger = settings.setup_logging()

settings_file = settings.SETTINGS_FILE
training_file = settings.TRAINING_FILE

left_file = settings.LEFT_FILE
right_file = settings.RIGHT_FILE
fields = settings.FIELDS


def read_and_process(_filename):
    """Read and process the data from the csv file

    Args:
        _filename: The name of the csv file

    Returns:
        A dictionary of the data
    """
    data_dict = {}
    with open(_filename) as file:
        _reader = csv.DictReader(file)
        for i, _row in enumerate(_reader):
            clean_row = {
                key: string_manipulations(value) for (key, value) in _row.items()
            }
            # if US or CA is the country then format the postal code
            if clean_row["country"] == "US" or clean_row["country"] == "CA":
                clean_row["postal"] = format_postal_code(
                    clean_row["country"], clean_row["postal"]
                )
            data_dict[i] = clean_row
    return data_dict


def read_data(_filename):
    """Read the data from the csv file

    Args:
        _filename: The name of the csv file

    Returns:
        A dictionary of the data
    """
    data_dict = {}
    with open(_filename) as file:
        _reader = csv.DictReader(file)
        for i, _row in enumerate(_reader):
            data_dict[i] = _row
    return data_dict

def train():
    """Train the model"""

    logger.info("Creating a labeled data set")

    linker = dedupe.RecordLink(fields, in_memory=True)

    logger.info("Record link complete")

    # set sample size to ten percent of the data
    # sample_size = int(len(left_data) * 0.1)

    if os.path.exists(training_file):
        logger.info("reading data from csv files")
        left_data = read_data(settings.LEFT_DATA_PROCESSED)
        right_data = read_data(settings.RIGHT_DATA_PROCESSED)
        logger.info("reading labeled examples from %s" % training_file)
        with open(training_file) as tf:
            linker.prepare_training(left_data, right_data, training_file=tf)
    else:
        logger.info("Reading and processing data")
        left_data = read_and_process(left_file)
        right_data = read_and_process(right_file)

        # save left and right data to csv files as
        # left_data_processed.csv and right_data_processed.csv
        left_df = pd.DataFrame.from_dict(left_data, orient="index")
        right_df = pd.DataFrame.from_dict(right_data, orient="index")
        left_df.to_csv("output/left_data_processed.csv", index=False)
        right_df.to_csv("output/right_data_processed.csv", index=False)

        logger.info("creating labeled examples from %s" % training_file)
        linker.prepare_training(left_data, right_data)

    logger.info("Starting active labeling...")

    dedupe.console_label(linker)

    logger.info("finished console labeling")

    linker.train(index_predicates=False, recall=0.80)

    logger.info("Training complete. Saving learned settings to %s" % settings_file)

    with open(settings_file, "wb") as sf:
        linker.write_settings(sf)

    logger.info("Learned settings saved. Saving training data to %s" % training_file)

    with open(training_file, "w") as tf:
        linker.write_training(tf)
    logger.info("Training data saved.")
