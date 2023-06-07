import re

import pandas as pd
from unidecode import unidecode

from settings import settings


def dataframe_processing():
    """Preprocess the data
    This step is necessary to make sure the data is in the same format
    and we can compare the data"""

    a_company = pd.read_csv(settings.A_COMPANY)
    a_geo = pd.read_csv(settings.A_GEO)
    # merge a_company and a_geo on geo_id to create df_a
    df_a = pd.merge(a_company, a_geo, on="geo_id")
    df_a = df_a[
        ["vendor_id", "name", "address", "city", "state", "zipcode_y", "country_x"]
    ]
    # rename cols
    df_a = df_a.rename(
        columns={
            "vendor_id": "id",
            "address": "street",
            "zipcode_y": "postal",
            "country_x": "country",
        }
    )

    b_company = pd.read_csv(settings.B_COMPANY)
    b_address = pd.read_csv(settings.B_ADDRESS)
    # merge b_company and b_address on address_id to create df_b
    df_b = pd.merge(b_company, b_address, on="b_entity_id")
    df_b = df_b[
        [
            "b_entity_id",
            "entity_name",
            "location_street1",
            "location_city",
            "state_province_x",
            "zip_postal_code",
            "iso_country_x",
        ]
    ]
    # rename cols
    df_b = df_b.rename(
        columns={
            "b_entity_id": "id",
            "entity_name": "name",
            "location_street1": "street",
            "location_city": "city",
            "state_province_x": "state",
            "zip_postal_code": "postal",
            "iso_country_x": "country",
        }
    )

    df_a.to_csv(settings.DF_A, index=False)
    df_b.to_csv(settings.DF_B, index=False)


def string_manipulations(column):
    """Remove special characters, extra spaces, extra whitespaces, newlines,
    punctuation, and deal with some messy nulls in the data"""
    column = unidecode(column)
    column = re.sub(r"[^\w\s]", "", column)  # remove special characters
    column = re.sub(r"\s+", " ", column)  # remove extra spaces
    column = column.replace("\n", "")  # remove newlines
    column = column.strip()  # remove whitespaces
    column = column.upper()  # make everything uppercase
    nulls_list = ["NAN", "NONE", "NULL", "N/A", "NA", "N A", "NOT_DEFINED", ""]
    if column in nulls_list:
        column = None

    return column


def format_postal_code(country, postal):
    """Format US and CA postal codes
    US zip should always be five digits so add a 0 to the front if it's only 4 digits
    CA zip should be in the format A1A 1A1
    Other countries should return the postal code as is
    None values should return None

    Args:
        country (str): country code
        postal (str): postal code

    Returns:
        str: formatted postal code
    """
    us_zip_regex = re.compile(r"^\d{5}(?:[-\s]\d{4})?$")
    ca_zip_regex = re.compile(r"^[A-Za-z]\d[A-Za-z][ -]?\d[A-Za-z]\d$")
    if country == "US" and postal is not None:
        if us_zip_regex.match(postal):
            return postal
        elif len(postal) == 4:
            return "0" + postal
        else:
            return None
    elif country == "CA" and postal is not None:
        if ca_zip_regex.match(postal):
            return postal
        else:
            return None
