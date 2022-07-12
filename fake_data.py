import datetime

from google.cloud import bigquery
import pytz
import pandas as pd


def fake_data(self):
    records = [
        {
            "title": u"The Meaning of Life",
            "release_year": 1983,
            "length_minutes": 112.5,
            "release_date": pytz.timezone("Europe/Paris")
                .localize(datetime.datetime(1983, 5, 9, 13, 0, 0))
                .astimezone(pytz.utc),
            # Assume UTC timezone when a datetime object contains no timezone.
            "dvd_release": datetime.datetime(2002, 1, 22, 7, 0, 0),
        },
        {
            "title": u"Monty Python and the Holy Grail",
            "release_year": 1975,
            "length_minutes": 91.5,
            "release_date": pytz.timezone("Europe/London")
                .localize(datetime.datetime(1975, 4, 9, 23, 59, 2))
                .astimezone(pytz.utc),
            "dvd_release": datetime.datetime(2002, 7, 16, 9, 0, 0),
        },
        {
            "title": u"Life of Brian",
            "release_year": 1979,
            "length_minutes": 94.25,
            "release_date": pytz.timezone("America/New_York")
                .localize(datetime.datetime(1979, 8, 17, 23, 59, 5))
                .astimezone(pytz.utc),
            "dvd_release": datetime.datetime(2008, 1, 14, 8, 0, 0),
        },
        {
            "title": u"And Now for Something Completely Different",
            "release_year": 1971,
            "length_minutes": 88.0,
            "release_date": pytz.timezone("Europe/London")
                .localize(datetime.datetime(1971, 9, 28, 23, 59, 7))
                .astimezone(pytz.utc),
            "dvd_release": datetime.datetime(2003, 10, 22, 10, 0, 0),
        },
    ]
    dataframe = pd.DataFrame(
        records,
        # In the loaded table, the column order reflects the order of the
        # columns in the DataFrame.
        columns=[
            "title",
            "release_year",
            "length_minutes",
            "release_date",
            "dvd_release",
        ],
        # Optionally, set a named index, which can also be written to the
        # BigQuery table.
        index=pd.Index(
            [u"Q24980", u"Q25043", u"Q24953", u"Q16403"], name="wikidata_id"
        ),
    )

    return dataframe


def fake_schema(self):
    return [
        # Specify the type of columns whose type cannot be auto-detected. For
        # example the "title" column uses pandas dtype "object", so its
        # data type is ambiguous.
        bigquery.SchemaField("title", bigquery.enums.SqlTypeNames.STRING),
        # Indexes are written if included in the schema by name.
        bigquery.SchemaField("wikidata_id", bigquery.enums.SqlTypeNames.STRING),
    ]
