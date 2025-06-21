import pytest
from collections import defaultdict
import requests

DATASET_ACCIDENT_ID = "53698f4ca3a729239d2036df"
DATASET_ACCIDENT_URL = f"https://www.data.gouv.fr/api/1/datasets/{DATASET_ACCIDENT_ID}/"


def test_four_files_per_year():
    """
    Test that there are exactly four files for each year in the downloaded files.
    """
    filename_list = []

    response = requests.get(DATASET_ACCIDENT_URL)

    if response.status_code == 200:
        data_ressources = response.json()

        for resource in data_ressources.get("resources", []):
            file_title = resource.get("title", "noname").lower()
            file_format = resource.get("format", "")

            if (
                any(keyword in file_title for keyword in ["usa", "vehi", "lieu", "car"])
                and "vehicules-immatricules-baac" not in file_title
                and "description" not in file_title
                and file_format == "csv"
            ):
                filename = file_title.replace(" ", "_")
                filename_list.append(filename)

    year_count = defaultdict(int)

    for file in filename_list:
        # Extract the year from the filename, considering both '_' and '-' as separators
        year = None
        if "_" in file:
            year = file.split("_")[-1].split(".")[0]
        elif "-" in file:
            year = file.split("-")[-1].split(".")[0]

        if year:
            year_count[year] += 1

    for year, count in year_count.items():
        assert count == 4, f"Expected 4 files for year {year}, but found {count}."
