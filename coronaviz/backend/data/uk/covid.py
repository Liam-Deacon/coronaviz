from typing import Iterable, Dict, Union, List, Optional
from json import dumps
from requests import get
from http import HTTPStatus
from enum import Enum


StructureType = Dict[str, Union[dict, str]]
FiltersType = Iterable[str]
APIResponseType = Union[List[StructureType], str]


class AreaTypeEnum(Enum):
    """
    overview
        Overview data for the United Kingdom
    nation
        Nation data (England, Northern Ireland, Scotland, and Wales)
    region
        Region data
    nhsRegion
        NHS Region data
    utla
        Upper-tier local authority data
    ltla
        Lower-tier local authority data
    """
    OVERVIEW: str = "overview"
    NATION: str = "nation"
    REGION: str = "region"
    NHS_REGION: str = "nhsRegion"
    UPPER_TIER_LOCAL_AUTHORITY: str = "utla"
    LOWER_TIER_LOCAL_AUTHORITY: str = "ltla"

    def __str__(self):
        return self.value


class GovUKCoronavirusData:

    DEFAULT_QUERY_STRUCTURE = {
        "areaType": "areaType",  # Area type as string
        "areaName": "areaName",  # Area name as string
        "areaCode": "areaCode",  # Area Code as string
        "date": "date",  # Date as string [YYYY-MM-DD]
        "hash": "hash",  # Unique ID as string
        "newCasesByPublishDate": "newCasesByPublishDate",  # New cases by publish date
        "cumCasesByPublishDate": "cumCasesByPublishDate",  # Cumulative cases by publish date
        "cumCasesBySpecimenDateRate": "cumCasesBySpecimenDateRate",  # Rate of cumulative cases by publish date per 100k resident population
        "newCasesBySpecimenDate": "newCasesBySpecimenDate",  # New cases by specimen date
        "cumCasesBySpecimenDateRate": "cumCasesBySpecimenDateRate",  # Rate of cumulative cases by specimen date per 100k resident population
        "cumCasesBySpecimenDate": "cumCasesBySpecimenDate",  # Cumulative cases by specimen date
        "maleCases": "maleCases",  # Male cases (by age)
        "femaleCases": "femaleCases",  # Female cases (by age)
        "newPillarOneTestsByPublishDate": "newPillarOneTestsByPublishDate",  # New pillar one tests by publish date
        "cumPillarOneTestsByPublishDate": "cumPillarOneTestsByPublishDate",  # Cumulative pillar one tests by publish date
        "newPillarTwoTestsByPublishDate": "newPillarTwoTestsByPublishDate",  # New pillar two tests by publish date
        "cumPillarTwoTestsByPublishDate": "cumPillarTwoTestsByPublishDate",  # Cumulative pillar two tests by publish date
        "newPillarThreeTestsByPublishDate": "newPillarThreeTestsByPublishDate",  # New pillar three tests by publish date
        "cumPillarThreeTestsByPublishDate": "cumPillarThreeTestsByPublishDate",  # Cumulative pillar three tests by publish date
        "newPillarFourTestsByPublishDate": "newPillarFourTestsByPublishDate",  # New pillar four tests by publish date
        "cumPillarFourTestsByPublishDate": "cumPillarFourTestsByPublishDate",  # Cumulative pillar four tests by publish date
        "newAdmissions": "newAdmissions",  # New admissions
        "cumAdmissions": "cumAdmissions",  # Cumulative number of admissions
        "cumAdmissionsByAge": "cumAdmissionsByAge",  # Cumulative admissions by age
        "cumTestsByPublishDate": "cumTestsByPublishDate",  # Cumulative tests by publish date
        "newTestsByPublishDate": "newTestsByPublishDate",  # New tests by publish date
        "covidOccupiedMVBeds": "covidOccupiedMVBeds",  # COVID-19 occupied beds with mechanical ventilators
        "hospitalCases": "hospitalCases",  # Hospital cases
        "plannedCapacityByPublishDate": "plannedCapacityByPublishDate",  # Planned capacity by publish date
        "newDeaths28DaysByPublishDate": "newDeaths28DaysByPublishDate",  # Deaths within 28 days of positive test
        "cumDeaths28DaysByPublishDate": "cumDeaths28DaysByPublishDate",  # Cumulative deaths within 28 days of positive test
        "cumDeaths28DaysByPublishDateRate": "cumDeaths28DaysByPublishDateRate",  # Rate of cumulative deaths within 28 days of positive test per 100k resident population
        "newDeaths28DaysByDeathDate": "newDeaths28DaysByDeathDate",  # Deaths within 28 days of positive test by death date
        "cumDeaths28DaysByDeathDate": "cumDeaths28DaysByDeathDate",  # Cumulative deaths within 28 days of positive test by death date
        "cumDeaths28DaysByDeathDateRate": "cumDeaths28DaysByDeathDateRate",  # Rate of cumulative deaths within 28 days of positive test by death date per 100k resident population
    }

    @classmethod
    def get_latest_data(cls, filters: Optional[FiltersType] = None,
                        structure: Optional[StructureType] = None) -> dict:
        if filters is None:
            filters = [f'areaType={AreaTypeEnum.REGION.value}']
        if structure is None:
            structure = cls.DEFAULT_QUERY_STRUCTURE
        return cls.get_paginated_dataset(filters, structure)

    @staticmethod
    def get_paginated_dataset(filters: FiltersType, structure: StructureType,
                              as_csv: bool = False) -> APIResponseType:
        """
        Extracts paginated data by requesting all of the pages
        and combining the results.

        Parameters
        ----------
        filters: Iterable[str]
            API filters. See the API documentations for additional
            information.

        structure: Dict[str, Union[dict, str]]
            Structure parameter. See the API documentations for
            additional information.

        as_csv: bool
            Return the data as CSV. [default: ``False``]

        Returns
        -------
        Union[List[StructureType], str]
            Comprehensive list of dictionaries containing all the data for
            the given ``filters`` and ``structure``.
        """
        endpoint = "https://api.coronavirus.data.gov.uk/v1/data"

        api_params = {
            "filters": str.join(";", filters),
            "structure": dumps(structure, separators=(",", ":")),
            "format": "json" if not as_csv else "csv"
        }

        data = list()

        page_number = 1

        while True:
            # Adding page number to query params
            api_params["page"] = page_number

            response = get(endpoint, params=api_params, timeout=10)

            if response.status_code >= HTTPStatus.BAD_REQUEST:
                raise RuntimeError(f'Request failed: {response.text}')
            elif response.status_code == HTTPStatus.NO_CONTENT:
                break

            if as_csv:
                csv_content = response.content.decode()

                # Removing CSV header (column names) where page
                # number is greater than 1.
                if page_number > 1:
                    data_lines = csv_content.split("\n")[1:]
                    csv_content = str.join("\n", data_lines)

                data.append(csv_content.strip())
                page_number += 1
                continue

            current_data = response.json()
            page_data: List[StructureType] = current_data['data']

            data.extend(page_data)

            # The "next" attribute in "pagination" will be `None`
            # when we reach the end.
            if current_data["pagination"]["next"] is None:
                break

            page_number += 1

        if not as_csv:
            return data

        # Concatenating CSV pages
        return str.join("\n", data)
