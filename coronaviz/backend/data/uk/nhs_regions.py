import requests
import json
import logging
# import geopandas as gpd

from pathlib import Path


class NHSBoundaryData:
    """Data taken from https://data.gov.uk/dataset/ae90ec0e-07ac-4f72-bfa9-1f6a99507180/nhs-regions-geography-april-2016-full-clipped-boundaries-in-england

    Published by:
        Office for National Statistics
    Last updated:
        12 June 2017
    Topic:
        Mapping
    Licence:
        Open Government Licence
    Summary:
        This file contains the digital vector boundaries for NHS Region (Geography) (NHSRG) in England as at April 2016. The boundaries available are:
    """

    filepath = 'nhs_regions.geojson'
    source_url = "http://geoportal1-ons.opendata.arcgis.com/datasets/0d03007bd13b48ebbeb1d95110338585_0.geojson"
    try:
        data = json.load(open(filepath))
    except IOError:
        logging.error('No NHS region data found - downloading...')
        response = requests.get(source_url)
        with open(filepath, 'w') as f:
            data = json.loads(response.text)
            json.dump(data, f)
            logging.info(f'Wrote data to {Path(filepath).absolute()}')
