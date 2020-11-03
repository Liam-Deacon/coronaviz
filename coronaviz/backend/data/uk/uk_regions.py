import requests
import json
import logging
# import geopandas as gpd

from pathlib import Path


class RegionalBoundaryData:
    """Data taken from https://data.gov.uk/dataset/d1647852-4b75-4ab2-8219-860bfef6ac9d/regions-december-2016-full-clipped-boundaries-in-england

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

    filepath = 'regions.geojson'
    source_url = "http://geoportal1-ons.opendata.arcgis.com/datasets/f99b145881724e15a04a8a113544dfc5_0.geojson?outSR={%22latestWkid%22:27700,%22wkid%22:27700}"
    try:
        data = json.load(open(filepath))
    except IOError:
        logging.error('No region data found - downloading...')
        response = requests.get(source_url)
        with open(filepath, 'w') as f:
            data = json.loads(response.text)
            json.dump(data, f)
            logging.info(f'Wrote data to {Path(filepath).absolute()}')
