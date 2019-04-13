from datetime import datetime
import os

import boto3
import utm

from datasources.stac.query import STACQuery
from datasources.stac.item import STACItem
from datasources.sources.base import Datasource

s3 = boto3.client('s3')

class NAIPAsset(object):

    def __init__(self, asset):
        self.asset = asset
        self.splits = asset['key'].split('/')

    @property
    def res(self):
        if self.splits[2] == '100cm':
            return 1.0
        else:
            return 0.6

    @property
    def filename(self):
        return self.splits[5]

    @property
    def utm(self):
        return self.asset['utm']

    @property
    def properties(self):
        return {
            'eo:gsd': self.res,
            'eo:epsg': self.utm
        }

class NAIP(Datasource):

    stac_compliant = False
    tags = ['EO', 'Aerial', 'Raster']

    def __init__(self, manifest):
        super().__init__(manifest)

    @staticmethod
    def leica_ads100_configuration(stac_item, nir=False):
        config = [{
            'name': 'B01',
            'common_name': 'red',
            'gsd': stac_item['properties']['eo:gsd'],
            'center_wavelength': 635,
            'full_width_half_max': 16,
            'accuracy': 6
        },
            {
                'name': 'B02',
                'common_name': 'green',
                'gsd': stac_item['properties']['eo:gsd'],
                'center_wavelength': 555,
                'full_width_half_max': 30,
                'accuracy': 6
            },
            {
                'name': 'B03',
                'common_name': 'blue',
                'gsd': stac_item['properties']['eo:gsd'],
                'center_wavelength': 465,
                'full_width_half_max': 30,
                'accuracy': 6
            }]
        if nir:
            config.append({'name': 'B04',
                           'common_name': 'nir',
                           'gsd': stac_item['properties']['eo:gsd'],
                           'center_wavelength': 845,
                           'full_width_half_max': 37,
                           'accuracy': 6
                           })
        return config

    @staticmethod
    def leica_ads80_configuration(stac_item, nir=False):
        config = [{
            'name': 'B01',
            'common_name': 'red',
            'gsd': stac_item['properties']['eo:gsd'],
            'center_wavelength': 634,
            'full_width_half_max': 30,
            'accuracy': 6
        },
            {
                'name': 'B02',
                'common_name': 'green',
                'gsd': stac_item['properties']['eo:gsd'],
                'center_wavelength': 560,
                'full_width_half_max': 27,
                'accuracy': 6
            },
            {
                'name': 'B03',
                'common_name': 'blue',
                'gsd': stac_item['properties']['eo:gsd'],
                'center_wavelength': 456,
                'full_width_half_max': 36,
                'accuracy': 6
            }]
        if nir:
            config.append({'name': 'B04',
                           'common_name': 'nir',
                           'gsd': stac_item['properties']['eo:gsd'],
                           'center_wavelength': 876.5,
                           'full_width_half_max': 43.5,
                           'accuracy': 6
                           })
        return config

    def search(self, spatial, temporal=None, properties=None, limit=10, **kwargs):
        from db import Database

        stac_query = STACQuery(spatial, temporal, properties)

        # Grab UTM zone epsg code
        xvals = [x[0] for x in stac_query.spatial['coordinates'][0]]
        yvals = [y[1] for y in stac_query.spatial['coordinates'][0]]
        centroid = [(min(xvals) + max(xvals)) / 2, (max(yvals) + min(yvals)) / 2]
        utm_zone = utm.from_latlon(*centroid[::-1])[2]
        utm_epsg = 26900 + int(utm_zone)

        with Database.load(read_only=True, deployed=True) as db:
            response = db.spatial_query({"type": "Feature", "geometry": stac_query.spatial})
            candidates = [{'key': x, 'utm': utm_epsg} for i in [item['keys'] for item in response] for x in i]

        searches = 0
        for idx, candidate in enumerate(candidates):
            asset = NAIPAsset(candidate)
            acquisition_date = asset.filename.split('_')[5]
            acquisition_date_str = f"{acquisition_date[0:4]}-{acquisition_date[4:6]}-{acquisition_date[6:8]}T00:00:00.000Z"
            if temporal:
                acquisition_date_time = datetime.strptime(acquisition_date_str,
                                                          "%Y-%m-%dT%H:%M:%S.%fZ")
                if not stac_query.check_temporal(acquisition_date_time):
                    continue

            if properties:
                if 'eo:instrument' in list(properties):
                    candidate.update({'eo:instrument': properties.pop('eo:instrument')['eq']})
                if not stac_query.check_properties(asset.properties):
                    continue

            if 'product' in kwargs:
                if kwargs['product'] == 'raw':
                    candidate.update({'bucket': 'naip-source'})
                elif kwargs['product'] == 'analytic':
                    candidate.update({'bucket': 'naip-analytic'})
                    candidate.update({'key': os.path.splitext(candidate['key'])[0] + '.mrf'})
                elif kwargs['product'] == 'visual':
                    candidate.update({'bucket': 'naip-visualization'})
            else:
                candidate.update({'bucket': 'naip-analytic'})

            candidate.update({'datetime': acquisition_date_str})
            candidate.update({'resolution': asset.res})

            # Metadata key
            md_key_splits = asset.splits
            md_key_splits[3] = 'fgdc'
            md_key_splits[-1] = '_'.join(asset.filename.split('_')[:-1]) + '.txt'
            candidate.update({'md_key': '/'.join(md_key_splits)})

            if searches < limit:
                self.manifest.searches.append([self, candidate])
                searches += 1

    def execute(self, query):
        # Base STAC Item
        stac_item = {
            'id': os.path.splitext(query['key'].split('/')[-1])[0],
            'type': 'Feature',
            'properties': {
                'datetime': query['datetime'],
                'eo:epsg': query['utm'],
                'eo:gsd': query['resolution']
            },
            'assets': {
                query['bucket'].split('-')[1]: {
                    'href': 's3://{}/{}'.format(query['bucket'], query['key']),
                    'title': 'Raster data'
                } ,
                'metadata': {
                    'href': 's3://{}/{}'.format(query['bucket'], query['md_key']),
                    'title': 'Raster metadata'
                }
            }
        }


        # Reading metadata from S3 (Requster Pays)
        try:
            md_obj = s3.get_object(Bucket=query['bucket'], Key=query['md_key'], RequestPayer="requester")
        except:
            print("WARNING: could not find metadata file {}".format(query['md_key']))
            return None
        metadata = md_obj['Body'].read().decode('utf-8')

        # Dynamically update stac item with metadata from external .txt file
        lines = metadata.splitlines()
        for idx, line in enumerate(lines):
            if line.endswith(':'):
                # Spatial properties
                if 'Bounding_Coordinates' in line:
                    xmin, xmax, ymax, ymin = [float("".join(x.split(':')[-1].split())) for x in lines[idx+1:idx+5]]
                    stac_item.update({'bbox': [xmin, ymin, xmax, ymax]})
                    stac_item.update({
                        'geometry': {
                            'type': 'Polygon',
                            'coordinates': [[[xmin, ymax], [xmax, ymax], [xmax, ymin], [xmin, ymin], [xmin, ymax]]]
                        }
                    })
                # Camera metadata
                if 'Process_Description' in line:
                    i = 1
                    x = lines[idx+1]
                    while x.endswith(':') != True:
                        i+=1
                        x = lines[idx+i].lower()
                        if 'ads100' in x:
                            stac_item['properties'].update({'eo:instrument': 'Leica ADS100'})
                            if query['bucket'] == 'naip-visualization':
                                stac_item['properties'].update({'eo:bands': self.leica_ads100_configuration(stac_item)})
                            else:
                                stac_item['properties'].update({'eo:bands': self.leica_ads100_configuration(stac_item, nir=True)})
                            break
                        elif 'ads80' in x:
                            stac_item['properties'].update({'eo:instrument': 'Leica ADS80'})
                            if query['bucket'] == 'naip-visualization':
                                stac_item['properties'].update({'eo:bands': self.leica_ads80_configuration(stac_item)})
                            else:
                                stac_item['properties'].update({'eo:bands': self.leica_ads80_configuration(stac_item, nir=True)})
                            break
            else:
                continue
        if 'eo:instrument' in query.keys():
            if 'eo:instrument' not in list(stac_item['properties']):
                return None
            else:
                if query['eo:instrument'] != stac_item['properties']['eo:instrument']:
                    return None

        # Validate item
        STACItem.load(stac_item)

        return [stac_item]