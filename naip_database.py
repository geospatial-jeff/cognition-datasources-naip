import os
import subprocess
from multiprocessing.pool import ThreadPool
from osgeo import gdal, osr, ogr


rtree_location = 'naip_rtree'
naip_static_location = 'static'

def download_manifest():
    subprocess.call("aws s3 cp s3://naip-analytic/manifest.txt {}/manifest.txt --request-payer requester".format(naip_static_location), shell=True)

def download_shapefile(key):
    subprocess.call("aws s3 sync s3://naip-analytic/{}/ {}/coverages/ --request-payer requester".format(key, naip_static_location), shell=True)

def download_coverages():
    keys = []
    with open(os.path.join(naip_static_location, 'manifest.txt'), 'r') as manifest:
        for line in manifest.readlines():
                line = line.rstrip()
                if line.endswith('.shp'):
                    keys.append(os.path.dirname(line))
    m = ThreadPool()
    m.map(download_shapefile, keys)

def build_database(outfile):
    # Create transformer from NAD 83 to WGS 84
    in_srs = osr.SpatialReference()
    in_srs.ImportFromEPSG(4269)
    out_srs = osr.SpatialReference()
    out_srs.ImportFromEPSG(4326)
    transformer = osr.CoordinateTransformation(in_srs, out_srs)

    print("Preprocessing coverage shapefiles")
    # Preprocess into dict
    coverage_location = os.path.join(naip_static_location, 'coverages')
    files = [os.path.join(coverage_location, x) for x in os.listdir(coverage_location) if x.endswith('.shp')]
    d = {}
    for file in files:
        ds = gdal.OpenEx(file)
        if ds:
            lyr = ds.GetLayer()
            for feat in lyr:
                geom = feat.GetGeometryRef()
                if feat['Res'] == 0:
                    res = '60cm'
                else:
                    res = '100cm'
                state = os.path.splitext(file)[0].split('_')[-1]
                if feat['USGSID']:
                    key = "{}/{}/{}/rgbir/{}/{}".format(state,
                                                        feat['SrcImgDate'][:4],
                                                        res,
                                                        feat['USGSID'][:-2],
                                                        feat['FileName'])
                    # QKEY is unique id of each quad
                    if feat['QKEY'] not in d.keys():
                        # Reproject geometry to WGS 84
                        geom.Transform(transformer)
                        d[feat['QKEY']] = {'geometry': geom.ExportToWkt(), 'object': {'keys': [key], 'utm': 26900 + int(feat['UTM'])}}
                    else:
                        d[feat['QKEY']]['object']['keys'].append(key)
        else:
            print("Bad shapefile: {}".format(file))

    # Create shapefile
    print("Creating geojson")
    out_ds = gdal.GetDriverByName('GeoJSON').Create(outfile,0,0,0)
    out_lyr = out_ds.CreateLayer('', out_srs, ogr.wkbPolygon)
    out_lyr.CreateField(ogr.FieldDefn("keys", ogr.OFTString))
    out_lyr.CreateField(ogr.FieldDefn("id", ogr.OFTInteger))
    for idx, quad in enumerate(d):
        out_feat = ogr.Feature(out_lyr.GetLayerDefn())
        out_feat.SetField('keys', str(d[quad]['object']['keys']))
        out_feat.SetField('id', idx)
        geom = ogr.CreateGeometryFromWkt(d[quad]['geometry'])
        geom.Transform(transformer)
        out_feat.SetGeometry(geom)
        out_lyr.CreateFeature(out_feat)
        out_feat = None
    out_lyr = None
    out_ds = None

build_database('/home/slingshot/Documents/Cognition/cognition-datasources-naip/static/naip_coverage.geojson')