from datasources import Manifest

def NAIP(event, context):
    manifest = Manifest()
    manifest['NAIP'].search(**event)
    response = manifest.execute()
    return response


