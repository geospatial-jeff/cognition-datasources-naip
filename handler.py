from datasources import Manifest

def NAIP(event, context):
    manifest = Manifest()
    manifest['NAIP'].search(event['spatial'], event['temporal'], event['properties'], **event['kwargs'])
    response = manifest.execute()
    return response


