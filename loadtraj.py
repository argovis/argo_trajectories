from pymongo import MongoClient
import datetime, xarray, numpy, pprint

client = MongoClient('mongodb://database/argo')
db = client.argo

def determine_metaid(metadict, existingmeta, prefix):
    # given a new metadata dict <metadic>,
    # the set of all existing metadata dictionaries in the series this metadata belongs to <existingmeta>,
    # (ie all the existing metadata dicts with the same platform number as <metadict> for dictionaries IDed by platform)
    # and a <prefix> for metadata IDs in this table,
    # return the _id property from the element of <existingmeta> that exactly matches <metadict> modulo _id,
    # or a new _id for metadict if it is not yet found in the database.

    ignore_keys = ['_id']
    metaid = None
    for meta in existingmeta:
        if {k: v for k,v in meta.items() if k not in ignore_keys} == {k: v for k,v in metadict.items() if k not in ignore_keys}:
            metaid = meta['_id']

    if metaid is None:
        metaid = prefix+str(len(existingmeta))

    return metaid

def stringcycle(cyclenumber):
    # given a numerical cyclenumber,
    # return a string left padded with 0s appropriate for use in a profile ID

    c = int(cyclenumber)
    if c < 10:
        return '00'+str(c)
    elif c < 100:
        return '0'+str(c)
    else:
        return str(c)

def mungetime(nssinceepoch):
    # ns since epoch -> datetime object

    return datetime.datetime.fromtimestamp(int(nssinceepoch/1000000000))

xar = xarray.open_dataset('Scripps_Argo_velocities_allPres_200101_202012_02132023.tap')

## munge positioning_system
ps = [bytes(xar['POSITIONING_SYSTEM'][i].data).decode("utf-8") for i in range(len(xar['POSITIONING_SYSTEM']))]
ps = list(zip(*ps))
ps = [''.join(k).strip() for k in ps]

## munge platform_type
pt = [bytes(xar['PLATFORM_TYPE'][i].data).decode("utf-8") for i in range(len(xar['PLATFORM_TYPE']))]
pt = list(zip(*pt))
pt = [''.join(k).strip() for k in pt]

data_keys = ['VELOCITY_ZONAL', 'VELOCITY_MERIDIONAL', 'VELOCITY_ZONAL_TRANSMITTED', 'VELOCITY_MERIDIONAL_TRANSMITTED', 'SPEED', 'SPEED_TRANSMITTED', 'DRIFT_PRES', 'DRIFT_TEMP', 'NUMBER_SURFACE_FIXES']

for i in [0]: #range(xar.dimensions['length'].size):
    data = {
        '_id': str(xar['WMO_NUMBER'][i].item()) + '_' + stringcycle(xar['CYCLE_NUMBER'][i].item()),
        'cycle_number': xar['CYCLE_NUMBER'][i].item(),
        'geolocation': {"type": "Point", "coordinates": [xar['LONGITUDE_MIDPOINT'][i].item(), xar['LATITUDE_MIDPOINT'][i].item()]},
        'timestamp': mungetime(xar['JULD_MIDPOINT'][i].item()),
        'geolocation_descending': {"type": "Point", "coordinates": [xar['LONGITUDE_DESCENDING'][i].item(), xar['LATITUDE_DESCENDING'][i].item()]},
        'timestamp_descending': mungetime(xar['JULD_DESCENDING'][i].item()),
        'geolocation_ascending': {"type": "Point", "coordinates": [xar['LONGITUDE_ASCENDING'][i].item(), xar['LATITUDE_ASCENDING'][i].item()]},
        'timestamp_ascending': mungetime(xar['JULD_ASCENDING'][i].item()),
        'geolocation_descending_transmitted': {"type": "Point", "coordinates": [xar['LONGITUDE_DESCENDING_TRANSMITTED'][i].item(), xar['LATITUDE_DESCENDING_TRANSMITTED'][i].item()]},
        'timestamp_descending_transmitted': mungetime(xar['JULD_DESCENDING_TRANSMITTED'][i].item()),
        'geolocation_ascending_transmitted': {"type": "Point", "coordinates": [xar['LONGITUDE_ASCENDING_TRANSMITTED'][i].item(), xar['LATITUDE_ASCENDING_TRANSMITTED'][i].item()]},
        'timestamp_ascending_transmitted': mungetime(xar['JULD_ASCENDING_TRANSMITTED'][i].item()),
        'geolocation_midpoint_transmitted': {"type": "Point", "coordinates": [xar['LONGITUDE_MIDPOINT_TRANSMITTED'][i].item(), xar['LATITUDE_MIDPOINT_TRANSMITTED'][i].item()]},
        'timestamp_midpoint_transmitted': mungetime(xar['JULD_MIDPOINT_TRANSMITTED'][i].item()),
        'data':[]       
    }

    metadata = {
        'wmo_number': float(xar['WMO_NUMBER'][i].item()),
        'positioning_system_flag': xar['POSITIONING_SYSTEM_FLAG'][i].item(),
        'sensor_type_flag': xar['SENSOR_TYPE_FLAG'][i].item(),
        'mission_flag': xar['MISSION_FLAG'][i].item(),
        'extrapolation_flag': xar['EXTRAPOLATION_FLAG'][i].item(),
        'positioning_system': ps[i],
        'platform_type': pt[i],
        'data_info': [
            [],
            ['long name', 'units'],
            []
        ] 
    }

    for key in data_keys:
        data['data'].append([xar[key][i].item()])
        metadata['data_info'][0].append(key.lower())
        metadata['data_info'][2].append([xar[key].attrs['long_name'], xar[key].attrs['units']])

    # determine if an appropriate pre-existing metadata record exists, and upsert metadata if required
    meta = []#list(db.trajectoryMeta.find({"wmo_number": metadata['wmo_number'] }))
    metadata['_id'] = determine_metaid(metadata, meta, str(metadata['wmo_number'])+'_m' )
    try:
        pprint.pprint(metadata, indent=4)
        # db.trajectoryMeta.replace_one({'_id': metadata['_id']}, metadata, True)
    except BaseException as err:
        print('error: metadata upsert failure on', metadata)
        print(err)

    # write data record to mongo
    data['metadata'] = [metadata['_id']]
    try:
        pprint.pprint(data, indent=4)
        #db.trajectory.replace_one({'_id': data['_id']}, data, True)
    except BaseException as err:
        print('error: data upsert failure on', data)
        print(err)
