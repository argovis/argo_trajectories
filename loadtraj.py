from pymongo import MongoClient
import datetime, xarray, numpy, pprint, math, shutil
from netCDF4 import Dataset

client = MongoClient('mongodb://database/argo')
db = client.argo

def determine_metaid(metadict, existingmeta, prefix):
    # given a new metadata dict <metadic>,
    # the set of all existing metadata dictionaries in the series this metadata belongs to <existingmeta>,
    # (ie all the existing metadata dicts with the same platform number as <metadict> for dictionaries IDed by platform)
    # and a <prefix> for metadata IDs in this table,
    # return the _id property from the element of <existingmeta> that exactly matches <metadict> modulo _id,
    # or a new _id for metadict if it is not yet found in the database.

    ignore_keys = ['_id', 'date_updated_argovis']
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

def cleanup(meas):
    # given a measurement, return the measurement after some generic cleanup

    if meas is None:
        return meas

    # use None as missing fill
    if math.isnan(meas):
        return None        

    return round(meas,6) # at most 6 significant decimal places


# PLATFORM_TYPE is both a data variable and a coordinate in the upstream data, this is a no no
shutil.copyfile('trajectories_J0NK3F7V.nc', 'modified_file.nc')
ds = Dataset('modified_file.nc',mode='a')
ds.renameVariable('PLATFORM_TYPE', 'PLATFORM_TYPE_VAR')
ds.close()

xar = xarray.open_dataset('modified_file.nc')

# ## munge positioning_system # no longer present in https://doi.org/10.6075/J0NK3F7V ?
# ps = [bytes(xar['POSITIONING_SYSTEM'][i].data).decode("utf-8") for i in range(len(xar['POSITIONING_SYSTEM']))]
# ps = list(zip(*ps))
# ps = [''.join(k).strip() for k in ps]

data_keys = ['VELOCITY_ZONAL', 'VELOCITY_MERIDIONAL', 'VELOCITY_ZONAL_TRANSMITTED', 'VELOCITY_MERIDIONAL_TRANSMITTED', 'SPEED', 'SPEED_TRANSMITTED', 'DRIFT_PRES', 'DRIFT_TEMP', 'NUMBER_SURFACE_FIXES']

# see what data IDs have already been uploaded, and skip them in case of interruption
completed = [ x['_id'] for x in list(db.trajectories.find({}, {'_id':1}))]

for i in range(len(xar['WMO_NUMBER'])):
    ID = str(int(xar['WMO_NUMBER'][i].item())) + '_' + stringcycle(xar['CYCLE_NUMBER'][i].item())
    if ID in completed:
        print('skipping', ID)
        continue
    data = {
        '_id': ID,
        'cycle_number': int(xar['CYCLE_NUMBER'][i].item()),
        'geolocation': {"type": "Point", "coordinates": [cleanup(xar['LONGITUDE_MIDPOINT'][i].item()), cleanup(xar['LATITUDE_MIDPOINT'][i].item())]},
        'timestamp': mungetime(xar['JULD_MIDPOINT'][i].item()),
        'geolocation_descending': {"type": "Point", "coordinates": [cleanup(xar['LONGITUDE_DESCENDING'][i].item()), cleanup(xar['LATITUDE_DESCENDING'][i].item())]},
        'timestamp_descending': mungetime(xar['JULD_DESCENDING'][i].item()),
        'geolocation_ascending': {"type": "Point", "coordinates": [cleanup(xar['LONGITUDE_ASCENDING'][i].item()), cleanup(xar['LATITUDE_ASCENDING'][i].item())]},
        'timestamp_ascending': mungetime(xar['JULD_ASCENDING'][i].item()),
        'geolocation_descending_transmitted': {"type": "Point", "coordinates": [cleanup(xar['LONGITUDE_DESCENDING_TRANSMITTED'][i].item()), cleanup(xar['LATITUDE_DESCENDING_TRANSMITTED'][i].item())]},
        'timestamp_descending_transmitted': mungetime(xar['JULD_DESCENDING_TRANSMITTED'][i].item()),
        'geolocation_ascending_transmitted': {"type": "Point", "coordinates": [cleanup(xar['LONGITUDE_ASCENDING_TRANSMITTED'][i].item()), cleanup(xar['LATITUDE_ASCENDING_TRANSMITTED'][i].item())]},
        'timestamp_ascending_transmitted': mungetime(xar['JULD_ASCENDING_TRANSMITTED'][i].item()),
        'geolocation_midpoint_transmitted': {"type": "Point", "coordinates": [cleanup(xar['LONGITUDE_MIDPOINT_TRANSMITTED'][i].item()), cleanup(xar['LATITUDE_MIDPOINT_TRANSMITTED'][i].item())]},
        'timestamp_midpoint_transmitted': mungetime(xar['JULD_MIDPOINT_TRANSMITTED'][i].item()),
        'data':[]       
    }

    metadata = {
        'platform': str(int(xar['WMO_NUMBER'][i].item())),
        'data_type': 'argo_trajectory',
        'source': [{
            'source': ['scripps_argo_trajectory'],
            'doi': 'https://doi.org/10.6075/J0NK3F7V'
        }],
        'date_updated_argovis': datetime.datetime.now(),
        'positioning_system_flag': int(xar['POSITIONING_SYSTEM_FLAG'][i].item()),
        'sensor_type_flag': int(xar['SENSOR_TYPE_FLAG'][i].item()),
        'mission_flag': int(xar['MISSION_FLAG'][i].item()),
        'extrapolation_flag': int(xar['EXTRAPOLATION_FLAG'][i].item()),
        #'positioning_system': ps[i],
        'platform_type': b''.join(xar['PLATFORM_TYPE_VAR'].isel(NUM_POINTS=i).values).decode('utf-8').strip(),
        'data_info': [
            [],
            ['long name', 'units'],
            []
        ] 
    }

    for key in data_keys:
        data['data'].append([cleanup(xar[key][i].item())])
        metadata['data_info'][0].append(key.lower())
        try:
            metadata['data_info'][2].append([xar[key].attrs['long_name'], xar[key].attrs['units']])
        except:
            metadata['data_info'][2].append([xar[key].attrs['long_name'], ''])

    # determine if an appropriate pre-existing metadata record exists, and upsert metadata if required
    meta = list(db.trajectoriesMeta.find({"platform": metadata['platform'] }))
    metadata['_id'] = determine_metaid(metadata, meta, str(metadata['platform'])+'_m' )
    try:
        db.trajectoriesMeta.replace_one({'_id': metadata['_id']}, metadata, True)
    except BaseException as err:
        print('error: metadata upsert failure on', metadata)
        print(err)

    # write data record to mongo
    data['metadata'] = [metadata['_id']]
    try:
        #pprint.pprint(data, indent=4)
        db.trajectories.replace_one({'_id': data['_id']}, data, True)
    except BaseException as err:
        print('error: data upsert failure on', data)
        print(err)
