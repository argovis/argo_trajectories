import xarray, math, numpy, datetime
from pymongo import MongoClient

client = MongoClient('mongodb://database/argo')
db = client.argo
xar = xarray.open_dataset('Scripps_Argo_velocities_allPres_200101_202012_02132023.tap')

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


def cleanup(meas):
    # given a measurement, return the measurement after some generic cleanup

    if meas is None:
        return meas

    # use None as missing fill
    if math.isnan(meas):
        return None        

    return round(meas,6) # at most 6 significant decimal places

def mungetime(nssinceepoch):
    # ns since epoch -> datetime object

    return datetime.datetime.fromtimestamp(int(nssinceepoch/1000000000))

def xarlist_num(ncvar):
    return numpy.sort(numpy.unique([cleanup(x.item()) for x in xar[ncvar]]))

def mongolist_geo(keystring, index):
    pipeline = [{'$project' : {"x" : { '$arrayElemAt': [ keystring, index ] }}}, {'$group' : {'_id' : "$x"}}]
    x = [a['_id'] for a in list(db.trajectories.aggregate(pipeline))]
    return numpy.sort(numpy.unique(x))

def xarlist_date(ncvar):
    return numpy.sort([mungetime(x.item()) for x in xar[ncvar]])

def mongolist_date(keystring):
    x = [a[keystring] for a in list(db.trajectories.find({}, {keystring:1}))]
    return numpy.sort(x)

def xarhist(varname):
    d = xar[varname]
    x = d.groupby(d).count()
    x = zip(x.coords[varname].data, x.data)
    return {a[0]:a[1] for a in x}

def mongohist(key):
    pipeline = [
        {
            '$lookup': {
                'from': 'trajectoriesMeta',
                'localField': 'metadata',
                'foreignField': '_id',
                'as': 'meta'
            }
        },
        {
            '$group': {
                '_id': '$meta.'+key,
                'count': {'$count': {}}
            }
        }
    ]
    return {x['_id'][0]: x['count'] for x in list(db.trajectories.aggregate(pipeline))}

# geolocation matches
geopairs = [
    ["$geolocation.coordinates", 0, 'LONGITUDE_MIDPOINT'],
    ["$geolocation.coordinates", 1, 'LATITUDE_MIDPOINT'],
    ["$geolocation_midpoint_transmitted.coordinates", 0, 'LONGITUDE_MIDPOINT_TRANSMITTED'],
    ["$geolocation_midpoint_transmitted.coordinates", 1, 'LATITUDE_MIDPOINT_TRANSMITTED'],
    ["$geolocation_descending.coordinates", 0, 'LONGITUDE_DESCENDING'],
    ["$geolocation_descending.coordinates", 1, 'LATITUDE_DESCENDING'],
    ["$geolocation_descending_transmitted.coordinates", 0, 'LONGITUDE_DESCENDING_TRANSMITTED'],
    ["$geolocation_descending_transmitted.coordinates", 1, 'LATITUDE_DESCENDING_TRANSMITTED'],
    ["$geolocation_ascending.coordinates", 0, 'LONGITUDE_ASCENDING'],
    ["$geolocation_ascending.coordinates", 1, 'LATITUDE_ASCENDING'],
    ["$geolocation_ascending_transmitted.coordinates", 0, 'LONGITUDE_ASCENDING_TRANSMITTED'],
    ["$geolocation_ascending_transmitted.coordinates", 1, 'LATITUDE_ASCENDING_TRANSMITTED']
]
for pair in geopairs:
    if not (mongolist_geo(pair[0], pair[1]) == xarlist_num(pair[2])).all():
        print('mismatch on', pair[2])
    else:
        print('clean match on', pair[2])

# timestamp matches
timepairs = [
    ['timestamp', 'JULD_MIDPOINT'],
    ['timestamp_midpoint_transmitted', 'JULD_MIDPOINT_TRANSMITTED'],
    ['timestamp_descending', 'JULD_DESCENDING'],
    ['timestamp_ascending', 'JULD_ASCENDING'],
    ['timestamp_descending_transmitted', 'JULD_DESCENDING_TRANSMITTED'],
    ['timestamp_ascending_transmitted', 'JULD_ASCENDING_TRANSMITTED']
]

for pair in timepairs:
    if not (mongolist_date(pair[0]) == xarlist_date(pair[1])).all():
        print('mismatch on', pair[1])
    else:
        print('clean match on', pair[1])

# metadata matches
metapairs = [
    ['mission_flag', 'MISSION_FLAG'],
    ['platform', 'WMO_NUMBER'],
    ['positioning_system_flag', 'POSITIONING_SYSTEM_FLAG'],
    ['sensor_type_flag', 'SENSOR_TYPE_FLAG'],
    ['extrapolation_flag', 'EXTRAPOLATION_FLAG']
]
for pair in metapairs:
    mongometa = mongohist(pair[0])
    ncmeta = xarhist(pair[1])
    print(mongometa)
    print(ncmeta)
    if not mongometa == ncmeta:
        print('mismatch on', pair[1])
    else:
        print('clean match on', pair[1])

# data hash matches
datakeys = [
    "VELOCITY_ZONAL",
    "VELOCITY_MERIDIONAL",
    "VELOCITY_ZONAL_TRANSMITTED",
    "VELOCITY_MERIDIONAL_TRANSMITTED",
    "SPEED",
    "SPEED_TRANSMITTED",
    "DRIFT_PRES",
    "DRIFT_TEMP",
    "NUMBER_SURFACE_FIXES"
]

for i, k in enumerate(datakeys):
    dps = { x['_id']: x['d'] for x in list(db.trajectories.aggregate([{'$project':{'_id':1, 'd':{'$arrayElemAt':[{'$arrayElemAt':['$data',i]},0]}}}]))}
    xsum = 0
    msum = 0
    clean = True 
    for j in range(len(xar[k])):
        ID = str(int(xar['WMO_NUMBER'][j].item())) + '_' + stringcycle(xar['CYCLE_NUMBER'][j].item())
        if cleanup(xar[k][j].item()) is not None:
            xsum += cleanup(xar[k][j].item())
            msum += dps[ID]
            if xsum != msum:
                print(ID, cleanup(xar[k][i].item()), dps[ID], xsum, msum)
                clean = False
        else:
           print(j, len(xar[k]), xar[k][j].item(), dps[ID], xsum, msum)
    if clean:
        print('clean match on', k)

# do db.trajectoriesMeta.distinct('data_info') to see there's only one value across all metadata for data_info, looks correct.

