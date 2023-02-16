FROM python:3.9

RUN apt-get update -y
RUN apt-get install -y nano

RUN pip install numpy pandas xarray netcdf4 pymongo
WORKDIR traj
