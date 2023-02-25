FROM python:3.9

RUN apt-get update -y
RUN apt-get install -y nano

RUN pip install numpy pandas xarray netCDF4 pymongo
WORKDIR traj
COPY . .
CMD python loadtraj.py
