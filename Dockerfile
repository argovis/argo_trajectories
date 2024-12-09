FROM python:3.9

RUN apt-get update -y
RUN apt-get install -y nano

RUN pip install numpy pandas xarray netCDF4 pymongo
WORKDIR /traj
#COPY Scripps_Argo_velocities_allPres_200101_202012_02132023.tap Scripps_Argo_velocities_allPres_200101_202012_02132023.tap
COPY trajectories_J0NK3F7V.nc trajectories_J0NK3F7V.nc
COPY loadtraj.py loadtraj.py
COPY doublecheck.py doublecheck.py

RUN chown -R 1000660000 /traj

CMD python loadtraj.py
