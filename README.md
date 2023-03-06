# Argo trajectories

Scripts to manage parsing and loading Argo trajectory data from [https://doi.org/10.6075/J0FQ9WS6](https://doi.org/10.6075/J0FQ9WS6) into Argovis' MongoDB instance.

## Basic usage

 - Create a schema-enforced collection via [https://github.com/argovis/db-schema/blob/main/trajectories.py](https://github.com/argovis/db-schema/blob/main/trajectories.py)
 - Make sure `Scripps_Argo_velocities_allPres_200101_202012_02132023.tap` is in the root of your repo (not included in VC).
 - Build the container image named in `pod.yaml` from `Dockerfile`, and push to Docker Hub
 - Run `pod.yaml` or the equivalent Swarm container to repopulate.

## Integrity checking

Some sanity checks are performed in `doublecheck.py`. Change the default command in `Dockerfile` to run this script, and re-run as described for building the original database. Container logs will indicate any suspicious comparisons between mongoDB and the upstream file.
