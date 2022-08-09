# Open Data Platform
** 	This project develops an approach to establish an open source data repository for multi modal tabular data using a MinIO S3 Data-Lake storage service in the backend and few other other micro services
**
## Repo Structure - Principle Components

All the required components should run  as individual services

###Storage service (Data Lake) 
### Data Management Service (Datawarehouse)
### Data Analysis
### visualization service (download portal and dashboard)

##Working structure


$$--------Working structure-------$$

each directory in the repo --> serves as new service that should be Integrated to the project

each new branch --> one new sub service to develop a functionality or a component

## Data Contract

This section sets the format for each kind of data that is stored in the Minio Server.

| Kind of data | Source | Format | Is Compressed? | Other allowed formats |
| ----------- | ---- | -------- | ------ | ---- |
| speech      | HoloLens sent over mqtt | CSV     | Optional | parquet |
| vision      | HoloLens camera | mp4.gz     |Mandatory| avi.gz |
| attention   | mqtt | CSV     |Optional| parquet |
| raw_eeg     | EEG sensors | XDF     |Optional| parquet |
| location    | Owntracks sent over mqtt | CSV     |Optional|  parquet |
| ui_event    | HoloLens screen events sent over mqtt | CSV     |Optional|  parquet |