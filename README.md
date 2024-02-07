This contains all the scripts, modules, and functions required to run the service and create the database that will run the service. The projcet is structure in modules and submodules as any other python package. The structure was defined so that each different component of the service will have a different modules. This way we have modules for:
* Data download, transformation and ingestion (`data` module)
* Running the model
* 

## data module
Data module is divided in three submodules: `download`, `database`, `ingest`, and `transform`