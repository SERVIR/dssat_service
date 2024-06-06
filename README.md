
This contains all the scripts, modules, and functions required to run the service and create the database that will run the service. The projcet is structure in modules and submodules as any other python package. The structure was defined so that each different component of the service will have a different modules. This way we have modules for:
* Data download, transformation and ingestion (`data` module)
* Running the model
* 

## data module
Data module is divided in three submodules: `download`, `database`, `ingest`, and `transform`

## License and Distribution

DSSAT Service is distributed by SERVIR under the terms of the MIT License. See
[LICENSE](https://github.com/SERVIR/SAMS/blob/master/LICENSE) in this directory for more information.

## Privacy & Terms of Use

DSSAT Service abides to all of SERVIR's privacy and terms of use as described
at [https://servirglobal.net/Privacy-Terms-of-Use](https://servirglobal.net/Privacy-Terms-of-Use).