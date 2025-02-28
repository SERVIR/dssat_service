
The modules in dssatservice handle the setting-up and operation of the service. There are four main modules, each handles different operations in the service. The dssat module handles the model run. The database module handles all the database operations. The data module handles the data downloading, transformation, and ingestion operations. Finally, the ui module handles the interaction between the user and the service; therefore, it has some classes that manage the django session, store user defined parameters, and generate the plots. All modules and functions are well documented using docstrings.

The PDF documentation in this repo (DSSAT Service documentation final.pdf) will walk you through the service setup. Running the functions in the debug.py file in the order they are presented must be enough to create and populate all the database tables required for the service. The debug.py file shows an example of setting the service for Rwanda at te admin1 level. The data_example.zip file contains all the necesary input files to set the service for Rwanda.

## License and Distribution

DSSAT Service is distributed by SERVIR under the terms of the MIT License. See
[LICENSE](https://github.com/SERVIR/SAMS/blob/master/LICENSE) in this directory for more information.

## Privacy & Terms of Use

DSSAT Service abides to all of SERVIR's privacy and terms of use as described
at [https://servirglobal.net/Privacy-Terms-of-Use](https://servirglobal.net/Privacy-Terms-of-Use).