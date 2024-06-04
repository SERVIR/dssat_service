from setuptools import setup

setup(
    name="dssatservice",
    package_dir={
        "dssatservice": "/home/dquintero/dssat_service/dssat_service/"
    },
    packages=['dssatservice', 'dssatservice.data', 'dssatservice.ui'],
    py_modules=["dssatservice.database", "dssatservice.dssat"]
)
