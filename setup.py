from setuptools import setup

setup(
    name="dssatservice",
    version='0.0.1',
    packages=['dssatservice', 'dssatservice.data', 'dssatservice.ui'],
    py_modules=["dssatservice.database", "dssatservice.dssat"],
)
