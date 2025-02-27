from setuptools import setup

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name="dssatservice",
    version='0.0.1',
    packages=['dssatservice', 'dssatservice.data', 'dssatservice.ui'],
    py_modules=["dssatservice.database", "dssatservice.dssat"],
    install_requires=requirements
)
