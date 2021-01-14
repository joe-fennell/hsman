import setuptools
import os

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="hsman",
    version="0.1.0",
    author="Joseph T. Fennell",
    author_email="info@joefennell.org",
    description="Hyperspectral Data Manager",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/joe-fennell/hsman/",
    packages=setuptools.find_packages(),
    package_dir={'hsman': 'hsman/'},
    package_data={'hsman': ['default_config.yaml']},
    include_package_data=True,
    install_requires=[
          'xarray',
          'boto3',
          'pyyaml',
          'numpy',
          'dask',
          'Click',
          'netcdf4',
          'gdal'
      ],
    entry_points='''
        [console_scripts]
        hsman=hsman.cli:hsman
    ''',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)
