#!/usr/bin/env python
import click
from hsman import scrape, config
from hsman import ingest as _ingest
import logging
import os


@click.group()
def hsman():
    """
    HSMAN
    ----------------------------------------

    This program is for running preprocessing on aerial and satellite imagery
    for storage. By default it handles a number of specific
    use cases:

    - HySpex VNIR and SWIR hyperspectral data, preprocessed with QUAC
    atmospheric correction submodule

    - Digital Surface and Terrain models produced by Correlator3D

    - RGB orthomosaic imagery.

    The file naming convention used by 2ExcelGeo is used extensively for file
    searching and organisation into missions (i.e. the Region-Of-Interest at a
    specific point in time) and product types. Other conventions can be
    accomodated by adapting the config file located at

        ~/.georis/config.yaml
    """
    config.logger()


@hsman.command()
def init():
    """
    Initialises the required files and folders needed for georis. This should
    be run before any of the other scripts and can be run again without
    causing any problems.
    """
    config.get_config()
    logging.info('GeoRIS initialised')


@hsman.command()
@click.argument('directory',
                type=click.Path('rb', file_okay=False,
                                resolve_path=True))
def ingest(directory):
    """
    Searches DIRECTORY for files matching the specification in the
    config and checks for any preprocessing steps necessary.

    Recipes with the 'RGB' and 'DSM' rasdaman_ingredients flag in the config
    file will be copied directly.

    'HSI' recipes will be first converted to NetCDF before ingestion. This also
    allows correct parsing of the wavelength dimension as well as infilling any
    missing wavelengths with NODATA (removed during preprocessing).

    This program generates all preprocessed data and files as specified in the
    config.
    After running this script, ingest into RASDAMAN by running the shell scipt
    `wcst_import_all.sh`. This is installed by default with this python module.
    """
    # Arg parsing, check is a directory
    target_dir = click.format_filename(directory)
    if not os.path.isdir(target_dir):
        raise ValueError('{} is not a directory'.format(target_dir))
    logging.info('Loading config...')
    recipes = config.get_config()['raster_dataset_recipes']
    if len(recipes) < 1:
        raise RuntimeError('No recipes supplied in config file')
    logging.info('{} recipes found'.format(len(recipes)))
    logging.info('Searching {} for relevant files...'.format(target_dir))
    for recipe_name, recipe in recipes.items():
        logging.info('Searching for files matching the {} recipe...'.format(
            recipe_name))
        try:
            _all_data = scrape.find_dataset_files(target_dir,
                                                  recipe_name)['data_path']
            # iterate all missions found
            logging.info('Found {} missions with matching files'.format(
                len(_all_data)))
            for mission_name, data_files in _all_data.items():
                logging.info('Processing recipe {} for mission {}...'.format(
                    recipe_name, mission_name
                ))
                coverage_id = scrape.generate_coverage_id(mission_name,
                                                          recipe_name)
                if recipe['ingest_type'] == 'hsi':
                    _ingest.ingest_hsi(data_files, coverage_id,
                                       recipe['dtype'])

                if recipe['ingest_type'] == 'image':
                    _ingest.ingest_image(data_files, coverage_id)
        except ValueError:
            logging.info('No datasets found matching {} template'.format(
                recipe_name))


@hsman.command()
def clean():
    """
    Removes empty datasets
    """
    config.remove_empty_datasets()
    logging.info('All empty datasets removed')
