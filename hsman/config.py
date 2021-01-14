"""
All configuration file generation and handling
"""

import logging
import os
import shutil
import yaml


def get_datasets():
    _datasets = os.listdir(DATA_PATH)
    datasets = {}
    for obj in _datasets:
        ds_path = os.path.join(DATA_PATH, obj)
        # if a directory and contains DATA dir
        if os.path.isdir(ds_path) and os.path.exists(os.path.join(ds_path,
                                                                  'DATA')):
            datasets[obj] = ds_path
    return datasets


def get_config(config=None):
    """
    Loads the config, by default available in the install directory

    Parameters
    ----------
    config : str, dict
        path to config file or config-like dictionary
    """
    _check_user_installed()
    if type(config) == dict:
        return config
    try:
        with open(config, 'r') as file:
            logging.debug('Using default config')
            return yaml.load(file, Loader=yaml.FullLoader)

    # if None supplied
    except TypeError:
        with open(CONFIG_PATH, 'r') as file:
            logging.debug('Using default config')
            return yaml.load(file, Loader=yaml.FullLoader)


def logger():
    logging.root.handlers = []
    level, path = _logging_params()
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(path),
            logging.StreamHandler()
            ])


def reset_config():
    """
    Resets config to default version
    """
    logging.debug('config reset to default')
    os.remove(CONFIG_PATH)
    _create_default_config()


def remove_empty_datasets():
    """
    Remove any directory in DATA_PATH that doesn't have any data files
    """
    datasets = get_datasets()
    for ds in datasets.values():
        contents = os.listdir(os.path.join(ds, 'DATA'))
        try:
            contents.remove('.DS_Store')
        except ValueError:
            pass
        if len(contents) == 0:
            shutil.rmtree(ds)
            logging.debug('Removing {}'.format(ds))


def set_collection_path(new_path):
    """
    Update the data collection path in the config

    Parameters
    ----------
    new_path : str
        path to the data collection
    """
    # test if path exists or is writeable
    if os.path.exists(new_path):
        logging.debug('collection path already exists')
    else:
        # if doesn't exist and not writeable, will raise PermissionError
        os.makedirs(new_path)
    update_config({'hsman_data_path': os.path.abspath(new_path)})


def update_config(updates):
    """
    Updates the config file with a new version of the config_dict

    Parameters
    ----------
    updates : dict
        dictionary with config parameters to add/replace
    """
    config = get_config()
    new_config = config.update(updates)
    with open(CONFIG_PATH, 'w') as file:
        return yaml.dump(new_config, file, Loader=yaml.FullLoader)


def _check_user_installed():
    if not os.path.isdir(USER_PATH):
        os.makedirs(os.path.join(os.environ['HOME'], '.hsman'))
        logging.debug('generated user .hsman dir')

    if not os.path.isfile(CONFIG_PATH):
        _create_default_config()
        logging.debug('generated user .hsman/config.yaml')


def _create_default_config():
    dst = os.path.join(USER_PATH, 'config.yaml')
    src = os.path.join(INSTALL_PATH, 'default_config.yaml')
    shutil.copyfile(src, dst)


def _logging_params():
    try:
        level = CONFIG['logging_level']
    except KeyError:
        level = 'logging.INFO'
    if level is None:
        return None, os.path.join(USER_PATH, 'hsman.log')
    return eval(level), os.path.join(USER_PATH, 'hsman.log')


INSTALL_PATH = os.path.abspath(os.path.dirname(__file__))
USER_PATH = os.path.abspath(os.path.join(os.environ['HOME'], '.hsman'))
CONFIG_PATH = os.path.join(USER_PATH, 'config.yaml')
CONFIG = get_config()
DATA_PATH = os.path.abspath(os.path.expanduser(CONFIG['hsman_data_path']))
