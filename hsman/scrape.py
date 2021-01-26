"""
Tools for preprocessing datasets prior to ingestion
"""
from .config import get_config
import os


def find_dataset_files(dir, recipe_name, config=None):
    """
    Returns a filepath dictionary for all files found matching the recipes.

    Parameters
    ----------
    dir : str
        Path to root directory, containing the files

    recipe_name : str
        Name of recipe

    config : dict, optional
        Default reads from config file

    Returns
    -------
    datasets : dict
        a dict with two entries (data_path, quality_path)
    """
    config = get_config(config)
    try:
        # clean entry to only contain relevant tags
        params = _clean_ds(config['raster_dataset_recipes'][recipe_name])
    except KeyError:
        raise ValueError('recipe {} not found'.format(recipe_name))

    data_matches = _search_directory(dir,
                                     params['data_flag'],
                                     params['data_suffix'])
    if len(data_matches) < 1:
        raise ValueError('No matching directory found for {}'
                         .format(params['data_flag']))

    # if params['quality_flag'] is not None:
    #     quality_matches = _match_quality_paths(data_matches, dir, params)
    #     return {'data_path': data_matches,
    #             'quality_path': quality_matches}
    # else:
    return {'data_path': data_matches}


def generate_coverage_id(dataset_name, recipe_name):
    """
    Generate a coverage ID from dataset and recipe name

    Parameters
    ----------
    dataset_name : str
        string name

    recipe_name : str
        name of recipe

    Returns
    -------
    coverage_ID : str
        coverage ID for dataset-recipe combination
    """
    return ''.join(dataset_name.split('-')) + '_' + recipe_name


def _clean_ds(ds):
    # checks the recipe contains a data_flag and returns only data
    # and quality flags
    out = {k: ds.get(k) for k in ['data_flag',
                                  'quality_flag',
                                  'data_suffix']}
    if out['data_flag'] is None:
        raise ValueError('data_flag parameter not found')
    if out['data_suffix'] is None:
        raise ValueError('data_suffix parameter not found')
    return out


def _search_directory(dir, tag, ext):
    # searches directory tree for any files containing tag
    matches = {}
    for root, _, files in os.walk(dir):
        for file in files:
            if (tag in file) & file.endswith(ext):
                m = _get_mission(file)
                try:
                    matches[m].append(os.path.abspath(
                        os.path.join(root, file)))
                except KeyError:
                    matches[m] = [os.path.abspath(
                        os.path.join(root, file))]
    return matches


def _get_mission(x):
    # returns the mission from path string
    f = os.path.basename(x)
    if f == '':
        raise ValueError('{} is not a file'.format(x))
    return f.split('_')[0]


def _get_swath(x):
    # returns the swath from a path string
    f = os.path.basename(x)
    if f == '':
        raise ValueError('{} is not a file'.format(x))
    return f.split('_')[1]


def _is_same_swath(str1, str2):
    # compare swath IDs (found by position) and test same
    id1 = _get_swath(str1)
    id2 = _get_swath(str2)
    return id1 == id2


# def _match_quality_paths(data_matches, dir, params):
#     # produced identical dict to data_matches for corresponding quality
#     # files
#     quality_matches = _search_directory(dir,
#                                         params['quality_flag'],
#                                         params['data_suffix'])
#     matches = {}
#     for mission, m in data_matches.items():
#         for data_file in m:
#             qpath = None
#             for quality_file in quality_matches[mission]:
#                 if _is_same_swath(data_file, quality_file):
#                     qpath = quality_file
#             try:
#                 matches[mission].append(qpath)
#             except KeyError:
#                 matches[mission] = [qpath]
#     return matches
