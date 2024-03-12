#!/usr/bin/env python3

import os
import sys
import glob
import argparse
import yaml
import re

import logging
from textwrap import dedent
from datetime import datetime
from datetime import timedelta

import pprint
import subprocess

# Find the absolute paths to various directories.
from pathlib import Path
crnt_script_fp = Path(__file__).resolve()
# Find the path to the directory containing miscellaneous scripts for the SRW App.
# The index of .parents will have to be changed if this script is moved elsewhere
# in the SRW App's directory structure.
ush_dir = crnt_script_fp.parents[1]
# The directory in which the SRW App is cloned.  This is one level up from ush_dir.
home_dir = Path(os.path.join(ush_dir, '..')).resolve()

# Add ush_dir to the path so that python_utils can be imported.
sys.path.append(str(ush_dir))
from python_utils import (
    log_info,
    load_config_file,
)

# Add directories for accessing scripts/modules in the workflow-tools repo.
wt_src_dir = os.path.join(ush_dir, 'python_utils', 'workflow-tools', 'src')
sys.path.append(str(wt_src_dir))
wt_scripts_dir = os.path.join(ush_dir, 'python_utils', 'workflow-tools', 'scripts')
sys.path.append(str(wt_scripts_dir))
from templater import (
    set_template,
)

#
# Note:
# * BIAS, RHST, SS dont use thresholds.
# * AUC, BRIER, FBIAS, RELY use thresholds.
#
# For BIAS, RHST, and SS:
# ======================
# fcst_field    level_or_accum             threshold    metric types
# ----------    --------------             ---------    ------------
# apcp          03hr, 06hr                 none         RHST, SS
# cape          L0                         none         RHST
# dpt           2m                         none         BIAS, RHST, SS
# hgt           500mb                      none         RHST, SS
# refc          L0 (BIAS only)             none         BIAS, RHST, SS
# tmp           2m, 500mb, 700mb, 850mb    none         BIAS, RHST, SS
# wind          500mb, 700mb, 850mb        none         BIAS, RHST, SS
#
# For AUC, BRIER, FBIAS, and RELY:
# ===============================
# fcst_field    level_or_accum    threshold
# ----------    --------------    ---------
# apcp          03hr              gt0.0mm (AUC,BRIER,FBIAS,RELY), ge2.54mm (AUC,BRIER,FBIAS,RELY)
# dpt           2m                ge288K (AUC,BRIER,RELY), ge293K (AUC,BRIER)
# refc          L0                ge20dBZ (AUC,BRIER,FBIAS,RELY), ge30dBZ (AUC,BRIER,FBIAS,RELY), ge40dBZ (AUC,BRIER,FBIAS,RELY), ge50dBZ (AUC,BRIER,FBIAS)
# tmp           2m, 850mb         ge288K (AUC,BRIER,RELY), ge293K (AUC,BRIER,RELY), ge298K (AUC,BRIER,RELY), ge303K (RELY)
# wind          10m, 700mb        ge5mps (AUC,BRIER,RELY), ge10mps (AUC,BRIER,RELY)
#

def get_pprint_str(var, indent_str=''):
    """
    Function to format a python variable as a pretty-printed string and add
    indentation.

    Arguments:
    ---------
    var:
      A variable.

    indent_str:
      String to be added to the beginning of each line of the pretty-printed
      form of var.

    Returns:
    -------
    var_str:
      Formatted string containing contents of variable.
    """

    var_str = pprint.pformat(var, compact=True)
    var_str = var_str.splitlines(True)
    var_str = [indent_str + s for s in var_str]
    var_str = ''.join(var_str)

    return var_str


def get_thresh_info(thresh_in_config):
    """
    Function to extract and form various pieces of threshold-related
    information from the threshold specified on the command line.

    Arguments:
    ---------
    thresh_in_config:
      Threshold setting specified on the command line.

    Returns:
    -------
    thresh_info:
      Dictionary containing varous threshold-related variables.
    """

    msg_invalid_thresh_fmt = dedent(f"""
        The input threshold must be either an empty string or a string of the
        form
          <comp_oper><value><units>
        where <comp_oper> is a string of one or more characters representing a
        comparison operator (e.g. 'ge' for 'greater than or equal to'), <value>
        is a stirng of one or more digits and possibly a decimal representing
        the threshold value, and <units> is a string of zero or more characters
        representing the value's units (zero characters allowed to account for
        the case of a unitless value).  Check the specified threshold to ensure
        it has a valid format and rerun.  Stopping.
        """)

    # Initialize to empty strings.
    thresh_comp_oper = ''
    thresh_value = ''
    thresh_units = ''
    thresh_in_db = ''
    thresh_in_plot_title = ''

    # Get threshold comparison operator, value, and units using regular expression.
    thresh_parts = re.findall(r'([A-Za-z]+)(\d*\.*\d+)([A-Za-z]*)', thresh_in_config)

    # If thresh_parts is not empty, then at least some parts of the threshold
    # were extracted.  In this case, continue parsing.
    if thresh_parts:
        thresh_comp_oper = thresh_parts[0][0]
        thresh_value = thresh_parts[0][1]
        thresh_units = thresh_parts[0][2]

        thresh_comp_oper_to_xml = {'lt': '&lt;',
                                   'le': '&lt;=',
                                   'gt': '&gt;',
                                   'ge': '&gt;='}
        valid_vals_thresh_comp_oper = list(thresh_comp_oper_to_xml.keys())
        if thresh_comp_oper in valid_vals_thresh_comp_oper:
            thresh_comp_oper_xml = thresh_comp_oper_to_xml[thresh_comp_oper]
        else:
            msg = dedent(f"""
                Invalid value for threshold comparison operator:
                  thresh_comp_oper = {get_pprint_str(thresh_comp_oper)}
                Valid values for the comparison operator are:
                  valid_vals_thresh_comp_oper = {get_pprint_str(valid_vals_thresh_comp_oper)}
                Specified threshold is:
                  thresh_in_config = {get_pprint_str(thresh_in_config)}""") + \
                msg_invalid_thresh_fmt
            logging.error(msg)
            raise ValueError(msg)

        # Form the threshold in the way that it appears in the database (for
        # METviewer to find).
        thresh_in_db = ''.join([thresh_comp_oper_xml, thresh_value])

        # For certain units, the character "p" represents "per", so in the plot
        # title, it should be replaced with a "/".  Make replacement here.
        thresh_in_plot_title = thresh_units.replace('mps', 'm/s')
        thresh_in_plot_title = thresh_units.replace('Jpkg', 'J/kg')
        # Form the threshold as it will appear in the plot title.  The
        #   filter(None, [...])
        # causes any empty strings in the list to be dropped so that unnecessary
        # spaces (separators) are not inadvertantly added.
        thresh_in_plot_title = ' '.join(filter(None,
                               [thresh_comp_oper_xml, thresh_value, thresh_in_plot_title]))

    # If thresh_parts is empty but thresh_in_config is not, then something
    # must have been wrong with thresh_in_config that caused thresh_parts
    # to be empty.
    elif thresh_in_config:
        msg = dedent(f"""
            Specified input threshold does not have a valid format:
              thresh_in_config = {get_pprint_str(thresh_in_config)}""") + \
            msg_invalid_thresh_fmt
        logging.error(msg)
        raise ValueError(msg)

    # Create a dictionary containing the values to return and return it.
    thresh_info = {'in_config': thresh_in_config,
                   'comp_oper': thresh_comp_oper,
                   'value': thresh_value,
                   'units': thresh_units,
                   'in_db': thresh_in_db,
                   'in_plot_title': thresh_in_plot_title}
    return thresh_info


def get_valid_vx_plot_params(valid_vx_plot_params_config_fp):
    """
    Function to read in valid values of verification (vx) plotting parameters.

    Arguments:
    ---------
    valid_vx_plot_params_config_fp:
      Path to yaml configuration file containing valid values of vx plotting
      parameters.

    Returns:
    -------
    valid_vx_plot_params:
      Dictionary containing processed set of valid values of plotting parameters.
    """

    # Load the yaml file that specifies valid values for various verification
    # (vx) plotting parameters.
    valid_vx_plot_params = load_config_file(valid_vx_plot_params_config_fp)

    # Get the list of valid vx metrics.  Then define local dictionaries
    # containing values that depend on the metric.
    valid_vx_metrics = valid_vx_plot_params['valid_vx_metrics'].keys()
    vx_metric_long_names = {}
    vx_metric_needs_thresh = {}
    for metric in valid_vx_metrics:
        vx_metric_long_names[metric] = valid_vx_plot_params['valid_vx_metrics'][metric]['long_name']
        vx_metric_needs_thresh[metric] = valid_vx_plot_params['valid_vx_metrics'][metric]['needs_thresh']

    # Get list of valid forecast fields.
    valid_fcst_fields = valid_vx_plot_params['valid_fcst_fields'].keys()

    # Get list of valid forecast field levels.  This is a list of all levels
    # regardless of field (i.e. a "master" list).
    valid_fcst_levels_to_levels_in_db = valid_vx_plot_params['valid_fcst_levels_to_levels_in_db']
    valid_fcst_levels_all_fields = list(valid_fcst_levels_to_levels_in_db.keys())

    # Form local dictionaries containing valid values that depend on the
    # forecast field.
    fcst_field_long_names = {}
    valid_fcst_levels_by_fcst_field = {}
    valid_units_by_fcst_field = {}
    for field in valid_fcst_fields:

        # Get and save long name of current the forecast field.
        fcst_field_long_names[field] \
        = valid_vx_plot_params['valid_fcst_fields'][field]['long_name']

        # Get and save the list of valid units for the current forecast field.
        valid_units_by_fcst_field[field] \
        = valid_vx_plot_params['valid_fcst_fields'][field]['valid_units']

        # Get and save the list of valid levels/accumulations for the current
        # forecast field.
        valid_fcst_levels_by_fcst_field[field] \
        = valid_vx_plot_params['valid_fcst_fields'][field]['valid_fcst_levels']

        # Make sure all the levels/accumulations specified for the current
        # forecast field are in the master list of valid levels and accumulations.
        for loa in valid_fcst_levels_by_fcst_field[field]:
            if loa not in valid_fcst_levels_all_fields:
                msg = dedent(f"""
                    One of the levels or accumulations (loa) in the set of valid forecast
                    levels and accumulations for the current forecast field (field) is not
                    in the master list of valid forecast levels and accumulations
                    (valid_fcst_levels_all_fields):
                      field = {get_pprint_str(field)}
                      loa = {get_pprint_str(loa)}
                      valid_fcst_levels_all_fields = """) + \
                    get_pprint_str(valid_fcst_levels_all_fields,
                                   ' '*(5 + len('valid_fcst_levels_all_fields'))).lstrip() + \
                    dedent(f"""
                    The master list of valid levels and accumulations as well as the list of
                    valid levels and accumulations for the current forecast field can be
                    found in the following configuration file:
                      valid_vx_plot_params_config_fp = {get_pprint_str(valid_vx_plot_params_config_fp)}
                    Please modify this file and rerun.  Stopping.
                    """)
                logging.error(msg)
                raise ValueError(msg)

    # Get dictionary containing the available METviewer color codes.  This
    # is a subset of all available colors in METviewer (of which there are
    # thousands) which we allow the user to specify as a plot color for the
    # the models to be plotted.  In this dictionary, the keys are the color
    # names (e.g. 'red'), and the values are the corresponding codes in
    # METviewer.  If more colors are needed, they should be added to the
    # list in the valid vx plotting parameters configuration file.
    avail_mv_colors_codes = valid_vx_plot_params['avail_mv_colors_codes']

    # Create dictionary containing valid choices for various parameters.
    # This is needed by the argument parsing function below.
    choices = {}
    choices['fcst_field'] = sorted(valid_fcst_fields)
    choices['fcst_level'] = valid_fcst_levels_all_fields
    choices['vx_metric'] = sorted(valid_vx_metrics)
    choices['color'] = list(avail_mv_colors_codes.keys())

    # Create dictionary containing return values and return it.
    valid_vx_plot_params = {}
    valid_vx_plot_params['valid_vx_plot_params_config_fp'] = valid_vx_plot_params_config_fp
    valid_vx_plot_params['valid_fcst_levels_to_levels_in_db'] = valid_fcst_levels_to_levels_in_db
    valid_vx_plot_params['fcst_field_long_names'] = fcst_field_long_names
    valid_vx_plot_params['valid_fcst_levels_by_fcst_field'] = valid_fcst_levels_by_fcst_field
    valid_vx_plot_params['valid_units_by_fcst_field'] = valid_units_by_fcst_field
    valid_vx_plot_params['vx_metric_long_names'] = vx_metric_long_names
    valid_vx_plot_params['vx_metric_needs_thresh'] = vx_metric_needs_thresh
    valid_vx_plot_params['avail_mv_colors_codes'] = avail_mv_colors_codes
    valid_vx_plot_params['choices'] = choices

    return valid_vx_plot_params


def get_database_info(mv_databases_config_fp):
    """
    Function to read in information about the METviewer database from which
    verification (vx) metrics will be plotted.

    Arguments:
    ---------
    mv_databases_config_fp:
      Path to yaml METviewer database configuration file.

    Returns:
    -------
    mv_databases_dict:
      Dictionary containing information about METviewer databases.
    """

    # Load the yaml file containing database information.
    mv_databases_dict = load_config_file(mv_databases_config_fp)

    return mv_databases_dict


def parse_args(argv, valid_vx_plot_params):
    """
    Function to parse arguments passed to the make_single_mv_vx_plot()
    function.

    Arguments:
    ---------
    argv:
      Arguments passed to make_single_mv_vx_plot().

    valid_vx_plot_params:
      Dictionary of valid values for various verification (vx) plotting
      parameters.

    Returns:
    -------
    cla:
      Namespace object containing parsed command line arguments and related
      information.
    """

    choices = valid_vx_plot_params['choices']

    parser = argparse.ArgumentParser(description=dedent(f"""
        Function to generate an xml file that METviewer can read in order
        to create a verification (vx) plot.
        """))

    parser.add_argument('--mv_host',
                        type=str,
                        required=True,
                        help='Host (name of machine) on which METviewer is installed')

    parser.add_argument('--mv_machine_config_fp',
                        type=str,
                        required=False, default='mv_machines.yaml',
                        help='METviewer machine (host) configuration file')

    parser.add_argument('--mv_databases_config_fp',
                        type=str,
                        required=False, default='mv_databases.yaml',
                        help='METviewer database configuration file')

    parser.add_argument('--mv_database_name',
                        type=str,
                        required=True,
                        help='Name of METviewer database')

    # Find the path to the directory containing the clone of the SRW App.
    # The index of .parents will have to be changed if this script is moved
    # elsewhere in the SRW App's directory structure.
    crnt_script_fp = Path(__file__).resolve()
    home_dir = crnt_script_fp.parents[2]
    expts_dir = Path(os.path.join(home_dir, '../expts_dir')).resolve()
    parser.add_argument('--mv_output_dir',
                        type=str,
                        required=False, default=os.path.join(expts_dir, 'mv_output'),
                        help='Directory in which to place output (e.g. plots) from METviewer')

    parser.add_argument('--model_names_short', nargs='+',
                        type=str.lower,
                        required=True,
                        help='Names of models to include in xml and plots')

    parser.add_argument('--model_colors', nargs='+',
                        type=str,
                        required=False, default=choices['color'],
                        choices=choices['color'],
                        help='Color of each model used in verification (vx) metrics plots')

    parser.add_argument('--vx_metric',
                        type=str.lower,
                        required=True,
                        choices=choices['vx_metric'],
                        help='Name of vx metric to plot')

    parser.add_argument('--incl_ens_means',
                        required=False, action=argparse.BooleanOptionalAction, default=argparse.SUPPRESS,
                        help='Flag for including ensemble mean curves in plot')

    parser.add_argument('--fcst_init_info', nargs=3,
                        type=str,
                        required=True,
                        help=dedent(f"""
                            Initialization time of first forecast (in YYYYMMDDHH), number of forecasts,
                            and forecast initialization interval (in HH)
                            """))

    parser.add_argument('--fcst_len_hrs',
                        type=int,
                        required=True,
                        help='Forecast length (in integer hours)')

    parser.add_argument('--fcst_field',
                        type=str.lower,
                        required=True,
                        choices=choices['fcst_field'],
                        help='Name of forecast field to verify')

    parser.add_argument('--level_or_accum',
                        type=str,
                        required=False,
                        choices=choices['fcst_level'],
                        help='Vertical level or accumulation period')

    parser.add_argument('--threshold',
                        type=str,
                        required=False, default='',
                        help='Threshold for the specified forecast field')

    # Parse the arguments.
    cla = parser.parse_args(argv)

    # Empty strings are included in this concatenation to force insertion
    # of delimiter.
    msg = dedent(f"""
        List of arguments passed to script:
          cla = """) + \
        get_pprint_str(vars(cla), ' '*(5 + len('cla'))).lstrip()
    logging.debug(msg)

    return cla


def generate_metviewer_xml(cla, valid_vx_plot_params, mv_databases_dict):
    """
    Function that generates an xml file that METviewer can read (to be used
    elsewhere to create a verification (vx) plot).

    Arguments:
    ---------
    cla:
      Command-line arguments

    valid_vx_plot_params:
      Dictionary of valid values for various vx plotting parameters.

    mv_databases_dict:
      Dictionary containing information about METviewer databases.

    Returns:
    -------
    [unnamed]:
      Path to yaml METviewer machine configuration file.

    output_xml_fp:
      Path to xml generated by this function.
    """

    # Get valid values for various verification (vx) plotting parameters.
    valid_vx_plot_params_config_fp = valid_vx_plot_params['valid_vx_plot_params_config_fp']
    valid_fcst_levels_to_levels_in_db = valid_vx_plot_params['valid_fcst_levels_to_levels_in_db']
    fcst_field_long_names = valid_vx_plot_params['fcst_field_long_names']
    valid_fcst_levels_by_fcst_field = valid_vx_plot_params['valid_fcst_levels_by_fcst_field']
    valid_units_by_fcst_field = valid_vx_plot_params['valid_units_by_fcst_field']
    vx_metric_long_names = valid_vx_plot_params['vx_metric_long_names']
    vx_metric_needs_thresh = valid_vx_plot_params['vx_metric_needs_thresh']
    avail_mv_colors_codes = valid_vx_plot_params['avail_mv_colors_codes']

    # Load the machine configuration file into a dictionary and find in it the
    # machine specified on the command line.
    mv_machine_config_fp = Path(os.path.join(cla.mv_machine_config_fp)).resolve()
    mv_machine_config = load_config_file(mv_machine_config_fp)

    all_hosts = sorted(list(mv_machine_config.keys()))
    if cla.mv_host not in all_hosts:
        msg = dedent(f"""
            The machine/host specified on the command line (cla.mv_host) does not
            have a corresponding entry in the METviewer host configuration file
            (mv_machine_config_fp):
              cla.mv_host = {get_pprint_str(cla.mv_host)}
              mv_machine_config_fp = {get_pprint_str(mv_machine_config_fp)}
            Machines that do have an entry in the host configuration file are:
              all_hosts = """) + \
            get_pprint_str(all_hosts, ' '*(5 + len('all_hosts'))).lstrip() + \
            dedent(f"""
            Either run on one of these hosts, or add an entry in the configuration
            file for '{cla.mv_host}'.  Stopping.
            """)
        logging.error(msg)
        raise ValueError(msg)

    mv_machine_config_dict = mv_machine_config[cla.mv_host]

    # Make sure that the database specified on the command line exists in the
    # list of databases in the database configuration file.
    if cla.mv_database_name not in mv_databases_dict.keys():
        msg = dedent(f"""
            The database specified on the command line (cla.mv_database_name) is not
            in the set of METviewer databases specified in the database configuration
            file (cla.mv_databases_config_fp):
              cla.mv_database_name = {get_pprint_str(cla.mv_database_name)}
              cla.mv_databases_config_fp = {get_pprint_str(cla.mv_databases_config_fp)}
            Stopping.
            """)
        logging.error(msg)
        raise ValueError(msg)

    # Extract the METviewer database information.
    database_info = mv_databases_dict[cla.mv_database_name]
    valid_threshes_in_db = list(database_info['valid_threshes'])
    model_info = list(database_info['models'])

    # METviewer expects the model (long) names passed to it to be in alphabetic
    # order.  Thus, the list of model (short) names passed via the command
    # line must be rearranged to make sure it is in the correct order.  We
    # do that later below, but for simplicity, we now also rearrange the list
    # of model dictionaries available in the database (model_info) so that
    # the dictionaries are listed in alphabetic order based on the long name
    # of the model, i.e. the name that is recognized by METviewer.
    model_info = sorted(model_info, key=lambda d: d['long_name'])

    num_models_avail_in_db = len(model_info)
    model_names_avail_in_db = [model_info[i]['long_name'] for i in range(0,num_models_avail_in_db)]
    model_names_short_avail_in_db = [model_info[i]['short_name'] for i in range(0,num_models_avail_in_db)]

    # Make sure model names on the command line are not duplicated because
    # METviewer will throw an error in this case.  Create a set (using curly
    # braces) to store duplicate values.  Note that a set must be used here
    # so that duplicate values are not duplicated!
    duplicates = {m for m in cla.model_names_short if cla.model_names_short.count(m) > 1}
    if len(duplicates) > 0:
        msg = dedent(f"""
            A model can appear only once in the set of models to plot specified on
            the command line.  However, the following models are duplicated:
              duplicates = {get_pprint_str(duplicates)}
            Please remove duplicated models from the command line and rerun.
            Stopping.
            """)
        logging.error(msg)
        raise ValueError(msg)

    # Make sure that the models specified on the command line exist in the
    # database.
    for model_name_short in cla.model_names_short:
        if model_name_short not in model_names_short_avail_in_db:
            msg = dedent(f"""
                A model specified on the command line (model_name_short) is not included
                in the entry for the specified database (cla.mv_database_name) in the
                METviewer database configuration file (cla.mv_databases_config_fp)
                  cla.mv_databases_config_fp = {get_pprint_str(cla.mv_databases_config_fp)}
                  cla.mv_database_name = {get_pprint_str(cla.mv_database_name)}
                  model_name_short = {get_pprint_str(model_name_short)}
                Models that are included in the database configuration file are:
                  model_names_short_avail_in_db = """) + \
                get_pprint_str(model_names_short_avail_in_db,
                               ' '*(5 + len('model_names_short_avail_in_db'))).lstrip() + \
                dedent(f"""
                Either change the command line to specify only one of these models, or
                add the new model to the database configuration file (the latter approach
                will work only if the new model actually exists in the METviewer database).
                Stopping.
                """)
            logging.error(msg)
            raise ValueError(msg)

    # If the threshold specified on the command line is not an empty string,
    # make sure that it is one of the valid ones for this database.
    if (cla.threshold) and (cla.threshold not in valid_threshes_in_db):
        msg = dedent(f"""
            The specified threshold is not in the list of valid thresholds for the
            specified database.  Database is:
              cla.mv_database_name = {get_pprint_str(cla.mv_database_name)}
            Threshold is:
              cla.threshold = {get_pprint_str(cla.threshold)}
            The list of valid thresholds for this database is:
              valid_threshes_in_db = """) + \
            get_pprint_str(valid_threshes_in_db,
                           ' '*(5 + len('valid_threshes_in_db'))).lstrip() + \
            dedent(f"""
            Make sure the specified threshold is one of the valid ones, or, if it
            exists in the database, add it to the 'valid_threshes' list in the
            METviewer database configuration file given by:
              cla.mv_databases_config_fp = {get_pprint_str(cla.mv_databases_config_fp)}
            Stopping.
            """)
        logging.error(msg)
        raise ValueError(msg)

    # Get the names of those models in the database that are to be plotted.
    inds_models_to_plot = [model_names_short_avail_in_db.index(m) for m in cla.model_names_short]
    num_models_to_plot = len(inds_models_to_plot)
    model_names_in_db_to_plot = [model_info[i]['long_name'] for i in inds_models_to_plot]

    # Alphabetically sort the list of model (long) names to plot.  This is
    # necesary because METviewer expects the model (long) names passed to it
    # to be in alphabetic order.  Then reset the indices of these models into
    # the model_info dictionary to make sure this alphabetical resorting is
    # taken into account.
    model_names_in_db_to_plot = sorted(model_names_in_db_to_plot) 
    inds_models_to_plot = [model_names_avail_in_db.index(m) for m in model_names_in_db_to_plot]

    # Now reset the model-related arguments on the command line to account
    # for the alphabetical resorting above.
    model_short_names_orig = cla.model_names_short
    cla.model_names_short = [model_info[i]['short_name'] for i in inds_models_to_plot]
    remap_inds = [model_short_names_orig.index(m) for m in cla.model_names_short]
    model_colors_orig = cla.model_colors
    cla.model_colors = [model_colors_orig[i] for i in remap_inds]

    # Get the number of ensemble members for each model and make sure all are
    # positive.
    num_ens_mems_by_model = [model_info[i]['num_ens_mems'] for i in inds_models_to_plot]
    for i,model in enumerate(cla.model_names_short):
        n_ens = num_ens_mems_by_model[i]
        if n_ens <= 0:
            msg = dedent(f"""
                The number of ensemble members for the current model must be greater
                than or equal to 0:
                  model = {get_pprint_str(model)}
                  n_ens = {get_pprint_str(n_ens)}
                Stopping.
                """)
            logging.error(msg)
            raise ValueError(msg)

    # Make sure that there are at least as many available colors as models to
    # plot.
    num_avail_colors = len(avail_mv_colors_codes)
    if num_models_to_plot > num_avail_colors:
        msg = dedent(f"""
            The number of models to plot (num_models_to_plot) must be less than
            or equal to the number of available colors:
              num_models_to_plot = {get_pprint_str(num_models_to_plot)}
              num_avail_colors = {get_pprint_str(num_avail_colors)}
            Either reduce the number of models to plot specified on the command
            line or add new colors in the following configuration file:
              valid_vx_plot_params_config_fp = {get_pprint_str(valid_vx_plot_params_config_fp)}
            Stopping.
            """)
        logging.error(msg)
        raise ValueError(msg)

    # Pick out the plot color associated with each model from the list of
    # available colors.  The following lists will contain the hex RGB color
    # codes of the colors to use for each model as well as the codes for
    # the light versions of those colors (needed for some types of metric
    # plots).
    model_color_codes = [avail_mv_colors_codes[m]['hex_code'] for m in cla.model_colors]
    model_color_codes_light = [avail_mv_colors_codes[m]['hex_code_light'] for m in cla.model_colors]

    # Set the initialization times for the forecasts.
    fcst_init_time_first = datetime.strptime(cla.fcst_init_info[0], '%Y%m%d%H')
    num_fcst_inits = int(cla.fcst_init_info[1])
    fcst_init_intvl_hrs = int(cla.fcst_init_info[2])
    fcst_init_intvl = timedelta(hours=fcst_init_intvl_hrs)
    fcst_init_times = list(range(0,num_fcst_inits))
    fcst_init_times = [fcst_init_time_first + i*fcst_init_intvl for i in fcst_init_times]
    fcst_init_times_YmDHMS = [i.strftime("%Y-%m-%d %H:%M:%S") for i in fcst_init_times]
    fcst_init_times_YmDH = [i.strftime("%Y-%m-%d %H") for i in fcst_init_times]
    fcst_init_info_str = ''.join([f'fcst_init_times = ',
                                  f'[{fcst_init_times_YmDH[0]}Z, ',
                                  f'{fcst_init_intvl_hrs} hr, ',
                                  f'{fcst_init_times_YmDH[-1]}Z] ',
                                  f'(num_fcst_inits = {num_fcst_inits})'])

    msg = dedent(f"""
        Forecast initialization times:
          fcst_init_times_YmDHMS = """) + \
        get_pprint_str(fcst_init_times_YmDHMS,
                       ' '*(5 + len('fcst_init_times_YmDHMS'))).lstrip() + \
        '\n'
    logging.debug(msg)

    if ('incl_ens_means' not in cla):
        incl_ens_means = False
        if (cla.vx_metric == 'bias'): incl_ens_means = True
    else:
        incl_ens_means = cla.incl_ens_means
    # Apparently we can just reset or create incl_ens_means within the cla Namespace
    # as follows:
    cla.incl_ens_means = incl_ens_means

    valid_fcst_levels_or_accums = valid_fcst_levels_by_fcst_field[cla.fcst_field]
    if cla.level_or_accum not in valid_fcst_levels_or_accums:
        msg = dedent(f"""
            The specified forecast level or accumulation is not compatible with the
            specified forecast field:
              cla.fcst_field = {get_pprint_str(cla.fcst_field)}
              cla.level_or_accum = {get_pprint_str(cla.level_or_accum)}
            Valid options for forecast level or accumulation for this forecast field
            are:
              valid_fcst_levels_or_accums = """) + \
            get_pprint_str(valid_fcst_levels_or_accums,
                           ' '*(5 + len('valid_fcst_levels_or_accums'))).lstrip() + \
            dedent(f"""
            Stopping.
            """)
        logging.error(msg)
        raise ValueError(msg)

    # Parse the level/accumulation specified on the command line (cla.level_or_accum)
    # to obtain its value and units.  The returned value is a list.  If the regular
    # expression doesn't match anything in cla.level_or_accum (e.g. if the latter is
    # set to 'L0'), an empty list will be returned.  In that case, the else portion
    # of the if-else construct below will set loa_value and loa_units to empty strings.
    loa = re.findall(r'(\d*\.*\d+)([A-Za-z]+)', cla.level_or_accum)

    if loa:
        # Parse specified level/threshold to obtain its value and units.
        loa_value, loa_units = list(loa[0])
    else:
        loa_value = ''
        loa_units = ''

    valid_loa_units = ['', 'h', 'm', 'mb']
    if loa_units not in valid_loa_units:
        msg = dedent(f"""
            Unknown units (loa_units) for level or accumulation:
              loa_units = {get_pprint_str(loa_units)}
            Valid units are:
              valid_loa_units = {get_pprint_str(valid_loa_units)}
            Related variables:
              cla.level_or_accum = {get_pprint_str(cla.level_or_accum)}
              loa_value = {get_pprint_str(loa_value)}
              loa_value_no0pad = {get_pprint_str(loa_value_no0pad)}
            Stopping.
            """)
        logging.error(msg)
        raise ValueError(msg)

    loa_value_no0pad = loa_value.lstrip('0')
    width_0pad = 0
    if loa_units == 'h':
        width_0pad = 2
    elif loa_units == 'm':
        width_0pad = 2
    elif loa_units == 'mb':
        width_0pad = 3
    elif (loa_units == '' and cla.level_or_accum == 'L0'):
        msg = dedent(f"""
            Since the specified level/accumulation is '{cla.level_or_accum}', we set loa_units to an empty
            string:
              cla.level_or_accum = {get_pprint_str(cla.level_or_accum)}
              loa_units = {get_pprint_str(loa_units)}
            Related variables:
              loa_value = {get_pprint_str(loa_value)}
              loa_value_no0pad = {get_pprint_str(loa_value_no0pad)}
            """)
        logging.debug(msg)

    loa_value_0pad = loa_value_no0pad.zfill(width_0pad)
    msg = dedent(f"""
        Level/accumulation parameters have been set as follows:
          loa_value = {get_pprint_str(loa_value)}
          loa_value_no0pad = {get_pprint_str(loa_value_no0pad)}
          loa_value_0pad = {get_pprint_str(loa_value_0pad)}
          loa_units = {get_pprint_str(loa_units)}
        """)
    logging.debug(msg)

    if (not vx_metric_needs_thresh[cla.vx_metric]) and (cla.threshold):
        no_thresh_metrics = [key for key,val in vx_metric_needs_thresh.items() if val]
        msg = dedent(f"""
            A threshold is not needed for the following verification (vx) metrics:
              no_thresh_metrics = """) + \
            get_pprint_str(no_thresh_metrics,
                           ' '*(5 + len('no_thresh_metrics'))).lstrip() + \
            dedent(f"""
            Thus, the threshold passed via the '--threshold' option on the command
            line, i.e.
              cla.threshold = {get_pprint_str(cla.threshold)}
            will be reset to an empty string.
            """)
        logging.debug(msg)
        cla.threshold = ''

    # Extract and set various pieces of threshold-related information from
    # the specified threshold.
    thresh_info = get_thresh_info(cla.threshold)
    msg = dedent(f"""
        Dictionary containing threshold information has been set as follows:
          thresh_info = """) + \
        get_pprint_str(thresh_info, ' '*(5 + len('thresh_info'))).lstrip() + \
        '\n'
    logging.debug(msg)

    # Get the list of valid units for the specified forecast field.
    valid_units = valid_units_by_fcst_field[cla.fcst_field]
    # If the specified threshold is not empty and its units do not match any
    # of the ones in the list of valid units, error out.
    if (cla.threshold) and (thresh_info['units'] not in valid_units):
        msg = dedent(f"""
            The units specified in the threshold are not compatible with the list
            of valid units for this field.  The specified field and threshold are:
              cla.fcst_field = {get_pprint_str(cla.fcst_field)}
              cla.threshold = {get_pprint_str(cla.threshold)}
            The units extracted from the threshold are:
              thresh_info[units] = {get_pprint_str(thresh_info['units'])}
            Valid units for this forecast field are:
              valid_units = {get_pprint_str(valid_units)}
            Stopping.
            """)
        logging.error(msg)
        raise ValueError(msg)

    # Form the plot title.
    plot_title = ' '.join(filter(None,
                          [vx_metric_long_names[cla.vx_metric], 'for',
                           ''.join([loa_value, loa_units]), fcst_field_long_names[cla.fcst_field],
                           thresh_info['in_plot_title']]))

    # Form the job title needed in the xml.
    fcst_field_uc = cla.fcst_field.upper()
    var_lvl_str = ''.join(filter(None, [fcst_field_uc, loa_value, loa_units]))
    thresh_str = ''.join(filter(None, [thresh_info['comp_oper'], thresh_info['value'], thresh_info['units']]))
    var_lvl_thresh_str = '_'.join(filter(None, [var_lvl_str, thresh_str]))
    models_str = '_'.join(cla.model_names_short)
    job_title = '_'.join([cla.vx_metric, var_lvl_thresh_str, models_str])

    msg = dedent(f"""
        Various auxiliary string values:
          plot_title = {get_pprint_str(plot_title)}
          var_lvl_str = {get_pprint_str(var_lvl_str)}
          thresh_str = {get_pprint_str(thresh_str)}
          var_lvl_thresh_str = {get_pprint_str(var_lvl_thresh_str)}
          job_title = {get_pprint_str(job_title)}
          models_str = {get_pprint_str(models_str)}
        """)
    logging.debug(msg)

    # Get names of level/accumulation, threshold, and models as they are set
    # in the database.
    level_in_db = valid_fcst_levels_to_levels_in_db[cla.level_or_accum]

    line_types = list()
    for imod in range(0,num_models_to_plot):
        if incl_ens_means: line_types.append('b')
        line_types.extend(["l" for imem in range(0,num_ens_mems_by_model[imod])])

    line_widths = [1 for imod in range(0,num_models_to_plot) for imem in range(0,num_ens_mems_by_model[imod])]

    # Set the frequency of x-axis tick labels (xtick_label_freq).

    # Reliability and rank histogram plots do not have forecast hour on
    # the x-axis (reliability has forecast probability, which ranges from
    # 0 to 1, while rank histogram has bin number, which can vary).  For
    # these, we set xtick_label_freq to 0 to let METviewer decide how to
    # handle things.
    if cla.vx_metric in ['rely', 'rhist']:
        xtick_label_freq = 0
    # The remaining plot (i.e. vx metric) types have forecast hour on the
    # x-axis.  For these, there are several aspects of the plotting to
    # consider for setting xtick_label_freq.
    elif cla.vx_metric in ['auc', 'bias', 'brier', 'fbias', 'ss']:

        # Create a list of the forecast hours at which the metric is available
        # (vx_metric_fcst_hrs).  This requires first determining the metric's
        # availability/recurrence interval (in hours), i.e. the time interval
        # with which the metric is calculated (vx_metric_avail_intvl_hrs).  This
        # in turn depends on the availability interval of both the observations
        # and the forecast fields are available.
        #
        # The default is to assume that the observations and forecasts are
        # available every hour.  Thus, the metric is available every hour.
        vx_metric_avail_intvl_hrs = 1
        # If the level is actually an accumulation, reset the metric availability
        # interval to the accumulation interval.
        if (cla.level_or_accum in ['01h', '03h', '06h', '24h']):
            vx_metric_avail_intvl_hrs = int(loa_value)
        # If the level is an upper air location, we consider values only at 12Z
        # because the number of observations at other hours of the day is very
        # low (so metrics are unreliable).  Thus, we set vx_metric_avail_intvl_hrs
        # to 12.
        elif (cla.level_or_accum in ['500mb', '700mb', '850mb']):
            vx_metric_avail_intvl_hrs = 12

        # Use the metric availability interval to set the forecast hours at
        # which the metric is available.  Then find the number of such hours.
        vx_metric_fcst_hrs = list(range(0, cla.fcst_len_hrs+1, vx_metric_avail_intvl_hrs))
        num_vx_metric_fcst_hrs = len(vx_metric_fcst_hrs)

        # In order to not have crowded x-axis labels, limit the number of labels
        # to a maximum value (num_xtick_labels_max).  If num_vx_metric_fcst_hrs
        # is less than this maximum, then xtick_label_freq will be set to 0 or
        # 1, which will cause METviewer to place a label at each tick mark.  If
        # num_vx_metric_fcst_hr is (sufficiently) larger than num_xtick_labels_max,
        # then xtick_label_freq will be set to a value greater than 1, which
        # will cause some tick marks to not have labels to avoid overcrowding.
        num_xtick_labels_max = 16
        xtick_label_freq = round(num_vx_metric_fcst_hrs/num_xtick_labels_max)

    num_series = sum(num_ens_mems_by_model[0:num_models_to_plot])
    if incl_ens_means: num_series = num_series + num_models_to_plot
    order_series = [s for s in range(1,num_series+1)]

    # Generate name of forecast field as it appears in the METviewer database.
    fcst_field_name_in_db = fcst_field_uc
    if fcst_field_uc == 'APCP':
        fcst_field_name_in_db = '_'.join([fcst_field_name_in_db, cla.level_or_accum[0:2]])
    if cla.vx_metric in ['auc', 'brier', 'rely']:
        fcst_field_name_in_db \
        = '_'.join(filter(None,[fcst_field_name_in_db, 'ENS_FREQ',
                                ''.join([thresh_info['comp_oper'], thresh_info['value']])]))
        #
        # For APCP thresholds of >= 6.35mm, >= 12.7mm, and >= 25.4mm, the SRW
        # App's vx tasks pad the names of variables in the .stat files with zeros
        # such that there are three digits after the decimal.  Thus, for example,
        # variable names in the database are
        #
        #   APCP_06_ENS_FREQ_ge6.350
        #   APCP_06_ENS_FREQ_ge12.700
        #   APCP_24_ENS_FREQ_ge25.400
        #
        # instead of
        #
        #   APCP_06_ENS_FREQ_ge6.35
        #   APCP_06_ENS_FREQ_ge12.7
        #   APCP_24_ENS_FREQ_ge25.4
        #
        # The following code appends the zeros to the variable name as it appears
        # in the database (fcst_field_name_in_db).  Note that these zeros are not
        # necessary; for simplicity, the METplus configuration files in the SRW
        # App should be changed so that these zeros are not added.  Once that is
        # done, the following code should be removed (otherwise the variables
        # will not be found in the database).
        #
        if thresh_info['value'] in ['6.35']:
           fcst_field_name_in_db = ''.join([fcst_field_name_in_db, '0'])
        elif thresh_info['value'] in ['12.7', '25.4']:
            fcst_field_name_in_db = ''.join([fcst_field_name_in_db, '00'])

    # Generate a name for the metric that METviewer understands.
    vx_metric_mv = cla.vx_metric.upper()
    if vx_metric_mv == 'BIAS': vx_metric_mv = 'ME'
    elif vx_metric_mv == 'AUC': vx_metric_mv = 'PSTD_ROC_AUC'
    elif vx_metric_mv == 'BRIER': vx_metric_mv = 'PSTD_BRIER'

    # For the given forecast field, generate a name for the corresponding
    # observation type in the METviewer database.
    obs_type = ''
    if cla.fcst_field == 'apcp' :
        obs_type = 'CCPA'
    elif cla.fcst_field in ['refc', 'retop'] :
        obs_type = 'MRMS'
    # The level for CAPE is 'L0', which means the surface, but its obtype is ADPUPA
    # (upper air).  It's a bit unintuitive...
    elif cla.fcst_field == 'cape':
        obs_type = 'ADPUPA'
    elif cla.fcst_field == 'vis':
        obs_type = 'ADPSFC'
    elif cla.level_or_accum in ['2m','02m','10m']:
        obs_type = 'ADPSFC'
    elif cla.level_or_accum in ['500mb','700mb','850mb']:
        obs_type = 'ADPUPA'

    msg = dedent(f"""
        Subset of strings passed to jinja2 template:
          fcst_field_uc = {get_pprint_str(fcst_field_uc)}
          fcst_field_name_in_db = {get_pprint_str(fcst_field_name_in_db)}
          vx_metric_mv = {get_pprint_str(vx_metric_mv)}
          obs_type = {get_pprint_str(obs_type)}
        """)
    logging.debug(msg)

    # Create dictionary containing values for the variables appearing in the
    # jinja2 template.
    jinja2_vars = {"mv_host": cla.mv_host,
                   "mv_machine_config_dict": mv_machine_config_dict,
                   "mv_database_name": cla.mv_database_name,
                   "mv_output_dir": cla.mv_output_dir,
                   "num_models_to_plot": num_models_to_plot,
                   "num_ens_mems_by_model": num_ens_mems_by_model,
                   "model_names_in_db": model_names_in_db_to_plot,
                   "model_names_short": cla.model_names_short,
                   "model_color_codes": model_color_codes,
                   "model_color_codes_light": model_color_codes_light,
                   "fcst_field_uc": fcst_field_uc,
                   "fcst_field_name_in_db": fcst_field_name_in_db,
                   "level_in_db": level_in_db,
                   "level_or_accum_no0pad": loa_value_no0pad,
                   "thresh_in_db": thresh_info['in_db'],
                   "obs_type": obs_type,
                   "vx_metric_uc": cla.vx_metric.upper(),
                   "vx_metric_lc": cla.vx_metric.lower(),
                   "vx_metric_mv": vx_metric_mv,
                   "num_fcst_inits": num_fcst_inits,
                   "fcst_init_times": fcst_init_times_YmDHMS,
                   "fcst_len_hrs": cla.fcst_len_hrs,
                   "job_title": job_title,
                   "plot_title": plot_title,
                   "caption": fcst_init_info_str,
                   "incl_ens_means": incl_ens_means,
                   "num_series": num_series,
                   "order_series": order_series,
                   "xtick_label_freq": xtick_label_freq,
                   "line_types": line_types,
                   "line_widths": line_widths}

    # Empty strings are included in this concatenation to force insertion
    # of delimiter.
    msg = dedent(f"""
        Jinja variables passed to template file:
          jinja2_vars = """) + \
        get_pprint_str(jinja2_vars, ' '*(5 + len('jinja2_vars'))).lstrip()
    logging.debug(msg)

    templates_dir = os.path.join(home_dir, 'parm', 'metviewer')
    template_fn = ''.join([cla.vx_metric, '.xml'])
    if (cla.vx_metric in ['auc', 'brier']):
        template_fn = 'auc_brier.xml'
    elif (cla.vx_metric in ['bias', 'fbias']):
        template_fn = 'bias_fbias.xml'
    elif (cla.vx_metric in ['rely', 'rhist']):
        template_fn = 'rely_rhist.xml'
    template_fp = os.path.join(templates_dir, template_fn)

    msg = dedent(f"""
        Template file information:
          template_fp = {get_pprint_str(template_fp)}
          templates_dir = {get_pprint_str(templates_dir)}
          template_fn = {get_pprint_str(template_fn)}
        """)
    logging.debug(msg)

    # Place xmls generated below in the same directory as the plots that
    # METviewer will generate from the xmls.
    output_xml_dir = Path(os.path.join(cla.mv_output_dir, 'plots')).resolve()
    if not os.path.exists(output_xml_dir):
        os.makedirs(output_xml_dir)
    output_xml_fn = '_'.join(filter(None,
                    ['plot', cla.vx_metric, var_lvl_str,
                     cla.threshold, models_str]))
    output_xml_fn = ''.join([output_xml_fn, '.xml'])
    output_xml_fp = os.path.join(output_xml_dir, output_xml_fn)
    msg = dedent(f"""
        Output xml file information:
          output_xml_fn = {get_pprint_str(output_xml_fn)}
          output_xml_dir = {get_pprint_str(output_xml_dir)}
          output_xml_fp = {get_pprint_str(output_xml_fp)}
        """)
    logging.debug(msg)

    # Convert the dictionary of jinja2 variable settings above to yaml format
    # and write it to a temporary yaml file for reading by the set_template
    # function.
    tmp_fn = 'tmp.yaml'
    with open(f'{tmp_fn}', 'w') as fn:
        yaml_vars = yaml.dump(jinja2_vars, fn)

    args_list = ['--quiet',
                 '--config_file', tmp_fn,
                 '--input_template', template_fp,
                 '--outfile', output_xml_fp]

    msg = dedent(f"""
        Generating xml from jinja2 template ...
        """)
    logging.info(msg)
    set_template(args_list)
    os.remove(tmp_fn)

    return(mv_machine_config_dict['mv_batch_fp'], output_xml_fp)


def run_mv_batch_script(mv_batch_fp, output_xml_fp):
    """
    Function that runs the METviewer batch script with the specified xml to
    generate a verification (vx) plot.

    Arguments:
    ---------
    mv_batch_fp:
      Path to METviewer batch plotting script.

    output_xml_fp:
      Path to the xml to pass to the batch script.

    Returns:
    -------
    result:
      Instance of subprocess.CompletedProcess class containing result of call
      to METviewer batch script.
    """

    # Generate full path to log file that will contain output from calling the
    # METviewer batch script.
    p = Path(output_xml_fp)
    mv_batch_log_fp = ''.join([os.path.join(p.parent, p.stem), '.log'])

    # Run METviewer in batch mode on the xml.
    msg = dedent(f"""
        Log file for call to METviewer batch script is:
          mv_batch_log_fp = {get_pprint_str(mv_batch_log_fp)}
        """)
    logging.debug(msg)
    with open(mv_batch_log_fp, "w") as outfile:
        result = subprocess.run([mv_batch_fp, output_xml_fp], stdout=outfile, stderr=outfile)
        msg = dedent(f"""
            Result of call to METviewer batch script:
              result = """) + \
            get_pprint_str(vars(result), ' '*(5 + len('result'))).lstrip()
        logging.debug(msg)

    return result


def make_single_mv_vx_plot(argv):
    """
    Driver function to generate a METviewer xml and generate an image file
    of the corresponding verification (vx) plot.

    Arguments:
    ---------
    argv:
      Arguments passed on the command line to this script.

    Returns:
    -------
    output_xml_fp:
      Path to xml generated by this function.
    """

    #
    # Create a logger if necessary.
    #
    # When a logger has not yet been created (e.g. by another script that calls
    # this one) and one gets the 'root' logger using
    #
    #  logger = logging.getLogger()
    #
    # then this logger will have no handlers yet.  Therefore, a script can check
    # whether the logger above results in a logger that has handlers by using
    #
    #  logging.getLogger().hasHandlers()
    #
    # Then the script can create a logger only if the root logger does not have
    # handlers; otherwise, it will use the existing logger (which has handlers).
    #
    if not logging.getLogger().hasHandlers():
        #
        # Here, we hard-code the logger's debugging level and format.
        #
        # It is possible to make these (as well as whether to write the logging output
        # to a file) user-specifiable, but it will complicate the code because it
        # requires the arguments to be parsed before this point in the code, which they
        # currently are not (and we want to avoid refactoring the code).  Therefore,
        # currently the way to change the logging level is to call this function
        # from the wrapper (make_mv_vx_plots.py), which does have arguments for
        # specifying the logging level and destination (e.g. screen vs. a log file),
        # in which case the "else" part of this if-statement is exectuted instead.
        #
        # Note that logging.basicConfig always retruns None.  It (from the "logging"
        # module's documentation):
        #
        # * Does basic configuration for the logging system by creating a StreamHandler
        #   with a default Formatter and adding it to the root logger.
        # * Does nothing if the root logger already has handlers configured, unless
        #   the keyword argument "force" is set to True.
        #
        log_level = 'INFO'
        FORMAT = "[%(levelname)s:%(name)s:  %(filename)s, line %(lineno)s: %(funcName)s()] %(message)s"
        logging.basicConfig(level=log_level, format=FORMAT)
        msg = dedent(f"""
            Root logger has been set up with logging level {get_pprint_str(log_level)}.
            """)
        logging.debug(msg)
    else:
        msg = dedent(f"""
            Using existing logger.
            """)
        logging.debug(msg)

    # Print out logger details.
    logger = logging.getLogger()
    msg = dedent(f"""
        Logger details:
          logger = """) + \
        get_pprint_str(vars(logger), ' '*(5 + len('logger'))).lstrip() + \
        '\n'
    logging.debug(msg)

    # Get valid values for various verification (vx) plotting parameters.
    # Some of these are needed below when parsing the command line arguments.
    valid_vx_plot_params_config_fp = 'valid_vx_plot_params.yaml'
    msg = dedent(f"""
        Obtaining valid values of verification (vx) parameters from file
          {get_pprint_str(valid_vx_plot_params_config_fp)}
        ...
        """)
    logging.info(msg)
    valid_vx_plot_params = get_valid_vx_plot_params(valid_vx_plot_params_config_fp)

    # Parse arguments.
    msg = dedent(f"""
        Processing command line arguments ...
        """)
    logging.info(msg)
    cla = parse_args(argv, valid_vx_plot_params)

    # Get METviewer database information.
    msg = dedent(f"""
        Obtaining METviewer database information from file
          {get_pprint_str(cla.mv_databases_config_fp)}
        ...
        """)
    logging.info(msg)
    mv_databases_dict = get_database_info(cla.mv_databases_config_fp)

    # Generate a METviewer xml.
    msg = dedent(f"""
        Generating a METviewer xml ...
        """)
    logging.info(msg)
    mv_batch_fp, output_xml_fp = generate_metviewer_xml(cla, valid_vx_plot_params, mv_databases_dict)

    # Run METviewer on the xml to create a plot of the vx metric.
    msg = dedent(f"""
        Running METviewer on xml file
          {get_pprint_str(output_xml_fp)}
        ...
        """)
    logging.info(msg)
    run_mv_batch_script(mv_batch_fp, output_xml_fp)

    return(output_xml_fp)
#
# -----------------------------------------------------------------------
#
# Call the function defined above.
#
# -----------------------------------------------------------------------
#
if __name__ == "__main__":
    make_single_mv_vx_plot(sys.argv[1:])

