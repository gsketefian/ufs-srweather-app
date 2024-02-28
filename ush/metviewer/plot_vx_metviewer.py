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
# fcst_field    level_or_accum             threshold    stat types
# ----------    --------------             ---------    ----------
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

def get_pprint_str(x, indent_str):
    """Format a python variable as a pretty-printed string and add indentation.

    Arguments:
      x:           A variable.
      indent_str:  String to be added to the beginning of each line of the
                   pretty-printed form of x.

    Return:
      x_str:       Formatted string containing contents of variable.
    """

    x_str = pprint.pformat(x, compact=True)
    x_str = x_str.splitlines(True)
    x_str = [indent_str + s for s in x_str]
    x_str = ''.join(x_str)

    return x_str


def get_thresh_info(thresh_in_config):
    """Extract and form various pieces of threshold-related information from 
       the threshold specified in the yaml plot configuration file.

    Arguments:
      thresh_in_config:  Threshold setting as it appears in the yaml plot configuration file.

    Return:
      thresh_info:       Dictionary containing varous threshold-related variables.
    """

    bad_thresh_fmt_msg = dedent('''
        The input threshold must be either an empty string or a string of the
        form
          <comp_oper><value><units>
        where <comp_oper> is a string of one or more characters representing a
        comparison operator (e.g. "ge" for "greater than or equal to"), <value>
        is a stirng of one or more digits and possibly a decimal representing
        the threshold value, and <units> is a string of zero or more characters
        representing the value's units (zero characters allowed to account for
        the case of a unitless value).  Check the specified threshold to ensure
        it has a valid format and rerun.  Stopping.''')

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
            err_msg = ''.join([dedent(f'''\n
                Invalid value for threshold comparison operator:
                  thresh_comp_oper = {thresh_comp_oper}
                Valid values for the comparison operator are:
                  valid_vals_thresh_comp_oper = {valid_vals_thresh_comp_oper}
                Specified threshold is:
                  thresh_in_config = {thresh_in_config}'''),
                bad_thresh_fmt_msg])
            logging.error(err_msg, stack_info=True)
            raise ValueError(err_msg)
    
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
        thresh_in_plot_title = ' '.join(filter(None, [thresh_comp_oper_xml, thresh_value, thresh_in_plot_title]))

    # If thresh_parts is empty but thresh_in_config is not, then something
    # must have been wrong with thresh_in_config that caused thresh_parts
    # to be empty.
    elif thresh_in_config:
        err_msg = ''.join([dedent(f'''\n
            Specified input threshold does not have a valid format:
              thresh_in_config = {thresh_in_config}'''),
            bad_thresh_fmt_msg])
        logging.error(err_msg, stack_info=True)
        raise ValueError(err_msg)

    # Create a dictionary containing the values to return and return it.
    thresh_info = {'in_config': thresh_in_config,
                   'comp_oper': thresh_comp_oper,
                   'value': thresh_value,
                   'units': thresh_units,
                   'in_db': thresh_in_db,
                   'in_plot_title': thresh_in_plot_title}
    return thresh_info


def get_static_info(static_info_config_fp):
    '''
    Function to read in values that are mostly static, i.e. they're usually
    not expected to change from one call to this script to another (e.g.
    valid values for various parameters).
    '''

    # Load the yaml file containing static verification parameters.
    static_data = load_config_file(static_info_config_fp)

    valid_levels_to_levels_in_db = static_data['valid_levels_to_levels_in_db']
    all_valid_levels = list(valid_levels_to_levels_in_db.keys())

    # Define local dictionaries containing static values that depend on the 
    # forecast field.
    valid_fcst_fields = static_data['valid_fcst_fields'].keys()
    fcst_field_long_names = {}
    valid_levels_by_fcst_field = {}
    valid_units_by_fcst_field = {}
    for fcst_field in valid_fcst_fields:
  
        # Get and save long name of current the forecast field.
        fcst_field_long_names[fcst_field] = static_data['valid_fcst_fields'][fcst_field]['long_name']

        # Get and save the list of valid units for the current forecast field.
        valid_units_by_fcst_field[fcst_field] = static_data['valid_fcst_fields'][fcst_field]['valid_units']

        # Get and save the list of valid levels/accumulations for the current
        # forecast field.
        valid_levels_by_fcst_field[fcst_field] = static_data['valid_fcst_fields'][fcst_field]['valid_levels']
        # Make sure all the levels/accumulations specified for the current 
        # forecast field are in the master list of valid levels and accumulations.
        for loa in valid_levels_by_fcst_field[fcst_field]:
            if loa not in all_valid_levels:
                err_msg = dedent(f"""
                    One of the levels or accumulations (loa) in the set of valid levels and
                    accumulations for the current forecast field (fcst_field) is not in the
                    master list of valid levels and accumulations (all_valid_levels):
                      fcst_field = {fcst_field}
                      loa = {loa}
                      all_valid_levels = {all_valid_levels}
                    The master list of valid levels and accumulations as well as the list of
                    valid levels and accumulations for the current forecast field can be
                    found in the following static information configuration file:
                      static_info_config_fp = {static_info_config_fp}
                    Please modify this file and rerun.
                    """)
                logging.error(err_msg, stack_info=True)
                raise ValueError(err_msg)

    # Define local dictionaries containing static values that depend on the
    # verification statistic.
    valid_stats = static_data['valid_stats'].keys()
    stat_long_names = {}
    stat_need_thresh = {}
    for stat in valid_stats:
        stat_long_names[stat] = static_data['valid_stats'][stat]['long_name']
        stat_need_thresh[stat] = static_data['valid_stats'][stat]['need_thresh']

    # Get dictionary containing the available METviewer color codes.  This 
    # is a subset of all available colors in METviewer (of which there are
    # thousands) which we allow the user to specify as a plot color for 
    # the models to be plotted.  In this dictionary, the keys are the color
    # names (e.g. 'red'), and values are the corresponding codes in METviewer.
    avail_mv_colors_codes = static_data['avail_mv_colors_codes']

    # Create dictionary containing valid choices for various parameters.
    # This is needed by the argument parsing function below.
    choices = {}
    choices['fcst_field'] = sorted(valid_fcst_fields)
    choices['level'] = all_valid_levels
    choices['vx_stat'] = sorted(valid_stats)
    choices['color'] = list(avail_mv_colors_codes.keys())

    static_info = {}
    static_info['static_info_config_fp'] = static_info_config_fp 
    static_info['valid_levels_to_levels_in_db'] = valid_levels_to_levels_in_db
    static_info['fcst_field_long_names'] = fcst_field_long_names
    static_info['valid_levels_by_fcst_field'] = valid_levels_by_fcst_field
    static_info['valid_units_by_fcst_field'] = valid_units_by_fcst_field
    static_info['stat_long_names'] = stat_long_names
    static_info['stat_need_thresh'] = stat_need_thresh
    static_info['avail_mv_colors_codes'] = avail_mv_colors_codes 
    static_info['choices'] = choices

    return static_info


def get_database_info(mv_database_config_fp):
    '''
    Function to read in information about the METviewer database from which
    verification statistics will be plotted.
    '''

    # Load the yaml file containing database information.
    mv_databases_dict = load_config_file(mv_database_config_fp)

    return mv_databases_dict


def parse_args(argv, static_info):
    '''
    Function to parse arguments for this script.
    '''

    choices = static_info['choices']

    parser = argparse.ArgumentParser(description=dedent(f'''
             Function to generate an xml file that METviewer can read in order 
             to create a verification plot.
             '''))

    parser.add_argument('--mv_host',
                        type=str,
                        required=True,
                        help='Host (name of machine) on which METviewer is installed')

    parser.add_argument('--mv_machine_config_fp',
                        type=str,
                        required=False, default='mv_machine_config.yaml', 
                        help='METviewer machine (host) configuration file')

    parser.add_argument('--mv_database_config_fp',
                        type=str,
                        required=False, default='mv_database_config.yaml',
                        help='METviewer database configuration file')

    parser.add_argument('--mv_database_name',
                        type=str,
                        required=True,
                        help='Name of METviewer database')

    # Find the path to the directory containing the clone of the SRW App.  The index of
    # .parents will have to be changed if this script is moved elsewhere in the SRW App's
    # directory structure.
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

    parser.add_argument('--colors', nargs='+',
                        type=str,
                        required=False, default=choices['color'],
                        choices=choices['color'],
                        help='Color of each model used in verification metrics plots')

    parser.add_argument('--vx_stat',
                        type=str.lower,
                        required=True,
                        choices=choices['vx_stat'],
                        help='Name of verification statistic/metric')

    parser.add_argument('--incl_ens_means',
                        required=False, action=argparse.BooleanOptionalAction, default=argparse.SUPPRESS,
                        help='Flag for including ensemble mean curves in plot')

    parser.add_argument('--fcst_init_info', nargs=3,
                        type=str,
                        required=True, 
                        help=dedent(f'''Initialization time of first forecast (in YYYYMMDDHH),
                                        number of forecasts, and forecast initialization interval (in HH)'''))

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
                        choices=choices['level'],
                        help='Vertical level or accumulation period')

    parser.add_argument('--threshold',
                        type=str,
                        required=False, default='',
                        help='Threshold for the specified forecast field')

    # Parse the arguments.
    cla = parser.parse_args(argv)

    # Empty strings are included in this concatenation to force insertion
    # of delimiter.
    logging.debug('\n'.join(['', 'List of arguments passed to script:',
                             'cla = ', get_pprint_str(vars(cla), '  '), '']))

    return cla


def generate_metviewer_xml(cla, static_info, mv_databases_dict):
    """Function that generates an xml file that METviewer can read (in order
       to create a verification plot).

    Args:
        argv:  Command-line arguments

    Returns:
        None
    """

    static_info_config_fp = static_info['static_info_config_fp']
    valid_levels_to_levels_in_db = static_info['valid_levels_to_levels_in_db']
    fcst_field_long_names = static_info['fcst_field_long_names']
    valid_levels_by_fcst_field = static_info['valid_levels_by_fcst_field']
    valid_units_by_fcst_field = static_info['valid_units_by_fcst_field']
    stat_long_names = static_info['stat_long_names']
    stat_need_thresh = static_info['stat_need_thresh']
    avail_mv_colors_codes = static_info['avail_mv_colors_codes']

    # Load the machine configuration file into a dictionary and find in it the
    # machine specified on the command line.
    mv_machine_config_fp = Path(os.path.join(cla.mv_machine_config_fp)).resolve()
    mv_machine_config = load_config_file(mv_machine_config_fp)

    all_hosts = sorted(list(mv_machine_config.keys()))
    if cla.mv_host not in all_hosts:
        err_msg = dedent(f"""
            The machine/host specified on the command line (cla.mv_host) does not have a
            corresponding entry in the METviewer host configuration file (mv_machine_config_fp):
              cla.mv_host = {cla.mv_host}
              mv_machine_config_fp = {mv_machine_config_fp}
            Machines that do have an entry in the host configuration file are:
              {all_hosts}
            Either run on one of these hosts, or add an entry in the configuration file for "{cla.mv_host}".
            """)
        logging.error(err_msg, stack_info=True)
        raise ValueError(err_msg)

    mv_machine_config_dict = mv_machine_config[cla.mv_host]

    # Make sure that the database specified on the command line exists in the
    # list of databases in the database configuration file.
    if cla.mv_database_name not in mv_databases_dict.keys():
        err_msg = dedent(f"""
            The database specified on the command line (cla.mv_database_name) is not
            in the set of METviewer databases specified in the database configuration
            file (cla.mv_database_config_fp):
              cla.mv_database_name = {cla.mv_database_name}
              cla.mv_database_config_fp = {cla.mv_database_config_fp}
            """)
        logging.error(err_msg, stack_info=True)
        raise ValueError(err_msg)

    # Extract the METviewer database information.
    database_info = mv_databases_dict[cla.mv_database_name]
    valid_threshes_in_db = list(database_info['valid_threshes'])
    model_info = list(database_info['models'])
    num_models_avail_in_db = len(model_info)
    model_names_avail_in_db = [model_info[i]['long_name'] for i in range(0,num_models_avail_in_db)]
    model_names_short_avail_in_db = [model_info[i]['short_name'] for i in range(0,num_models_avail_in_db)]

    # Make sure that the models specified on the command line exist in the
    # database.
    for i,model_name_short in enumerate(cla.model_names_short):
        if model_name_short not in model_names_short_avail_in_db:
            err_msg = dedent(f"""
                A model specified on the command line (model_name_short) is not included
                in the entry for the specified database (cla.mv_database_name) in the 
                METviewer database configuration file (cla.mv_database_config_fp)
                  cla.mv_database_config_fp = {cla.mv_database_config_fp}
                  cla.mv_database_name = {cla.mv_database_name}
                  model_name_short = {model_name_short}
                Models that are included in the database configuration file are:
                  {model_names_short_avail_in_db}
                Either change the command line to specify only one of these models, or
                add the new model to the database configuration file (the latter approach
                will work only if the new model actually exists in the METviewer database).
                """)
            logging.error(err_msg, stack_info=True)
            raise ValueError(err_msg)

    # If the threshold specified on the command line is not an empty string,
    # make sure that it is one of the valid ones for this database.
    if (cla.threshold) and (cla.threshold not in valid_threshes_in_db):
        err_msg = dedent(f"""
                  The specified threshold is not in the list of valid thresholds for the
                  specified database.  Database is:
                    cla.mv_database_name = {cla.mv_database_name}
                  Threshold is:
                    cla.threshold = {cla.threshold}
                  The list of valid thresholds for this database is:
                    valid_threshes_in_db = """)
        indent_str = ' '*(5 + len('valid_threshes_in_db'))
        err_msg = err_msg + get_pprint_str(valid_threshes_in_db, indent_str).lstrip()
        err_msg = err_msg + dedent(f"""
                  Make sure the specified threshold is one of the valid ones, or, if it
                  exists in the database, add it to the 'valid_threshes' list in the 
                  METviewer database configuration file given by:
                      cla.mv_database_config_fp = {cla.mv_database_config_fp})""")
        logging.error(err_msg, stack_info=True)
        raise ValueError(err_msg)

    # Get the names in the database of those models that are to be plotted.
    inds_models_to_plot = [model_names_short_avail_in_db.index(m) for m in cla.model_names_short]
    num_models_to_plot = len(inds_models_to_plot)
    model_names_in_db = [model_info[i]['long_name'] for i in inds_models_to_plot]

    # Get the number of ensemble members for each model and make sure all are
    # positive.
    num_ens_mems_by_model = [model_info[i]['num_ens_mems'] for i in inds_models_to_plot]
    for i,model in enumerate(cla.model_names_short):
        n_ens = num_ens_mems_by_model[i]
        if n_ens <= 0:
            err_msg = dedent(f"""
                The number of ensemble members for the current model must be greater
                than or equal to 0:
                  model = {model}
                  n_ens = {n_ens}
                """)
            logging.error(err_msg, stack_info=True)
            raise ValueError(err_msg)

    # Make sure no model names are duplicated because METviewer will throw an 
    # error in this case.  Create a set (using curly braces) to store duplicate
    # values.  Note that a set must be used here so that duplicate values are
    # not duplicated!
    duplicates = {m for m in cla.model_names_short if cla.model_names_short.count(m) > 1}
    if len(duplicates) > 0:
        err_msg = dedent(f"""
            A model can appear only once in the set of models to plot specified on
            the command line.  However, the following models are duplicated:
              duplicates = {duplicates}
            Please remove duplicated models from the command line and rerun.
            """)
        logging.error(err_msg, stack_info=True)
        raise ValueError(err_msg)

    # Make sure that there are at least as many available colors as models to
    # plot.
    num_avail_colors = len(avail_mv_colors_codes)
    if num_models_to_plot > num_avail_colors:
        err_msg = dedent(f"""
            The number of models to plot (num_models_to_plot) must be less than
            or equal to the number of available colors:
              num_models_to_plot = {num_models_to_plot}
              num_avail_colors = {num_avail_colors}
            Either reduce the number of models to plot specified on the command 
            line or add new colors in the static information configuration file
            (static_info_config_fp):
              static_info_config_fp = {static_info_config_fp}
            """)
        logging.error(err_msg, stack_info=True)
        raise ValueError(err_msg)

    # Pick out the plot color associated with each model from the list of 
    # available colors.  The following lists will contain the hex RGB color
    # codes of the colors to use for each model as well as the codes for
    # the light versions of those colors (needed for some types of stat plots).
    model_color_codes = [avail_mv_colors_codes[m]['hex_code'] for m in cla.colors]
    model_color_codes_light = [avail_mv_colors_codes[m]['hex_code_light'] for m in cla.colors]

    # Set the initialization times for the forecasts.
    fcst_init_time_first = datetime.strptime(cla.fcst_init_info[0], '%Y%m%d%H')
    num_fcst_inits = int(cla.fcst_init_info[1])
    fcst_init_intvl_hrs = int(cla.fcst_init_info[2])
    fcst_init_intvl = timedelta(hours=fcst_init_intvl_hrs)
    fcst_init_times = list(range(0,num_fcst_inits))
    fcst_init_times = [fcst_init_time_first + i*fcst_init_intvl for i in fcst_init_times]
    fcst_init_times_YmDHMS = [i.strftime("%Y-%m-%d %H:%M:%S") for i in fcst_init_times]
    fcst_init_times_YmDH = [i.strftime("%Y-%m-%d %H") for i in fcst_init_times]
    fcst_init_info_str = f'fcst_init_times = [{fcst_init_times_YmDH[0]}Z, {fcst_init_intvl_hrs} hr, {fcst_init_times_YmDH[-1]}Z] (num_fcst_inits = {num_fcst_inits})'

    fcst_init_times_str = '\n          '.join(fcst_init_times_YmDHMS)
    logging.info(dedent(f"""
        Forecast initialization times (fcst_init_times_str):
          {fcst_init_times_str}
        """))

    if ('incl_ens_means' not in cla):
        incl_ens_means = False
        if (cla.vx_stat == 'bias'): incl_ens_means = True
    else:
        incl_ens_means = cla.incl_ens_means
    # Apparently we can just reset or create incl_ens_means within the cla Namespace
    # as follows:
    cla.incl_ens_means = incl_ens_means

    valid_levels_or_accums = valid_levels_by_fcst_field[cla.fcst_field]
    if cla.level_or_accum not in valid_levels_or_accums:
        err_msg = dedent(f"""
            The specified level or accumulation is not compatible with the specified
            forecast field:
              cla.fcst_field = {cla.fcst_field}
              cla.level_or_accum = {cla.level_or_accum}
            Valid options for level or accumulation for this forecast field are:
              {valid_levels_or_accums}
            """)
        logging.error(err_msg, stack_info=True)
        raise ValueError(err_msg)

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
        err_msg = dedent(f"""
            Unknown units (loa_units) for level or accumulation:
              loa_units = {loa_units}
            Valid units are:
              valid_loa_units = {valid_loa_units}
            Related variables:
              cla.level_or_accum = {cla.level_or_accum}
              loa_value = {loa_value}
              loa_value_no0pad = {loa_value_no0pad}
            """)
        logging.error(err_msg, stack_info=True)
        raise ValueError(err_msg)

    loa_value_no0pad = loa_value.lstrip('0')
    width_0pad = 0
    if loa_units == 'h':
        width_0pad = 2
    elif loa_units == 'm':
        width_0pad = 2
    elif loa_units == 'mb':
        width_0pad = 3
    elif (loa_units == '' and cla.level_or_accum == 'L0'):
        logging.debug(dedent(f"""
            Since the specified level/accumulation is "{cla.level_or_accum}", we set loa_units to an empty
            string:
              cla.level_or_accum = {cla.level_or_accum}
              loa_units = {loa_units}
            Related variables:
              loa_value = {loa_value}
              loa_value_no0pad = {loa_value_no0pad}
            """))

    loa_value_0pad = loa_value_no0pad.zfill(width_0pad)
    logging.info(dedent(f"""
        Level/accumulation parameters have been set as follows:
          loa_value = {loa_value}
          loa_value_no0pad = {loa_value_no0pad}
          loa_value_0pad = {loa_value_0pad}
          loa_units = {loa_units}
        """))

    if (not stat_need_thresh[cla.vx_stat]) and (cla.threshold):
        no_thresh_stats = [key for key,val in stat_need_thresh.items() if val]
        no_thresh_stats_fmt_str = ",\n".join("              {!r}: {!r}".format(k, v)
                                             for k, v in stat_long_names.items() if k in no_thresh_stats).lstrip()
        logging.debug(dedent(f"""
            A threshold is not needed when working with one of the following verification
            stats:
              {no_thresh_stats_fmt_str}
            Thus, the threshold specified in the argument list ("{cla.threshold}") will be reset to
            an empty string.
            """))
        cla.threshold = ''

    # Extract and set various pieces of threshold-related information from
    # the specified threshold.
    thresh_info = get_thresh_info(cla.threshold)
    logging.info('\n'.join(['', 'Dictionary containing threshold information has been set as follows:',
                             'thresh_info = ', get_pprint_str(thresh_info, '  '), '']))

    # Get the list of valid units for the specified forecast field.
    valid_units = valid_units_by_fcst_field[cla.fcst_field]
    # If the specified threshold is not empty and its units do not match any
    # of the ones in the list of valid units, error out.
    if (cla.threshold) and (thresh_info['units'] not in valid_units):
        err_msg = dedent(f"""
            The units specified in the threshold are not compatible with the list
            of valid units for this field.  The specified field and threshold are:
              cla.fcst_field = {cla.fcst_field}
              cla.threshold = {cla.threshold}
            The units extracted from the threshold are:
              thresh_info[units] = {thresh_info['units']}
            Valid units for this forecast field are:
              {valid_units}
            """)
        logging.error(err_msg, stack_info=True)
        raise ValueError(err_msg)

    # Form the plot title.
    plot_title = ' '.join(filter(None,
                          [stat_long_names[cla.vx_stat], 'for',
                           ''.join([loa_value, loa_units]), fcst_field_long_names[cla.fcst_field],
                           thresh_info['in_plot_title']]))

    # Form the job title needed in the xml.
    fcst_field_uc = cla.fcst_field.upper()
    var_lvl_str = ''.join(filter(None, [fcst_field_uc, loa_value, loa_units]))
    thresh_str = ''.join(filter(None, [thresh_info['comp_oper'], thresh_info['value'], thresh_info['units']]))
    var_lvl_thresh_str = '_'.join(filter(None, [var_lvl_str, thresh_str]))
    models_str = '_'.join(cla.model_names_short)
    job_title = '_'.join([cla.vx_stat, var_lvl_thresh_str, models_str])

    logging.debug(dedent(f"""
        Various auxiliary string values:
          plot_title = {plot_title}
          var_lvl_str = {var_lvl_str}
          thresh_str = {thresh_str}
          var_lvl_thresh_str = {var_lvl_thresh_str}
          job_title = {job_title}
          models_str = {models_str}
        """))

    # Get names of level/accumulation, threshold, and models as they are set
    # in the database.
    level_in_db = valid_levels_to_levels_in_db[cla.level_or_accum]

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
    if cla.vx_stat in ['rely', 'rhist']:
        xtick_label_freq = 0
    # The remaining plot (vx stat) types have forecast hour on the x-axis.  
    # For these, there are several aspects of the plotting to consider for
    # setting xtick_label_freq.
    elif cla.vx_stat in ['auc', 'bias', 'brier', 'fbias', 'ss']:

        # Get the list of forecast hours at which the statistic is available
        # (stat_fcst_hrs).  This requires first determining the time interval
        # (in hours) with which the statistic is calculated (stat_avail_intvl_hrs).
        # This in turn depends on the frequency with which both the observations
        # and the forecast fields are available.
        #
        # The default is to assume that the observations and forecasts are
        # available every hour.  Thus, the statistic is available every hour.
        stat_avail_intvl_hrs = 1
        # If the level is actually an accumulation, reset the statistic availability
        # interval to the accumulation interval.
        if (cla.level_or_accum in ['01h', '03h', '06h', '24h']):
            stat_avail_intvl_hrs = int(loa_value)
        # If the level is an upper air location, we consider values only at 12Z 
        # because the number of observations at other hours of the day is very
        # low (so statistics are unreliable).  Thus, we set stat_avail_intvl_hrs
        # to 12.
        elif (cla.level_or_accum in ['500mb', '700mb', '850mb']):
            stat_avail_intvl_hrs = 12

        # Use the statistic availability interval to set the forecast hours at
        # which the statistic is available.  Then find the number of such hours.
        stat_fcst_hrs = list(range(0, cla.fcst_len_hrs+1, stat_avail_intvl_hrs))
        num_stat_fcst_hrs = len(stat_fcst_hrs)

        # In order to not have crowded x-axis labels, limit the number of such
        # labels to some maximum value (num_xtick_labels_max).  If num_stat_fcst_hrs
        # is less than this maximum, then xtick_label_freq will be set to 0 or
        # 1, which will cause METviewer to place a label at each tick mark.  
        # If num_stat_fcst_hr is (sufficiently) larger than num_xtick_labels_max,
        # then xtick_label_freq will be set to a value greater than 1, which 
        # will cause some number of tick marks to not have labels to avoid 
        # overcrowding.
        num_xtick_labels_max = 16
        xtick_label_freq = round(num_stat_fcst_hrs/num_xtick_labels_max)

    num_series = sum(num_ens_mems_by_model[0:num_models_to_plot])
    if incl_ens_means: num_series = num_series + num_models_to_plot
    order_series = [s for s in range(1,num_series+1)]

    # Generate name of forecast field as it appears in the METviewer database.
    fcst_field_name_in_db = fcst_field_uc
    if fcst_field_uc == 'APCP': fcst_field_name_in_db = '_'.join([fcst_field_name_in_db, cla.level_or_accum[0:2]])
    if cla.vx_stat in ['auc', 'brier', 'rely']:
        fcst_field_name_in_db = '_'.join(filter(None,[fcst_field_name_in_db, 'ENS_FREQ', 
                                                      ''.join([thresh_info['comp_oper'], thresh_info['value']])]))
        #
        # For APCP thresholds of >= 6.35mm, >= 12.7mm, and >= 25.4mm, the SRW App's
        # verification tasks pad the names of variables in the stat files with zeros
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
        # The following code appends the zeros to the variable name in the database
        # (fcst_field_name_in_db).  Note that these zeros are not necessary; for simplicity,
        # the METplus configuration files in the SRW App should be changed so that these
        # zeros are not added.  Once that is done, the following code should be removed
        # (otherwise the variables will not be found in the database).
        #
        if thresh_info['value'] in ['6.35']:
           fcst_field_name_in_db = ''.join([fcst_field_name_in_db, '0'])
        elif thresh_info['value'] in ['12.7', '25.4']:
            fcst_field_name_in_db = ''.join([fcst_field_name_in_db, '00'])

    # Generate name for the verification statistic that METviewer understands.
    vx_stat_mv = cla.vx_stat.upper()
    if vx_stat_mv == 'BIAS': vx_stat_mv = 'ME'
    elif vx_stat_mv == 'AUC': vx_stat_mv = 'PSTD_ROC_AUC'
    elif vx_stat_mv == 'BRIER': vx_stat_mv = 'PSTD_BRIER'

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

    logging.debug(dedent(f"""
        Subset of strings passed to jinja2 template:
          fcst_field_uc = {fcst_field_uc}
          fcst_field_name_in_db = {fcst_field_name_in_db}
          vx_stat_mv = {vx_stat_mv}
          obs_type = {obs_type}
        """))

    # Create dictionary containing values for the variables appearing in the
    # jinja2 template.
    jinja2_vars = {"mv_host": cla.mv_host,
                   "mv_machine_config_dict": mv_machine_config_dict,
                   "mv_database_name": cla.mv_database_name,
                   "mv_output_dir": cla.mv_output_dir,
                   "num_models_to_plot": num_models_to_plot,
                   "num_ens_mems_by_model": num_ens_mems_by_model,
                   "model_names_in_db": model_names_in_db,
                   "model_names_short": cla.model_names_short,
                   "model_color_codes": model_color_codes,
                   "model_color_codes_light": model_color_codes_light,
                   "fcst_field_uc": fcst_field_uc,
                   "fcst_field_name_in_db": fcst_field_name_in_db,
                   "level_in_db": level_in_db,
                   "level_or_accum_no0pad": loa_value_no0pad,
                   "thresh_in_db": thresh_info['in_db'],
                   "obs_type": obs_type,
                   "vx_stat_uc": cla.vx_stat.upper(),
                   "vx_stat_lc": cla.vx_stat.lower(),
                   "vx_stat_mv": vx_stat_mv,
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
    logging.debug('\n'.join(['', 'Jinja variables passed to template file:',
                             'jinja2_vars = ', get_pprint_str(jinja2_vars, '  '), '']))

    templates_dir = os.path.join(home_dir, 'parm', 'metviewer')
    template_fn = "".join([cla.vx_stat, '.xml'])
    if (cla.vx_stat in ['auc', 'brier']):
        template_fn = 'auc_brier.xml'
    elif (cla.vx_stat in ['bias', 'fbias']):
        template_fn = 'bias_fbias.xml'
    elif (cla.vx_stat in ['rely', 'rhist']):
        template_fn = 'rely_rhist.xml'
    template_fp = os.path.join(templates_dir, template_fn)

    logging.info(dedent(f"""
        Template file is:
          templates_dir = {templates_dir}
          template_fn = {template_fn}
          template_fp = {template_fp}
        """))

    # Place xmls generated below in the same directory as the plots that 
    # METviewer will generate from the xmls.
    output_xml_dir = Path(os.path.join(cla.mv_output_dir, 'plots')).resolve()
    if not os.path.exists(output_xml_dir):
        os.makedirs(output_xml_dir)
    output_xml_fn = '_'.join(filter(None,
                    ['plot', cla.vx_stat, var_lvl_str,
                     cla.threshold, models_str]))
    output_xml_fn = ''.join([output_xml_fn, '.xml'])
    output_xml_fp = os.path.join(output_xml_dir, output_xml_fn)
    logging.info(dedent(f"""
        Output xml file information:
          output_xml_fn = {output_xml_fn}
          output_xml_dir = {output_xml_dir}
          output_xml_fp = {output_xml_fp}
        """))

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
    logging.info(dedent(f"""
        Generating xml from jinja2 template ...
        """))
    set_template(args_list)
    os.remove(tmp_fn)

    return(mv_machine_config_dict['mv_batch'], output_xml_fp)


def run_mv_batch(mv_batch, output_xml_fp):
    """Function that generates a verification plot using METviewer.

    Args:
        mv_batch:       Path to METviewer batch plotting script.
        output_xml_fp:  Full path to the xml to pass to the batch script.

    Returns:
        result:         Instance of subprocess.CompletedProcess class containing
                        result of call to METviewer batch script.
    """

    # Generate full path to log file that will contain output from calling the
    # METviewer batch script.
    p = Path(output_xml_fp)
    mv_batch_log_fp = ''.join([os.path.join(p.parent, p.stem), '.log'])

    # Run METviewer in batch mode on the xml.
    logging.info(dedent(f"""
        Log file for call to METviewer batch script is:
          mv_batch_log_fp = {mv_batch_log_fp}
        """))
    with open(mv_batch_log_fp, "w") as outfile:
        result = subprocess.run([mv_batch, output_xml_fp], stdout=outfile, stderr=outfile)
        logging.debug('\n'.join(['', 'Result of call to METviewer batch script:',
                                 'result = ', get_pprint_str(vars(result), '  '), '']))


def plot_vx_metviewer(argv):
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
        logging.info(dedent(f"""
            Root logger has been set up with logging level {log_level}.
            """))
    else:
        logging.info(dedent(f"""
            Using existing logger.
            """))

    # Print out logger details.
    logger = logging.getLogger()
    logging.info('\n'.join(['', 'Logger details:',
                            'logger = ', get_pprint_str(vars(logger), '  '), '']))

    # Get static parameters.  These include parameters (e.g. valid values) 
    # needed to parse the command line arguments.
    static_info_config_fp = 'vx_plots_static_info.yaml'
    logging.info(dedent(f"""
        Obtaining static verification info from file {static_info_config_fp} ...
        """))
    static_info = get_static_info(static_info_config_fp)

    # Parse arguments.
    logging.info(dedent(f"""
        Processing command line arguments ...
        """))
    cla = parse_args(argv, static_info)

    # Get METviewer database information.
    logging.info(dedent(f"""
        Obtaining METviewer database info from file {cla.mv_database_config_fp} ...
        """))
    mv_databases_dict = get_database_info(cla.mv_database_config_fp)

    # Generate a METviewer xml.
    logging.info(dedent(f"""
        Generating a METviewer xml ...
        """))
    mv_batch, output_xml_fp = generate_metviewer_xml(cla, static_info, mv_databases_dict)

    # Run METviewer on the xml to create a verification plot.
    logging.info(dedent(f"""
        Running METviewer on xml file: {output_xml_fp}
        """))
    run_mv_batch(mv_batch, output_xml_fp)

    return(output_xml_fp)
#
# -----------------------------------------------------------------------
#
# Call the function defined above.
#
# -----------------------------------------------------------------------
#
if __name__ == "__main__":
    plot_vx_metviewer(sys.argv[1:])

