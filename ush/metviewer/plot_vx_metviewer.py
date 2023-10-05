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
# fcst_var    level_or_accum             threshold    stat types
# --------    --------------             ---------    ----------
# apcp        03hr, 06hr                 none         RHST, SS
# cape        ??                         none         RHST
# dpt         2m                         none         BIAS, RHST, SS
# hgt         500mb                      none         RHST, SS
# refc        L0 (BIAS only)             none         BIAS, RHST, SS
# tmp         2m, 500mb, 700mb, 850mb    none         BIAS, RHST, SS
# wind        500mb, 700mb, 850mb        none         BIAS, RHST, SS
#
# For AUC, BRIER, FBIAS, and RELY:
# ===============================
# fcst_var    level_or_accum    threshold
# --------    --------------    ---------
# apcp        03hr              gt0.0mm (AUC,BRIER,FBIAS,RELY), ge2.54mm (AUC,BRIER,FBIAS,RELY)
# dpt         2m                ge288K (AUC,BRIER,RELY), ge293K (AUC,BRIER)
# refc        L0                ge20dBZ (AUC,BRIER,FBIAS,RELY), ge30dBZ (AUC,BRIER,FBIAS,RELY), ge40dBZ (AUC,BRIER,FBIAS,RELY), ge50dBZ (AUC,BRIER,FBIAS)
# tmp         2m, 850mb         ge288K (AUC,BRIER,RELY), ge293K (AUC,BRIER,RELY), ge298K (AUC,BRIER,RELY), ge303K (RELY)
# wind        10m, 700mb        ge5mps (AUC,BRIER,RELY), ge10mps (AUC,BRIER,RELY)
#

def get_static_info(static_fp):
    '''
    Function to read in values that are mostly static, i.e. they're usually
    not expected to change from one call to this script to another (e.g.
    valid values for various parameters).
    '''

    # Load the yaml file containing static values.
    static_data = load_config_file(static_fp)

    levels_to_levels_in_db = static_data['levels_to_levels_in_db']
    all_valid_levels = list(levels_to_levels_in_db.keys())

    threshs_to_threshs_in_db = static_data['threshs_to_threshs_in_db']
    all_valid_threshs = list(threshs_to_threshs_in_db.keys())

    # Define local dictionaries containing static values that depend on the 
    # forecast variable.
    valid_fcst_vars = static_data['fcst_vars'].keys()
    fcst_var_long_names = {}
    valid_levels_by_fcst_var = {}
    valid_threshs_by_fcst_var = {}
    for fcst_var in valid_fcst_vars:
  
        fcst_var_long_names[fcst_var] = static_data['fcst_vars'][fcst_var]['long_name']

        # Get list of valid levels/accumulations for the current forecast
        # variable.
        valid_levels_by_fcst_var[fcst_var] = static_data['fcst_vars'][fcst_var]['valid_levels']
        # Make sure all the levels/accumulations specified for the current
        # forecast variable are in the master list of valid levels/accumulations.
        for loa in valid_levels_by_fcst_var[fcst_var]:
            if loa not in all_valid_levels:
                err_msg = dedent(f"""
                    One of the levels/accumulations (loa) in the set of valid levels/accumulations
                    for the current forecast variable (fcst_var) is not in the master list of valid
                    levels/accumulations (all_valid_levels):
                      fcst_var = {fcst_var}
                      loa = {loa}
                      all_valid_levels = {all_valid_levels}
                    The master list of valid levels/accumulations as well as the list of valid levels/
                    accumulations for the current forecast variable can be found in the following static
                    information configuration file:
                      static_fp = {static_fp}
                    Please modify this file and rerun.
                    """)
                logging.error(err_msg, stack_info=True)
                raise Exception(err_msg)

        # Get list of valid thresholds for the current forecast variable.
        valid_threshs_by_fcst_var[fcst_var] = static_data['fcst_vars'][fcst_var]['valid_thresholds']
        for thresh in valid_threshs_by_fcst_var[fcst_var]:
            if thresh not in all_valid_threshs:
                err_msg = dedent(f"""
                    One of the thresholds (thresh) in the set of valid thresholds for the current
                    forecast variable (fcst_var) is not in the master list of valid thresholds
                    (all_valid_threshs):
                      fcst_var = {fcst_var}
                      thresh = {thresh}
                      all_valid_threshs = {all_valid_threshs}
                    The master list of valid thresholds as well as the list of valid threhsolds for
                    the current forecast variable can be found in the following static information
                    configuration file:
                      static_fp = {static_fp}
                    Please modify this file and rerun.
                    """)
                logging.error(err_msg, stack_info=True)
                raise Exception(err_msg)

    # Define local dictionaries containing static values that depend on the 
    # verification statistic.
    valid_stats = static_data['stats'].keys()
    stat_long_names = {}
    stat_need_thresh = {}
    for stat in valid_stats:
        stat_long_names[stat] = static_data['stats'][stat]['long_name']
        stat_need_thresh[stat] = static_data['stats'][stat]['need_thresh']

    # Get dictionary containing MetViewer color codes.  Keys are the color
    # names (e.g. 'red'), and values are the corresponding codes in MetViewer.
    mv_color_codes = static_data['mv_color_codes']

    # Create dictionary containing valid choices for various parameters.
    # This is needed by the argument parsing function below.
    choices = {}
    choices['fcst_var'] = sorted(valid_fcst_vars)
    choices['level'] = all_valid_levels
    choices['threshold'] = all_valid_threshs
    choices['vx_stat'] = sorted(valid_stats)
    choices['color'] = list(mv_color_codes.keys())

    static_info = {}
    static_info['levels_to_levels_in_db'] = levels_to_levels_in_db
    static_info['threshs_to_threshs_in_db'] = threshs_to_threshs_in_db
    static_info['fcst_var_long_names'] = fcst_var_long_names
    static_info['valid_levels_by_fcst_var'] = valid_levels_by_fcst_var
    static_info['valid_threshs_by_fcst_var'] = valid_threshs_by_fcst_var
    static_info['stat_long_names'] = stat_long_names
    static_info['stat_need_thresh'] = stat_need_thresh
    static_info['mv_color_codes'] = mv_color_codes 
    static_info['choices'] = choices

    return static_info


def get_database_info(mv_database_config_fp):
    '''
    Function to read in information about the MetViewer database from which
    verification statistics will be plotted.
    '''

    # Load the yaml file containing database information.
    mv_database_info = load_config_file(mv_database_config_fp)

    return mv_database_info


def parse_args(argv, static_info):
    '''
    Function to parse arguments for this script.
    '''

    choices = static_info['choices']

    parser = argparse.ArgumentParser(description=dedent(f'''
             Function to generate an xml file that MetViewer can read in order 
             to create a verification plot.
             '''))

    parser.add_argument('--mv_host',
                        type=str,
                        required=False, default='mohawk', 
                        help='Host (name of machine) on which MetViewer is installed')

    parser.add_argument('--mv_machine_config',
                        type=str,
                        required=False, default='mv_machine_config.yml', 
                        help='MetViewer machine (host) configuration file')

    parser.add_argument('--mv_database_config',
                        type=str,
                        required=False, default='mv_database_config.yml',
                        help='MetViewer database configuration file')

    parser.add_argument('--mv_database_name',
                        type=str,
                        required=True,
                        help='Name of MetViewer database')

    # Find the path to the directory containing the clone of the SRW App.  The index of
    # .parents will have to be changed if this script is moved elsewhere in the SRW App's
    # directory structure.
    crnt_script_fp = Path(__file__).resolve()
    home_dir = crnt_script_fp.parents[2]
    expts_dir = Path(os.path.join(home_dir, '../expts_dir')).resolve()
    parser.add_argument('--mv_output_dir',
                        type=str,
                        required=False, default=os.path.join(expts_dir, 'mv_output'),
                        help='Directory in which to place output (e.g. plots) from MetViewer')

    parser.add_argument('--model_names', nargs='+',
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

    parser.add_argument('--fcst_var',
                        type=str.lower,
                        required=True, 
                        choices=choices['fcst_var'],
                        help='Name of forecast variable to verify')

    parser.add_argument('--level_or_accum',
                        type=str,
                        required=False,
                        choices=choices['level'],
                        help='Vertical level or accumulation period')

    parser.add_argument('--threshold',
                        type=str,
                        required=False, default='',
                        choices=choices['threshold'],
                        help='Threshold for specified forecast variable')

    cla = parser.parse_args(argv)

    cla_str = pprint.pformat(vars(cla))
    cla_str = '\n                '.join(cla_str.splitlines())
    logging.info(dedent(f"""
        List of arguments passed to script:
          cla = {cla_str}
        """))

    return cla


def generate_metviewer_xml(cla, static_info, mv_database_info):
    """Function that generates an xml file that MetViewer can read (in order
       to create a verification plot).

    Args:
        argv:  Command-line arguments

    Returns:
        None
    """

    levels_to_levels_in_db = static_info['levels_to_levels_in_db']
    threshs_to_threshs_in_db = static_info['threshs_to_threshs_in_db']
    fcst_var_long_names = static_info['fcst_var_long_names']
    valid_levels_by_fcst_var = static_info['valid_levels_by_fcst_var']
    valid_threshs_by_fcst_var = static_info['valid_threshs_by_fcst_var']
    stat_long_names = static_info['stat_long_names']
    stat_need_thresh = static_info['stat_need_thresh']
    mv_color_codes = static_info['mv_color_codes']

    # Load the machine configuration file into a dictionary and find in it the
    # machine specified on the command line.
    mv_machine_config_fp = Path(os.path.join(cla.mv_machine_config)).resolve()
    mv_machine_config = load_config_file(mv_machine_config_fp)

    all_hosts = sorted(list(mv_machine_config.keys()))
    if cla.mv_host not in all_hosts:
        err_msg = dedent(f"""
            The machine/host specified on the command line (cla.mv_host) does not have a
            corresponding entry in the MetViewer host configuration file (mv_machine_config_fp):
              cla.mv_host = {cla.mv_host}
              mv_machine_config_fp = {mv_machine_config_fp}
            Machines that do have an entry in the host configuration file are:
              {all_hosts}
            Either run on one of these hosts, or add an entry in the configuration file for "{cla.mv_host}".
            """)
        logging.error(err_msg, stack_info=True)
        raise Exception(err_msg)

    mv_machine_config_dict = mv_machine_config[cla.mv_host]

    # Make sure that the database specified on the command line exists in the
    # list of databases in the database configuration file.
    if cla.mv_database_name not in mv_database_info.keys():
        err_msg = dedent(f"""
            The database specified on the command line (cla.mv_database_name) is not
            in the set of MetViewer databases specified in the database configuration
            file (cla.mv_database_config):
              cla.mv_database_name = {cla.mv_database_name}
              cla.mv_database_config = {cla.mv_database_config}
            """)
        logging.error(err_msg, stack_info=True)
        raise Exception(err_msg)

    # Extract the MetViewer database information.
    model_info = mv_database_info[cla.mv_database_name]
    num_models = len(model_info)

    # Get the number of ensemble members for each model and make sure all are
    # positive.
    num_ens_mems = [model_info[m]['num_ens_mems'] for m in cla.model_names]
    for i,model in enumerate(cla.model_names):
        n_ens = num_ens_mems[i]
        if n_ens <= 0:
            err_msg = dedent(f"""
                The number of ensemble members for the current model must be greater
                than or equal to 0:
                  model = {model}
                  n_ens = {n_ens}
                """)
            logging.error(err_msg, stack_info=True)
            raise Exception(err_msg)

    # Get the model names in the database as well as the model short names.
    model_names_in_db = [model_info[m]['name_in_db'] for m in cla.model_names]
    model_names_short_uc = [m.upper() for m in cla.model_names]

    # Pick out the plot color associated with each model from the list of 
    # available colors.
    model_color_codes = [mv_color_codes[m] for m in cla.colors]

    # Set the initialization times for the forecasts.
    fcst_init_time_first = datetime.strptime(cla.fcst_init_info[0], '%Y%m%d%H')
    num_fcsts = int(cla.fcst_init_info[1])
    fcst_init_intvl = timedelta(hours=int(cla.fcst_init_info[2]))
    fcst_init_times = list(range(0,num_fcsts))
    fcst_init_times = [fcst_init_time_first + i*fcst_init_intvl for i in fcst_init_times]
    fcst_init_times = [i.strftime("%Y-%m-%d %H:%M:%S") for i in fcst_init_times]

    fcst_init_times_str = '\n          '.join(fcst_init_times)
    logging.info(dedent(f"""
        Forecast initialization times (fcst_init_times):
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

    valid_levels_or_accums = valid_levels_by_fcst_var[cla.fcst_var]
    if cla.level_or_accum not in valid_levels_or_accums:
        err_msg = dedent(f"""
            The specified level or accumulation is not compatible with the specified forecast variable:
              cla.fcst_var = {cla.fcst_var}
              cla.level_or_accum = {cla.level_or_accum}
            Valid options for level or accumulation for this forecast variable are:
              {valid_levels_or_accums}
            """)
        logging.error(err_msg, stack_info=True)
        raise Exception(err_msg)

    # Parse the level/accumulation specified on the command line (cla.level_or_accum) 
    # to obtain its value and units.  The returned value is a list.  If the regular
    # expression doesn't match anything in cla.level_or_accum (e.g. if the latter is
    # set to 'L0'), an empty list will be returned.  In that case, the else portion 
    # of the if-else construct below will set loa_value and loa_units to empty strings.
    loa = re.findall(r'(\d*\.*\d+)([A-Za-z]+)', cla.level_or_accum)

    if loa:
        logging.info(dedent(f"""
            Parsing specified level or accumulation...
            """))
        loa_value, loa_units = list(loa[0])
    else:
        loa_value = ''
        loa_units = ''

    valid_thresh_units = ['', 'h', 'm', 'mb']
    if loa_units not in valid_thresh_units:
        err_msg = dedent(f"""
            Unknown units (loa_units) for level or accumulation:
              loa_units = {loa_units}
            Valid units are:
              valid_thresh_units = {valid_thresh_units}
            Related variables:
              cla.level_or_accum = {cla.level_or_accum}
              loa_value = {loa_value}
              loa_value_no0pad = {loa_value_no0pad}
            """)
        logging.error(err_msg, stack_info=True)
        raise Exception(err_msg)

    loa_value_no0pad = loa_value.lstrip('0')
    width_0pad = 0
    if loa_units == 'h':
        width_0pad = 2
    elif loa_units == 'm':
        width_0pad = 2
    elif loa_units == 'mb':
        width_0pad = 3
    elif (loa_units == '' and cla.level_or_accum == 'L0'):
        logging.info(dedent(f"""
            Since the specified level/accumulation is {cla.level_or_accum}, setting loa_units
            to an empty string:
              cla.level_or_accum = {cla.level_or_accum}
              loa_units = {loa_units}
            Related variables:
              loa_value = {loa_value}
              loa_value_no0pad = {loa_value_no0pad}
            """))

    loa_value_0pad = loa_value_no0pad.zfill(width_0pad)
    logging.info(dedent(f"""
        Level or accumulation parameters are set as follows:
          loa_value = {loa_value}
          loa_value_no0pad = {loa_value_no0pad}
          loa_value_0pad = {loa_value_0pad}
          loa_units = {loa_units}
        """))

    if (not stat_need_thresh[cla.vx_stat]) and (cla.threshold):
        no_thresh_stats = [key for key,val in stat_need_thresh.items() if val]
        no_thresh_stats_fmt_str = ",\n".join("              {!r}: {!r}".format(k, v)
                                             for k, v in stat_long_names.items() if k in no_thresh_stats).lstrip()
        logging.info(dedent(f"""
            A threshold is not needed when working with one of the following verification stats:
              {no_thresh_stats_fmt_str}
            Thus, the threshold specified in the argument list ("{cla.threshold}") will be reset to an empty string.
            """))
        cla.threshold = ''

    elif (stat_need_thresh[cla.vx_stat]):
        valid_thresholds = valid_threshs_by_fcst_var[cla.fcst_var]
        if cla.threshold not in valid_thresholds:
            err_msg = dedent(f"""
                The specified threshold is not compatible with the specified forecast variable:
                  fcst_var = {cla.fcst_var}
                  threshold = {cla.threshold}
                Valid options for threshold for this forecast variable are:
                  {valid_thresholds}
                """)
            logging.error(err_msg, stack_info=True)
            raise Exception(err_msg)

    thresh = re.findall(r'([A-Za-z]+)(\d*\.*\d+)([A-Za-z]+)', cla.threshold)
    if thresh:
        logging.info(dedent(f"""
            Parsing specified threshold to obtain comparison operator, value, and units...
            """))
        thresh_comp_oper, thresh_value, thresh_units = list(thresh[0])

        if thresh_comp_oper[0] == 'l': 
            thresh_comp_oper_xml = '&lt;'
        elif thresh_comp_oper[0] == 'g': 
            thresh_comp_oper_xml = '&gt;'

        if thresh_comp_oper[1] == 'e': 
            thresh_comp_oper_xml = "".join([thresh_comp_oper_xml, '='])

        thresh_in_plot_title = " ".join([thresh_comp_oper_xml, thresh_value, thresh_units])

    else:
        thresh_comp_oper = ''
        thresh_value = ''
        thresh_units = ''
        thresh_in_plot_title = ''

    logging.info(dedent(f"""
        Threshold parameters are set as follows:
          thresh_comp_oper = {thresh_comp_oper}
          thresh_value = {thresh_value}
          thresh_units = {thresh_units}
          thresh_in_plot_title = {thresh_in_plot_title}
        """))

    plot_title = " ".join(filter(None,
                          [stat_long_names[cla.vx_stat], 'for',
                           loa_value, loa_units, fcst_var_long_names[cla.fcst_var],
                           thresh_in_plot_title]))
    fcst_var_uc = cla.fcst_var.upper()
    var_lvl_str = ''.join(filter(None, [fcst_var_uc, loa_value, loa_units]))
    thresh_str = ''.join(filter(None, [thresh_comp_oper, thresh_value, thresh_units]))
    var_lvl_thresh_str = '_'.join(filter(None, [var_lvl_str, thresh_str]))

    models_str = '_'.join(cla.model_names)
    job_title = '_'.join([cla.vx_stat, var_lvl_thresh_str, models_str])

    logging.info(dedent(f"""
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
    level_in_db = levels_to_levels_in_db[cla.level_or_accum]
    thresh_in_db = threshs_to_threshs_in_db[cla.threshold]

    line_types = list()
    for imod in range(0,num_models):
        if incl_ens_means: line_types.append('b')
        line_types.extend(["l" for imem in range(0,num_ens_mems[imod])])

    line_widths = [1 for imod in range(0,num_models) for imem in range(0,num_ens_mems[imod])]

    num_series = sum(num_ens_mems[0:num_models])
    if incl_ens_means: num_series = num_series + num_models
    order_series = [s for s in range(1,num_series+1)]

    # Generate name of forecast variable as it appears in the MetViewer database.
    fcst_var_name_in_db = fcst_var_uc
    # The following only works if the accumulations are specifed with a leading zero, e.g. 03h, 06h (24h doesn't matter).
    if fcst_var_uc == 'APCP': fcst_var_name_in_db = '_'.join([fcst_var_name_in_db, cla.level_or_accum[0:2]])
    if cla.vx_stat in ['auc', 'brier', 'rely']:
        fcst_var_name_in_db = '_'.join(filter(None,[fcst_var_name_in_db, 'ENS_FREQ', 
                                                    ''.join([thresh_comp_oper, thresh_value])]))
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
        # (fcst_var_name_in_db).  Note that these zeros are not necessary; for simplicity,
        # the METplus configuration files in the SRW App should be changed so that these
        # zeros are not added.  Once that is done, the following code should be removed
        # (otherwise the variables will not be found in the database).
        #
        if thresh_value in ['6.35']: fcst_var_name_in_db = ''.join([fcst_var_name_in_db, '0'])
        elif thresh_value in ['12.7', '25.4']: fcst_var_name_in_db = ''.join([fcst_var_name_in_db, '00'])

    # Generate name for the verification statistic that MetViewer understands.
    vx_stat_mv = cla.vx_stat.upper()
    if vx_stat_mv == 'BIAS': vx_stat_mv = 'ME'
    elif vx_stat_mv == 'AUC': vx_stat_mv = 'PSTD_ROC_AUC'
    elif vx_stat_mv == 'BRIER': vx_stat_mv = 'PSTD_BRIER'

    # For the given forecast variable, generate a name for the corresponding
    # observation type in the MetViewer database.
    obs_type = ''
    if cla.fcst_var == 'apcp' :
        obs_type = 'CCPA'
    elif cla.fcst_var == 'refc' :
        obs_type = 'MRMS'
    # The level for CAPE is 'L0', which means the surface, but its obtype is ADPUPA
    # (upper air).  It's a bit unintuitive...
    elif cla.fcst_var == 'cape' :
        obs_type = 'ADPUPA'
    elif cla.level_or_accum in ['2m','02m','10m']:
        obs_type = 'ADPSFC'
    elif cla.level_or_accum in ['500mb','700mb','850mb']:
        obs_type = 'ADPUPA'

    logging.info(dedent(f"""
        Subset of strings passed to jinja template:
          fcst_var_uc = {fcst_var_uc}
          fcst_var_name_in_db = {fcst_var_name_in_db}
          vx_stat_mv = {vx_stat_mv}
          obs_type = {obs_type}
        """))

    # Create dictionary containing values for the variables appearing in the
    # jinja template.
    jinja_vars = {"mv_host": cla.mv_host,
                  "mv_machine_config_dict": mv_machine_config_dict,
                  "mv_database_name": cla.mv_database_name,
                  "mv_output_dir": cla.mv_output_dir,
                  "num_models": num_models,
                  "num_ens_mems": num_ens_mems,
                  "model_names_in_db": model_names_in_db,
                  "model_names_short_uc": model_names_short_uc,
                  "model_color_codes": model_color_codes,
                  "fcst_var_uc": fcst_var_uc,
                  "fcst_var_name_in_db": fcst_var_name_in_db,
                  "level_in_db": level_in_db,
                  "level_or_accum_no0pad": loa_value_no0pad,
                  "thresh_in_db": thresh_in_db,
                  "obs_type": obs_type,
                  "vx_stat_uc": cla.vx_stat.upper(),
                  "vx_stat_lc": cla.vx_stat.lower(),
                  "vx_stat_mv": vx_stat_mv,
                  "num_fcsts": num_fcsts,
                  "fcst_init_times": fcst_init_times,
                  "fcst_len_hrs": cla.fcst_len_hrs,
                  "job_title": job_title,
                  "plot_title": plot_title,
                  "incl_ens_means": incl_ens_means,
                  "num_series": num_series,
                  "order_series": order_series,
                  "line_types": line_types,
                  "line_widths": line_widths}

    jinja_vars_str = pprint.pformat(jinja_vars, compact=True)
    jinja_vars_str = '\n          '.join(jinja_vars_str.splitlines())
    logging.info(dedent(f"""
        Jinja variables (jinja_vars) passed to template:
          {jinja_vars_str}
        """))

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
    # MetViewer will generate from the xmls.
    output_xml_dir = Path(os.path.join(cla.mv_output_dir, 'plots')).resolve()
    print(f"cla.mv_output_dir = {cla.mv_output_dir}")
    print(f"output_xml_dir = {output_xml_dir}")
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

    # Convert the dictionary of jinja variable settings above to yaml format
    # and write it to a temporary yaml file for reading by the set_template
    # function.
    tmp_fn = 'tmp.yaml'
    with open(f'{tmp_fn}', 'w') as fn:
        yaml_vars = yaml.dump(jinja_vars, fn)

    args_list = ['--quiet',
                 '--config_file', tmp_fn,
                 '--input_template', template_fp,
                 '--outfile', output_xml_fp]
    set_template(args_list)
    os.remove(tmp_fn)

    return(mv_machine_config_dict['mv_batch'], output_xml_fp)


def run_mv_batch(mv_batch, output_xml_fp):
    """Function that generates a verification plot using MetViewer.

    Args:
        mv_batch:       Path to MetViewer batch plotting script.
        output_xml_fp:  Full path to the xml to pass to the batch script.

    Returns:
        None
    """

    # Run MetViewer in batch mode on the xml.
    subprocess.run([mv_batch, output_xml_fp])


def plot_vx_metviewer(argv):

    # Set the logging level.
    logging.basicConfig(level=logging.INFO)

    # Get static parameters.  These include parameters (e.g. valid values) 
    # needed to parse the command line arguments.
    static_info_config_fp = 'vx_plots_static_info.yml'
    logging.info(dedent(f"""
        Obtaining static verification info from file {static_info_config_fp}...
        """))
    static_info = get_static_info(static_info_config_fp)

    # Parse arguments.
    logging.info(dedent(f"""
        Processing command line arguments...
        """))
    cla = parse_args(argv, static_info)

    # Get MetViewer database information.
    logging.info(dedent(f"""
        Obtaining MetViewer database info from file {cla.mv_database_config}...
        """))
    mv_database_info = get_database_info(cla.mv_database_config)

    # Generate a MetViewer xml.
    logging.info(dedent(f"""
        Generating a MetViewer xml...
        """))
    mv_batch, output_xml_fp = generate_metviewer_xml(cla, static_info, mv_database_info)

    # Run MetViewer on the xml to create a verification plot.
    logging.info(dedent(f"""
        Running MetViewer on xml file: {output_xml_fp}
        """))
    run_mv_batch(mv_batch, output_xml_fp)
#
# -----------------------------------------------------------------------
#
# Call the function defined above.
#
# -----------------------------------------------------------------------
#
if __name__ == "__main__":
    plot_vx_metviewer(sys.argv[1:])

