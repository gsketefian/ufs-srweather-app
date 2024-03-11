#!/usr/bin/env python3

import os
import sys
import shutil
from datetime import datetime
import glob
import argparse
import yaml

import logging
import copy
import textwrap
from textwrap import dedent

import pprint
import subprocess

from make_single_mv_vx_plot import make_single_mv_vx_plot
from make_single_mv_vx_plot import get_pprint_str

from pathlib import Path
file = Path(__file__).resolve()
ush_dir = file.parents[1]
sys.path.append(str(ush_dir))

from python_utils import (
    log_info,
    load_config_file,
)

def check_for_preexisting_dir_file(dir_or_file, preexist_method):
    """
    Function to check and handle preexisting directory or file.

    Arguments:
    ---------
    dir_or_file:
      Name of directory or file.

    preexist_method:
      Method to use to deal with a preexisting version of dir_or_file.  This
      has 3 valid values:
        'rename':  Causes the existing dir_or_file to be renamed.
        'delete':  Causes the existing dir_or_file to be deleted.
        'quit':    Causes the script to quit if dir_or_file already exists.

    Returns:
    -------
    None
    """

    valid_vals_preexist_method = ['rename', 'delete', 'quit']
    msg_invalid_preexist_method = dedent(f"""
        Invalid value for preexist_method:
          {get_pprint_str(preexist_method)}
        Valid values are:
          {get_pprint_str(valid_vals_preexist_method)}
        Stopping.
        """)

    if preexist_method not in valid_vals_preexist_method:
        logging.error(msg_invalid_preexist_method)
        raise ValueError(msg_invalid_preexist_method)

    if os.path.exists(dir_or_file):
        if preexist_method == 'rename':
            now = datetime.now()
            renamed_dir_or_file = dir_or_file + now.strftime('.old_%Y%m%d_%H%M%S')
            msg = dedent(f"""
                Output directory already exists:
                  {get_pprint_str(dir_or_file)}
                Moving (renaming) preexisting directory to:
                  {get_pprint_str(renamed_dir_or_file)}
                """)
            logging.debug(msg)
            os.rename(dir_or_file, renamed_dir_or_file)
        elif preexist_method == 'delete':
            msg = dedent(f"""
                Output directory already exists:
                  {get_pprint_str(dir_or_file)}
                Removing existing directory...
                """)
            logging.info(msg)
            shutil.rmtree(dir_or_file)
        elif preexist_method == 'quit':
            msg = dedent(f"""
                Output directory already exists:
                  {get_pprint_str(dir_or_file)}
                Stopping.
                """)
            logging.error(msg)
            raise FileExistsError(msg)
        else:
            logging.error(msg_invalid_preexist_method)
            raise ValueError(msg_invalid_preexist_method)


def make_multi_mv_vx_plots(args, valid_vals, vx_metric_needs_thresh):
    """
    Function to make multiple verification (vx) plots using METviewer.

    Arguments:
    ---------
    args:
      Dictionary of arguments.

    valid_vals:
      Dictionary of valid values of various parameters.

    vx_metric_needs_thresh:
      Dictionary that specifies whether or not each valid vx metric requires a
      threshold.

    Returns:
    -------
    None
    """

    # Set up logging.
    # If the name/path of a log file has been specified in the command line
    # arguments, place the logging output in it (existing log files of the
    # same name are overwritten).  Otherwise, direct the output to the screen.
    log_level = str.upper(args.log_level)
    msg_format = "[%(levelname)s:%(name)s:  %(filename)s, line %(lineno)s: %(funcName)s()] %(message)s"
    if args.log_fp:
        logging.basicConfig(level=log_level, format=msg_format, filename=args.log_fp, filemode='w')
    else:
        logging.basicConfig(level=log_level, format=msg_format)

    # Read in the plot configuration file.
    plot_config_fp = args.plot_config_fp
    plot_config_dict = load_config_file(plot_config_fp)
    msg = dedent(f"""
        Reading in plot configuration file: {get_pprint_str(plot_config_fp)}
        """)
    logging.debug(msg)
    mv_host = plot_config_dict['mv_host']
    mv_database_name = plot_config_dict['mv_database_name']
    model_names = plot_config_dict['model_names']
    fcst_init_info = plot_config_dict['fcst_init_info']
    fcst_len_hrs = plot_config_dict['fcst_len_hrs']
    metrics_fields_levels_threshes_dict = plot_config_dict["metrics_fields_levels_threshes"]

    # Load the yaml-format METviewer database configuration file and extract
    # from it the list of valid threshold values for the database specified
    # in the plot configuration file.
    mv_databases_config_fp = 'mv_databases.yaml'
    mv_databases_dict = load_config_file(mv_databases_config_fp)
    valid_threshes_for_db = list(mv_databases_dict[mv_database_name]['valid_threshes'])

    # Some of the values in the fcst_init_info dictionary are strings while
    # others are integers.  Also, we don't need the keys.  Thus, convert
    # that variable into a list containing only string values since that's
    # what jinja2 templates expect.
    fcst_init_info = [str(elem) for elem in fcst_init_info.values()]

    # Convert fcst_len_hrs from an integer to a string since that's what
    # the jinja2 templates exptect.
    fcst_len_hrs = str(fcst_len_hrs)

    # Check if output directory exists and take action according to how the
    # args.preexisting_dir_method flag is set.
    check_for_preexisting_dir_file(args.output_dir, args.preexisting_dir_method)

    # If the flag create_ordered_plots is set to True, create (if it doesn't
    # already exist) a new directory in which we will store copies of all
    # the images (png files) that METviewer will generate such that the
    # images are ordered via an index in their name.  This allows a pdf to
    # quickly be created from this directory (e.g. using tools available in
    # Adobe Acrobat) that contains all the plots in the order they were
    # listed in the plot configuration file that this script reads in.
    if args.create_ordered_plots:
        ordered_plots_dir = os.path.join(args.output_dir, 'ordered_plots')
        Path(ordered_plots_dir).mkdir(parents=True, exist_ok=True)

    # Get valid values for verification (vx) metrics, forecast fields, and
    # forecast levels.
    valid_vx_metrics = valid_vals['vx_metrics']
    valid_fcst_fields = valid_vals['fcst_fields']
    valid_fcst_levels = valid_vals['fcst_levels']

    # Ensure that any thresholds passed to the --incl_only_threshes or
    # --excl_threshes option are valid ones for the METviewer database
    # specified in the plot configuration file.
    options = ['incl_only_threshes', 'excl_threshes']
    for option, threshes_list_for_option in {opt: getattr(args, opt) for opt in options}.items():
        threshes_not_in_db = list(set(threshes_list_for_option).difference(valid_threshes_for_db))
        if threshes_not_in_db:
            msg = dedent(f"""
                One or more thresholds passed to the '--{option}' option are
                not valid for the specified database.  The name of this database is:
                  mv_database_name = {get_pprint_str(mv_database_name)}
                The specified thresholds that are not valid for this database are:
                  threshes_not_in_db = {get_pprint_str(threshes_not_in_db)}
                If these thresholds are in fact in the database, then add them to the
                list of valid thresholds in the database configuration file and rerun.
                The database configuration file is:
                  mv_databases_config_fp = {get_pprint_str(mv_databases_config_fp)}
                Thresholds that are currently specified in this file as valid for the
                database are:
                  valid_threshes_for_db
                  = """) + \
                get_pprint_str(valid_threshes_for_db, ' '*4).lstrip() + \
                dedent(f"""
                Stopping.
                """)
            logging.error(msg)
            raise ValueError(msg)

    # Ensure that in the plot configuration file, metric-field-level
    # combinations with metrics that require thresholds have a non-empty
    # thresholds list, and those combinations that do not need thresholds
    # have an empty threshold lists.
    for metric, fields_levels_threshes_dict in metrics_fields_levels_threshes_dict.copy().items():
        for field, levels_threshes_dict in fields_levels_threshes_dict.copy().items():
            for level, threshes_list in levels_threshes_dict.copy().items():
                if vx_metric_needs_thresh[metric] and (not threshes_list):
                    msg = dedent(f"""
                        In the plot configuration file
                          plot_config_fp = {get_pprint_str(plot_config_fp)}
                        the metric-field-level combination
                          metric = {get_pprint_str(metric)}
                          field = {get_pprint_str(field)}
                          level = {get_pprint_str(level)}
                        is assigned an empty threshold list, i.e.
                          threshes_list = {get_pprint_str(threshes_list)}
                        but any such combination involving this metric must have (and should be
                        assigned) associated thresholds.  Please reset the threshold list for
                        this metric-field-level combination in the plot configuration file to a
                        list of valid thresholds.  Stopping.
                        """)
                    logging.error(msg)
                    raise Exception(msg)
                elif (not vx_metric_needs_thresh[metric]) and threshes_list:
                    msg = dedent(f"""
                        In the plot configuration file
                          plot_config_fp = {get_pprint_str(plot_config_fp)}
                        the metric-field-level combination
                          metric = {get_pprint_str(metric)}
                          field = {get_pprint_str(field)}
                          level = {get_pprint_str(level)}
                        is assigned the non-empty threshold list
                          threshes_list = {get_pprint_str(threshes_list)}
                        but any such combination involving this metric does not need (and should
                        not be assigned) any thresholds.  Please reset the threshold list for
                        this metric-field-level combination in the plot configuration file to an
                        empty list and rerun.  Stopping.
                        """)
                    logging.error(msg)
                    raise Exception(msg)

    # Below, changes to args might be made.  Save the original copy.  We do
    # this using deepcopy() since otherwise args_orig and args will be pointing
    # to the same object in memory.
    args_orig = copy.deepcopy(args)

    # Ensure that all the metrics passed to the --incl_only_metrics or
    # --excl_metrics option appear in the plot configuration file.  For each
    # such metric that is absent from the configuration file, remove it from
    # the list passed to the option and issue a warning.
    options = ['incl_only_metrics', 'excl_metrics']
    for option, metrics_list_for_option in {opt: getattr(args, opt) for opt in options}.copy().items():
        for metric in metrics_list_for_option.copy():
            metric_count = 0
            if metric in metrics_fields_levels_threshes_dict: metric_count += 1
            if metric_count == 0:
                getattr(args, option).remove(metric)
                msg = dedent(f"""
                    The metric
                      metric = {get_pprint_str(metric)}
                    passed to the '--{option}' option does not appear in the plot
                    configuration file, which is at
                      plot_config_fp = {get_pprint_str(plot_config_fp)}
                    The plot configuration dictionary read in from this file is:
                      metrics_fields_levels_threshes_dict
                      = """) + \
                    get_pprint_str(metrics_fields_levels_threshes_dict, ' '*4).lstrip() + \
                    dedent(f"""
                    Removing this metric from the list passed to the '--{option}' option.
                    The new set of metrics passed to this option is now considered to be:
                      args.{option} = {getattr(args, option)}""")
                msg_extra = ''
                if option == 'incl_only_metrics':
                    msg_extra = dedent(f"""
                        Thus, no plots for metric '{metric}' will be generated.
                        """)
                elif option == 'excl_metrics':
                   msg_extra = dedent(f"""
                       The metric '{metric}' passed to '--{option}' will be ignored.
                       """)
                msg = msg + msg_extra
                logging.warning(msg)

    # Ensure that all the fields passed to the --incl_only_fields or
    # --excl_fields option appear under at least one metric in the plot
    # configuration file.  For each such field that is absent from the
    # configuration file, remove it from the list passed to the option and
    # issue a warning.
    options = ['incl_only_fields', 'excl_fields']
    for option, fields_list_for_option in {opt: getattr(args, opt) for opt in options}.copy().items():
        for field in fields_list_for_option.copy():
            field_count = 0
            for metric, fields_levels_threshes_dict in metrics_fields_levels_threshes_dict.items():
                if field in fields_levels_threshes_dict: field_count += 1
            if field_count == 0:
                getattr(args, option).remove(field)
                msg = dedent(f"""
                    The field
                      field = {get_pprint_str(field)}
                    passed to the '--{option}' option does not appear in the plot
                    configuration file, which is at
                      plot_config_fp = {get_pprint_str(plot_config_fp)}
                    The plot configuration dictionary read in from this file is:
                      metrics_fields_levels_threshes_dict
                      = """) + \
                    get_pprint_str(metrics_fields_levels_threshes_dict, ' '*4).lstrip() + \
                    dedent(f"""
                    Removing this field from the list passed to the '--{option}' option.
                    The new set of fields passed to this option is now considered to be:
                      args.{option} = {getattr(args, option)}""")
                msg_extra = ''
                if option == 'incl_only_fields':
                    msg_extra = dedent(f"""
                        Thus, no plots for field '{field}' will be generated.
                        """)
                elif option == 'excl_fields':
                   msg_extra = dedent(f"""
                       The field '{field}' passed to '--{option}' will be ignored.
                       """)
                msg = msg + msg_extra
                logging.warning(msg)

    # Ensure that all the levels passed to the --incl_only_levels or
    # --excl_levels option appear under at least one metric-field combination
    # in the plot configuration file.  For each such field that is absent
    # from the configuration file, remove it from the list passed to the
    # option and issue a warning.
    options = ['incl_only_levels', 'excl_levels']
    for option, levels_list_for_option in {opt: getattr(args, opt) for opt in options}.copy().items():
        for level in levels_list_for_option.copy():
            level_count = 0
            for metric, fields_levels_threshes_dict in metrics_fields_levels_threshes_dict.items():
                for field, levels_threshes_dict in fields_levels_threshes_dict.items():
                    if level in levels_threshes_dict: level_count += 1
            if level_count == 0:
                getattr(args, option).remove(level)
                msg = dedent(f"""
                    The level
                      level = {get_pprint_str(level)}
                    passed to the '--{option}' option does not appear in the plot
                    configuration file, which is at
                      plot_config_fp = {get_pprint_str(plot_config_fp)}
                    The plot configuration dictionary read in from this file is:
                      metrics_fields_levels_threshes_dict
                      = """) + \
                    get_pprint_str(metrics_fields_levels_threshes_dict, ' '*4).lstrip() + \
                    dedent(f"""
                    Removing this level from the list passed to the '--{option}' option.
                    The new set of levels passed to this option is now considered to be:
                      args.{option} = {getattr(args, option)}""")
                msg_extra = ''
                if option == 'incl_only_levels':
                    msg_extra = dedent(f"""
                        Thus, no plots at level '{level}' will be generated.
                        """)
                elif option == 'excl_levels':
                   msg_extra = dedent(f"""
                       The level '{level}' passed to '--{option}' will be ignored.
                       """)
                msg = msg + msg_extra
                logging.warning(msg)

    # Ensure that all the thresholds passed to the --incl_only_threshes or
    # --excl_threshes option appear under at least one metric-field-level
    # combination in the plot configuration file.  For each such threshold
    # that is absent from the configuration file, remove it from the list
    # passed to the option and issue a warning.
    options = ['incl_only_threshes', 'excl_threshes']
    for option, threshes_list_for_option in {opt: getattr(args, opt) for opt in options}.copy().items():
        for thresh in threshes_list_for_option.copy():
            thresh_count = 0
            for metric, fields_levels_threshes_dict in metrics_fields_levels_threshes_dict.items():
                for field, levels_threshes_dict in fields_levels_threshes_dict.items():
                    for level, threshes_list in levels_threshes_dict.copy().items():
                        if thresh in threshes_list: thresh_count += 1
            if thresh_count == 0:
                getattr(args, option).remove(thresh)
                msg = dedent(f"""
                    The threshold
                      thresh = {get_pprint_str(thresh)}
                    passed to the '--{option}' option does not appear in the plot
                    configuration file, which is at
                      plot_config_fp = {get_pprint_str(plot_config_fp)}
                    The plot configuration dictionary read in from this file is:
                      metrics_fields_levels_threshes_dict
                      = """) + \
                    get_pprint_str(metrics_fields_levels_threshes_dict, ' '*4).lstrip() + \
                    dedent(f"""
                    Removing this threshold from the list passed to the '--{option}' option.
                    The new set of thresholds passed to this option is now considered to be:
                      args.{option} = {getattr(args, option)}""")
                msg_extra = ''
                if option == 'incl_only_threshes':
                    msg_extra = dedent(f"""
                        Thus, no plots for threshold '{thresh}' will be generated.
                        """)
                elif option == 'excl_threshes':
                   msg_extra = dedent(f"""
                       The threshold '{thresh}' passed to '--{option}' will be ignored.
                       """)
                msg = msg + msg_extra
                logging.warning(msg)

    # After removing metrics, fields, levels, and/or thresholds passed to
    # the --incl_only_[metrics|fields|levels|threshes] options that do not
    # appear in the plot configuration file, check that there are still
    # metric-field-level-threshold combinations left to plot.  If not, print
    # out an error message and stop.
    plot_params = {'metric': {'name': 'metric', 'in_option': 'metrics'},
                   'field': {'name': 'field', 'in_option': 'fields'},
                   'level': {'name': 'level',  'in_option': 'levels'},
                   'thresh': {'name': 'threshold', 'in_option': 'threshes'}}
    for param_abbr, param_dict in plot_params.items():
        param_name = param_dict['name']
        option = 'incl_only_' + param_dict['in_option']
        if getattr(args_orig, option) and not getattr(args, option):
            msg = dedent(f"""
                All the {param_name}s originally passed to the '--{option}' option on
                the command line have been removed because they do not appear in the
                plot configuration file, which is at
                  plot_config_fp = {get_pprint_str(plot_config_fp)}
                The original set of {param_name}s passed to '--{option}' was:
                  args_orig.{option} = {getattr(args_orig, option)}
                After removing {param_name}s in this list that do not appear in the plot
                configuration file, the list is emtpy:
                  args.{option} = {getattr(args, option)}
                Since '--{option}' specifies an exclusive list, the remaining
                metric-field-level-threshold combinations in the plot configuration file
                correspond to {param_name}s that should not be plotted.  Stopping.
                """)
            logging.error(msg)
            raise Exception(msg)

    ############
    # In the next few steps, reduce (prune) the plot configuration dictionary
    # to keep only those metric-field-level-threshold combinations that should
    # be plotted (i.e. they are not excluded via command-line options).
    ############

    # If the --incl_only_metrics option was specified on the command line
    # (i.e. if args_orig.incl_only_metrics is not empty) and has been passed
    # at least one metric that appears in the plot configuration file (i.e.
    # if args.incl_only_metrics is not empty), then remove from the plot
    # configuration dictionary any metric that is NOT in args.incl_only_metrics.
    if args_orig.incl_only_metrics and args.incl_only_metrics:
        [metrics_fields_levels_threshes_dict.pop(metric, None)
         for metric in valid_vx_metrics if metric not in args.incl_only_metrics]

    # If the --excl_metrics option was specified on the command line (i.e.
    # if args_orig.excl_metrics is not empty) and has been passed at least
    # one metric that appears in the plot configuration file (i.e. if
    # args.excl_metrics is not empty), then remove from the plot configuration
    # dictionary any metric that is in args.excl_metrics.
    if args_orig.excl_metrics and args.excl_metrics:
        [metrics_fields_levels_threshes_dict.pop(metric, None) for metric in args.excl_metrics]

    # If after removing the necessary metrics from the plot configuration
    # dictionary there are no metric-field-level-threshold combinations left
    # in the dictionary to plot (i.e. if the dictionary has become empty),
    # print out an error message and exit.
    if not metrics_fields_levels_threshes_dict:
        msg = dedent(f"""
            After removing verification (vx) metrics from the plot configuration
            dictionary according to the arguments passed to the '--incl_only_metrics'
            or '--excl_metrics' option, there are no remaining metric-field-level-
            threshold combinations in the dictionary to plot, i.e. the plot
            configuration dictionary is empty:
              metrics_fields_levels_threshes_dict = {get_pprint_str(metrics_fields_levels_threshes_dict)}
            Please modify the plot configuration file and/or the arguments to one of
            the options above and rerun.  The plot configuration file is:
              plot_config_fp = {get_pprint_str(plot_config_fp)}
            Stopping.
            """)
        logging.error(msg)
        raise Exception(msg)

    # If the --incl_only_fields option was specified on the command line
    # (i.e. if args_orig.incl_only_fields is not empty) and has been passed
    # at least one field that appears in the plot configuration file (i.e.
    # if args.incl_only_fields is not empty), then for each metric to be
    # plotted, remove from the corresponding sub-dictionary in the plot
    # configuration dictionary any field that is NOT in args.incl_only_fields.
    if args_orig.incl_only_fields and args.incl_only_fields:
        for metric in metrics_fields_levels_threshes_dict.copy().keys():
            [metrics_fields_levels_threshes_dict[metric].pop(field, None)
             for field in valid_fcst_fields if field not in args.incl_only_fields]

    # If the --excl_fields option was specified on the command line (i.e. if
    # args_orig.excl_fields is not empty) and has been passed at least one
    # field that appears in the plot configuration file (i.e. if args.excl_fields
    # is not empty), then for each metric to be plotted, remove from its sub-
    # dictionary in the plot configuration dictionary any field that is in
    # args.excl_fields.
    if args_orig.excl_fields and args.excl_fields:
        for metric in metrics_fields_levels_threshes_dict.copy().keys():
            [metrics_fields_levels_threshes_dict[metric].pop(field, None)
             for field in args.excl_fields]

    # If after removing the necessary fields the values of any of the metric
    # keys in the plot configuration dictionary have become empty, remove
    # those metric keys.
    for metric, fields_levels_threshes_dict in metrics_fields_levels_threshes_dict.copy().items():
        if not fields_levels_threshes_dict:
            metrics_fields_levels_threshes_dict.pop(metric, None)

    # If after removing the necessary metrics and fields from the plot
    # configuration dictionary there are no metric-field-level-threshold
    # combinations left in the dictionary to plot (i.e. if the dictionary
    # has become empty), print out an error message and exit.
    if not metrics_fields_levels_threshes_dict:
        msg = dedent(f"""
            After removing verification (vx) metrics and/or forecast fields from the
            plot configuration dictionary according to the arguments passed to the
            '--incl_only_[metrics|fields]' and/or '--excl_[metrics|fields]' options,
            there are no remaining metric-field-level-threshold combinations in the
            dictionary to plot, i.e. the plot configuration dictionary is empty:
              metrics_fields_levels_threshes_dict = {get_pprint_str(metrics_fields_levels_threshes_dict)}
            Please modify the plot configuration file and/or the arguments to one or
            more of the options above and rerun.  The plot configuration file is:
              plot_config_fp = {get_pprint_str(plot_config_fp)}
            Stopping.
            """)
        logging.error(msg)
        raise Exception(msg)

    # If the --incl_only_levels option was specified on the command line
    # (i.e. if args_orig.incl_only_levels is not empty) and has been passed
    # at least one level that appears in the plot configuration file (i.e.
    # if args.incl_only_levels is not empty), then for each metric-field
    # combination to be plotted, remove from the corresponding sub-sub-
    # dictionary in the plot configuration dictionary any level that is NOT
    # in args.incl_only_levels.
    if args_orig.incl_only_levels and args.incl_only_levels:
        for metric, fields_levels_threshes_dict in metrics_fields_levels_threshes_dict.copy().items():
            for field, levels_threshes_dict in fields_levels_threshes_dict.copy().items():
                [metrics_fields_levels_threshes_dict[metric][field].pop(level, None)
                 for level in valid_fcst_levels if level not in args.incl_only_levels]

    # If the --excl_levels option was specified on the command line (i.e. if
    # args_orig.excl_levels is not empty) and has been passed at least one
    # level that appears in the plot configuration file (i.e. if args.excl_levels
    # is not empty), then for each metric-field combination to be plotted,
    # remove from its sub-sub-dictionary in the plot configuration dictionary
    # any level that is in args.excl_levels.
    if args_orig.excl_levels and args.excl_levels:
        for metric, fields_levels_threshes_dict in metrics_fields_levels_threshes_dict.copy().items():
            for field, levels_threshes_dict in fields_levels_threshes_dict.copy().items():
                [metrics_fields_levels_threshes_dict[metric][field].pop(level, None)
                 for level in args.excl_levels]

    # If after removing the necessary levels the values of any of the field
    # keys in the plot configuration dictionary have become empty, remove
    # those field keys.  If that in turn causes the values of parent metric
    # keys to become empty, remove those as well.
    for metric, fields_levels_threshes_dict in metrics_fields_levels_threshes_dict.copy().items():
        for field, levels_threshes_dict in fields_levels_threshes_dict.copy().items():
            if not levels_threshes_dict:
                metrics_fields_levels_threshes_dict[metric].pop(field, None)
        if not fields_levels_threshes_dict:
            metrics_fields_levels_threshes_dict.pop(metric, None)

    # If after removing the necessary metrics, fields, and levels from the
    # plot configuration dictionary there are no metric-field-level-threshold
    # combinations left in the dictionary to plot (i.e. if the dictionary
    # has become empty), print out an error message and exit.
    if not metrics_fields_levels_threshes_dict:
        msg = dedent(f"""
            After removing verification (vx) metrics, forecast fields, and/or forecast
            levels from the plot configuration dictionary according to the arguments
            passed to the '--incl_only_[metrics|fields|levels]' and/or '--excl_[metrics|
            fields|levels] options, there are no remaining metric-field-level-threshold
            combinations in the dictionary to plot, i.e. the plot configuration
            dictionary is empty:
              metrics_fields_levels_threshes_dict = {get_pprint_str(metrics_fields_levels_threshes_dict)}
            Please modify the plot configuration file and/or the arguments to one or
            more of the options above and rerun.  The plot configuration file is:
              plot_config_fp = {get_pprint_str(plot_config_fp)}
            Stopping.
            """)
        logging.error(msg)
        raise Exception(msg)

    # If the --incl_only_threshes option was specified on the command line
    # (i.e. if args_orig.incl_only_threshes is not empty) and has been passed
    # at least one threshold that appears in the plot configuration file (i.e.
    # if args.incl_only_threshes is not empty), then for each metric-field-
    # level combination to be plotted, remove from the corresponding list
    # of thresholds in the plot configuration dictionary any threshold that
    # is NOT in args.incl_only_threshes (or, equivalently, keep only those
    # that are in args.incl_only_threshes).
    if args_orig.incl_only_threshes and args.incl_only_threshes:
        for metric, fields_levels_threshes_dict in metrics_fields_levels_threshes_dict.copy().items():
            for field, levels_threshes_dict in fields_levels_threshes_dict.copy().items():
                for level, threshes_list in levels_threshes_dict.copy().items():
                    # Use the intersection() method on sets to retain only those elements
                    # in threshes_list that also appear in args.incl_only_threshes.
                    threshes_list_filtered = list(set(threshes_list).intersection(args.incl_only_threshes))
                    metrics_fields_levels_threshes_dict[metric][field][level] = threshes_list_filtered

    # If the --excl_threshes option was specified on the command line (i.e.
    # if args_orig.excl_threshes is not empty) and has been passed at least
    # one threshold that appears in the plot configuration file (i.e. if
    # args.excl_threshes is not empty), then for each metric-field-level
    # combination to be plotted, remove from the corresponding list of
    # thresholds in the plot configuration dictionary any threshold that is
    # in args.excl_threshes.
    if args_orig.excl_threshes and args.excl_threshes:
        for metric, fields_levels_threshes_dict in metrics_fields_levels_threshes_dict.copy().items():
            for field, levels_threshes_dict in fields_levels_threshes_dict.copy().items():
                for level, threshes_list in levels_threshes_dict.copy().items():
                    # Use the difference() method on sets to get those elements in threshes_list
                    # that are NOT in args.excl_threshes.  Note that with this method, an
                    # element that appears in args.excl_threshes but not in threshes_list is
                    # ignored.
                    threshes_list_filtered = list(set(threshes_list).difference(args.excl_threshes))
                    metrics_fields_levels_threshes_dict[metric][field][level] = threshes_list_filtered

    # If after removing the necessary thresholds the values of any of the
    # level keys in the plot configuration dictionary have been set to
    # empty lists, and if the corresponding metric are ones that require
    # thresholds, then remove those level keys.  If that in turn causes
    # the values of parent field keys to become empty, remove those as well.
    # If that in turn causes the values of parent metric keys to become
    # empty, remove those as well.
    for metric, fields_levels_threshes_dict in metrics_fields_levels_threshes_dict.copy().items():
        for field, levels_threshes_dict in fields_levels_threshes_dict.copy().items():
            for level, threshes_list in levels_threshes_dict.copy().items():
                # If the current metric needs a threshold but threshes_list for the
                # current level is empty, remove the key (which is the level) from the
                # dictionary.  If the metric doesn't need a threshold, it is acceptable
                # for the current level to have an empty threhold list, so don't remove
                # the level key in this case.
                if vx_metric_needs_thresh[metric] and (not threshes_list):
                    metrics_fields_levels_threshes_dict[metric][field].pop(level, None)
            if not levels_threshes_dict:
                metrics_fields_levels_threshes_dict[metric].pop(field, None)
        if not fields_levels_threshes_dict:
            metrics_fields_levels_threshes_dict.pop(metric, None)

    # If after removing the necessary metrics, fields, levels, and thresholds
    # from the plot configuration dictionary there are no metric-field-level-
    # threshold combinations left in the dictionary to plot (i.e. if the
    # dictionary has become empty), print out an error message and exit.
    if not metrics_fields_levels_threshes_dict:
        msg = dedent(f"""
            After removing verification (vx) metrics, forecast fields, forecast levels,
            and/or thresholds from the plot configuration dictionary according to the
            arguments passed to the '--incl_only_[metrics|fields|levels|threshes]'
            and/or '--excl_[metrics|fields|levels|threshes]' options, there are no
            remaining metric-field-level-threshold combinations in the dictionary to
            plot, i.e. the plot configuration dictionary is empty:
              metrics_fields_levels_threshes_dict = {get_pprint_str(metrics_fields_levels_threshes_dict)}
            Please modify the plot configuration file and/or the arguments to one or
            more of the options above and rerun.  The plot configuration file is:
              plot_config_fp = {get_pprint_str(plot_config_fp)}
            Stopping.
            """)
        logging.error(msg)
        raise Exception(msg)

    ############
    # After using the arguments to the --incl_only_[metrics|fields|levels|
    # threshes] and --excl_[metrics|fields|levels|threshes] options specified
    # on the command line to reduce the plot configuration dictionary, it is
    # possible that some of the metrics, fields, levels, and/or thresholds
    # passed to the --incl_only_[metrics|fields|levels|threshes] options no
    # longer appear in the dictionary.  Check for this and issue warnings
    # as necessary.
    #
    # As an example, assume the plot configuration file contains the following
    # entry for metrics_fields_levels_threshes:
    #
    # metrics_fields_levels_threshes:
    #     metric1:
    #         field1:
    #             ...
    #         field2:
    #             ...
    #     metric1:
    #         field2:
    #             ...
    #
    # Then the original (i.e. before reduction) plot configuration dictionary
    # will be:
    #
    #   {metric1: {field1: {...}, field2: {...}},
    #    metric2: {field2: {...}}}
    #
    # Now assume the options passed on the command line are:
    #
    #   --incl_only_metrics metric1 metric2 --excl_fields fields2
    #
    # After processing the --incl_only_metrics option, the plot configuration
    # dictionary will not change, but after processing the --excl_fields
    # option, the (now reduced) plot configuration dictionary will be:
    #
    #   {metric1: {field1: {...}}
    #
    # Thus, metric2 will no longer be plotted even though it is passed to
    # --incl_only_metrics.  For situations like this, we use the code section
    # below to issue warnings.
    #
    ############

    # Check that all the metrics passed to the --incl_only_metrics option
    # appear as a key in the reduced (i.e. after removing metrics, fields,
    # levels, and/or thresholds according to the --incl_only_[metrics|fields|
    # levels|threshes] and --excl_[metrics|fields|levels|threshes] options
    # specified on the command line) plot configuration dictionary.  If not,
    # issue a warning for each missing metric.
    for metric in args.incl_only_metrics:
        metric_count = 0
        if metric in metrics_fields_levels_threshes_dict: metric_count += 1
        if metric_count == 0:
            msg = dedent(f"""
                The metric '{metric}' passed to the '--incl_only_metrics' option does not
                appear as a key in the reduced plot configuration dictionary (i.e. the
                plot configuration dictionary after removing metrics, fields, levels,
                and/or thresholds according to the --incl_only_... and --excl_... options
                specified on the command line).  The reduced plot configuration dictionary
                is:
                  metrics_fields_levels_threshes_dict
                  = """) + \
                get_pprint_str(metrics_fields_levels_threshes_dict, ' '*4).lstrip() + \
                dedent(f"""
                Thus, no plots for metric '{metric}' will be generated.
                """)
            logging.warning(msg)

    # Check that all the fields passed to the --incl_only_fields option
    # appear as a key in at least one metric sub-dictionary in the reduced
    # (i.e. after removing metrics, fields, levels, and/or thresholds according
    # to the --incl_only_[metrics|fields|levels|threshes] and --excl_[metrics|
    # fields|levels|threshes] options specified on the command line) plot
    # configuration dictionary.  If not, issue a warning for each missing
    # field.
    for field in args.incl_only_fields:
        field_count = 0
        for metric, fields_levels_threshes_dict in metrics_fields_levels_threshes_dict.items():
            if field in fields_levels_threshes_dict: field_count += 1
        if field_count == 0:
            msg = dedent(f"""
                The field '{field}' passed to the '--incl_only_fields' option does not
                appear as a key in any of the metric (sub-)dictionaries in the reduced
                plot configuration dictionary (i.e. the plot configuration dictionary
                after removing metrics, fields, levels, and/or thresholds according to
                the --incl_only_... and --excl_... options specified on the command line).
                The reduced plot configuration dictionary is
                  metrics_fields_levels_threshes_dict
                  = """) + \
                get_pprint_str(metrics_fields_levels_threshes_dict, ' '*4).lstrip() + \
                dedent(f"""
                Thus, no plots for field '{field}' will be generated.
                """)
            logging.warning(msg)

    # Check that all the levels passed to the --incl_only_levels option
    # appear as a key in at least one metric-field sub-sub-dictionary in the
    # reduced (i.e. after removing metrics, fields, levels, and/or thresholds
    # according to the --incl_only_[metrics|fields|levels|threshes] and
    # --excl_[metrics|fields|levels|threshes] options specified on the
    # command line) plot configuration dictionary.  If not, issue a warning
    # for each missing level.
    for level in args.incl_only_levels:
        level_count = 0
        for metric, fields_levels_threshes_dict in metrics_fields_levels_threshes_dict.items():
            for field, levels_threshes_dict in fields_levels_threshes_dict.items():
                if level in levels_threshes_dict: level_count += 1
        if level_count == 0:
            msg = dedent(f"""
                The level '{level}' passed to the '--incl_only_levels' option does not
                appear as a key in any of the metric-field (sub-sub-)dictionaries in the
                reduced plot configuration dictionary (i.e. the plot configuration
                dictionary after removing metrics, fields, levels, and/or thresholds
                according to the --incl_only_... and --excl_... options specified on the
                command line).  The reduced plot configuration dictionary is:
                  metrics_fields_levels_threshes_dict
                  = """) + \
                get_pprint_str(metrics_fields_levels_threshes_dict, ' '*4).lstrip() + \
                dedent(f"""
                Thus, no plots at level '{level}' will be generated.
                """)
            logging.warning(msg)

    # Check that all the thresholds passed to the --incl_only_threshes
    # option appear in the threshold list of at least one metric-field-
    # level combination in the reduced (i.e. after removing metrics, fields,
    # levels, and/or thresholds according to the --incl_only_[metrics|fields|
    # levels|threshes] and --excl_[metrics|fields|levels|threshes] options
    # specified on the command line) plot configuration dictionary.  If not,
    # issue a warning for each missing threshold.
    for thresh in args.incl_only_threshes:
        thresh_count = 0
        for metric, fields_levels_threshes_dict in metrics_fields_levels_threshes_dict.items():
            for field, levels_threshes_dict in fields_levels_threshes_dict.items():
                for level, threshes_list in levels_threshes_dict.copy().items():
                    if thresh in threshes_list: thresh_count += 1
        if thresh_count == 0:
            msg = dedent(f"""
                The threshold '{thresh}' passed to the '--incl_only_threshes' option
                does not appear in the threshold list of any of the metric-field-level
                combinations in the reduced plot configuration dictionary (i.e. the plot
                configuration dictionary after removing metrics, fields, levels, and/or
                thresholds according to the --incl_only_... and --excl_... options
                specified on the command line).  The reduced plot configuration dictionary
                is:
                  metrics_fields_levels_threshes_dict
                  = """) + \
                get_pprint_str(metrics_fields_levels_threshes_dict, ' '*4).lstrip() + \
                dedent(f"""
                Thus, no plots for threshold '{thresh}' will be generated.
                """)
            logging.warning(msg)

    # Print out the final (reduced) plot configuration dictionary that will
    # be looped through to generate verification plots.
    msg = dedent(f"""
        After removing (if necessary) verification (vx) metrics, forecast fields,
        forecast levels, and/or thresholds from the plot configuration dictionary
        according to the arguments passed to the '--incl_only_[metrics|fields|
        levels|threshes]' and/or '--excl_[metrics|fields|levels|threshes]'
        options, the reduced plot configuration dictionary is:
          metrics_fields_levels_threshes_dict
          = """) + \
        get_pprint_str(metrics_fields_levels_threshes_dict, ' '*4).lstrip() + \
        dedent(f"""
        A plot will be generated for each metric-field-level-threshold combination
        in this dictionary.
        """)
    logging.info(msg)

    # Initialize (1) the counter that keeps track of the number of times the
    # script that generates a METviewer xml and calls METviewer is called and
    # (2) the counter that keeps track of the number of images (png files)
    # that were successfully generated.  Each call to the script should
    # generate an image, so these two counters can be compared at the end to
    # see how many images were (not) successfully generated.
    num_mv_calls = 0
    num_images_generated = 0
    missing_image_fns = []

    # Loop through the plot configuration dictionary and plot all metric-
    # field-level-threshold combinations it contains (with the threshold set
    # to an empty string for those metrics that do not need a threshold).
    # Note that this dictionary has been filtered to contain only those
    # metric-field-level-threshold combinations that are consistent with the
    #
    #   --incl_only_[metrics|fields|levels|threshes]
    #
    # and/or
    #
    #   --excl_[metrics|fields|levels|threshes]
    #
    # options.  For each such combination, the loop below calls the script
    # make_single_mv_vx_plot(), which for a single metric-field-level-
    # threshold (with the threshold being unnecessary for certain metrics)
    # combination generates a METviewer xml and then calls the METviewer
    # batch plotting script to create a plot (png image file).

    separator_str = '='*72 + '\n'
    separator_str = '\n' + separator_str*2

    for metric, fields_levels_threshes_dict in metrics_fields_levels_threshes_dict.items():
        msg = dedent(f"""
            Plotting verification (vx) metric '{metric}' for various forecast fields ...
            """)
        logging.debug(msg)

        msg = dedent(f"""
            Dictionary of fields, levels, and thresholds (if applicable) for this
            metric is:
              fields_levels_threshes_dict = """) + \
            get_pprint_str(fields_levels_threshes_dict,
                           ' '*(5 + len('fields_levels_threshes_dict'))).lstrip()
        logging.debug(msg)

        # If args.make_vx_metric_subdirs is set to True, place the output for
        # each metric in a separate subdirectory under the main output directory.
        # Otherwise, place the output directly under the main output directory.
        if args.make_vx_metric_subdirs:
            output_dir_crnt_vx_metric = os.path.join(args.output_dir, metric)
        else:
            output_dir_crnt_vx_metric = args.output_dir

        for field, levels_threshes_dict in fields_levels_threshes_dict.items():
            msg = dedent(f"""
                Plotting vx metric '{metric}' for forecast field '{field}' at various
                levels ...
                """)
            logging.debug(msg)

            msg = dedent(f"""
                Dictionary of levels and thresholds (if applicable) for this field is:
                  levels_threshes_dict = """) + \
                get_pprint_str(levels_threshes_dict,
                               ' '*(5 + len('levels_threshes_dict'))).lstrip()
            logging.debug(msg)

            for level, threshes_list in levels_threshes_dict.items():
                msg = dedent(f"""
                    Plotting vx metric '{metric}' for forecast field '{field}' at forecast
                    level '{level}' ...
                    """)
                logging.debug(msg)

                if vx_metric_needs_thresh[metric]:
                    msg = dedent(f"""
                        Dictionary of thresholds (if applicable) for this level is:
                          threshes_list = """) + \
                        get_pprint_str(threshes_list,
                                       ' '*(5 + len('threshes_list'))).lstrip()
                    logging.debug(msg)
                else:
                    if threshes_list:
                        msg = dedent(f"""
                            The current metric does not need a threshold, but it has been assigned a
                            non-empty list of thresholds (threshes_list) in the plot configuration
                            file:
                              metric = {get_pprint_str(metric)}
                              vx_metric_needs_thresh[metric] = {get_pprint_str(vx_metric_needs_thresh[metric])}
                              threshes_list = {get_pprint_str(threshes_list)}
                            Please correct this in the plot configuration file, which is:
                              plot_config_fp = {get_pprint_str(plot_config_fp)}
                            Ignoring specified thresholds and resetting threshes_list to a list
                            containing a single empty string.
                            """)
                        logging.warning(msg)
                    threshes_list = ['']

                for thresh in threshes_list:

                    msg = separator_str.rstrip() + dedent(f"""
                        Plotting vx metric '{metric}' for forecast field '{field}' at forecast
                        level '{level}' and threshold '{thresh}' (threshold may be empty for
                        certain metrics) ...
                        """)
                    logging.info(msg)

                    args_list = ['--mv_host', mv_host, \
                                 '--mv_database_name', mv_database_name, \
                                 '--model_names', ] + model_names \
                              + ['--vx_metric', metric,
                                 '--fcst_init_info'] + fcst_init_info \
                              + ['--fcst_len_hrs', fcst_len_hrs,
                                 '--fcst_field', field,
                                 '--level_or_accum', level,
                                 '--threshold', thresh,
                                 '--mv_output_dir', output_dir_crnt_vx_metric]

                    msg = dedent(f"""
                        Argument list passed to plotting script is:
                          args_list = """) + \
                        get_pprint_str(args_list, ' '*(5 + len('args_list'))).lstrip()
                    logging.debug(msg)

                    num_mv_calls += 1
                    msg = dedent(f"""
                        Calling METviewer plotting script ...
                          num_mv_calls = {get_pprint_str(num_mv_calls)}
                        """)
                    logging.info(msg)
                    output_xml_fp = make_single_mv_vx_plot(args_list)

                    # Keep track of the number of images that are successfully created.
                    #
                    # First, use the absolute path to the xml file created to generate the
                    # path to and name of the image that should have been created.
                    output_image_fp = os.path.splitext(output_xml_fp)[0] + '.' + 'png'
                    output_image_fn = os.path.basename(output_image_fp)
                    # If the image file exists, increment the count of successfully created
                    # images.
                    if os.path.isfile(output_image_fp):
                        num_images_generated += 1
                    else:
                        missing_image_fns.append(output_image_fn)

                    msg = dedent(f"""
                        Done calling METviewer plotting script.  Number of calls to METviewer
                        and number of images successfully generated thus far are:
                          num_mv_calls = {get_pprint_str(num_mv_calls)}
                          num_images_generated = {get_pprint_str(num_images_generated)}
                        """) + separator_str.lstrip()
                    logging.info(msg)

                    # If the image was successfully created and args.create_ordered_plots
                    # is set True, make a copy of the image in a designated subdirectory
                    # that will contain renamed versions of the images such that their
                    # alphabetical order corresponds to the order in which they appear in
                    # the plot configuration file.
                    if os.path.isfile(output_image_fp) and args.create_ordered_plots:
                        # Generate the name of/path to a copy of the image file such that this
                        # name contains an index used for alphabetically ordering the files.
                        # This ordering is useful when creating a presentation, e.g. a pdf file,
                        # from the images.
                        output_image_fn_ordered = '_'.join([f'p{num_mv_calls:03}', output_image_fn])
                        output_image_fp_ordered = os.path.join(ordered_plots_dir, output_image_fn_ordered)
                        # Copy and rename the image.
                        shutil.copy(output_image_fp, output_image_fp_ordered)

    msg = dedent(f"""
        Total number of calls to METviewer plotting script:
          num_mv_calls = {get_pprint_str(num_mv_calls)}
        Total number of image files generated:
          num_images_generated = {get_pprint_str(num_images_generated)}
        """)
    logging.info(msg)

    # If any images were not generated, print out their names.
    num_missing_images = len(missing_image_fns)
    if num_missing_images > 0:
        msg = dedent(f"""
            The following images failed to generate:
              missing_image_fns = """) + \
            get_pprint_str(missing_image_fns, ' '*(5 + len('missing_image_fns'))).lstrip()
        logging.info(msg)


def main():
    """
    Function to set up arguments list and call make_multi_mv_vx_plots() to
    generate multiple METviewer verification (vx) plots.

    Arguments:
    ---------
    None

    Returns:
    -------
    None
    """

    parser = argparse.ArgumentParser(
        description='Call METviewer to create vx plots.'
    )

    # Find the path to the directory containing the clone of the SRW App.
    # The index of .parents will have to be changed if this script is moved
    # elsewhere in the SRW App's directory structure.
    crnt_script_fp = Path(__file__).resolve()
    home_dir = crnt_script_fp.parents[2]
    expts_dir = Path(os.path.join(home_dir, '../expts_dir')).resolve()
    parser.add_argument('--output_dir',
                        type=str,
                        required=False, default=os.path.join(expts_dir, 'mv_output'),
                        help=dedent(f"""
                            Base directory in which to place output files (generated xmls, METviewer
                            generated plots, log files, etc).  These will usually be placed in
                            subdirectories under this output directory.
                            """))

    parser.add_argument('--plot_config_fp',
                        type=str,
                        required=False, default='plot_config.default.yaml',
                        help=dedent(f"""
                            Name of or path (absolute or relative) to yaml user plot configuration
                            file for METviewer plot generation.  Among other pieces of information,
                            this file specifies the initial set of vx metric, forecast field,
                            forecast level, and field threshold (if the metric requires a threshold)
                            combinations to consider for plotting.  Combinations are then removed
                            from this set according to the --incl_only_[metrics|fields|levels|threshes]
                            --excl_[metrics|fields|levels|threshes] options specified on the command
                            line to obtain a final set of metric-field-level-threshold combinations
                            for which to generate verification (vx) plots.
                            """))

    parser.add_argument('--log_fp',
                        type=str,
                        required=False, default='',
                        help=dedent(f"""
                            Name of or path (absolute or relative) to log file.  If not specified,
                            the output goes to screen.
                            """))

    choices_log_level = [pair for lvl in list(logging._nameToLevel.keys())
                              for pair in (str.lower(lvl), str.upper(lvl))]
    parser.add_argument('--log_level',
                        type=str,
                        required=False, default='info',
                        choices=choices_log_level,
                        help=dedent(f"""
                            Logging level to use with the 'logging' module.
                            """))

    # Load the yaml file containing valid values of verification (vx) plotting
    # parameters and get valid values.
    valid_vx_plot_params_config_fp = 'valid_vx_plot_params.yaml'
    valid_vx_plot_params = load_config_file(valid_vx_plot_params_config_fp)
    valid_vx_metrics = list(valid_vx_plot_params['valid_vx_metrics'].keys())
    valid_fcst_fields = list(valid_vx_plot_params['valid_fcst_fields'].keys())
    valid_fcst_levels = list(valid_vx_plot_params['valid_fcst_levels_to_levels_in_db'].keys())

    # Create dictionary that specifies whether each metric (the keys) needs
    # a threshold.
    vx_metric_needs_thresh = {}
    for metric in valid_vx_metrics:
        vx_metric_needs_thresh[metric] = valid_vx_plot_params['valid_vx_metrics'][metric]['needs_thresh']

    parser.add_argument('--incl_only_metrics', nargs='+',
                        type=str.lower,
                        required=False, default=[],
                        choices=valid_vx_metrics,
                        help=dedent(f"""
Verification metrics to exclusively include in the vx plot generation.
This is a convenience option that provides a way to override the settings
in the plot configuration file.  If this option is not specified, then
the initial set of metric-field-level-threshold combinations read in from
the configuration is not reduced based on metric.  If it is specified,
then only those combinations in this initial set that contain one of the
metrics passed to this option are retained for plotting.  Note that any
metric passed to this option must appear in the configuration file because
METviewer needs to know the fields, levels, and (possibly) thresholds
for which to generate plots for that metric, and these are all specified
in that file.  For simplicity, this option cannot be specified together
with the '--excl_metrics' option.
                            """))

    parser.add_argument('--excl_metrics', nargs='+',
                        type=str.lower,
                        required=False, default=[],
                        choices=valid_vx_metrics,
                        help=dedent(f"""
Verification metrics to exclude from the vx plot generation.  This is a
convenience option that provides a way to override the settings in the
plot configuration file.  If this option is not specified, then the
initial set of metric-field-level-threshold combinations read in from
the configuration is not reduced based on metric.  If it is specified,
then any combination in this initial set that contains one of the metrics
passed to this option is removed from plotting.  If one or more metrics
passed to this option do not appear in the configuration file, an
informational message is issued and no plots involving those metrics are
generated.  For simplicity, this option cannot be specified together
with the '--incl_only_metrics' option.
                            """))

    parser.add_argument('--incl_only_fields', nargs='+',
                        type=str.lower,
                        required=False, default=[],
                        choices=valid_fcst_fields,
                        help=dedent(f"""
Forecast fields to exclusively include in the vx plot generation.  This
is a convenience option that provides a way to override the settings in
the plot configuration file.  If this option is not specified, then the
initial set of metric-field-level-threshold combinations read in from
the configuration file is not reduced based on field.  If it is specified,
then only those combinations in this initial set that contain one of the
fields passed to this option are retained for plotting.  If a field
passed to this option is not listed in the configuration file, then a
warning message is issued and no plots involving that field are generated.
For simplicity, this option cannot be specified together with the
'--excl_fields' option.
                            """))

    parser.add_argument('--excl_fields', nargs='+',
                        type=str.lower,
                        required=False, default=[],
                        choices=valid_fcst_fields,
                        help=dedent(f"""
Forecast fields to exclude from the vx plot generation.  This is a
convenience option that provides a way to override the settings in the
plot configuration file.  If this option is not specified, then for a
metric in the configuration file that is not excluded from plotting via
the '--incl_only_metrics' or '--excl_metrics' option, all fields listed
under the metric are plotted.
all
fields in the configuration file are plotted (as long as the metric
under which they're specified is
If it is specified, then plots will be
generated only for those fields in the configuration file that are not
passed to this option.  For simplicity, this option cannot be specified together with

the '--incl_only_fields' option.
                            """))

    parser.add_argument('--incl_only_levels', nargs='+',
                        required=False, default=[],
                        choices=valid_fcst_levels,
                        help=dedent(f"""
                            Forecast levels to exclusively include in verification plot generation.
                            This is a convenience option that provides a way to override the settings
                            in the plot configuration file.  If this option is not used, then all
                            levels listed under a given vx metric and field combination in the
                            configuration file are plotted (as long as that metric and field
                            combination is to be plotted, i.e. it is not excluded via the '--excl_metrics'
                            and/or '--excl_fields' options).  If it is used, then plots for that
                            metric-field combination will be generated only for the levels passed
                            to this option.  For a metric-field combination that is to be plotted,
                            if a level specified here is not listed in the configuration file under
                            that metric and field, then no plots are generated for that metric-
                            field-level combination.  For simplicity, this option cannot be used
                            together with the '--excl_levels' option.
                            """))

    parser.add_argument('--excl_levels', nargs='+',
                        required=False, default=[],
                        choices=valid_fcst_levels,
                        help=dedent(f"""
                            Forecast levels to exclude from verification plot generation.  This is a
                            convenience option that provides a way to override the settings in the
                            plot configuration file.  If this option is not used, then all levels in
                            the configuration file are plotted.  If it is used, then plots will be
                            generated only for those levels in the configuration file that are not
                            listed here.  For simplicity, this option cannot be used together with
                            the '--incl_only_levels' option.
                            """))

    parser.add_argument('--incl_only_threshes', nargs='+',
                        required=False, default=[],
                        help=dedent(f"""
                            Forecast thresholds to exclusively include in verification plot generation.
                            This is a convenience option that provides a way to override the settings
                            in the plot configuration file.  This option has no effect on the plotting
                            of vx metrics that do not require a threshold.  For metrics that
                            require a threshold, the behavior is as follows.  If this option is not
                            used, then all thresholds listed under a given vx metric, field, and
                            level combination in the configuration file are plotted (as long as that
                            metric, field, and threshold combination is to be plotted, i.e. it is
                            not excluded via the '--excl_metrics', '--excl_fields', and/or '--excl_levels'
                            options).  If it is used, then plots for that metric-field-level
                            combination will be generated only for the thresholds passed to this
                            option.  For a metric-field-level combination that is to be plotted,
                            if a threshold specified here is not listed in the configuration file
                            under that metric, field, and level, then no plots are generated for
                            that metric-field-level-threshold combination.  For simplicity, this
                            option cannot be used together with the '--excl_threshes'
                            option.
                            """))

    parser.add_argument('--excl_threshes', nargs='+',
                        required=False, default=[],
                        help=dedent(f"""
                            Forecast thresholds to exclude from verification plot generation.  This
                            is a convenience option that provides a way to override the settings in
                            the plot configuration file.  This option has no effect on the plotting
                            of vx metrics that do not require a threshold.  For metrics that
                            require a threshold, the behavior is as follows.  If this option is not
                            used, then all thresholds in the configuration file are plotted.  If it
                            is used, then plots will be generated only for those thresholds in the
                            configuration file that are not listed here.  For simplicity, this option
                            cannot be used together with the '--incl_only_threshes' option.
                            """))

    parser.add_argument('--preexisting_dir_method',
                        type=str.lower,
                        required=False, default='rename',
                        choices=['rename', 'delete', 'quit'],
                        help=dedent(f"""
                            Method for dealing with pre-existing output directories.
                            """))

    parser.add_argument('--make_vx_metric_subdirs',
                        required=False, action=argparse.BooleanOptionalAction,
                        help=dedent(f"""
                            Flag for placing output for each metric to be plotted in a separate
                            subdirectory under the output directory.
                            """))

    parser.add_argument('--create_ordered_plots',
                        required=False, action=argparse.BooleanOptionalAction,
                        help=dedent(f"""
                            Flag for creating a directory that contains copies of all the generated
                            images (png files) and renamed such that they are alphabetically in the
                            same order as the user has specified in the plot configuration file (the
                            one passed to the optional '--plot_config_fp' argument).  This is useful
                            for creating a pdf of the plots from the images that includes the plots
                            in the same order as in the plot configuration file.
                            """))

    args = parser.parse_args()

    # For simplicity, do not allow the --incl_only_metrics and --excl_metrics
    # options to be specified simultaneously.
    if args.incl_only_metrics and args.excl_metrics:
        msg = dedent(f"""
            For simplicity, the '--incl_only_metrics' and '--excl_metrics' options
            cannot simultaneously be specified on the command line:
              args.incl_only_metrics = {get_pprint_str(args.incl_only_metrics)}
              args.excl_metrics = {get_pprint_str(args.excl_metrics)}
            Please remove one or the other from the command line and rerun.  Stopping.
            """)
        logging.error(msg)
        raise ValueError(msg)

    # For simplicity, do not allow the --incl_only_fields and --excl_fields
    # options to be specified simultaneously.
    if args.incl_only_fields and args.excl_fields:
        msg = dedent(f"""
            For simplicity, the '--incl_only_fields' and '--excl_fields' options
            cannot simultaneously be specified on the command line:
              args.incl_only_fields = {get_pprint_str(args.incl_only_fields)}
              args.excl_fields = {get_pprint_str(args.excl_fields)}
            Please remove one or the other from the command line and rerun.  Stopping.
            """)
        logging.error(msg)
        raise ValueError(msg)

    # For simplicity, do not allow the --incl_only_levels and --excl_levels
    # options to be specified simultaneously.
    if args.incl_only_levels and args.excl_levels:
        msg = dedent(f"""
            For simplicity, the '--incl_only_levels' and '--excl_levels' options
            cannot simultaneously be specified on the command line:
              args.incl_only_levels = {get_pprint_str(args.incl_only_levels)}
              args.excl_levels = {get_pprint_str(args.excl_levels)}
            Please remove one or the other from the command line and rerun.  Stopping.
            """)
        logging.error(msg)
        raise ValueError(msg)

    # For simplicity, do not allow the --incl_only_threshes and --excl_threshes
    # options to be specified simultaneously.
    if args.incl_only_threshes and args.excl_threshes:
        msg = dedent(f"""
            For simplicity, the '--incl_only_threshes' and '--excl_threshes' options
            cannot simultaneously be specified on the command line:
              args.incl_only_threshes = {get_pprint_str(args.incl_only_threshes)}
              args.excl_threshes = {get_pprint_str(args.excl_threshes)}
            Please remove one or the other from the command line and rerun.  Stopping.
            """)
        logging.error(msg)
        raise ValueError(msg)

    # Call the driver function to read and parse the plot configuration
    # dictionary and call the METviewer batch script to generate plots.
    valid_vals = {'vx_metrics': valid_vx_metrics,
                  'fcst_fields': valid_fcst_fields,
                  'fcst_levels': valid_fcst_levels}
    make_multi_mv_vx_plots(args, valid_vals, vx_metric_needs_thresh)
#
# -----------------------------------------------------------------------
#
# Call the function defined above.
#
# -----------------------------------------------------------------------
#
if __name__ == "__main__":
    main()

