#!/usr/bin/env python3

import os
import sys
import shutil
from datetime import datetime
import glob
import argparse
import yaml

import logging
import textwrap
from textwrap import dedent

import pprint
import subprocess

from plot_vx_metviewer import plot_vx_metviewer
from plot_vx_metviewer import get_pprint_str

from pathlib import Path
file = Path(__file__).resolve()
ush_dir = file.parents[1]
sys.path.append(str(ush_dir))

from python_utils import (
    log_info,
    load_config_file,
)

def check_for_preexisting_dir_file(dir_or_file, preexist_method):
    """Check and handle preexisting directory or file.

    Arguments:
      dir_or_file:      Name of directory or file.
      preexist_method:  Method to use to deal with a preexisting version of dir_or_file.
                        This has 3 valid values:
                          'rename':  Causes the existing dir_or_file to be renamed.
                          'delete':  Causes the existing dir_or_file to be deleted.
                          'quit':    Causes the script to quit if dir_or_file already exists.

    Return:
      None
    """

    valid_vals_preexist_method = ['rename', 'delete', 'quit']

    if os.path.exists(dir_or_file):
        if preexist_method == 'rename':
            now = datetime.now()
            renamed_dir_or_file = dir_or_file + now.strftime('.old_%Y%m%d_%H%M%S')
            logging.info(dedent(f'''\n
                Output directory already exists:
                  {dir_or_file}
                Moving (renaming) preexisting directory to:
                  {renamed_dir_or_file}'''))
            os.rename(dir_or_file, renamed_dir_or_file)
        elif preexist_method == 'delete':
            logging.info(dedent(f'''\n
                Output directory already exists:
                  {dir_or_file}
                Removing existing directory...'''))
            shutil.rmtree(dir_or_file)
        elif preexist_method == 'quit':
            err_msg = dedent(f'''\n
                Output directory already exists:
                  {dir_or_file}
                Stopping.''')
            logging.error(err_msg, stack_info=True)
            raise FileExistsError(err_msg)
        else:
            err_msg = dedent(f'''\n
                Invalid value for preexist_method:
                  {preexist_method}
                Valid values are:
                  {valid_vals_preexist_method}
                Stopping.''')
            logging.error(err_msg, stack_info=True)
            raise ValueError(err_msg)


def make_mv_vx_plots(args):
    """Make multiple verification plots using METviewer and the settings
    file specified as part of args.

    Arguments:
      args:  Dictionary of arguments.
    """

    # Set up logging.
    # If the name/path of a log file has been specified in the command line
    # arguments, place the logging output in it (existing log files of the
    # same name are overwritten).  Otherwise, direct the output to the screen.
    log_level = str.upper(args.log_level)
    FORMAT = "[%(levelname)s:%(name)s:  %(filename)s, line %(lineno)s: %(funcName)s()] %(message)s"
    if args.log_fp:
        logging.basicConfig(level=log_level, format=FORMAT, filename=args.log_fp, filemode='w')
    else:
        logging.basicConfig(level=log_level, format=FORMAT)

    config_fp = args.config_fp
    config_dict = load_config_file(config_fp)
    logging.info(dedent(f"""
        Reading in plot configuration file: {config_fp}
        """))

    mv_host = config_dict['mv_host']
    mv_database_name = config_dict['mv_database_name']
    model_names = config_dict['model_names']
    fcst_init_info = config_dict['fcst_init_info']
    vx_stats_dict = config_dict["vx_stats"]

    fcst_init_info = map(str, list(fcst_init_info.values()))
    # fcst_init_info is a list containing both strings and integers.  For
    # use below, convert it to a list of strings only.
    fcst_init_info = [str(elem) for elem in fcst_init_info]
    fcst_len_hrs = str(config_dict['fcst_len_hrs'])

    # Check if output directory exists and take action according to how the
    # args.preexisting_dir_method flag is set.
    check_for_preexisting_dir_file(args.output_dir, args.preexisting_dir_method)

    # If the flag create_ordered_plots is set to True, create (if it doesn't
    # already exist) a new directory in which we will store copies of all
    # the images (png files) that METviewer will generate such that the
    # images are ordered via an index in their name.  This allows a pdf to
    # quickly be created from this directory (e.g. using tools available in
    # Adobe Acrobat) that contains all the plots in the order they were
    # listed in the yaml plot configuration file that this script reads in.
    if args.create_ordered_plots:
        ordered_plots_dir = os.path.join(args.output_dir, 'ordered_plots')
        Path(ordered_plots_dir).mkdir(parents=True, exist_ok=True)

    # For simplicity, do not allow both the --incl_only_stat and --excl_stat
    # options to be specified.
    if args.incl_only_stats and args.excl_stats:
        err_msg = dedent(f'''\n
            For simplicity, the "--incl_only_stat" and "--excl_stat" options cannot
            both be specified on the command line:
              args.incl_only_stats = {args.incl_only_stats}
              args.excl_stats = {args.excl_stats}
            Please remove one or the other from the command line and rerun.  Stopping.''')
        logging.error(err_msg, stack_info=True)
        raise ValueError(err_msg)

    # Ensure that any statistics passed to the --incl_only_stat option also
    # appear in the plot configuration file.
    vx_stats_in_config = list(vx_stats_dict.keys())
    stats_not_in_config = list(set(args.incl_only_stats).difference(vx_stats_in_config))
    if stats_not_in_config:
        err_msg = dedent(f'''\n
            One or more statistics passed to the "--incl_only_stats" option are not
            included in the plot configuration file.  These are:
              stats_not_in_config = {stats_not_in_config}
            The plot configuration file is:
              config_fp = {config_fp}
            Statistics included in the plot configuration file are:
              {vx_stats_in_config}
            Stopping.''')
        logging.error(err_msg, stack_info=True)
        raise ValueError(err_msg)

    # Remove from the configuration dictionary any statistic in the list of
    # statistics to exclude.
    [vx_stats_dict.pop(stat, None) for stat in args.excl_stats]

    # Remove from the configuration dictionary any statistic that is not
    # in the exclusive list of statistics to include.
    if args.incl_only_stats:
        vx_stats_dict = {stat: vx_stats_dict[stat] for stat in args.incl_only_stats}

    # For simplicity, do not allow both the --incl_only_field and --excl_field
    # options to be specified.
    if args.incl_only_fields and args.excl_fields:
        err_msg = dedent(f'''\n
            For simplicity, the "--incl_only_field" and "--excl_field" options cannot
            both be specified on the command line:
              args.incl_only_fields = {args.incl_only_fields}
              args.excl_fields = {args.excl_fields}
            Please remove one or the other from the command line and rerun.  Stopping.''')
        logging.error(err_msg, stack_info=True)
        raise ValueError(err_msg)

    # For each statistic to be plotted, remove from its dictionary in the
    # configuration file any forecast field in the list of fields to be
    # excluded.
    for stat, stat_dict in vx_stats_dict.items():
        [stat_dict.pop(field, None) for field in args.excl_fields]
        vx_stats_dict[stat] = stat_dict

    # For each statistic to be plotted, remove from its dictionary in the
    # configuration file any forecast field that is in the exclusive list
    # of fields to include.
    if args.incl_only_fields:
        for stat, stat_dict in vx_stats_dict.items():
            new_dict = {field: stat_dict[field] for field in args.incl_only_fields if field in stat_dict.keys()}
            vx_stats_dict[stat] = new_dict

    # Check that all the fields passed to the "--incl_only_fields" option
    # appear under at least one statistic.
    for field in args.incl_only_fields:
        field_count = 0
        for stat, stat_dict in vx_stats_dict.items():
            if field in stat_dict: field_count += 1
        if field_count == 0:
            msg = dedent(f"""
                The field "{field}" passed to the "--incl_only_fields" option is not a
                key in any of the dictionaries of the statistics to be plotted.  Thus,
                no vx plots for "{field}" will be generated.  The statistics to be
                plotted are:
                  {list(vx_stats_dict.keys())}
                """)
            logging.warning(msg)

    print(f'')
    print(f'FFFFFFFFFFFFFFFF')
    print(f'  vx_stats_dict = {vx_stats_dict}')
#    lasdkjf


    # Initialze (1) the counter that keeps track of the number of times the
    # script that generates a METviewer xml and calls METviewer is called and
    # (2) the counter that keeps track of the number of images (png files)
    # that were successfully generated.  Each call to the script should
    # generate an image, so these two counters can be compared at the end to
    # see how many images were (not) successfully generated.
    num_mv_calls = 0
    num_images_generated = 0
    missing_image_fns = []

    for stat, stat_dict in vx_stats_dict.items():
        #
        # Don't procecess the current statistic if it is passed as an argument
        # to the "--excl_stats" option.
        #
        if stat in args.excl_stats:
            logging.info(dedent(f"""\n
                Skipping plotting of statistic "{stat}" because it is in the list of
                stats to exclude ...
                  args.excl_stats = {args.excl_stats}
                """))
        #
        # Process the current statistic if either one of the following is true:
        #
        #   1) The "--incl_only_stats" option has not been used (so that the
        #      args.incl_only_stats list is empty).
        #   2) The "--incl_only_stats" option has been used and has been passed
        #      the current statistic.
        #
        elif (not args.incl_only_stats) or (stat in args.incl_only_stats):
            logging.info(dedent(f"""
                Plotting statistic "{stat}" for various forecast fields ...
                """))
            msg = dedent(f"""
                Dictionary of fields, levels, and thresholds (if applicable) for this statistic is:
                  stat_dict = """)
            indent_str = ' '*(5 + len('stat_dict'))
            msg = msg + get_pprint_str(stat_dict, indent_str).lstrip()
            logging.debug(msg)

            # If ars.make_stat_subdirs is set to True, place the output for each
            # statistic in a separate subdirectory under the main output directory.
            # Otherwise, place the output directly under the main output directory.
            if args.make_stat_subdirs:
                output_dir_crnt_stat = os.path.join(args.output_dir, stat)
            else:
                output_dir_crnt_stat = args.output_dir

            for fcst_field, fcst_field_dict in stat_dict.items():
                #
                # Don't procecess the current field if it is passed as an argument
                # to the "--excl_fields" option.
                #
                if fcst_field in args.excl_fields:
                    logging.info(dedent(f"""\n
                        Skipping plotting of field "{fcst_field}" because it is in the list of
                        fields to exclude ...
                          args.excl_fields = {args.excl_fields}
                        """))
                #
                # Process the current field if either one of the following is true:
                #
                #   1) The "--incl_only_fields" option has not been used (so that the
                #      args.incl_only_fields list is empty).
                #   2) The "--incl_only_fields" option has been used and has been passed
                #      the current field.
                #
                elif (not args.incl_only_fields) or (fcst_field in args.incl_only_fields):

                    for level, level_dict in fcst_field_dict.items():
                        logging.info(dedent(f"""
                            Plotting statistic "{stat}" for forecast field "{fcst_field}" at level "{level}" ...
                            """))
                        msg = dedent(f"""
                            Dictionary of thresholds (if applicable) for this level is:
                              level_dict = """)
                        indent_str = ' '*(5 + len('level_dict'))
                        msg = msg + get_pprint_str(level_dict, indent_str).lstrip()
                        logging.debug(msg)

                        thresholds = level_dict['thresholds']
                        for thresh in thresholds:
                            logging.info(dedent(f"""
                                Plotting statistic "{stat}" for forecast field "{fcst_field}" at level "{level}"
                                and threshold "{thresh}" (threshold may be empty for certain stats) ...
                                """))

                            args_list = ['--mv_host', mv_host, \
                                         '--mv_database_name', mv_database_name, \
                                         '--model_names', ] + model_names \
                                      + ['--vx_stat', stat,
                                         '--fcst_init_info'] + fcst_init_info \
                                      + ['--fcst_len_hrs', fcst_len_hrs,
                                         '--fcst_field', fcst_field,
                                         '--level_or_accum', level,
                                         '--threshold', thresh,
                                         '--mv_output_dir', output_dir_crnt_stat]

                            msg = dedent(f"""
                                Argument list passed to plotting script is:
                                  args_list = """)
                            indent_str = ' '*(5 + len('args_list'))
                            msg = msg + get_pprint_str(args_list, indent_str).lstrip()
                            logging.debug(msg)

                            num_mv_calls += 1
                            logging.debug(dedent(f"""
                                Calling METviewer plotting script ...
                                  num_mv_calls = {num_mv_calls}
                                """))
                            output_xml_fp = plot_vx_metviewer(args_list)
                            logging.debug(dedent(f"""
                                Done calling METviewer plotting script.
                                """))

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

                            # If the image was successfully created and args.create_ordered_plots
                            # is set True, make a copy of the image in a designated subdirectory
                            # that will contain renamed versions of the images such that their
                            # alphabetical order corresponds to the order in which they appear in
                            # the yaml plot configuration file.
                            if os.path.isfile(output_image_fp) and args.create_ordered_plots:
                                # Generate the name of/path to a copy of the image file such that this
                                # name contains an index used for alphabetically ordering the files.
                                # This ordering is useful when creating a presentation, e.g. a pdf file,
                                # from the images.
                                output_image_fn_ordered = '_'.join([f'p{num_mv_calls:03}', output_image_fn])
                                output_image_fp_ordered = os.path.join(ordered_plots_dir, output_image_fn_ordered)
                                # Copy and rename the image.
                                shutil.copy(output_image_fp, output_image_fp_ordered)

    logging.info(dedent(f"""
        Total number of calls to METviewer plotting script:
          num_mv_calls = {num_mv_calls}
        Total number of image files generated:
          num_images_generated = {num_images_generated}
        """))

    # If any images were not generated, print out their names.
    num_missing_images = len(missing_image_fns)
    if num_missing_images > 0:
        msg = dedent(f"""
            The following images failed to generate:
              missing_image_fns = """)
        indent_str = ' '*(5 + len('missing_image_fns'))
        msg = msg + get_pprint_str(missing_image_fns, indent_str).lstrip()
        logging.info(msg)
#
# -----------------------------------------------------------------------
#
# Call the function defined above.
#
# -----------------------------------------------------------------------
#
if __name__ == "__main__":

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
                        help=dedent(f'''Base directory in which to place output files (generated xmls,
                                        METviewer generated plots, log files, etc).  These will usually
                                        be placed in subdirectories under this output directory.'''))

    parser.add_argument('--config_fp',
                        type=str,
                        required=False, default='config_mv_plots.default.yml',
                        help=dedent(f'''Name of or path (absolute or relative) to yaml user
                                        plot configuration file for METviewer plot generation.'''))

    parser.add_argument('--log_fp',
                        type=str,
                        required=False, default='',
                        help=dedent(f'''Name of or path (absolute or relative) to log file.  If
                                        not specified, the output goes to screen.'''))

    choices_log_level = [pair for lvl in list(logging._nameToLevel.keys())
                              for pair in (str.lower(lvl), str.upper(lvl))]
    parser.add_argument('--log_level',
                        type=str,
                        required=False, default='info',
                        choices=choices_log_level,
                        help=dedent(f'''Logging level to use with the "logging" module.'''))

    # Load the yaml file containing static verification parameters
    # and get valid values.
    static_info_config_fp = 'vx_plots_static_info.yaml'
    static_data = load_config_file(static_info_config_fp)
    valid_stats = list(static_data['valid_stats'].keys())
    valid_fcst_fields = list(static_data['valid_fcst_fields'].keys())
    valid_fcst_levels = list(static_data['valid_levels_to_levels_in_db'].keys())

    parser.add_argument('--incl_only_stats', nargs='+',
                        type=str.lower,
                        required=False, default=[],
                        choices=valid_stats,
                        help=dedent(f'''Verification statistics to exclusively include in verification plot
                                        generation.  This is a convenience option that provides a way to override
                                        the settings in the yaml plot configuration file.  If this option is not
                                        used, then all statistics in the configuration file are plotted.  If it
                                        is used, then plots will be generated only for the statistics passed to
                                        this option.  Note that any statistic specified here must also appear in
                                        the yaml user plot configuration file (because METviewer needs to know
                                        the fields, levels, and possibly thresholds for which to generate plots
                                        for that statistic).  For simplicity, this option cannot be used together
                                        with the "--excl_stats" option.'''))

    parser.add_argument('--excl_stats', nargs='+',
                        type=str.lower,
                        required=False, default=[],
                        choices=valid_stats,
                        help=dedent(f'''Verification statistics to exclude from verification plot generation.
                                        This is a convenience option that provides a way to override the settings
                                        in the yaml plot configuration file.  If this option is not used, then
                                        all statistics in the configuration file are plotted.  If it is used,
                                        then plots will be generated only for those statistics in the configuration
                                        file that are not also listed here.  If a statistic listed here does not
                                        appear in the configuration file, an informational message is issued and
                                        no plot is generated for the statistic.  For simplicity, this option
                                        cannot be used together with the "--incl_only_stats" option.'''))

    parser.add_argument('--incl_only_fields', nargs='+',
                        type=str.lower,
                        required=False, default=[],
                        choices=valid_fcst_fields,
                        help=dedent(f'''Forecast fields to exclusively include in verification plot generation.
                                        This is a convenience option that provides a way to override the settings
                                        in the yaml plot configuration file.  If this option is not used, then
                                        all fields listed under a given vx statistic in the configuration file
                                        are plotted (as long as that statistic is to be plotted, i.e. it is not
                                        excluded via the "--excl_stat" option).  If it is used, then plots for
                                        that statistic will be generated only for the fields passed to this
                                        option.  For a statistic that is to be plotted, if a field specified
                                        here is not listed in the configuration file under that statistic, then
                                        no plots are generated for that statistic-field combination.  For
                                        simplicity, this option cannot be used together with the "--excl_fields"
                                        option.'''))

    parser.add_argument('--excl_fields', nargs='+',
                        type=str.lower,
                        required=False, default=[],
                        choices=valid_fcst_fields,
                        help=dedent(f'''Forecast fields to exclude from verification plot generation.  This is a
                                        convenience option that provides a way to override the settings in the
                                        yaml plot configuration file.  If this option is not used, then all fields
                                        in the configuration file are plotted.  If it is used, then plots will be
                                        generated only for those fields in the configuration file that are not
                                        also listed here.  For simplicity, this option cannot be used together
                                        with the "--incl_only_fields" option.'''))

    parser.add_argument('--incl_only_levels', nargs='+',
                        type=str.lower,
                        required=False, default=[],
                        choices=valid_fcst_levels,
                        help=dedent(f'''Forecast levels to exclusively include in verification plot generation.
                                        This is a convenience option that provides a way to override the settings
                                        in the yaml plot configuration file.  If this option is not used, then
                                        all levels listed under a given vx statistic and field combination in
                                        the configuration file are plotted (as long as that statistic and field
                                        combination is to be plotted, i.e. it is not excluded via the "--excl_stat"
                                        and/or "--excl_field" options).  If it is used, then plots for that
                                        statistic-field combination will be generated only for the levels passed
                                        to this option.  For a statistic-field combination that is to be plotted,
                                        if a level specified here is not listed in the configuration file under
                                        that statistic and field, then no plots are generated for that statistic-
                                        field-level combination.  For simplicity, this option cannot be used
                                        together with the "--excl_levels" option.'''))

    parser.add_argument('--excl_levels', nargs='+',
                        type=str.lower,
                        required=False, default=[],
                        choices=valid_fcst_levels,
                        help=dedent(f'''Forecast levels to exclude from verification plot generation.  This is a
                                        convenience option that provides a way to override the settings in the
                                        yaml plot configuration file.  If this option is not used, then all levels
                                        in the configuration file are plotted.  If it is used, then plots will be
                                        generated only for those levels in the configuration file that are not
                                        also listed here.  For simplicity, this option cannot be used together
                                        with the "--incl_only_levels" option.'''))

    parser.add_argument('--preexisting_dir_method',
                        type=str.lower,
                        required=False, default='rename',
                        choices=['rename', 'delete', 'quit'],
                        help=dedent(f'''Method for dealing with pre-existing output directories.'''))

    parser.add_argument('--make_stat_subdirs',
                        required=False, action=argparse.BooleanOptionalAction,
                        help=dedent(f'''Flag for placing output for each statistic to be plotted in a separate
                                        subdirectory under the output directory.'''))

    parser.add_argument('--create_ordered_plots',
                        required=False, action=argparse.BooleanOptionalAction,
                        help=dedent(f'''Flag for creating a directory that contains copies of all the generated
                                        images (png files) and renamed such that they are alphabetically in the
                                        same order as the user has specified in the yaml plot configuration file
                                        (the one passed to the optional "--config_fp" argument).  This is useful
                                        for creating a pdf of the plots from the images that includes the plots
                                        in the same order as in the plot configuration file.'''))

    args = parser.parse_args()

    make_mv_vx_plots(args)

