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


def make_mv_vx_plots(args):
    """Make multiple verification plots using MetViewer and the settings
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
    fcst_init_info = map(str, list(fcst_init_info.values()))
    # fcst_init_info is a list containing both strings and integers.  For
    # use below, convert it to a list of strings only.
    fcst_init_info = [str(elem) for elem in fcst_init_info]
    fcst_len_hrs = str(config_dict['fcst_len_hrs'])

    # Check if output directory exists and take action according to how the
    # args.preexisting_dir_method flag is set.
    if os.path.exists(args.output_dir):
        if args.preexisting_dir_method == 'rename':
            now = datetime.now()
            renamed_dir = args.output_dir + now.strftime('.old_%Y%m%d_%H%M%S')
            logging.info(dedent(f'''\n
                Output directory already exists:
                  {args.output_dir}
                Moving (renaming) preexisting directory to:
                  {renamed_dir}'''))
            os.rename(args.output_dir, renamed_dir)
        elif args.preexisting_dir_method == 'delete':
            logging.info(dedent(f'''\n
                Output directory already exists:
                  {args.output_dir}
                Removing existing directory...'''))
            shutil.rmtree(args.output_dir)
        else:
            raise FileExistsError(dedent(f'''\n
                Output directory already exists:
                  {args.output_dir}
                Stopping.'''))

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

    # Initialze (1) the counter that keeps track of the number of times the
    # script that generates a MetViewer xml and calls MetViewer is called and
    # (2) the counter that keeps track of the number of images (png files)
    # that were successfully generated.  Each call to the script should
    # generate an image, so these two counters can be compared at the end to
    # see how many images were (not) successfully generated.
    num_mv_calls = 0
    num_images_generated = 0

    vx_stats_dict = config_dict["vx_stats"]
    for stat, stat_dict in vx_stats_dict.items():

        if stat in args.exclude_stats:
            logging.info(dedent(f"""\n
                Skipping plotting of statistic "{stat}" because it is in the list of 
                stats to exclude ...
                  args.exclude_stats = {args.exclude_stats}
                """))

        elif (not args.include_stats) or (args.include_stats and stat in args.include_stats):
            logging.info(dedent(f"""
                Plotting statistic "{stat}" for various forecast fields ...
                """))
            msg = dedent(f"""
                Dictionary of fields, levels, and thresholds (if applicable) for this statistic is:
                  stat_dict = """)
            indent_str = ' '*(5 + len('stat_dict'))
            msg = msg + get_pprint_str(stat_dict, indent_str).lstrip()
            logging.debug(msg)

            for fcst_field, fcst_field_dict in stat_dict.items():

                logging.info(dedent(f"""
                    Plotting statistic "{stat}" for forecast field "{fcst_field}" at various levels ...
                    """))
                msg = dedent(f"""
                    Dictionary of levels and thresholds (if applicable) for this field is:
                      fcst_field_dict = """)
                indent_str = ' '*(5 + len('fcst_field_dict'))
                msg = msg + get_pprint_str(fcst_field_dict, indent_str).lstrip()
                logging.debug(msg)

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
                                     '--mv_output_dir', args.output_dir]

                        msg = dedent(f"""
                            Argument list passed to plotting script is:
                              args_list = """)
                        indent_str = ' '*(5 + len('args_list'))
                        msg = msg + get_pprint_str(args_list, indent_str).lstrip()
                        logging.debug(msg)

                        num_mv_calls += 1
                        logging.debug(dedent(f"""
                            Calling MetViewer plotting script ...
                              num_mv_calls = {num_mv_calls}
                            """))
                        output_xml_fp = plot_vx_metviewer(args_list)
                        logging.debug(dedent(f"""
                            Done calling MetViewer plotting script.
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
        Total number of calls to MetViewer plotting script:
          num_mv_calls = {num_mv_calls}
        Total number of image files generated:
          num_images_generated = {num_images_generated}
        """))
#
# -----------------------------------------------------------------------
#
# Call the function defined above.
#
# -----------------------------------------------------------------------
#
if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description='Call MetViewer to create vx plots.'
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
                                        MetViewer generated plots, log files, etc).  These will usually
                                        be placed in subdirectories under this output directory.'''))

    parser.add_argument('--config_fp',
                        type=str,
                        required=False, default='config_mv_plots.default.yml',
                        help=dedent(f'''Name of or path (absolute or relative) to yaml user
                                        plot configuration file for MetViewer plot generation.'''))

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

    parser.add_argument('--include_stats', nargs='+',
                        type=str.lower,
                        required=False, default=[],
                        choices=['auc', 'bias', 'brier', 'fbias', 'rely', 'rhist', 'ss'],
                        help=dedent(f'''Stats to include in verification plot generation.  A stat
                                        included here will still be excluded if it is not in the
                                        yaml user plot configuration file.'''))

    parser.add_argument('--exclude_stats', nargs='+',
                        type=str.lower,
                        required=False, default=[],
                        choices=['auc', 'bias', 'brier', 'fbias', 'rely', 'rhist', 'ss'],
                        help='Stats to exclude from verification plot generation.')

    parser.add_argument('--preexisting_dir_method',
                        type=str.lower,
                        required=False, default='rename',
                        choices=['rename', 'delete', 'quit'], 
                        help=dedent(f'''Method for dealing with pre-existing output directories.'''))

    parser.add_argument('--create_ordered_plots',
                        required=False, action=argparse.BooleanOptionalAction,
                        help=dedent(f'''Boolean flag for creating a directory that contains copies of
                                        all the generated images (png files) and renamed such that they
                                        are alphabetically in the same order as the user has specified
                                        in the yaml plot configuration file (the one passed to the optional
                                        --config_fp argument).  This is useful for creating a pdf of 
                                        the plots from the images that includes the plots in the same 
                                        order as in the plot configuration file.'''))

    args = parser.parse_args()

    make_mv_vx_plots(args)

