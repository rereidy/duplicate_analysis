# -*- coding: UTF-8 -*-

import os
from os.path import basename
import sys
import datetime as dt
import argparse
from pandas import ExcelWriter
from functools import partial
from pathlib import Path

import DupEvaluator as de

if __name__ == "__main__":
    rv = 0
    startdt = dt.datetime.now()

    try:
        parser = argparse.ArgumentParser(description="duplication analyzer",
                                         usage="%(prog)s [options]"
                                        )

        check_dup_val   = partial(de.dup_type_check, vals=de.CONSTANTS.eval_types)
        parser.add_argument('-d',
                            dest='duplication_type',
                            type=check_dup_val,
                            help='type of duplication processing to perform - RPA or COLLAB (default is %(default)s)',
                            default="COLLAB",
                            action='store'
                           )

        check_threshold_pct = partial(de.range_check, minv=de.CONSTANTS.min_pct, maxv=de.CONSTANTS.max_pct)
        parser.add_argument('-t',
                            dest='threshold_pct',
                            type=check_threshold_pct,
                            help='threshold percent for determining duplicates [1-100] (default is %(default)s)',
                            action='store',
                            default=50
                           )

        parser.add_argument('-r',
                            dest='rpa_file',
                            help='path to RPA spreadsheet file',
                            action='store'
                           )

        parser.add_argument('-c',
                            dest='collaboration_file',
                            help='path to Collaboration submissions worklist spreadsheet file',
                            required=True,
                            action='store'
                           )

        parser.add_argument('-o',
                            dest='output_file',
                            help="output file name (default is '%(default)s')",
                            default= Path("{0}/Users/{1}/Documents/{2}-analysis-{3}.xlsx".format(os.environ.get("SystemDrive"),
                                                                                                 os.environ.get("USERNAME"),
                                                                                                 os.path.splitext(basename(parser.prog))[0],
                                                                                                 dt.date.today().strftime("%Y%m%d")
                                                                                                )
                                         ),
                            action='store'
                           )

        args = parser.parse_args()
        if args.duplication_type == "RPA" and args.rpa_file is None:
            argparse.ArgumentTypeError("no RPA file specified for duplication type of {0}".format(args.duplication_type))

        if args.duplication_type == 'RPA':
            print("evaluating RPA inventory to ETA submissions worklist")
        elif args.duplication_type == "COLLAB":
            print("evaluating ETA submissions worklist against itself")

        evaluator = de.DupEvaluator(args)
        dup_df = evaluator.dup_eval()

        print("{0:d} duplicates found".format(dup_df.shape[0]))
        print("saving duplicates to {0}".format(args.output_file))
        with ExcelWriter(os.fspath(args.output_file)) as writer:
            dup_df.to_excel(writer, index=False)

    except Exception as e:
        print(e, file=sys.stdout)
        rv+= 1
    finally:
        print("end {0} (elapsed time: {1})".format(parser.prog, dt.datetime.now() - startdt))
        sys.exit(rv)

