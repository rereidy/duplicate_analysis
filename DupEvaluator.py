# -*- coding: UTF-8 -*-

import pandas   as pd
import datetime as dt
import argparse

from typing import NewType

DataFrame_t = NewType("DataFrame_t", pd.core.frame.DataFrame)

from difflib     import SequenceMatcher
from collections import OrderedDict
from collections import namedtuple

#
# import method to avoid use of the pure Python Sequence Matcher:
#
# UserWarning: Using slow pure-python SequenceMatcher. Install python-Levenshtein to remove this warning
#
import warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    from fuzzywuzzy import fuzz

"""
constants used in the duplicate evaluator

eval_types  : the types of evaluators defined for the process
                RPA    - use the inventory from the RPA COE against the ETA sumissions worklist
                COLLAB - use the ETA submissions worklist against itself
collab_cols : the columns from the ETA worklist that will be used
rpa_cols    : the columns from the RPA inventory report that will be used
min_pct     : the lowest percent value used for determining likelihood of duplication
max_pct     : the maximum percent value used for determining likelihood of duplication
"""
Constants = namedtuple('Constants', ['eval_types', 'collab_cols', 'rpa_cols', 'min_pct', 'max_pct'])
CONSTANTS = Constants(["RPA", "COLLAB"],
                      ["Assigned to",
                       "Collaboration Phase",
                       "Opportunity ID",
                       "Collaboration Opportunity Name",
                       "Collaboration Idea Summary",
                       "Operational Division"
                      ],
                      ['Status',
                       'LOB Unit',
                       'LOB SubUnit',
                       'Automation Name',
                       'Short Project Description'
                      ],
                      1, 100
                     )

"""
argparse helper functions
"""
def range_check(arg: str, minv: int, maxv: int) -> int:
    """
    range_check - helper function for command line argument '-t'

    Parameters
        arg  : the command line argument value
        minv : the minimum value
        maxv : the maximum avalue

    Return
        the arg converted to an int type
    """
    value = int(arg)

    if value < minv or value > maxv:
        raise argparse.ArgumentTypeError("unexpected value: {0:d}: expected {1:d}-{2:d}".format(value, minv, maxv))

    return value

def dup_type_check(arg: str, vals: list) -> str:
    """
    dup_type_check - helper function for command line argument '-d'

    Parameters
        arg  : the command line argument value
        vals : list type of the valid values

    Return
        the arg converted to an int type
    """
    value = None
    if arg.upper() in vals:
        value = arg.upper()

    if value is None:
        raise argparse.ArgumentTypeError("unexpected value for duplication type - {0}; value={1}".format(', '.join(vals), arg))

    return value

class DoubleD(dict):
    """
    DoubleD - access and delete dictionary elements by key or value.
    """
    def __getitem__(self, key):
        if key not in self:
            inv_dict = {v:k for k,v in self.items()}
            return inv_dict[key]
        return dict.__getitem__(self, key)

    def __delitem__(self, key):
        if key not in self:
            inv_dict = {v:k for k,v in self.items()}
            dict.__delitem__(self, inv_dict[key])
        else:
            dict.__delitem__(self, key)

class DupCalculator(object):
    """
    DupCalculator - class to perform duplicate calculations
    """
    def __init__(self):
        pass

    def calc_fuzz(self, name1: str, name2: str, desc1: str, desc2: str) -> dict:
        """
        calc_fuzz - calculate fuzz values for text

        Parameters
            name1 : the first name
            name2 : the second name
            desc1 : the description corresponding to name1
            desc2 : the description corresponding to name2

        Return
            a dict type containint fuzzed values
        """
        data = {}

        data['names'] = {'ratio'            : fuzz.ratio(name1.lower(), name2.lower()),
                         'partial_ratio'    : fuzz.partial_ratio(name1.lower(), name2.lower()),
                         'token_sort_ratio' : fuzz.token_sort_ratio(name1, name2)
                        }

        # do not calculate ratios for blank columns
        if (desc1 != "" and desc2 != "") and \
                (desc1 != "TBD" and desc2 != "TBD"):
            data['desc'] = {'ratio'            : fuzz.ratio(desc1.lower(), desc2.lower()),
                            'partial_ratio'    : fuzz.partial_ratio(desc1.lower(), desc2.lower()),
                            'token_sort_ratio' : fuzz.token_sort_ratio(desc1, desc2)
                           }
        else:
            data['desc'] = {'ratio'            : 0,
                            'partial_ratio'    : 0,
                            'token_sort_ratio' : 0,
                           }

        return data

    def dup_likelihood(self, sr: dict, fv: dict) -> float:
        """
        dup_likelihood - sum values for names and desc; calculate average of these values

        Parameters
            sr : the Sequence Matcher rations for the names and the descriptions
            fv : the fuzz ratios calculated for the names and descriptions

        Return
            a float type representing the average of the values of the parameters
        """
        names = sum([sr['name'],
                     fv['names']['ratio'],
                     fv['names']['partial_ratio'],
                     fv['names']['token_sort_ratio']
                    ]
                   )
        desc  = sum([sr['desc'],
                     fv['desc']['ratio'],
                     fv['desc']['partial_ratio'],
                     fv['desc']['token_sort_ratio']
                    ]
                   )

        likely = round((names + desc)/(len(sr) + len(fv['names']) + len(fv['desc'])), 2)

        return likely

    def seq_matcher(self, name1: str, name2: str, desc1: str, desc2: str) -> set:
        """
        seq_matcher - use difflib.SequenceMatcher.ratio() to determine the
                      weight of duplication

        Parameters
            name1 : the first name
            name2 : the second name
            desc1 : the description corresponding to name1
            desc2 : the description corresponding to name2

        Return
            a set type
        """
        s1 = SequenceMatcher(isjunk=None, a=name1, b=name2)
        sr1 = round(s1.ratio()*100, 2)

        # do not calculate a ratio for blank descriptions
        sr2 = 0.0
        if desc1 != "" and desc2 != "":
            s2 = SequenceMatcher(isjunk=None, a=desc1, b=desc2)
            sr2 = round(s2.ratio()*100, 2)

        return sr1, sr2

class RPA(object):
    """
    RPA - class to perform duplicate testing of RPA project data against the
          ETA submissions worklist
    """
    def __init__(self,
                 collab_df: DataFrame_t,
                 rpa_df: DataFrame_t,
                 dup_pct: float
                ):
        """
        constructor

        Parameters
            collab_df : a pandas DataFrame type with ETA submissions worklist data
            rpa_df    : a pandas DataFrame type with RPA data
            dup_pct   : the threshold percent for saving duplicate data
        """
        self.collab_df  = collab_df
        self.rpa_df     = rpa_df
        self.dup_pct    = dup_pct
        self.calculator = DupCalculator()

    def df_eval(self) -> DataFrame_t:
        data = OrderedDict()
        data['RPA AutomationName']            = []
        data['RPA Status']                    = []
        data['ETA Name']                      = []
        data['ETA phase']                     = []
        data['ETA ID']                        = []
        data['Name SequenceMatcher ratio']    = []
        data['Name fuzz.ratio']               = []
        data['Name fuzz.partial_ratio']       = []
        data['Name fuzz.token_sort_ratio']    = []
        data['RPA Descrip']                   = []
        data['ETA IdeaSummary']               = []
        data['Descrip SequenceMatcher ratio'] = []
        data['Descrip fuzz.ratio']            = []
        data['Descrip fuzz.partial_ratio']    = []
        data['Descrip fuzz.token_sort_ratio'] = []
        data['Likelyhood of duplication']     = []

        seen_dups = DoubleD()

        for outer_i, outer_row in self.rpa_df.iterrows():
            if outer_row['Automation Name'] in seen_dups:
                continue

            if outer_row['Status'] is not None and \
                isinstance(outer_row['Automation Name'], float) and \
                isinstance(outer_row['Short Project Description'], float):
                continue

            rpaStatus = outer_row['Status']
            rpaName   = outer_row['Automation Name']
            rpaDesc   = outer_row['Short Project Description']

            for inner_i, inner_row in self.collab_df.iterrows():
                if inner_row['Collaboration Opportunity Name'] in seen_dups:
                    continue

                subName  = inner_row['Collaboration Opportunity Name']
                subDesc  = inner_row['Collaboration Idea Summary']
                subPhase = inner_row['Collaboration Phase']
                subID    = inner_row['Opportunity ID']

                nameSeq, descSeq = self.calculator.seq_matcher(rpaName, subName, rpaDesc, subDesc)
                fuzz_vals = self.calculator.calc_fuzz(rpaName, subName, rpaDesc, subDesc)
                likely_dup = self.calculator.dup_likelihood({'name' : nameSeq, 'desc' : descSeq}, fuzz_vals)

                if likely_dup >= self.dup_pct:
                    data['RPA AutomationName'].append(rpaName)
                    data['RPA Status'].append(rpaStatus)
                    data['ETA Name'].append(subName)
                    data['ETA phase'].append(subPhase)
                    data['ETA ID'].append(subID)
                    data['Name SequenceMatcher ratio'].append(nameSeq)
                    data['Name fuzz.ratio'].append(fuzz_vals['names']['ratio'])
                    data['Name fuzz.partial_ratio'].append(fuzz_vals['names']['partial_ratio'])
                    data['Name fuzz.token_sort_ratio'].append(fuzz_vals['names']['token_sort_ratio'])
                    data['RPA Descrip'].append(rpaDesc)
                    data['ETA IdeaSummary'].append(subDesc)
                    data['Descrip SequenceMatcher ratio'].append(descSeq)
                    data['Descrip fuzz.ratio'].append(fuzz_vals['desc']['ratio'])
                    data['Descrip fuzz.partial_ratio'].append(fuzz_vals['desc']['partial_ratio'])
                    data['Descrip fuzz.token_sort_ratio'].append(fuzz_vals['desc']['token_sort_ratio'])
                    data['Likelyhood of duplication'].append(likely_dup)

                    seen_dups[rpaName] = subName
                    break

        return pd.DataFrame.from_dict(data)

class COLLAB(object):
    """
    COLLAB - class to perform duplicate testing of data withing the ETA
             submissions worklist
    """
    def __init__(self, df: DataFrame_t, dup_pct: float):
        """
        constructor

        Parameters
            df      : a pandas DataFrame type with ETA submissions worklist data
            dup_pct : the threshold percent for saving duplicate data
        """
        self.df         = df
        self.dup_pct    = dup_pct
        self.calculator = DupCalculator()

    def df_eval(self) -> DataFrame_t:
        data = OrderedDict()
        data['From ID']                       = []
        data['From Name']                     = []
        data['From OpDiv']                    = []
        data['To ID']                         = []
        data['To Name']                       = []
        data['To OpDiv']                      = []
        data['Name SequenceMatcher ratio']    = []
        data['Name fuzz.ratio']               = []
        data['Name fuzz.partial_ratio']       = []
        data['Name fuzz.token_sort_ratio']    = []
        data['From IdeaSummary']              = []
        data['To IdeaSummary']                = []
        data['Descrip SequenceMatcher ratio'] = []
        data['Descrip fuzz.ratio']            = []
        data['Descrip fuzz.partial_ratio']    = []
        data['Descrip fuzz.token_sort_ratio'] = []
        data['Likelyhood of duplication']     = []

        ndf = self.df[self.df.columns[1:]]
        for outer_i, outer_row in ndf.iterrows():
            fromID    = outer_row["Opportunity ID"]
            fromName  = outer_row["Collaboration Opportunity Name"]
            fromSumm  = outer_row["Collaboration Idea Summary"]
            fromOpDiv = outer_row["Operational Division"]
            fromDesc  = outer_row["Collaboration Idea Summary"]

            for inner_i, inner_row in ndf.iterrows():
                toID    = inner_row["Opportunity ID"]
                toName  = inner_row["Collaboration Opportunity Name"]
                toSumm  = inner_row["Collaboration Idea Summary"]
                toOpDiv = inner_row["Operational Division"]
                toDesc  = outer_row["Collaboration Idea Summary"]

                if (fromID == toID and fromName == toName):
                    continue

                # common functions defined outside the classes
                nameSeq, descSeq = self.calculator.seq_matcher(fromName, toName, fromSumm, toSumm)
                fuzz_vals = self.calculator.calc_fuzz(fromName, toName, fromSumm, toSumm)
                likely_dup = self.calculator.dup_likelihood({'name' : nameSeq, 'desc' : descSeq }, fuzz_vals)

                if likely_dup >= self.dup_pct:
                    data['From ID'].append(fromID)
                    data['From Name'].append(fromName)
                    data['From OpDiv'].append(fromOpDiv)
                    data['To ID'].append(toID)
                    data['To Name'].append(toName)
                    data['To OpDiv'].append(toOpDiv)
                    data['Name SequenceMatcher ratio'].append(nameSeq)
                    data['Name fuzz.ratio'].append(fuzz_vals['names']['ratio'])
                    data['Name fuzz.partial_ratio'].append(fuzz_vals['names']['partial_ratio'])
                    data['Name fuzz.token_sort_ratio'].append(fuzz_vals['names']['token_sort_ratio'])
                    data['From IdeaSummary'].append(fromDesc)
                    data['To IdeaSummary'].append(toDesc)
                    data['Descrip SequenceMatcher ratio'].append(descSeq)
                    data['Descrip fuzz.ratio'].append(fuzz_vals['desc']['ratio'])
                    data['Descrip fuzz.partial_ratio'].append(fuzz_vals['desc']['partial_ratio'])
                    data['Descrip fuzz.token_sort_ratio'].append(fuzz_vals['desc']['token_sort_ratio'])
                    data['Likelyhood of duplication'].append(likely_dup)

                    break

        return pd.DataFrame.from_dict(data)

class DupEvaluator(object):
    def __init__(self, args):
        self.args   = args
        if self.args.duplication_type not in CONSTANTS.eval_types:
            raise ValueError("evaluation type not found: {0} not in {1}".format(eval_type,
                                                                                ','.join(CONSTANTS.eval_types)
                                                                               )
                            )


        self.collab_df = pd.read_excel(self.args.collaboration_file,
                                       header=0,
                                       usecols=CONSTANTS.collab_cols,
                                       na_filter=False
                                      )

        if self.args.duplication_type == "RPA":
            if self.args.rpa_file is not None:
                df = self.rpa_cleanup(pd.read_excel(self.args.rpa_file,
                                                    header=0,
                                                    usecols=CONSTANTS.rpa_cols
                                                   )
                                     )
                self.rpa_df = df.dropna(how='all')
            else:
                raise argparse.ArgumentTypeError(
                    "no RPA file specified for duplication type of {0}".format(self.args.duplication_type))


    def dup_eval(self) -> DataFrame_t:
        if self.args.duplication_type == "COLLAB":
            evaluator = COLLAB(self.collab_df, self.args.threshold_pct)
        elif self.args.duplication_type == "RPA":
            evaluator = RPA(self.collab_df, self.rpa_df, self.args.threshold_pct)

        return evaluator.df_eval()

    def rpa_cleanup(self, df: DataFrame_t) -> DataFrame_t:
        """
        rpa_cleanup - cleanup data in rpa spreadsheet

        Parameters
            df: pandas DataFrame object

        Steps
            1. carry 'Status', 'LOB Unit' , and 'LOB SubUnit' columns to all rows where no values are entered
            2. drop any rows w/o values

        Returns
            a pandas DataFrame object
        """
        stat = 'Deployed'
        lob_unit = None
        lob_subunit = None
        for index, row in df.iterrows():
            if row['Status'] == "":
                df.loc[index, 'Status'] = stat
            elif stat != row['Status']:
                stat = row['Status']

            if lob_unit is None:
                lob_unit = row['LOB Unit']

            if lob_subunit is None:
                lob_subunit = row['LOB SubUnit']

            if row['LOB Unit'] == "":
                df.loc[index, 'LOB Unit'] = lob_unit
            else:
                lob_unit = row['LOB Unit']

            if row['LOB SubUnit'] == "":
                df.loc[index, 'LOB SubUnit'] = lob_subunit
            else:
                lob_subunit = row['LOB SubUnit']

            if row['Short Project Description'] == '(blank)':
                df.loc[index, 'Short Project Description'] = ""

        return df

