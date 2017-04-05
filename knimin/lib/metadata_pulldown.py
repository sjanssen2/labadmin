"""
Metadata pulldown for American Gut operates in X phases:
1) collection of all available data from different sources, currently I am
   aware of the following sources
    a) internal AG surveys, e.g. main, food, survers, ...
    b) vioscreen
2) correction of data, e.g. fix weight data if user confused feet/inch with
   centimeters, ...
3) masking data that are sensitive and cannot leave our system.
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from knimin.lib.configuration import config


def gather_agsurveys():
    # open the connection to the database
    engine = create_engine('postgresql://%s:%s@%s:%i/%s' %
                           (config.db_user, config.db_password, config.db_host,
                            config.db_port, config.db_database))

    def _retrieve_answers():
        # retrieve survey data from SQL database in two runs, because the are
        # two different tables. Table ag.survey_answers holds all "normal"
        # questions and table ag.survey_answers_other holds all questions that
        # can be answered with a free text; thus be careful because they can
        # contain HIPPA information!
        sql_answers = """SELECT survey_question_id, survey_id, response
                         FROM ag.survey_answers"""
        pd_answers = pd.read_sql_query(sql_answers, con=engine)
        sql_answers_other = """SELECT survey_question_id, survey_id, response
                               FROM ag.survey_answers_other"""
        pd_answers_other = pd.read_sql_query(sql_answers_other, con=engine)

        # concat both tables
        pd_answers_all = pd.concat([pd_answers, pd_answers_other],
                                   ignore_index=True)
        return pd_answers_all

    def _set_multiindex(pd_answers):
        # Questions of type "Multiple" are special, because there is a defined
        # set of valid answers, say a1, a2, a3. Participants can choose any
        # sub-set of those answers. We want to get one column for each of the
        # potential answer. Assume the question has name q1, than we expect to
        # get columns q1_a1, q1_a2, q1_a3. This requires a multi level index
        # for the pandas table, where level 1 is the question name (here q1)
        # and level 2 is (if question is of type multi) all possible answers,
        # here a1, a2, a3.
        sql_question_type = """SELECT survey_question_id, response
                               FROM ag.survey_question_response_type
                               JOIN ag.survey_question_response
                               USING (survey_question_id)
                               WHERE survey_response_type='MULTIPLE'"""
        pd_multiple_possResponses = pd.read_sql_query(sql_question_type,
                                                      con=engine)

        # find responses that belong to MULTI questions
        # all question IDs of type MULTIPLE
        uni = pd_multiple_possResponses.survey_question_id.unique()
        # flag answers belonging to questions of type MULTIPLE. Note: it
        # assigns True or False to every answer, it does not filter to answers
        # which evaluate to True. That is done below...
        sub = pd_answers.survey_question_id.isin(uni)
        # ... here is the filtering happening
        idx = pd_answers[sub].index
        # for MULTI questions responses: copy response into new column 'answer'
        # which will serve as index level 2 later on
        pd_answers.loc[idx, 'answer'] = pd_answers.loc[idx, 'response']

        # set multi level index
        x = pd_answers.set_index(['survey_id', 'survey_question_id', 'answer'])

        return x, pd_multiple_possResponses

    def _insert_missed_responses(res, pd_multiple_possResponses):
        # For MULTIPLE questions, table res only contains columns for every
        # possible response that have been chosen by at least one participant
        # so far. Thus, there might be available responses missing, if noone
        # chose them. Here, we insert columns for those never chosen responses.

        # all used responses for MULTIPLE questions
        mult_cols_used = set([c for c in res.columns.values
                              if pd.notnull(c[1])])
        # all available responses for MULTIPLE questions
        mult_cols_avail = set([(c.survey_question_id, c.response)
                               for _, c
                               in pd_multiple_possResponses.iterrows()])

        # create "empty" columns for those answers that have not been chosen by
        # any participant. empty means filled with np.nan
        for mult_col in mult_cols_avail - mult_cols_used:
            res[mult_col] = np.nan

    def _use_shortnames(pd_answers):
        # replaces survey_question_id with question_shortname in the multi-
        # index

        # retrieve two column table with survey_question_id and
        # question_shortname
        sql_questionnames = """SELECT survey_question_id, question_shortname
                               FROM ag.survey_question"""
        # execute SQL query and set the index of the returned pandas table to
        # survey_question_id
        sql_questionnames = pd.read_sql_query(sql_questionnames, con=engine)\
            .set_index('survey_question_id')

        # map survey_question_id to question_shortname
        shortnames = sql_questionnames.loc[pd_answers.columns.levels[0]]\
            .question_shortname.values

        # replace the index labels inplace
        pd_answers.columns.set_levels(shortnames, level=0, inplace=True)

    # retrieve answers from database
    pd_answers = _retrieve_answers()

    # create multi level index: level 1 = question_id, level 2 = response if
    # question is of type MULTIPLE
    x, pd_multiple_possResponses = _set_multiindex(pd_answers)

    # use unstack to transform response table, i.e. make 1D table into 2D
    res = x.unstack(level='survey_id').T

    # re-insert responses for MULTIPLE questions that haven't been used by
    # anyone.
    _insert_missed_responses(res, pd_multiple_possResponses)

    # replace survey_question_id with question_shortname in the multi-index
    _use_shortnames(res)

    return res
