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
from sqlalchemy import create_engine
from knimin.lib.configuration import config


def gather_agsurveys():
    # open the connection to the database
    engine = create_engine('postgresql://%s:%s@%s:%i/%s' %
                           (config.db_user, config.db_password, config.db_host,
                            config.db_port, config.db_database))

    # retrieve survey data from SQL database
    sql_answers = """SELECT survey_question_id, survey_id, response
                     FROM ag.survey_answers"""
    pd_answers = pd.read_sql_query(sql_answers, con=engine)
    sql_answers_other = """SELECT survey_question_id, survey_id, response
                           FROM ag.survey_answers_other"""
    pd_answers_other = pd.read_sql_query(sql_answers_other, con=engine)

    # concat both tables
    pd_answers_all = pd.concat([pd_answers, pd_answers_other],
                               ignore_index=True)

    # retrieve information which survey questions are of type MULTI
    sql_question_type = """SELECT survey_question_id, response
                           FROM ag.survey_question_response_type
                           JOIN ag.survey_question_response
                           USING (survey_question_id)
                           WHERE survey_response_type='MULTIPLE'"""
    pd_multiple_possResponses = pd.read_sql_query(sql_question_type,
                                                  con=engine)

    # find responses that belong to MULTI questions
    uni = pd_multiple_possResponses.survey_question_id.unique()
    sub = pd_answers_all.survey_question_id.isin(uni)
    idx = pd_answers_all[sub].index
    # for MULTI questions responses: copy response into column 'preset'
    # which will surve as index level 2 later on
    pd_answers_all.loc[idx, 'preset'] = pd_answers_all.loc[idx, 'response']

    # set multi level index
    x = pd_answers_all.set_index(['survey_id', 'survey_question_id', 'preset'])
    # use unstack to transform response table
    res = x.unstack(level='survey_id').T

    return res
