from knimin.lib.exceptions import *

from sql_connection import TRN


def add_processing_robot(name, notes=None):
    """ Adds a new processing-robot.

    Parameters
    ----------
    name: str
        Name of the new processing-robot.
    notes: str
        Some notes about new new processing-robot.

    Raises
    ------
    ValueError
        If the processing robot name is missing or empty.
    LabadminDBDuplicateError
        If a processing robot of the same name already exists.
    """

    # ensure that the processing robot name is not empty
    if not name or len(name) <= 0:
        raise ValueError('The processing robot name cannot be empty.')

    with TRN:
        # check if a processing robot with the same name already exists
        sql = """SELECT name FROM pm.processing_robot WHERE name = %s"""
        TRN.add(sql, [name])
        if len(TRN.execute_fetchindex()) > 0:
            raise LabadminDBDuplicateError('processing_robot',
                                           'name: %s' % name)

        # add the new processing robot to the DB
        sql = """INSERT INTO pm.processing_robot (name, notes)
                 VALUES (%s, %s)
                 RETURNING processing_robot_id"""
        TRN.add(sql, [name, notes])
        TRN.execute()


def add_tm300_8_tool(name, notes=None):
    """ Adds a new TM 300-8 tool.

    Parameters
    ----------
    name: str
        Name of the new TM 300-8 tool.
    notes: str
        Some notes about new new TM 300-8 tool.

    Raises
    ------
    ValueError
        If the TM 300-8 tool name is missing or empty.
    LabadminDBDuplicateError
        If a TM 300-8 tool of the same name already exists.
    """

    # ensure that the TM 300-8 tool name is not empty
    if not name or len(name) <= 0:
        raise ValueError('The TM 300-8 tool name cannot be empty.')

    with TRN:
        # check if a TM 300-8 tool with the same name already exists
        sql = """SELECT name FROM pm.tm300_8_tool WHERE name = %s"""
        TRN.add(sql, [name])
        if len(TRN.execute_fetchindex()) > 0:
            raise LabadminDBDuplicateError('tm300_8_tool',
                                           'name: %s' % name)

        # add the new TM 300-8 tool to the DB
        sql = """INSERT INTO pm.tm300_8_tool (name, notes)
                 VALUES (%s, %s)
                 RETURNING tm300_8_tool_id"""
        TRN.add(sql, [name, notes])
        TRN.execute()


def add_tm50_8_tool(name, notes=None):
    """ Adds a new TM 50-8 tool.

    Parameters
    ----------
    name: str
        Name of the new TM 50-8 tool.
    notes: str
        Some notes about new new TM 50-8 tool.

    Raises
    ------
    ValueError
        If the TM 50-8 tool name is missing or empty.
    LabadminDBDuplicateError
        If a TM 50-8 tool of the same name already exists.
    """

    # ensure that the TM 50-8 tool name is not empty
    if not name or len(name) <= 0:
        raise ValueError('The TM 50-8 tool name cannot be empty.')

    with TRN:
        # check if a TM 50-8 tool with the same name already exists
        sql = """SELECT name FROM pm.tm50_8_tool WHERE name = %s"""
        TRN.add(sql, [name])
        if len(TRN.execute_fetchindex()) > 0:
            raise LabadminDBDuplicateError('tm50_8_tool',
                                           'name: %s' % name)

        # add the new TM 50-8 tool to the DB
        sql = """INSERT INTO pm.tm50_8_tool (name, notes)
                 VALUES (%s, %s)
                 RETURNING tm50_8_tool_id"""
        TRN.add(sql, [name, notes])
        TRN.execute()
