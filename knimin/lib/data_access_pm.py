from knimin.lib.exceptions import *

from sql_connection import TRN


def _add_object(tbl_name, name, notes=None):
    """ Generalized function to add a new object in a table with three columns:
        (object_id, name, notes), e.g. processing_robot, tm300_8_tool, ...

    Parameters
    ----------
    tbl_name: str
        The DB table name into which the new object should be added.
    name: str
        Name of the new processing-robot.
    notes: str
        Some notes about new new processing-robot.

    Raises
    ------
    LabadminDBError
        a) If the tbl_name does not exist in the DB.
        b) If the table in which the object should be added does not follow the
           expected design, which is that the table must have exactly the three
           columns: [tbl_name_id, name, notes]
    ValueError
        If the object name is missing or empty.
    LabadminDBDuplicateError
        If an object of the same name already exists.
    """

    with TRN:
        # check that tbl_name exists in DB
        sql = """SELECT DISTINCT table_name from INFORMATION_SCHEMA.COLUMNS
                 WHERE table_schema = 'pm' AND table_name = %s"""
        TRN.add(sql, [tbl_name])
        if len(TRN.execute_fetchindex()) != 1:
            raise LabadminDBError('Table %s does not exist in data base.' %
                                  tbl_name)

        # check that table has exactly three columns
        sql = """SELECT column_name from INFORMATION_SCHEMA.COLUMNS
                 WHERE table_schema = 'pm' AND table_name = %s"""
        TRN.add(sql, [tbl_name])
        column_names = [c[0] for c in TRN.execute_fetchindex()]
        if [tbl_name+'_id', 'name', 'notes'] != column_names:
            raise LabadminDBError(('Table %s does not have the expected column'
                                   ' design. It must have exactly the three '
                                   'columns: "%s_id", "name", "notes"') %
                                  (tbl_name, tbl_name))

    # ensure that the object name is not empty
    if not name or len(name) <= 0:
        raise ValueError('The %s name cannot be empty.' % tbl_name)

    with TRN:
        # check if an object with the same name already exists
        sql = """SELECT name FROM pm."""+tbl_name+""" WHERE name = %s"""
        TRN.add(sql, [name])
        if len(TRN.execute_fetchindex()) > 0:
            raise LabadminDBDuplicateError(tbl_name,
                                           'name: %s' % name)

        # add the new object into the DB
        sql = """INSERT INTO pm."""+tbl_name+""" (name, notes)
                 VALUES (%s, %s)
                 RETURNING %s_id"""
        TRN.add(sql, [name, notes, tbl_name])
        TRN.execute()


def add_master_mix_lot(name, notes=None):
    """ Adds a new master mix lot.

    Parameters
    ----------
    name: str
        Name of the new master mix lot.
    notes: str
        Some notes about new master mix lot.

    Raises
    ------
    ValueError
        If the master mix lot name is missing or empty.
    LabadminDBDuplicateError
        If a master mix lot of the same name already exists.
    """
    _add_object('master_mix_lot', name, notes)


def add_processing_robot(name, notes=None):
    """ Adds a new processing-robot.

    Parameters
    ----------
    name: str
        Name of the new processing-robot.
    notes: str
        Some notes about new processing-robot.

    Raises
    ------
    ValueError
        If the processing robot name is missing or empty.
    LabadminDBDuplicateError
        If a processing robot of the same name already exists.
    """
    _add_object('processing_robot', name, notes)


def add_tm300_8_tool(name, notes=None):
    """ Adds a new TM 300-8 tool.

    Parameters
    ----------
    name: str
        Name of the new TM 300-8 tool.
    notes: str
        Some notes about new TM 300-8 tool.

    Raises
    ------
    ValueError
        If the TM 300-8 tool name is missing or empty.
    LabadminDBDuplicateError
        If a TM 300-8 tool of the same name already exists.
    """
    _add_object('tm300_8_tool', name, notes)


def add_tm50_8_tool(name, notes=None):
    """ Adds a new TM 50-8 tool.

    Parameters
    ----------
    name: str
        Name of the new TM 50-8 tool.
    notes: str
        Some notes about new TM 50-8 tool.

    Raises
    ------
    ValueError
        If the TM 50-8 tool name is missing or empty.
    LabadminDBDuplicateError
        If a TM 50-8 tool of the same name already exists.
    """
    _add_object('tm50_8_tool', name, notes)


def add_water_lot(name, notes=None):
    """ Adds a new water lot.

    Parameters
    ----------
    name: str
        Name of the new water lot.
    notes: str
        Some notes about new water lot.

    Raises
    ------
    ValueError
        If the water lot name is missing or empty.
    LabadminDBDuplicateError
        If a water lot of the same name already exists.
    """
    _add_object('water_lot', name, notes)
