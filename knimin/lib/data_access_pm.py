from knimin.lib.exceptions import *

from sql_connection import TRN


def _check_user(email):
    """ Checks that email is an existing user in the system.

    Parameters
    ----------
    email: str
        The email address that specifies a system user.

    Returns
    -------
    True, iff user with email exists in the system.

    Raises
    ------
    LabadminDBUnknownIDError
        If no user with the given email exists.
    """
    with TRN:
        sql = """SELECT email FROM ag.labadmin_users WHERE email = %s"""
        TRN.add(sql, [email])
        if len(TRN.execute_fetchindex()) == 0:
            raise LabadminDBUnknownIDError(email, 'ag.labadmin_users')
        else:
            return True


def _check_table_layout(tbl_name):
    """ Check if the given table exists and is a three column one.

    Parameters
    ----------
    tbl_name: str
        The name of the table to be checked.

    Returns
    -------
    Bool
        True, iff the table exists and has the three columns
        [tbl_name_id, name, notes]

    Raises
    ------
    LabadminDBError
        a) If the tbl_name does not exist in the DB.
        b) If the table does not follow the expected design, which is that the
           table must have exactly the three columns:
           [tbl_name_id, name, notes]
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
        return True


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

    Returns
    -------
    int
        ID of the newly created database entry.

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
    _check_table_layout(tbl_name)

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
                 RETURNING """+tbl_name+"""_id"""
        TRN.add(sql, [name, notes])
        return int(TRN.execute_fetchindex()[0][0])


def _get_objects(tbl_name):
    """ Generalized function to return the content of a table with three
        columns: (object_id, name, notes), e.g. processing_robot, tm300_8_tool,

    Parameters
    ----------
    tbl_name: str
        The DB table name into which the new object should be added.
    name: str
        Name of the new processing-robot.

    Returns
    -------
    list of dicts per row: [{object_id:int, name:str, notes:str}]

    Raises
    ------
    LabadminDBError
        a) If the tbl_name does not exist in the DB.
        b) If the table in which the object should be added does not follow the
           expected design, which is that the table must have exactly the three
           columns: [tbl_name_id, name, notes]
    """
    _check_table_layout(tbl_name)
    with TRN:
        sql = """SELECT * FROM pm.""" + tbl_name
        TRN.add(sql, [])
        return [dict(x) for x in TRN.execute_fetchindex()]


def add_master_mix_lot(name, notes=None):
    """ Adds a new master mix lot.

    Parameters
    ----------
    name: str
        Name of the new master mix lot.
    notes: str
        Some notes about new master mix lot.

    Returns
    -------
    int
        ID of the newly created master mix lot.

    Raises
    ------
    ValueError
        If the master mix lot name is missing or empty.
    LabadminDBDuplicateError
        If a master mix lot of the same name already exists.
    """
    return _add_object('master_mix_lot', name, notes)


def get_master_mix_lots():
    """ Lists all existing master mix lots.

    Returns
    -------
    List of dicts per row: [{master_mix_lot_id:int, name:str, notes:str}]
    """
    return _get_objects('master_mix_lot')


def add_processing_robot(name, notes=None):
    """ Adds a new processing robot.

    Parameters
    ----------
    name: str
        Name of the new processing robot.
    notes: str
        Some notes about new processing robot.

    Returns
    -------
    int
        ID of the newly created processing robot.

    Raises
    ------
    ValueError
        If the processing robot name is missing or empty.
    LabadminDBDuplicateError
        If a processing robot of the same name already exists.
    """
    return _add_object('processing_robot', name, notes)


def get_processing_robots():
    """ Lists all existing processing robots.

    Returns
    -------
    List of dicts per row: [{processing_robot_id:int, name:str, notes:str}]
    """
    return _get_objects('processing_robot')


def add_tm300_8_tool(name, notes=None):
    """ Adds a new TM 300-8 tool.

    Parameters
    ----------
    name: str
        Name of the new TM 300-8 tool.
    notes: str
        Some notes about new TM 300-8 tool.

    Returns
    -------
    int
        ID of the newly created TM 300-8 tool.

    Raises
    ------
    ValueError
        If the TM 300-8 tool name is missing or empty.
    LabadminDBDuplicateError
        If a TM 300-8 tool of the same name already exists.
    """
    return _add_object('tm300_8_tool', name, notes)


def get_tm300_8_tools():
    """ Lists all existing TM 300-8 tools.

    Returns
    -------
    List of dicts per row: [{tm300_8_tool_id:int, name:str, notes:str}]
    """
    return _get_objects('tm300_8_tool')


def add_tm50_8_tool(name, notes=None):
    """ Adds a new TM 50-8 tool.

    Parameters
    ----------
    name: str
        Name of the new TM 50-8 tool.
    notes: str
        Some notes about new TM 50-8 tool.

    Returns
    -------
    int
        ID of the newly created TM 50-8 tool.

    Raises
    ------
    ValueError
        If the TM 50-8 tool name is missing or empty.
    LabadminDBDuplicateError
        If a TM 50-8 tool of the same name already exists.
    """
    return _add_object('tm50_8_tool', name, notes)


def get_tm50_8_tools():
    """ Lists all existing TM 50-8 tools.

    Returns
    -------
    List of dicts per row: [{tm50_8_tool_id:int, name:str, notes:str}]
    """
    return _get_objects('tm50_8_tool')


def add_water_lot(name, notes=None):
    """ Adds a new water lot.

    Parameters
    ----------
    name: str
        Name of the new water lot.
    notes: str
        Some notes about new water lot.

    Returns
    -------
    int
        ID of the newly created water lot.

    Raises
    ------
    ValueError
        If the water lot name is missing or empty.
    LabadminDBDuplicateError
        If a water lot of the same name already exists.
    """
    return _add_object('water_lot', name, notes)


def get_water_lots():
    """ Lists all existing water lots.

    Returns
    -------
    List of dicts per row: [{water_lot_id:int, name:str, notes:str}]
    """
    return _get_objects('water_lot')


def extract_dna_from_sample_plate(name, email, sample_plate_id,
                                  extraction_robot_id, extraction_tool_id,
                                  extraction_kit_lot_id, notes=None,
                                  created_on=None):
    """ Creates a new DNA plate for a given sample plate.

    Parameters
    ----------
    name: str
        The name for the new DNA plate.
    email: str
        The email from the user that creates the new DNA plate.
    sample_plate_id: int
        ID of the sample plate from which the DNA plate shall be extracted.
    extraction_robot_id: int
        ID of the used extraction robot.
    extraction_tool_id: int
        ID of the used extraction tool.
    extraction_kit_lot_id: int
        ID of the used extraction kit lot.
    notes: str
        Arbitrary notes. Optional.
    created_on: str
        A time stamp for the creation of the DNA plate.

    Returns
    -------
    int
        ID of the newly created DNA plate.

    Raises
    ------
    ValueError
        If the name for the new DNA plate is missing or empty.
    LabadminDBUnknownIDError
        If no user with the given email exists.
    LabadminDBDuplicateError
        If the given object IDs do not exist: sample_plate_id,
        extraction_robot_id, extraction_tool_id or extraction_kit_lot_id.
    """
    # ensure that the object name is not empty
    if not name or len(name) <= 0:
        raise ValueError('The dna_plate name cannot be empty.')

    with TRN:
        # check if an object with the same name already exists
        sql = """SELECT name FROM pm.dna_plate WHERE name = %s"""
        TRN.add(sql, [name])
        if len(TRN.execute_fetchindex()) > 0:
            raise LabadminDBDuplicateError('pm.dna_plate',
                                           'name: %s' % name)

        # check that email is an existing user in the system
        sql = """SELECT email FROM ag.labadmin_users WHERE email = %s"""
        TRN.add(sql, [email])
        if len(TRN.execute_fetchindex()) == 0:
            raise LabadminDBUnknownIDError(email, 'ag.labadmin_users')

        # check that sample_plate with this ID already exists
        sql = """SELECT sample_plate_id FROM pm.sample_plate
                 WHERE sample_plate_id = %s"""
        TRN.add(sql, [sample_plate_id])
        if len(TRN.execute_fetchindex()) == 0:
            raise LabadminDBUnknownIDError(sample_plate_id, 'pm.sample_plate')

        # check that extraction_robot with this ID already exists
        sql = """SELECT extraction_robot_id FROM pm.extraction_robot
                 WHERE extraction_robot_id = %s"""
        TRN.add(sql, [extraction_robot_id])
        if len(TRN.execute_fetchindex()) == 0:
            raise LabadminDBUnknownIDError(extraction_robot_id,
                                           'pm.extraction_robot')

        # check that extraction_tool with this ID already exists
        sql = """SELECT extraction_tool_id FROM pm.extraction_tool
                 WHERE extraction_tool_id = %s"""
        TRN.add(sql, [extraction_tool_id])
        if len(TRN.execute_fetchindex()) == 0:
            raise LabadminDBUnknownIDError(extraction_tool_id,
                                           'pm.extraction_tool')

        # check that extraction_kit with this ID already exists
        sql = """SELECT extraction_kit_lot_id FROM pm.extraction_kit_lot
                 WHERE extraction_kit_lot_id = %s"""
        TRN.add(sql, [extraction_kit_lot_id])
        if len(TRN.execute_fetchindex()) == 0:
            raise LabadminDBUnknownIDError(extraction_kit_lot_id,
                                           'pm.extraction_kit_lot')

        # add the new object into the DB
        sql = """INSERT INTO pm.dna_plate (name,
                                           email,
                                           created_on,
                                           sample_plate_id,
                                           extraction_robot_id,
                                           extraction_kit_lot_id,
                                           extraction_tool_id,
                                           notes)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                 RETURNING dna_plate_id"""
        TRN.add(sql, [name, email, created_on, sample_plate_id,
                      extraction_robot_id, extraction_kit_lot_id,
                      extraction_tool_id, notes])
        return int(TRN.execute_fetchindex()[0][0])


def remove_dna_plate(dna_plate_id):
    """ Removes a DNA plate, if no library plate uses it.

    Parameters
    ----------
    dna_plate_id: int
        ID of the DNA plate to be deleted.

    Raises
    ------
    LabadminDBUnknownIDError
        If no DNA plate with the given dna_plate_id exists.
    LabadminDBArtifactDeletionError
        If DNA plate cannot be deleted, because existing library plates
        are using it.
    """

    with TRN:
        # check that DNA plate exists
        sql = """SELECT dna_plate_id FROM pm.dna_plate
                 WHERE dna_plate_id = %s"""
        TRN.add(sql, [dna_plate_id])
        if len(TRN.execute_fetchindex()) == 0:
            raise LabadminDBUnknownIDError(dna_plate_id, 'pm.dna_plate')

        # check that DNA plate is not used by any library plate
        sql = """SELECT library_plate_id FROM pm.library_plate
                 WHERE dna_plate_id = %s"""
        TRN.add(sql, [dna_plate_id])
        if len(TRN.execute_fetchindex()) > 0:
            raise LabadminDBArtifactDeletionError(dna_plate_id,
                                                  ('since existing library '
                                                   'plates are using it.'))

        sql = """DELETE FROM pm.dna_plate WHERE dna_plate_id = %s"""
        TRN.add(sql, [dna_plate_id])
        TRN.execute()


def get_dna_plate(dna_plate_id):
    """ Retrieves data of a DNA plate.

    Parameters
    ----------
    dna_plate_id: int
        The ID of an existing DNA plate.

    Returns
    -------
    A dict for a DNA plate with the following keys:
        'sample_plate_id': int,
        'name': str,
        'extraction_kit_lot_id': int,
        'extraction_robot_id': int,
        'notes': str,
        'dna_plate_id': int,
        'created_on': datetime,
        'extraction_tool_id': int,
        'email': str

    Raises
    ------
    LabadminDBUnknownIDError
        If the given dna_plate_id does not exist.
    """

    with TRN:
        # check that DNA plate exists
        sql = """SELECT dna_plate_id FROM pm.dna_plate
                 WHERE dna_plate_id = %s"""
        TRN.add(sql, [dna_plate_id])
        if len(TRN.execute_fetchindex()) == 0:
            raise LabadminDBUnknownIDError(dna_plate_id, 'pm.dna_plate')

        # retrieve entry and return it
        sql = """SELECT * FROM pm.dna_plate WHERE dna_plate_id = %s"""
        TRN.add(sql, [dna_plate_id])
        return [dict(x) for x in TRN.execute_fetchindex()][0]


def update_dna_plate(dna_plate_id, name, email, sample_plate_id,
                     extraction_robot_id, extraction_kit_lot_id,
                     extraction_tool_id, notes, created_on):
    """ Updates information of a DNA plate.

    Raises
    ------
    LabadminDBUnknownIDError
        If the given dna_plate_id does not exist.
    ValueError
        If the name of the DNA plate is missing or empty.
    LabadminDBDuplicateError
        If the name already exists in the data base for another DNA plate.
    """

    # ensures that a user with email exists
    _check_user(email)

    with TRN:
        # check that DNA plate exists
        sql = """SELECT dna_plate_id FROM pm.dna_plate
                 WHERE dna_plate_id = %s"""
        TRN.add(sql, [dna_plate_id])
        if len(TRN.execute_fetchindex()) == 0:
            raise LabadminDBUnknownIDError(dna_plate_id, 'pm.dna_plate')

        # ensure that the object name is not empty
        if not name or len(name) <= 0:
            raise ValueError('The DNA plate name cannot be empty.')

        with TRN:
            # check if an object with the same name already exists, when
            # changing the name.
            sql = """SELECT name FROM pm.dna_plate
                     WHERE name = %s AND dna_plate_id != %s"""
            TRN.add(sql, [name, dna_plate_id])
            if len(TRN.execute_fetchindex()) > 0:
                raise LabadminDBDuplicateError('dna_plate', 'name: %s' % name)

            # check that a sample plate with the given ID exists
            sql = """SELECT sample_plate_id FROM pm.sample_plate
                     WHERE sample_plate_id = %s"""
            TRN.add(sql, [sample_plate_id])
            if len(TRN.execute_fetchindex()) == 0:
                raise LabadminDBUnknownIDError(sample_plate_id,
                                               'pm.sample_plate')

            # check that an extraction robot with the given ID exists
            sql = """SELECT extraction_robot_id FROM pm.extraction_robot
                     WHERE extraction_robot_id = %s"""
            TRN.add(sql, [extraction_robot_id])
            if len(TRN.execute_fetchindex()) == 0:
                raise LabadminDBUnknownIDError(extraction_robot_id,
                                               'pm.extraction_robot')

            # check that an extraction kit lot with the given ID exists
            sql = """SELECT extraction_kit_lot_id FROM pm.extraction_kit_lot
                     WHERE extraction_kit_lot_id = %s"""
            TRN.add(sql, [extraction_kit_lot_id])
            if len(TRN.execute_fetchindex()) == 0:
                raise LabadminDBUnknownIDError(extraction_kit_lot_id,
                                               'pm.extraction_kit_lot')

            # check that an extraction tool with the given ID exists
            sql = """SELECT extraction_tool_id FROM pm.extraction_tool
                     WHERE extraction_tool_id = %s"""
            TRN.add(sql, [extraction_tool_id])
            if len(TRN.execute_fetchindex()) == 0:
                raise LabadminDBUnknownIDError(extraction_tool_id,
                                               'pm.extraction_tool')

            # actually updating the DB
            sql = """UPDATE pm.dna_plate
                     SET name = %s, email = %s, created_on = %s,
                         sample_plate_id = %s, extraction_robot_id = %s,
                         extraction_kit_lot_id = %s, extraction_tool_id = %s,
                         notes = %s
                     WHERE dna_plate_id = %s"""
            TRN.add(sql, [name, email, created_on, sample_plate_id,
                          extraction_robot_id, extraction_kit_lot_id,
                          extraction_tool_id, notes, dna_plate_id])
            TRN.execute()
