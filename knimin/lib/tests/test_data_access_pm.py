from unittest import TestCase, main
from datetime import datetime

from knimin import db
from knimin.lib.data_access_pm import *
from knimin.lib.data_access_pm import _add_object, _check_user, _get_objects
from knimin.lib.sql_connection import TRN
from knimin.lib.exceptions import *


class TestDataAccessPM(TestCase):
    def tearDown(self):
        # _add_object
        with TRN:
            sql = """DELETE FROM pm.master_mix_lot
                     WHERE name = %s"""
            TRN.add(sql, ['ut_mml1'])
            TRN.execute()

        # add_processing_robot
        with TRN:
            sql = """DELETE FROM pm.processing_robot
                     WHERE name = %s"""
            TRN.add(sql, ['ut_robi1'])
            TRN.add(sql, ['ut_robi2'])
            TRN.execute()

        # test_add_tm300_8_tool
        with TRN:
            sql = """DELETE FROM pm.tm300_8_tool
                     WHERE name = %s"""
            TRN.add(sql, ['ut_007ABC'])
            TRN.execute()

        # test_add_tm50_8_tool
        with TRN:
            sql = """DELETE FROM pm.tm50_8_tool
                     WHERE name = %s"""
            TRN.add(sql, ['ut_007ABC'])
            TRN.execute()

        # test_add_water_lot
        with TRN:
            sql = """DELETE FROM pm.water_lot
                     WHERE name = %s"""
            TRN.add(sql, ['ut_007ABC'])
            TRN.execute()

        # test_extract_dna_from_sample_plate
        with TRN:
            sql = """DELETE FROM pm.dna_plate
                     WHERE name = %s"""
            TRN.add(sql, ['ut_dna11'])
            TRN.execute()

        # test_remove_dna_plate
        with TRN:
            sql = """DELETE FROM pm.library_plate WHERE name = %s"""
            TRN.add(sql, ['ut_libplate'])
            sql = """DELETE FROM pm.dna_plate WHERE name = %s"""
            TRN.add(sql, ['ut_dnaR'])
            sql = """DELETE FROM pm.protocol WHERE name = %s"""
            TRN.add(sql, ['ut_protocol1'])
            TRN.execute()

        # test_get_dna_plate
        with TRN:
            sql = """DELETE FROM pm.dna_plate WHERE name = %s"""
            TRN.add(sql, ['ut_dnaR'])
            TRN.execute()

        # test_update_dna_plate
        with TRN:
            sql = """DELETE FROM pm.dna_plate WHERE name = %s"""
            TRN.add(sql, ['ut_dnaR'])
            TRN.add(sql, ['ut_dnaS'])
            TRN.add(sql, ['ut_dnaX'])
            TRN.execute()

    def setUp(self):
        db.add_processing_robot = add_processing_robot
        db.get_processing_robots = get_processing_robots
        db.add_tm300_8_tool = add_tm300_8_tool
        db.get_tm300_8_tools = get_tm300_8_tools
        db.add_tm50_8_tool = add_tm50_8_tool
        db.get_tm50_8_tools = get_tm50_8_tools
        db.add_water_lot = add_water_lot
        db.get_water_lots = get_water_lots
        db.add_master_mix_lot = add_master_mix_lot
        db.get_master_mix_lots = get_master_mix_lots
        db.extract_dna_from_sample_plate = extract_dna_from_sample_plate
        db.remove_dna_plate = remove_dna_plate
        db.get_dna_plate = get_dna_plate
        db.update_dna_plate = update_dna_plate

    def test__add_object(self):
        # test check for table existence
        self.assertRaisesRegexp(LabadminDBError,
                                'does not exist in data base.',
                                _add_object,
                                'p1rocessing_robot',
                                'ut_robi2',
                                'some notes')

        # test check for table design
        self.assertRaisesRegexp(LabadminDBError,
                                'does not have the expected column',
                                _add_object,
                                'dna_plate',
                                'ut_robi2',
                                'some notes')
        # regular addition
        _add_object('processing_robot', 'ut_robi2', 'some notes')
        # check that duplicates cannot be added
        self.assertRaisesRegexp(LabadminDBDuplicateError,
                                'already exists.',
                                _add_object,
                                'processing_robot',
                                'ut_robi2',
                                'some notes')

    def test__check_user(self):
        self.assertRaisesRegexp(LabadminDBUnknownIDError,
                                "The object with ID '%s' does not" % 'ut_dna1',
                                _check_user,
                                'ut_dna1')
        self.assertTrue(_check_user('test'))

    def test__get_objects(self):
        # test check for table existence
        self.assertRaisesRegexp(LabadminDBError,
                                'does not exist in data base.',
                                _get_objects,
                                'p1rocessing_robot')

        # test check for table design
        self.assertRaisesRegexp(LabadminDBError,
                                'does not have the expected column',
                                _get_objects,
                                'dna_plate')
        # regular get
        self.assertIn([1L, 'HOWE_KF1', None], _get_objects('extraction_robot'))

    def test_add_master_mix_lot(self):
        self.assertRaisesRegexp(ValueError,
                                'The master_mix_lot name cannot be empty.',
                                db.add_master_mix_lot,
                                None,
                                'some notes')
        self.assertRaisesRegexp(ValueError,
                                'The master_mix_lot name cannot be empty.',
                                db.add_master_mix_lot,
                                '',
                                'some notes')
        # add a master mix lot
        name = 'ut_mml1'
        db.add_master_mix_lot(name, 'some notes')
        # indirect test if the upper statement could add the robot, since it
        # cannot be added twice with the same name.
        self.assertRaisesRegexp(LabadminDBDuplicateError,
                                'already exists.',
                                db.add_master_mix_lot,
                                name,
                                'some notes')

    def test_get_master_mix_lots(self):
        self.assertEqual([[1L, '14459', None]], db.get_master_mix_lots())

    def test_add_processing_robot(self):
        self.assertRaisesRegexp(ValueError,
                                'The processing_robot name cannot be empty.',
                                db.add_processing_robot,
                                None,
                                'some notes')
        self.assertRaisesRegexp(ValueError,
                                'The processing_robot name cannot be empty.',
                                db.add_processing_robot,
                                '',
                                'some notes')
        # add a processing robot
        name = 'ut_robi1'
        db.add_processing_robot(name, 'some notes')
        # indirect test if the upper statement could add the robot, since it
        # cannot be added twice with the same name.
        self.assertRaisesRegexp(LabadminDBDuplicateError,
                                'already exists.',
                                db.add_processing_robot,
                                name,
                                'some notes')

    def test_get_processing_robots(self):
        self.assertEqual([[1L, 'ROBE', None],
                          [2L, 'RIKE', None],
                          [3L, 'JERE', None],
                          [4L, 'CARMEN', None]], db.get_processing_robots())

    def test_add_tm300_8_tool(self):
        self.assertRaisesRegexp(ValueError,
                                'The tm300_8_tool name cannot be empty.',
                                db.add_tm300_8_tool,
                                None,
                                'some notes')
        self.assertRaisesRegexp(ValueError,
                                'The tm300_8_tool name cannot be empty.',
                                db.add_tm300_8_tool,
                                '',
                                'some notes')
        # add a TM 300-8 tool
        name = 'ut_007ABC'
        db.add_tm300_8_tool(name, 'some notes')
        # indirect test if the upper statement could add the tool, since it
        # cannot be added twice with the same name.
        self.assertRaisesRegexp(LabadminDBDuplicateError,
                                'already exists.',
                                db.add_tm300_8_tool,
                                name,
                                'some notes')

    def test_get_tm300_8_tools(self):
        self.assertEqual([[1L, '208484Z', None],
                          [2L, '311318B', None],
                          [3L, '109375A', None],
                          [4L, '3076189', None]], db.get_tm300_8_tools())

    def test_add_tm50_8_tool(self):
        self.assertRaisesRegexp(ValueError,
                                'The tm50_8_tool name cannot be empty.',
                                db.add_tm50_8_tool,
                                None,
                                'some notes')
        self.assertRaisesRegexp(ValueError,
                                'The tm50_8_tool name cannot be empty.',
                                db.add_tm50_8_tool,
                                '',
                                'some notes')
        # add a TM 50-8 tool
        name = 'ut_007ABC'
        db.add_tm50_8_tool(name, 'some notes')
        # indirect test if the upper statement could add the tool, since it
        # cannot be added twice with the same name.
        self.assertRaisesRegexp(LabadminDBDuplicateError,
                                'already exists.',
                                db.add_tm50_8_tool,
                                name,
                                'some notes')

    def test_get_tm50_8_tools(self):
        self.assertEqual([[1L, '108364Z', None],
                          [2L, '311426B', None],
                          [3L, '311441B', None],
                          [4L, '409172Z', None]], db.get_tm50_8_tools())

    def test_add_water_lot(self):
        self.assertRaisesRegexp(ValueError,
                                'The water_lot name cannot be empty.',
                                db.add_water_lot,
                                None,
                                'some notes')
        self.assertRaisesRegexp(ValueError,
                                'The water_lot name cannot be empty.',
                                db.add_water_lot,
                                '',
                                'some notes')
        # add a water_lot
        name = 'ut_007ABC'
        db.add_water_lot(name, 'some notes')
        # indirect test if the upper statement could add the tool, since it
        # cannot be added twice with the same name.
        self.assertRaisesRegexp(LabadminDBDuplicateError,
                                'already exists.',
                                db.add_water_lot,
                                name,
                                'some notes')

    def test_get_water_lots(self):
        self.assertEqual([[1L, 'RNBD9959', None]], db.get_water_lots())

    def test_extract_dna_from_sample_plate(self):
        self.assertRaisesRegexp(ValueError,
                                'The dna_plate name cannot be empty.',
                                db.extract_dna_from_sample_plate,
                                None,
                                'test', '1', '', '', '')

        self.assertRaisesRegexp(LabadminDBUnknownIDError,
                                "The object with ID '%s' does not" % 'noUser',
                                db.extract_dna_from_sample_plate,
                                'dna1', 'noUser', '', '', '', '')

        self.assertRaisesRegexp(LabadminDBUnknownIDError,
                                "The object with ID '%i' does not" % 99999,
                                db.extract_dna_from_sample_plate,
                                'dna1', 'test', 99999, '', '', '')

        self.assertRaisesRegexp(LabadminDBUnknownIDError,
                                "The object with ID '%i' does not" % 99999,
                                db.extract_dna_from_sample_plate,
                                'dna1', 'test', 1, 99999, '', '')

        self.assertRaisesRegexp(LabadminDBUnknownIDError,
                                "The object with ID '%i' does not" % 99999,
                                db.extract_dna_from_sample_plate,
                                'dna1', 'test', 1, 1, 99999, '')

        self.assertRaisesRegexp(LabadminDBUnknownIDError,
                                "The object with ID '%i' does not" % 99999,
                                db.extract_dna_from_sample_plate,
                                'dna1', 'test', 1, 1, 1, 99999)

        # add a new dna_plate
        db.extract_dna_from_sample_plate('ut_dna11', 'test', 1, 1, 1, 1,
                                         'my first dna plate', 'Jan-08-1999')

        self.assertRaisesRegexp(LabadminDBDuplicateError,
                                "already exists.",
                                db.extract_dna_from_sample_plate,
                                'ut_dna11', 'test', 1, 1, 1, 1,
                                'my first dna plate', 'Jan-08-1999')

    def test_remove_dna_plate(self):
        self.assertRaisesRegexp(LabadminDBUnknownIDError,
                                "The object with ID '%i' does not" % 99999,
                                db.remove_dna_plate,
                                99999)

        # add a DNA plate
        dna_plate_id = db.extract_dna_from_sample_plate('ut_dnaR', 'test', 1,
                                                        1, 1, 1,
                                                        'my first dna plate',
                                                        'Jan-08-1999')
        with TRN:
            # make sure a protocol exists
            sql = """INSERT INTO pm.protocol (name) VALUES (%s)
                     RETURNING protocol_id"""
            TRN.add(sql, ['ut_protocol1'])
            protocol_id = TRN.execute_fetchindex()[0][0]

            # add a new libarary plate, that uses the DNA plate
            sql = """INSERT INTO pm.library_plate
                     (name, email, dna_plate_id, protocol_id)
                     VALUES (%s, %s, %s,%s) RETURNING library_plate_id"""
            TRN.add(sql, ['ut_libplate', 'test', dna_plate_id, protocol_id])
            library_plate_id = TRN.execute_fetchindex()[0][0]

        # check that DNA plate cannot be deleted if used by any library plate
        self.assertRaisesRegexp(LabadminDBArtifactDeletionError,
                                "Cannot delete artifact",
                                db.remove_dna_plate,
                                dna_plate_id)

    def test_get_dna_plate(self):
        self.assertRaisesRegexp(LabadminDBUnknownIDError,
                                "The object with ID '%i' does not" % 99999,
                                db.get_dna_plate,
                                99999)

        # add a DNA plate
        dna_plate_id = db.extract_dna_from_sample_plate('ut_dnaR', 'test', 1,
                                                        1, 1, 1,
                                                        'my first dna plate',
                                                        'Jan-08-1999')

        self.assertEqual(db.get_dna_plate(dna_plate_id),
                         (dna_plate_id, 'ut_dnaR', 'test',
                          datetime(1999, 1, 8,), 1, 1, 1, 1,
                          'my first dna plate'))

    def test_update_dna_plate(self):
        # add tow DNA plates
        dna_plate_id1 = db.extract_dna_from_sample_plate('ut_dnaR', 'test', 1,
                                                         1, 1, 1,
                                                         'my first dna plate',
                                                         'Jan-08-1999')
        dna_plate_id2 = db.extract_dna_from_sample_plate('ut_dnaS', 'test', 1,
                                                         1, 1, 1,
                                                         'my second dna plate',
                                                         'Jan-08-1999')

        self.assertRaisesRegexp(LabadminDBUnknownIDError,
                                "The object with ID '%i' does not" % 99999,
                                db.update_dna_plate,
                                99999, 'ut_dnaR', 'test', 1, 1, 1, 1,
                                'my first dna plate', 'Jan-08-1999')

        self.assertRaisesRegexp(ValueError,
                                "name cannot be empty",
                                db.update_dna_plate,
                                dna_plate_id1, '', 'test', 1, 1, 1, 1,
                                'my first dna plate', 'Jan-08-1999')

        self.assertRaisesRegexp(LabadminDBDuplicateError,
                                "already exists.",
                                db.update_dna_plate,
                                dna_plate_id1, 'ut_dnaS', 'test', 1, 1, 1, 1,
                                'my first dna plate', 'Jan-08-1999')

        self.assertRaisesRegexp(LabadminDBUnknownIDError,
                                "The object with ID '%s' does not" % 'noUser',
                                db.update_dna_plate,
                                dna_plate_id1, 'ut_dnaS', 'noUser', 1, 1, 1, 1,
                                'my first dna plate', 'Jan-08-1999')

        self.assertRaisesRegexp(LabadminDBUnknownIDError,
                                "The object with ID '%s' does not" % "99999",
                                db.update_dna_plate,
                                dna_plate_id1, 'ut_dnaX', 'test', 99999, 1, 1,
                                1, 'my first dna plate', 'Jan-08-1999')

        self.assertRaisesRegexp(LabadminDBUnknownIDError,
                                "The object with ID '%s' does not" % "99999",
                                db.update_dna_plate,
                                dna_plate_id1, 'ut_dnaX', 'test', 1, 99999, 1,
                                1, 'my first dna plate', 'Jan-08-1999')

        self.assertRaisesRegexp(LabadminDBUnknownIDError,
                                "The object with ID '%s' does not" % "99999",
                                db.update_dna_plate,
                                dna_plate_id1, 'ut_dnaX', 'test', 1, 1, 99999,
                                1, 'my first dna plate', 'Jan-08-1999')

        self.assertRaisesRegexp(LabadminDBUnknownIDError,
                                "The object with ID '%s' does not" % "99999",
                                db.update_dna_plate,
                                dna_plate_id1, 'ut_dnaX', 'test', 1, 1, 1,
                                99999, 'my first dna plate', 'Jan-08-1999')

        db.update_dna_plate(dna_plate_id1, 'ut_dnaX', 'test', 1, 1, 1, 1,
                            'my third dna plate', 'Feb-08-1999')
        self.assertEqual(db.get_dna_plate(dna_plate_id1),
                         (dna_plate_id1, 'ut_dnaX', 'test',
                          datetime(1999, 2, 8,), 1, 1, 1, 1,
                          'my third dna plate'))


if __name__ == "__main__":
    main()
