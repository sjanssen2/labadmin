from unittest import TestCase, main

from knimin import db
from knimin.lib.data_access_pm import *
from knimin.lib.data_access_pm import _add_object
from knimin.lib.sql_connection import TRN
from knimin.lib.exceptions import *


class TestDataAccessPM(TestCase):
    def tearDown(self):
        # _add_object
        with TRN:
            sql = """DELETE FROM pm.processing_robot
                     WHERE name = %s"""
            TRN.add(sql, ['robi2'])
            TRN.execute()

        # add_processing_robot
        with TRN:
            sql = """DELETE FROM pm.processing_robot
                     WHERE name = %s"""
            TRN.add(sql, ['robi1'])
            TRN.execute()

        # test_add_tm300_8_tool
        with TRN:
            sql = """DELETE FROM pm.tm300_8_tool
                     WHERE name = %s"""
            TRN.add(sql, ['007ABC'])
            TRN.execute()

        # test_add_tm50_8_tool
        with TRN:
            sql = """DELETE FROM pm.tm50_8_tool
                     WHERE name = %s"""
            TRN.add(sql, ['007ABC'])
            TRN.execute()

        # test_add_water_lot
        with TRN:
            sql = """DELETE FROM pm.water_lot
                     WHERE name = %s"""
            TRN.add(sql, ['007ABC'])
            TRN.execute()

        # test_extract_dna_from_sample_plate
        with TRN:
            sql = """DELETE FROM pm.dna_plate
                     WHERE name = %s"""
            TRN.add(sql, ['dna11'])
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

    def setUp(self):
        db.add_processing_robot = add_processing_robot
        db.add_tm300_8_tool = add_tm300_8_tool
        db.add_tm50_8_tool = add_tm50_8_tool
        db.add_water_lot = add_water_lot
        db.add_master_mix_lot = add_master_mix_lot
        db.extract_dna_from_sample_plate = extract_dna_from_sample_plate
        db.remove_dna_plate = remove_dna_plate

    def test__add_object(self):
        # test check for table existence
        self.assertRaisesRegexp(LabadminDBError,
                                'does not exist in data base.',
                                _add_object,
                                'p1rocessing_robot',
                                'robi2',
                                'some notes')

        # test check for table design
        self.assertRaisesRegexp(LabadminDBError,
                                'does not have the expected column',
                                _add_object,
                                'dna_plate',
                                'robi2',
                                'some notes')
        # regular addition
        _add_object('processing_robot', 'robi2', 'some notes')
        # check that duplicates cannot be added
        self.assertRaisesRegexp(LabadminDBDuplicateError,
                                'already exists.',
                                _add_object,
                                'processing_robot',
                                'robi2',
                                'some notes')

    def test_add_processing_robot(self):
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
        name = 'robi1'
        db.add_master_mix_lot(name, 'some notes')
        # indirect test if the upper statement could add the robot, since it
        # cannot be added twice with the same name.
        self.assertRaisesRegexp(LabadminDBDuplicateError,
                                'already exists.',
                                db.add_master_mix_lot,
                                name,
                                'some notes')

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
        name = 'robi1'
        db.add_processing_robot(name, 'some notes')
        # indirect test if the upper statement could add the robot, since it
        # cannot be added twice with the same name.
        self.assertRaisesRegexp(LabadminDBDuplicateError,
                                'already exists.',
                                db.add_processing_robot,
                                name,
                                'some notes')

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
        name = '007ABC'
        db.add_tm300_8_tool(name, 'some notes')
        # indirect test if the upper statement could add the tool, since it
        # cannot be added twice with the same name.
        self.assertRaisesRegexp(LabadminDBDuplicateError,
                                'already exists.',
                                db.add_tm300_8_tool,
                                name,
                                'some notes')

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
        name = '007ABC'
        db.add_tm50_8_tool(name, 'some notes')
        # indirect test if the upper statement could add the tool, since it
        # cannot be added twice with the same name.
        self.assertRaisesRegexp(LabadminDBDuplicateError,
                                'already exists.',
                                db.add_tm50_8_tool,
                                name,
                                'some notes')

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
        name = '007ABC'
        db.add_water_lot(name, 'some notes')
        # indirect test if the upper statement could add the tool, since it
        # cannot be added twice with the same name.
        self.assertRaisesRegexp(LabadminDBDuplicateError,
                                'already exists.',
                                db.add_water_lot,
                                name,
                                'some notes')

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
        db.extract_dna_from_sample_plate('dna11', 'test', 1, 1, 1, 1,
                                         'my first dna plate', 'Jan-08-1999')

        self.assertRaisesRegexp(LabadminDBDuplicateError,
                                "already exists.",
                                db.extract_dna_from_sample_plate,
                                'dna11', 'test', 1, 1, 1, 1,
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


if __name__ == "__main__":
    main()
