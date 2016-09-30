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

    def setUp(self):
        db.add_processing_robot = add_processing_robot
        db.add_tm300_8_tool = add_tm300_8_tool
        db.add_tm50_8_tool = add_tm50_8_tool
        db.add_water_lot = add_water_lot
        db.add_master_mix_lot = add_master_mix_lot

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

if __name__ == "__main__":
    main()
