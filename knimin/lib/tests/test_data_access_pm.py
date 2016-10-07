from unittest import TestCase, main
from datetime import datetime

from knimin import db
from knimin.lib.data_access_pm import *
from knimin.lib.data_access_pm import _add_object, _check_user, _get_objects, \
                                      _exists, _update_object, \
                                      _check_grid_format
from knimin.lib.sql_connection import TRN
from knimin.lib.exceptions import *


class TestDataAccessPM(TestCase):
    def _remove_db_id(self, results):
        """ Database indices are not stable, thus for assertEqual I need to
            remove those IDs"""
        if len(results) > 0:
            key_id = [k for k in results[0].keys() if k.endswith('_id')][0]
            for row in results:
                del row[key_id]
        return results

    def _add_sample_plate(self):
        with TRN:
            sql = """INSERT INTO pm.sample_plate (name, email, plate_type_id)
                     VALUES (%s, %s, %s) RETURNING sample_plate_id"""
            TRN.add(sql, ['ut_sampleplate1', 'test', 1])
            return TRN.execute_fetchindex()[0][0]

    def tearDown(self):
        with TRN:
            sql = """DELETE FROM pm.processing_robot WHERE name = %s"""
            TRN.add(sql, ['ut_robi2'])
            TRN.add(sql, ['ut_robi1'])
            sql = """DELETE FROM pm.master_mix_lot WHERE name = %s"""
            TRN.add(sql, ['ut_mml1'])
            sql = """DELETE FROM pm.tm300_8_tool WHERE name = %s"""
            TRN.add(sql, ['ut_007ABC'])
            sql = """DELETE FROM pm.tm50_8_tool WHERE name = %s"""
            TRN.add(sql, ['ut_007ABC'])
            sql = """DELETE FROM pm.water_lot WHERE name = %s"""
            TRN.add(sql, ['ut_007ABC'])
            sql = """DELETE FROM pm.library_plate WHERE name = %s"""
            TRN.add(sql, ['ut_libplate'])
            sql = """DELETE FROM pm.protocol WHERE name = %s"""
            TRN.add(sql, ['ut_protocol1'])
            sql = """DELETE FROM pm.dna_plate WHERE name = %s"""
            TRN.add(sql, ['ut_dna11'])
            TRN.add(sql, ['ut_dnaR'])
            TRN.add(sql, ['ut_dnaS'])
            TRN.add(sql, ['ut_dnaX'])
            sql = """DELETE FROM pm.sample_plate WHERE name = %s"""
            TRN.add(sql, ['ut_sampleplate1'])
            sql = """UPDATE pm.processing_robot set notes = %s
                     WHERE notes = 'ut_newNotes'"""
            TRN.add(sql, [None])
            sql = """UPDATE pm.master_mix_lot set notes = %s
                     WHERE notes = 'ut_newNotes'"""
            TRN.add(sql, [None])
            sql = """UPDATE pm.tm300_8_tool set notes = %s
                     WHERE notes = 'ut_newNotes'"""
            TRN.add(sql, [None])
            sql = """UPDATE pm.tm50_8_tool set notes = %s
                     WHERE notes = 'ut_newNotes'"""
            TRN.add(sql, [None])
            sql = """UPDATE pm.water_lot set notes = %s
                     WHERE notes = 'ut_newNotes'"""
            TRN.add(sql, [None])
            sql = """UPDATE pm.plate_type set notes = %s
                     WHERE notes = 'ut_newNotes'"""
            TRN.add(sql, [None])
            TRN.add(sql, ['ut_newNotes_UPDATED'])
            sql = """DELETE FROM pm.plate_type WHERE name = %s"""
            TRN.add(sql, ['ut_pt1'])
            TRN.execute()

    def setUp(self):
        db.add_processing_robot = add_processing_robot
        db.get_processing_robots = get_processing_robots
        db.update_processing_robot = update_processing_robot
        db.add_tm300_8_tool = add_tm300_8_tool
        db.get_tm300_8_tools = get_tm300_8_tools
        db.update_tm300_8_tool = update_tm300_8_tool
        db.add_tm50_8_tool = add_tm50_8_tool
        db.get_tm50_8_tools = get_tm50_8_tools
        db.update_tm50_8_tool = update_tm50_8_tool
        db.add_water_lot = add_water_lot
        db.get_water_lots = get_water_lots
        db.update_water_lot = update_water_lot
        db.add_master_mix_lot = add_master_mix_lot
        db.get_master_mix_lots = get_master_mix_lots
        db.update_master_mix_lot = update_master_mix_lot
        db.extract_dna_from_sample_plate = extract_dna_from_sample_plate
        db.remove_dna_plate = remove_dna_plate
        db.get_dna_plate = get_dna_plate
        db.update_dna_plate = update_dna_plate
        db.add_plate_type = add_plate_type
        db.get_plate_types = get_plate_types
        db.update_plate_type = update_plate_type

    def test__check_table_layout(self):
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

    def test__exists(self):
        # test check for table existence
        self.assertRaisesRegexp(LabadminDBError,
                                'does not exist in data base.',
                                _exists,
                                'p1rocessing_robot',
                                'col_no',
                                'newValue')

        # test check that column exists
        self.assertRaisesRegexp(LabadminDBError,
                                'does not have the given column',
                                _exists,
                                'dna_plate',
                                'col_no',
                                'newValue')

        # check that duplicates cannot be added
        self.assertTrue(_exists('processing_robot', 'name', 'ROBE'))

        # check if True is reported, if new row with name for column field
        # can be added without collisions.
        self.assertFalse(_exists('processing_robot', 'name', 'ut_newR'))

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
                                "The object with ID '%s' does not" % 'ut_nous',
                                _check_user,
                                'ut_nous')
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
        self.assertIn({'notes': None, 'name': 'HOWE_KF1'},
                      self._remove_db_id(_get_objects('extraction_robot')))

    def test__update_object(self):
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

        # test check if entry does not exist
        self.assertRaisesRegexp(LabadminDBUnknownIDError,
                                "The object with ID '%i' does not" % 99999,
                                _update_object,
                                'processing_robot', 99999, 'ut_newNotes')

        id = [r['processing_robot_id'] for r in db.get_processing_robots()
              if r['name'] == 'RIKE'][0]
        _update_object('processing_robot', id, 'ut_newNotes')
        res = self._remove_db_id(db.get_processing_robots())
        self.assertIn({'notes': 'ut_newNotes', 'name': 'RIKE'}, res)

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
        res = self._remove_db_id(db.get_master_mix_lots())
        self.assertIn({'notes': None, 'name': '14459'}, res)

    def test_update_master_mix_lot(self):
        self.assertRaisesRegexp(LabadminDBUnknownIDError,
                                "The object with ID '%i' does not" % 99999,
                                db.update_master_mix_lot, 99999, 'ut_newNotes')

        id = db.get_master_mix_lots()[0]['master_mix_lot_id']
        db.update_master_mix_lot(id, 'ut_newNotes')
        res = self._remove_db_id([x for x in db.get_master_mix_lots()
                                  if x['master_mix_lot_id'] == id])
        self.assertEqual(res[0]['notes'], 'ut_newNotes')

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
        res = self._remove_db_id(db.get_processing_robots())
        self.assertIn({'notes': None, 'name': 'ROBE'}, res)
        self.assertIn({'notes': None, 'name': 'RIKE'}, res)
        self.assertIn({'notes': None, 'name': 'JERE'}, res)
        self.assertIn({'notes': None, 'name': 'CARMEN'}, res)

    def test_update_processing_robot(self):
        self.assertRaisesRegexp(LabadminDBUnknownIDError,
                                "The object with ID '%i' does not" % 99999,
                                db.update_processing_robot, 99999,
                                'ut_newNotes')

        id = db.get_processing_robots()[0]['processing_robot_id']
        db.update_processing_robot(id, 'ut_newNotes')
        res = self._remove_db_id([x for x in db.get_processing_robots()
                                  if x['processing_robot_id'] == id])
        self.assertEqual(res[0]['notes'], 'ut_newNotes')

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
        res = self._remove_db_id(db.get_tm300_8_tools())
        self.assertIn({'notes': None, 'name': '208484Z'}, res)
        self.assertIn({'notes': None, 'name': '311318B'}, res)
        self.assertIn({'notes': None, 'name': '109375A'}, res)
        self.assertIn({'notes': None, 'name': '3076189'}, res)

    def test_update_tm300_8_tool(self):
        self.assertRaisesRegexp(LabadminDBUnknownIDError,
                                "The object with ID '%i' does not" % 99999,
                                db.update_tm300_8_tool, 99999, 'ut_newNotes')

        id = db.get_tm300_8_tools()[0]['tm300_8_tool_id']
        db.update_tm300_8_tool(id, 'ut_newNotes')
        res = self._remove_db_id([x for x in db.get_tm300_8_tools()
                                  if x['tm300_8_tool_id'] == id])
        self.assertEqual(res[0]['notes'], 'ut_newNotes')

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
        res = self._remove_db_id(db.get_tm50_8_tools())
        self.assertIn({'notes': None, 'name': '108364Z'}, res)
        self.assertIn({'notes': None, 'name': '311426B'}, res)
        self.assertIn({'notes': None, 'name': '311441B'}, res)
        self.assertIn({'notes': None, 'name': '409172Z'}, res)

    def test_update_tm50_8_tool(self):
        self.assertRaisesRegexp(LabadminDBUnknownIDError,
                                "The object with ID '%i' does not" % 99999,
                                db.update_tm50_8_tool, 99999, 'ut_newNotes')

        id = db.get_tm50_8_tools()[0]['tm50_8_tool_id']
        db.update_tm50_8_tool(id, 'ut_newNotes')
        res = self._remove_db_id([x for x in db.get_tm50_8_tools()
                                  if x['tm50_8_tool_id'] == id])
        self.assertEqual(res[0]['notes'], 'ut_newNotes')

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
        res = self._remove_db_id(db.get_water_lots())
        self.assertIn({'notes': None, 'name': 'RNBD9959'}, res)

    def test_update_water_lot(self):
        self.assertRaisesRegexp(LabadminDBUnknownIDError,
                                "The object with ID '%i' does not" % 99999,
                                db.update_water_lot, 99999, 'ut_newNotes')

        id = db.get_water_lots()[0]['water_lot_id']
        db.update_water_lot(id, 'ut_newNotes')
        res = self._remove_db_id([x for x in db.get_water_lots()
                                  if x['water_lot_id'] == id])
        self.assertEqual(res[0]['notes'], 'ut_newNotes')

    def test_extract_dna_from_sample_plate(self):
        sample_plate_id = self._add_sample_plate()

        self.assertRaisesRegexp(ValueError,
                                'The dna_plate name cannot be empty.',
                                db.extract_dna_from_sample_plate,
                                None,
                                'test', sample_plate_id, '', '', '')

        self.assertRaisesRegexp(LabadminDBUnknownIDError,
                                "The object with ID '%s' does not" % 'noUser',
                                db.extract_dna_from_sample_plate,
                                'dna1', 'noUser', sample_plate_id, '', '', '')

        self.assertRaisesRegexp(LabadminDBUnknownIDError,
                                "The object with ID '%i' does not" % 99999,
                                db.extract_dna_from_sample_plate,
                                'dna1', 'test', 99999, '', '', '')

        self.assertRaisesRegexp(LabadminDBUnknownIDError,
                                "The object with ID '%i' does not" % 99999,
                                db.extract_dna_from_sample_plate,
                                'dna1', 'test', sample_plate_id, 99999, '', '')

        self.assertRaisesRegexp(LabadminDBUnknownIDError,
                                "The object with ID '%i' does not" % 99999,
                                db.extract_dna_from_sample_plate,
                                'dna1', 'test', sample_plate_id, 1, 99999, '')

        self.assertRaisesRegexp(LabadminDBUnknownIDError,
                                "The object with ID '%i' does not" % 99999,
                                db.extract_dna_from_sample_plate,
                                'dna1', 'test', sample_plate_id, 1, 1, 99999)

        # add a new dna_plate
        db.extract_dna_from_sample_plate('ut_dna11', 'test', sample_plate_id,
                                         1, 1, 1, 'my first dna plate',
                                         'Jan-08-1999')

        self.assertRaisesRegexp(LabadminDBDuplicateError,
                                "already exists.",
                                db.extract_dna_from_sample_plate,
                                'ut_dna11', 'test', sample_plate_id, 1, 1, 1,
                                'my first dna plate', 'Jan-08-1999')

    def test_remove_dna_plate(self):
        self.assertRaisesRegexp(LabadminDBUnknownIDError,
                                "The object with ID '%i' does not" % 99999,
                                db.remove_dna_plate,
                                99999)

        sample_plate_id = self._add_sample_plate()
        dna_plate_id = db.extract_dna_from_sample_plate('ut_dnaR', 'test',
                                                        sample_plate_id, 1, 1,
                                                        1,
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
        sample_plate_id = self._add_sample_plate()

        # add a DNA plate
        dna_plate_id = db.extract_dna_from_sample_plate('ut_dnaR', 'test',
                                                        sample_plate_id, 1, 1,
                                                        1,
                                                        'my first dna plate',
                                                        'Jan-08-1999')
        self.assertEqual({
            'sample_plate_id': sample_plate_id, 'name': 'ut_dnaR',
            'extraction_kit_lot_id': 1L, 'extraction_robot_id': 1L,
            'notes': 'my first dna plate', 'dna_plate_id': dna_plate_id,
            'created_on': datetime(1999, 1, 8, 0, 0),
            'extraction_tool_id': 1L, 'email': 'test'},
            db.get_dna_plate(dna_plate_id))

    def test_update_dna_plate(self):
        sample_plate_id = self._add_sample_plate()

        # add tow DNA plates
        dna_plate_id1 = db.extract_dna_from_sample_plate('ut_dnaR', 'test',
                                                         sample_plate_id,
                                                         1, 1, 1,
                                                         'my first dna plate',
                                                         'Jan-08-1999')
        dna_plate_id2 = db.extract_dna_from_sample_plate('ut_dnaS', 'test',
                                                         sample_plate_id,
                                                         1, 1, 1,
                                                         'my second dna plate',
                                                         'Jan-08-1999')

        self.assertRaisesRegexp(LabadminDBUnknownIDError,
                                "The object with ID '%i' does not" % 99999,
                                db.update_dna_plate, 99999, 'ut_dnaR', 'test',
                                sample_plate_id, 1, 1, 1,
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
                                dna_plate_id1, 'ut_dnaX', 'test',
                                sample_plate_id, 99999, 1,
                                1, 'my first dna plate', 'Jan-08-1999')

        self.assertRaisesRegexp(LabadminDBUnknownIDError,
                                "The object with ID '%s' does not" % "99999",
                                db.update_dna_plate,
                                dna_plate_id1, 'ut_dnaX', 'test',
                                sample_plate_id, 1, 99999,
                                1, 'my first dna plate', 'Jan-08-1999')

        self.assertRaisesRegexp(LabadminDBUnknownIDError,
                                "The object with ID '%s' does not" % "99999",
                                db.update_dna_plate,
                                dna_plate_id1, 'ut_dnaX', 'test',
                                sample_plate_id, 1, 1,
                                99999, 'my first dna plate', 'Jan-08-1999')

        db.update_dna_plate(dna_plate_id1, 'ut_dnaX', 'test', sample_plate_id,
                            1, 1, 1, 'my third dna plate', 'Feb-08-1999')

        self.assertEqual({
            'sample_plate_id': sample_plate_id, 'name': 'ut_dnaX',
            'extraction_kit_lot_id': 1L, 'extraction_robot_id': 1L,
            'notes': 'my third dna plate', 'dna_plate_id': dna_plate_id1,
            'created_on': datetime(1999, 2, 8, 0, 0), 'extraction_tool_id': 1L,
            'email': 'test'}, db.get_dna_plate(dna_plate_id1))

    def test_add_plate_type(self):
        self.assertRaisesRegexp(ValueError,
                                'The plate_type name cannot be empty.',
                                db.add_plate_type,
                                None, 3, 5, 'some notes')
        self.assertRaisesRegexp(ValueError,
                                'The plate_type name cannot be empty.',
                                db.add_plate_type,
                                '', 3, 5, 'some notes')
        self.assertRaisesRegexp(ValueError,
                                ('Number of columns cannot be empty or '
                                 'negative.'),
                                db.add_plate_type,
                                'ut_pt1', None, 5, 'some notes')
        self.assertRaisesRegexp(ValueError,
                                ('Number of columns cannot be empty or '
                                 'negative.'),
                                db.add_plate_type,
                                'ut_pt1', -1, 5, 'some notes')
        self.assertRaisesRegexp(ValueError,
                                'Number of rows cannot be empty or negative.',
                                db.add_plate_type,
                                'ut_pt1', 3, None, 'some notes')
        self.assertRaisesRegexp(ValueError,
                                'Number of rows cannot be empty or negative.',
                                db.add_plate_type,
                                'ut_pt1', 3, -5, 'some notes')
        db.add_plate_type('ut_pt1', 3, 5, 'some notes')
        self.assertRaisesRegexp(LabadminDBDuplicateError,
                                'already exists.',
                                db.add_plate_type,
                                'ut_pt1', 3, 5, 'some notes')

    def test_get_plate_types(self):
        db.add_plate_type('ut_pt1', 3, 5, 'some notes')
        res = self._remove_db_id(db.get_plate_types())
        self.assertIn({'notes': 'some notes', 'name': 'ut_pt1',
                       'rows': 5, 'cols': 3}, res)
        self.assertIn({'notes': 'Standard 96-well plate', 'name': '96-well',
                       'rows': 8, 'cols': 12}, res)

    def test_update_plate_type(self):
        id = db.add_plate_type('ut_pt1', 3, 5, 'ut_newNotes')
        self.assertRaisesRegexp(LabadminDBUnknownIDError,
                                "The object with ID '%i' does not" % 99999,
                                db.update_plate_type, 99999, 'ut_newNotes')

        db.update_plate_type(id, 'ut_newNotes_UPDATED')
        res = self._remove_db_id([x for x in db.get_plate_types()
                                  if x['plate_type_id'] == id])
        self.assertEqual(res[0]['notes'], 'ut_newNotes_UPDATED')

    def test__check_grid_format(self):
        self.assertRaisesRegexp(LabadminGridFormatError,
                                'Gird must be a list of dictionaries.',
                                _check_grid_format, 5)
        self.assertRaisesRegexp(LabadminGridFormatError,
                                'Gird must be a list of dictionaries.',
                                _check_grid_format, [5])
        self.assertRaisesRegexp(LabadminGridFormatError,
                                'At least one of the cells in the grid mis a key "col".',
                                _check_grid_format, [{'col': 1, 'row': 2}, {'row': 3}])
        self.assertRaisesRegexp(LabadminGridFormatError,
                                'At least one of the cells in the grid mis a key "row".',
                                _check_grid_format, [{'col': 1, 'row': 2}, {'col': 3}])
        self.assertRaisesRegexp(LabadminGridFormatError,
                                'At least one of the cells in the grid mis a key "barcode".',
                                _check_grid_format, [{'col': 1, 'row': 2, 'barcode': 'AC'}, {'col': 3, 'row': 4}], ['barcode'])
        self.assertRaisesRegexp(LabadminGridFormatError,
                                'must be positive',
                                _check_grid_format, [{'col': 1, 'row': -2}, {'col': 3, 'row': 4}], maxrows=8)
        self.assertRaisesRegexp(LabadminGridFormatError,
                                'is larger than maximal row number',
                                _check_grid_format, [{'col': 1, 'row': 20}, {'col': 3, 'row': 4}], maxrows=8)
        self.assertRaisesRegexp(LabadminGridFormatError,
                                'must be positive',
                                _check_grid_format, [{'col': -1, 'row': 2}, {'col': 3, 'row': 4}], maxcols=8)
        self.assertRaisesRegexp(LabadminGridFormatError,
                                'is larger than maximal col number',
                                _check_grid_format, [{'col': 20, 'row': 2}, {'col': 3, 'row': 4}], maxcols=8)
        self.assertRaisesRegexp(LabadminGridFormatError,
                                'The grid contains duplicate cells.',
                                _check_grid_format, [{'col': 1, 'row': 2}, {'row': 1, 'col': 2}])
        self.assertTrue(_check_grid_format, [{'col': 1, 'row': 1}, {'row': 1, 'col': 2}])

if __name__ == "__main__":
    main()
