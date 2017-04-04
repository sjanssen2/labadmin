from unittest import main
import os

from tornado.escape import url_escape, xhtml_escape
from tornado.httpclient import HTTPError

from knimin.tests.tornado_test_base import TestHandlerBase
from knimin import db


class TestAGBarcodePrintoutHandler(TestHandlerBase):
    def test_get_not_authed(self):
        response = self.get('/ag_new_barcode/download/')
        self.assertEqual(response.code, 405)  # Method Not Allowed

    def test_post(self):
        self.mock_login_admin()
        response = self.post('/ag_new_barcode/download/',
                             {'barcodes': "1111,222,33,4"})
        self.assertEqual(response.code, 200)
        self.assertEqual(response.headers['Content-Disposition'],
                         'attachment; filename=barcodes.pdf')
        # check that the files is a PDF ...
        self.assertIn('%PDF-1.', response.body)
        # ... and it is not empty
        self.assertEqual(len(response.body) > 1000, True)


class TestAGBarcodeAssignedHandler(TestHandlerBase):
    def test_get_not_authed(self):
        response = self.get('/ag_new_barcode/assigned/')
        self.assertEqual(response.code, 405)  # Method Not Allowed

    def test_post(self):
        self.mock_login_admin()
        projects = ["American Gut Project", "Autism Spectrum Disorder"]
        barcodes = ["1111", "222", "33", "4"]
        response = self.post('/ag_new_barcode/assigned/',
                             {'barcodes': barcodes,
                              'projects': projects})
        self.assertEqual(response.code, 200)
        text = "".join(["%s\t%s\n" % (xhtml_escape(b),
                                      ",".join(map(xhtml_escape, projects)))
                        for b in barcodes])
        self.assertEqual(response.body, text)


class TestAGNewBarcodeHandler(TestHandlerBase):
    def test_get_not_authed(self):
        response = self.get('/ag_new_barcode/')
        self.assertEqual(response.code, 200)
        port = self.get_http_port()
        self.assertEqual(response.effective_url,
                         'http://localhost:%d/login/?next=%s' %
                         (port, url_escape('/ag_new_barcode/')))

    def test_get(self):
        self.mock_login_admin()
        response = self.get('/ag_new_barcode/')
        self.assertEqual(response.code, 200)

        obs = response.body.decode('utf-8')

        for project in db.getProjectNames():
            self.assertIn("<option value='%s'>%s</option>" %
                          ((xhtml_escape(project),) * 2), obs)
        self.assertIn("Number of barcodes (%i unassigned)" %
                      len(db.get_unassigned_barcodes()), obs)

    def test_post(self):
        self.mock_login_admin()
        action = 'unknown'
        num_barcodes = 4
        projects = [p.encode('utf-8') for p in db.getProjectNames()[:2]]
        newProject = 'newProject' + str(os.getpid())

        # check that unkown action results in a response code 400
        response = self.post('/ag_new_barcode/', {'action': action,
                                                  'numbarcodes': num_barcodes})
        self.assertRaises(HTTPError)
        self.assertEqual(response.code, 400)
        self.assertIn("HTTPError: HTTP %i: Bad Request (Unknown action: %s)"
                      % (400, action), response.body)

        # TODO: test if exception for 0 barcodes to create is raised issue #105

        # check creation of new barcodes
        response = self.post('/ag_new_barcode/', {'action': 'create',
                                                  'numbarcodes': num_barcodes})
        self.assertIn("%i Barcodes created! Please wait for barcode download" %
                      num_barcodes, response.body)

        # check assignment of barcodes
        # check that error is raised if project(s) cannot be found in DB
        prj_nonexist = 'a non existing project name'
        response = self.post('/ag_new_barcode/',
                             {'action': 'assign',
                              'numbarcodes': num_barcodes,
                              'projects': projects + [prj_nonexist],
                              'newproject': ""})
        self.assertEqual(response.code, 200)
        exp = xhtml_escape('ERROR! Project(s) given don\'t exist in '
                           'database: %s' % prj_nonexist)
        self.assertIn(exp, response.body)

        # check correct assignment report on HTML side
        response = self.post('/ag_new_barcode/',
                             {'action': 'assign',
                              'numbarcodes': num_barcodes,
                              'projects': projects,
                              'newproject': ""})
        self.assertEqual(response.code, 200)
        exp = xhtml_escape(
            "%i barcodes assigned to %s, please wait for download." %
            (num_barcodes, ", ".join(projects)))
        self.assertIn(exp, response.body)

        # check if SQL error is thrown if number of barcodes is 0.
        # See issue: #107
        response = self.post('/ag_new_barcode/',
                             {'action': 'assign',
                              'numbarcodes': 0,
                              'projects': projects,
                              'newproject': ""})
        self.assertEqual(response.code, 200)
        self.assertIn("Error running SQL query: UPDATE barcodes.barcode",
                      response.body)

        # check recognition of existing projects
        response = self.post('/ag_new_barcode/',
                             {'action': 'assign',
                              'numbarcodes': num_barcodes,
                              'newproject': projects[0]})
        self.assertEqual(response.code, 200)
        self.assertIn("ERROR! Project %s already exists!" %
                      xhtml_escape(projects[0]),
                      response.body)

        # check recognition of unkown action
        response = self.post('/ag_new_barcode/', {'action': 'unkown'})
        self.assertEqual(response.code, 400)
        self.assertRaises(HTTPError)

        # test that new project is appended to list of projects
        response = self.post('/ag_new_barcode/',
                             {'action': 'assign',
                              'numbarcodes': num_barcodes,
                              'projects': projects,
                              'newproject': newProject})
        self.assertEqual(response.code, 200)
        self.assertIn("%i barcodes assigned to %s, please wait for download." %
                      (num_barcodes, ", ".join(map(xhtml_escape,
                                                   projects + [newProject]))),
                      response.body)


if __name__ == "__main__":
    main()
