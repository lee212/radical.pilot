#!/usr/bin/env python
# encoding: utf-8

"""Database conncetion layer tests
"""

import unittest

from copy import deepcopy
from sinon.db import Session
from pymongo import MongoClient

DBURL = 'mongodb://ec2-184-72-89-141.compute-1.amazonaws.com:27017/'

#-----------------------------------------------------------------------------
#
class Test_Database(unittest.TestCase):
    # silence deprecation warnings under py3

    def setUp(self):
        # clean up fragments from previous tests
        client     = MongoClient(DBURL)
        db         = client.sinon
        for collection in ['new_session', 'non-existing-session', 'my_new_session', 'my_new_session.p', 'my_new_session.w']:
            collection = db[collection]
            collection.drop()

    def tearDown(self):
        # shutodwn
        pass

    def failUnless(self, expr):
        # St00pid speling.
        return self.assertTrue(expr)

    def failIf(self, expr):
        # St00pid speling.
        return self.assertFalse(expr)

    #-------------------------------------------------------------------------
    #
    def test__new_session(self):
        """ Test if Session.new() behaves as expected.
        """
        try:
            # this should fail
            s = Session.new(db_url="unknownhost", sid="new_session")
            assert False, "Prev. call should have failed."
        except Exception, ex:
            assert True

        s = Session.new(db_url=DBURL, sid="new_session")
        assert s.session_id == "new_session"
        s.delete()

    #-------------------------------------------------------------------------
    #
    def test__delete_session(self):
        """ Test if session.delete() behaves as expceted.
        """
        try:
            s = Session.new(db_url=DBURL, sid="new_session")
            s.delete()
            assert False, "Prev. call should have failed."
        except Exception, ex:
            assert True

    #-------------------------------------------------------------------------
    #
    def test__reconnect_session(self):
        """ Test if Session.reconnect() behaves as expceted.
        """
        try:
            s = Session.reconnect(db_url=DBURL, sid="non-existing-session")
            assert False, "Prev. call should have failed."
        except Exception, ex:
            assert True

        s1 = Session.new(db_url=DBURL, sid="my_new_session")
        s2 = Session.reconnect(db_url=DBURL, sid="my_new_session")
        assert s1.session_id == s2.session_id
        s2.delete()

    #-------------------------------------------------------------------------
    #
    def test__add_remove_work_units(self):
        """ Test if work_units_add(), work_units_update() and 
            work_units_get() behave as expected.
        """
        s = Session.new(db_url=DBURL, sid="my_new_session")

        wus = s.work_units_get()
        assert len(wus) == 0, "There shouldn't be any workunits in the collection."

        wu = {
            "work_unit_id"  : "unique work unit ID",
            "description"   : {
                "x" : "y"
            },
            "assignment"    : { 
                "queue" : "queue id",
                "pilot" : "pilot id"
            }
        }

        inserts = []
        for x in range(0,128):
            inserts.append(deepcopy(wu))
        ids = s.work_units_add(inserts)
        assert len(ids) == 128, "Wrong number of workunits added."




