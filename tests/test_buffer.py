import time
import json
import redis
import pytest

from pprint import pprint
from swsscommon import swsscommon


class TestBufferMgrDyn(object):
    self.initialized = False
    self.cableLenTest0 = "17m"
    self.cableLenTest1 = "23m"
    self.cableLenTest2 = "29m"
    self.speedToTest1 = "50000"
    self.speedToTest2 = "10000"

    def init(self, dvs, testlog):
        self.app_db = dvs.get_app_db()
        self.config_db = dvs.get_config_db()

        fvs = self.config_db.wait_for_entry("PORT", "Ethernet0")
        self.originalSpeed = fvs["speed"]
        if self.originalSpeed == "10000":
            self.speedToTest1 = "50000"
            self.speedToTest2 = "100000"
        elif self.originalSpeed = "50000":
            self.speedToTest1 = "10000"
            self.speedToTest2 = "100000"

        # Check whether cabel length has been configured
        fvs = self.config_db.wait_for_entry("CABLE_LENGTH", "AZURE")
        self.originalCableLen = fvs["Ethernet0"]

        self.initialized = True

    def make_lossless_profile_name(self, speed, cable_length):
        return "pg_lossless_" + speed + "_" + cable_length + "_profile"

    def test_changeSpeed(self, dvs, testlog):
        # Change speed to speed1 and verify whether the profile has been updated
        dvs.runcmd("config interface speed " + self.speedToTest1)

        expectedProfile = self.make_lossless_profile_name(self.speedToTest1, self.cableLenTest0)
        self.app_db.wait_for_entry("BUFFER_PROFILE", expectedProfile)

        # Check whether bufer pg align
        bufferPg = self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:3-4", {"profile", "[" + expectedProfile + "]"})

        # Remove PG
        dvs.runcmd("config interface lossless_pg remove Ethernet0 3-4")
        self.app_db.wait_for_deleted_entry("BUFFER_PG", "Ethernet0:3-4")

        # Change speed to speed2 and verify
        dvs.runcmd("config interface speed " + self.speedToTest2)
        expectedProfile = self.make_lossless_profile_name(self.speedToTest2, self.cableLenTest0)

        # Re-add another PG
        dvs.runcmd("config interface lossless_pg add Ethernet0 6")
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:6", {"profile", "[" + expectedProfile + "]"})

        # Revert speed and PG to original value
        dvs.runcmd("config interface lossless_pg remove Ethernet0 6")
        self.app_db.wait_for_deleted_entry("BUFFER_PG", "Ethernet0:6")

        dvs.runcmd("config interface speed " + self.originalSpeed)
        dvs.runcmd("config interface lossless_pg add Ethernet0 3-4")

        expectedProfile = self.make_lossless_profile_name(self.originalSpeed, self.cableLenTest0)
        self.app_db.wait_for_entry("BUFFER_PROFILE", expectedProfile)
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:3-4", {"profile", "[" + expectedProfile + "]"})

    def test_changeCableLen(self, dvs, testlog):
        # Change to new cable length
        dvs.runcmd("config interface cable_length Ethernet0 " + self.cableLenTest1)
        expectedProfile = self.make_lossless_profile_name(self.originalSpeed, self.cableLenTest1)
        self.app_db.wait_for_entry("BUFFER_PROFILE", expectedProfile)
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:3-4", {"profile", "[" + expectedProfile + "]"})

        # Remove the lossless PGs
        dev.runcmd("config interface lossless_pg remove Ethernet0")
        self.app_db.wait_for_deleted_entry("BUFFER_PG", "Ethernet0:3-4")

        # Change to another cable length
        dvs.runcmd("config interface cable_length Ethernet0 " + self.cableLenTest2)
        # Check whether the old profile has been removed
        self.app_db.wait_for_deleted_entry("BUFFER_PROFILE", expectedProfile)

        # Re-add lossless PGs
        dvs.runcmd("config interface lossless_pg add Ethernet0 3-4")
        expectedProfile = self.make_lossless_profile_name(self.originalSpeed, self.cableLenTest2)
        # Check the BUFFER_PROFILE and BUFFER_PG
        self.app_db.wait_for_entry("BUFFER_PROFILE", expectedProfile)
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:3-4", {"profile", "[" + expectedProfile + "]"})

        # Revert the cable length
        dvs.runcmd("config interface cable_length Ethernet0 " + self.cableLenTest0)
        expectedProfile = self.make_lossless_profile_name(self.originalSpeed, self.cableLenTest0)
        # Check the BUFFER_PROFILE and BUFFER_PG
        self.app_db.wait_for_deleted_entry("BUFFER_PROFILE", expectedProfile)
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:3-4", {"profile", "[" + expectedProfile + "]"})

    def test_MultipleLosslessPg(self, dvs, testlog):
        # Add another lossless PG
        dvs.runcmd("config interface lossless_pg add Ethernt0 6")
        expectedProfile = self.make_lossless_profile_name(self.originalSpeed, self.cableLenTest0)
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:6", {"profile", "[" + expectedProfile + "]"})

        # change speed and check
        dvs.runcmd("config interface speed Ethernet0 " + self.speedToTest1)
        expectedProfile = self.make_lossless_profile_name(self.speedToTest1, self.cableLenTest0)
        self.app_db.wait_for_entry("BUFFER_PROFILE", expectedProfile)
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:3-4", {"profile", "[" + expectedProfile + "]"})
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:6", {"profile", "[" + expectedProfile + "]"})

        # change cable length and check
        dvs.runcmd("config interface cable_length Ethernet0 " + self.cableLenTest1)
        self.app_db.wait_for_deleted_entry("BUFFER_PROFILE", expectedProfile)
        expectedProfile = self.make_lossless_profile_name(self.speedToTest1, self.cableLenTest1)
        self.app_db.wait_for_entry("BUFFER_PROFILE", expectedProfile)
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:3-4", {"profile", "[" + expectedProfile + "]"})
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:6", {"profile", "[" + expectedProfile + "]"})

        # revert the speed and cable length and check
        dvs.runcmd("config interface cable_length Ethernet0 " + self.cableLenTest0)
        dvs.runcmd("config interface speed Ethernet0 " + self.originalSpeed)
        self.app_db.wait_for_deleted_entry("BUFFER_PROFILE", expectedProfile)
        expectedProfile = self.make_lossless_profile_name(self.originalSpeed, self.cableLenTest0)
        self.app_db.wait_for_entry("BUFFER_PROFILE", expectedProfile)
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:3-4", {"profile", "[" + expectedProfile + "]"})
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:6", {"profile", "[" + expectedProfile + "]"})

    def test_headroomOverride(self, dvs, testlog):
        # Configure static profile
        dvs.runcmd("config buffer_profile add -profile test -xon 18432 -xoff 16384")
        self.app_db.wait_for_exact_match("BUFFER_PROFILE", "test",
                        { "pool" : "BUFFER_POOL:ingress_lossless_pool",
                          "xon" : "18432",
                          "xoff" : "16384",
                          "size" : "34816",
                          "dynamic_th" : "0"
                          })

        dvs.runcmd("config interface headroom_override add Ethernet0 test 3-4")
        # This command should not take effect since dynamic PGs are not removed.
        self.app_db.wait_for_entry("BUFFER_PG", "Ethernet0:3-4")

        dvs.runcmd("config interface cable_length Ethernet0 " + self.cableLenTest1)
        expectedProfile = self.make_lossless_profile_name(self.originalSpeed, self.cableLenTest1)
        self.app_db.wait_for_entry("BUFFER_PROFILE", expectedProfile)
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:3-4", {"profile", "[" + expectedProfile + "]"})

        dvs.runcmd("config interface lossless_pg remove Ethernet0")
        self.app_db.wait_for_deleted_entry("BUFFER_PG", "Ethernet0:3-4")

        dvs.runcmd("config interface headroom_override add Ethernet0 3-4")
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:3-4", {"profile", "[test]"})
        dvs.runcmd("config interface headroom_override add Ethernet0 6")
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:6", {"profile", "[test]"})

        dvs.runcmd("config interface headroom_override remove Ethernet0")
        self.app_db.wait_for_deleted_entry("BUFFER_PG", "Ethernet0:3-4")
        self.app_db.wait_for_deleted_entry("BUFFER_PG", "Ethernet0:6")

        dvs.runcmd("config interface lossless_pg add Ethernet0 3-4")
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:3-4", {"profile", "[" + expectedProfile + "]"})

        dvs.runcmd("config interface cable_length Ethernet0 " + self.originalCableLen)
        self.app_db.wait_for_deleted_entry("BUFFER_PROFILE", expectedProfile)
        expectedProfile = self.make_lossless_profile_name(self.originalSpeed, self.originalCableLen)
        self.app_db.wait_for_entry("BUFFER_PG", expectedProfile)
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:3-4", {"profile", "[" + expectedProfile + "]"})
