import time
import json
import redis
import pytest

from pprint import pprint
from swsscommon import swsscommon


class TestBufferMgrDyn(object):
    def setup_db(self, dvs):
        self.initialized = False
        self.cableLenTest1 = "23m"
        self.cableLenTest2 = "29m"
        self.speedToTest1 = "50000"
        self.speedToTest2 = "10000"

        self.app_db = dvs.get_app_db()
        self.config_db = dvs.get_config_db()
        self.state_db = dvs.get_state_db()

        fvs = self.config_db.wait_for_entry("PORT", "Ethernet0")
        self.originalSpeed = fvs["speed"]
        if self.originalSpeed == self.speedToTest1:
            self.speedToTest1 = "100000"
        elif self.originalSpeed == self.speedToTest2:
            self.speedToTest2 = "100000"
        elif self.originalSpeed == "":
            self.originalSpeed = "100000"

        # Check whether cabel length has been configured
        fvs = self.config_db.wait_for_entry("CABLE_LENGTH", "AZURE")
        self.originalCableLen = fvs["Ethernet0"]
        if self.originalCableLen == self.cableLenTest1:
            self.cableLenTest1 = "19m"
        elif self.originalCableLen == self.cableLenTest2:
            self.cableLenTest2 = "19m"

        fvs = {"mmu_size": "12766208"}
        self.state_db.create_entry("BUFFER_MAX_PARAM", "global", fvs)

        self.initialized = True

    def make_lossless_profile_name(self, speed, cable_length):
        return "pg_lossless_" + speed + "_" + cable_length + "_profile"

    def test_changeSpeed(self, dvs, testlog):
        self.setup_db(dvs)

        # Change speed to speed1 and verify whether the profile has been updated
        dvs.runcmd("config interface speed Ethernet0 " + self.speedToTest1)

        expectedProfile = self.make_lossless_profile_name(self.speedToTest1, self.originalCableLen)
        self.app_db.wait_for_entry("BUFFER_PROFILE", expectedProfile)

        # Check whether bufer pg align
        bufferPg = self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:3-4", {"profile": "[BUFFER_PROFILE:" + expectedProfile + "]"})

        # Remove PG
        dvs.runcmd("config interface lossless_pg remove Ethernet0 3-4")
        self.app_db.wait_for_deleted_entry("BUFFER_PG", "Ethernet0:3-4")

        # Change speed to speed2 and verify
        dvs.runcmd("config interface speed Ethernet0 " + self.speedToTest2)
        expectedProfile = self.make_lossless_profile_name(self.speedToTest2, self.originalCableLen)

        # Re-add another PG
        dvs.runcmd("config interface lossless_pg add Ethernet0 6")
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:6", {"profile": "[BUFFER_PROFILE:" + expectedProfile + "]"})

        # Revert speed and PG to original value
        dvs.runcmd("config interface lossless_pg remove Ethernet0 6")
        self.app_db.wait_for_deleted_entry("BUFFER_PG", "Ethernet0:6")

        dvs.runcmd("config interface speed Ethernet0 " + self.originalSpeed)
        dvs.runcmd("config interface lossless_pg add Ethernet0 3-4")

        expectedProfile = self.make_lossless_profile_name(self.originalSpeed, self.originalCableLen)
        self.app_db.wait_for_entry("BUFFER_PROFILE", expectedProfile)
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:3-4", {"profile": "[BUFFER_PROFILE:" + expectedProfile + "]"})

    def test_changeCableLen(self, dvs, testlog):
        self.setup_db(dvs)

        # Change to new cable length
        dvs.runcmd("config interface cable_length Ethernet0 " + self.cableLenTest1)
        expectedProfile = self.make_lossless_profile_name(self.originalSpeed, self.cableLenTest1)
        self.app_db.wait_for_entry("BUFFER_PROFILE", expectedProfile)
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:3-4", {"profile": "[BUFFER_PROFILE:" + expectedProfile + "]"})

        # Remove the lossless PGs
        dvs.runcmd("config interface lossless_pg remove Ethernet0")
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
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:3-4", {"profile": "[BUFFER_PROFILE:" + expectedProfile + "]"})

        # Revert the cable length
        dvs.runcmd("config interface cable_length Ethernet0 " + self.originalCableLen)
        # Check the BUFFER_PROFILE and BUFFER_PG
        self.app_db.wait_for_deleted_entry("BUFFER_PROFILE", expectedProfile)
        expectedProfile = self.make_lossless_profile_name(self.originalSpeed, self.originalCableLen)
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:3-4", {"profile": "[BUFFER_PROFILE:" + expectedProfile + "]"})

    def test_MultipleLosslessPg(self, dvs, testlog):
        self.setup_db(dvs)

        # Add another lossless PG
        dvs.runcmd("config interface lossless_pg add Ethernet0 6")
        expectedProfile = self.make_lossless_profile_name(self.originalSpeed, self.originalCableLen)
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:6", {"profile": "[BUFFER_PROFILE:" + expectedProfile + "]"})

        # change speed and check
        dvs.runcmd("config interface speed Ethernet0 " + self.speedToTest1)
        expectedProfile = self.make_lossless_profile_name(self.speedToTest1, self.originalCableLen)
        self.app_db.wait_for_entry("BUFFER_PROFILE", expectedProfile)
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:3-4", {"profile": "[BUFFER_PROFILE:" + expectedProfile + "]"})
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:6", {"profile": "[BUFFER_PROFILE:" + expectedProfile + "]"})

        # change cable length and check
        dvs.runcmd("config interface cable_length Ethernet0 " + self.cableLenTest1)
        self.app_db.wait_for_deleted_entry("BUFFER_PROFILE", expectedProfile)
        expectedProfile = self.make_lossless_profile_name(self.speedToTest1, self.cableLenTest1)
        self.app_db.wait_for_entry("BUFFER_PROFILE", expectedProfile)
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:3-4", {"profile": "[BUFFER_PROFILE:" + expectedProfile + "]"})
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:6", {"profile": "[BUFFER_PROFILE:" + expectedProfile + "]"})

        # revert the speed and cable length and check
        dvs.runcmd("config interface cable_length Ethernet0 " + self.originalCableLen)
        dvs.runcmd("config interface speed Ethernet0 " + self.originalSpeed)
        self.app_db.wait_for_deleted_entry("BUFFER_PROFILE", expectedProfile)
        expectedProfile = self.make_lossless_profile_name(self.originalSpeed, self.originalCableLen)
        self.app_db.wait_for_entry("BUFFER_PROFILE", expectedProfile)
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:3-4", {"profile": "[BUFFER_PROFILE:" + expectedProfile + "]"})
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:6", {"profile": "[BUFFER_PROFILE:" + expectedProfile + "]"})

    def test_headroomOverride(self, dvs, testlog):
        self.setup_db(dvs)

        # Configure static profile
        dvs.runcmd("config buffer_profile add -profile test -xon 18432 -xoff 16384")
        self.app_db.wait_for_exact_match("BUFFER_PROFILE", "test",
                        { "pool" : "[BUFFER_POOL:ingress_lossless_pool]",
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
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:3-4", {"profile": "[BUFFER_PROFILE:" + expectedProfile + "]"})

        dvs.runcmd("config interface lossless_pg remove Ethernet0")
        self.app_db.wait_for_deleted_entry("BUFFER_PG", "Ethernet0:3-4")

        dvs.runcmd("config interface headroom_override add Ethernet0 test 3-4")
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:3-4", {"profile": "[BUFFER_PROFILE:test]"})
        dvs.runcmd("config interface headroom_override add Ethernet0 test 6")
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:6", {"profile": "[BUFFER_PROFILE:test]"})

        dvs.runcmd("config interface headroom_override remove Ethernet0 test")
        self.app_db.wait_for_deleted_entry("BUFFER_PG", "Ethernet0:3-4")
        self.app_db.wait_for_deleted_entry("BUFFER_PG", "Ethernet0:6")

        dvs.runcmd("config interface lossless_pg add Ethernet0 3-4")
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:3-4", {"profile": "[BUFFER_PROFILE:" + expectedProfile + "]"})

        dvs.runcmd("config interface cable_length Ethernet0 " + self.originalCableLen)
        self.app_db.wait_for_deleted_entry("BUFFER_PROFILE", expectedProfile)
        expectedProfile = self.make_lossless_profile_name(self.originalSpeed, self.originalCableLen)
        self.app_db.wait_for_entry("BUFFER_PROFILE", expectedProfile)
        self.app_db.wait_for_field_match("BUFFER_PG", "Ethernet0:3-4", {"profile": "[BUFFER_PROFILE:" + expectedProfile + "]"})
