import time
import json
import redis
import pytest

from pprint import pprint
from swsscommon import swsscommon


class TestBufferMgrDyn(object):
    def setup_db(self, dvs):
        self.initialized = False
        self.cableLenTest1 = "15m"
        self.cableLenTest2 = "25m"
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
            self.cableLenTest1 = "20m"
        elif self.originalCableLen == self.cableLenTest2:
            self.cableLenTest2 = "20m"

        fvs = {"mmu_size": "12766208"}
        self.state_db.create_entry("BUFFER_MAX_PARAM_TABLE", "global", fvs)

        self.initialized = True

    def make_lossless_profile_name(self, speed, cable_length):
        return "pg_lossless_" + speed + "_" + cable_length + "_profile"

    @pytest.mark.skip(reason="Traditional buffer model runs on VS image currently.")
    def test_changeSpeed(self, dvs, testlog):
        self.setup_db(dvs)

        # configure lossless PG 3-4 on interface
        dvs.runcmd("config interface buffer priority-group lossless add Ethernet0 3-4")

        # Change speed to speed1 and verify whether the profile has been updated
        dvs.runcmd("config interface speed Ethernet0 " + self.speedToTest1)

        expectedProfile = self.make_lossless_profile_name(self.speedToTest1, self.originalCableLen)
        self.app_db.wait_for_entry("BUFFER_PROFILE_TABLE", expectedProfile)

        # Check whether bufer pg align
        bufferPg = self.app_db.wait_for_field_match("BUFFER_PG_TABLE", "Ethernet0:3-4", {"profile": "[BUFFER_PROFILE_TABLE:" + expectedProfile + "]"})

        # Remove PG
        dvs.runcmd("config interface buffer priority-group lossless remove Ethernet0 3-4")
        self.app_db.wait_for_deleted_entry("BUFFER_PG_TABLE", "Ethernet0:3-4")

        # Change speed to speed2 and verify
        dvs.runcmd("config interface speed Ethernet0 " + self.speedToTest2)
        expectedProfile = self.make_lossless_profile_name(self.speedToTest2, self.originalCableLen)

        # Re-add another PG
        dvs.runcmd("config interface buffer priority-group lossless add Ethernet0 6")
        self.app_db.wait_for_field_match("BUFFER_PG_TABLE", "Ethernet0:6", {"profile": "[BUFFER_PROFILE_TABLE:" + expectedProfile + "]"})

        # Revert speed and PG to original value
        dvs.runcmd("config interface buffer priority-group lossless remove Ethernet0 6")
        self.app_db.wait_for_deleted_entry("BUFFER_PG_TABLE", "Ethernet0:6")

        dvs.runcmd("config interface speed Ethernet0 " + self.originalSpeed)
        dvs.runcmd("config interface buffer priority-group lossless add Ethernet0 3-4")

        expectedProfile = self.make_lossless_profile_name(self.originalSpeed, self.originalCableLen)
        self.app_db.wait_for_entry("BUFFER_PROFILE_TABLE", expectedProfile)
        self.app_db.wait_for_field_match("BUFFER_PG_TABLE", "Ethernet0:3-4", {"profile": "[BUFFER_PROFILE_TABLE:" + expectedProfile + "]"})

        # remove lossless PG 3-4 on interface
        dvs.runcmd("config interface buffer priority-group lossless remove Ethernet0 3-4")

    @pytest.mark.skip(reason="Traditional buffer model runs on VS image currently.")
    def test_changeCableLen(self, dvs, testlog):
        self.setup_db(dvs)

        # configure lossless PG 3-4 on interface
        dvs.runcmd("config interface buffer priority-group lossless add Ethernet0 3-4")

        # Change to new cable length
        dvs.runcmd("config interface cable-length Ethernet0 " + self.cableLenTest1)
        expectedProfile = self.make_lossless_profile_name(self.originalSpeed, self.cableLenTest1)
        self.app_db.wait_for_entry("BUFFER_PROFILE_TABLE", expectedProfile)
        self.app_db.wait_for_field_match("BUFFER_PG_TABLE", "Ethernet0:3-4", {"profile": "[BUFFER_PROFILE_TABLE:" + expectedProfile + "]"})

        # Remove the lossless PGs
        dvs.runcmd("config interface buffer priority-group lossless remove Ethernet0")
        self.app_db.wait_for_deleted_entry("BUFFER_PG_TABLE", "Ethernet0:3-4")

        # Change to another cable length
        dvs.runcmd("config interface cable-length Ethernet0 " + self.cableLenTest2)
        # Check whether the old profile has been removed
        self.app_db.wait_for_deleted_entry("BUFFER_PROFILE_TABLE", expectedProfile)

        # Re-add lossless PGs
        dvs.runcmd("config interface buffer priority-group lossless add Ethernet0 3-4")
        expectedProfile = self.make_lossless_profile_name(self.originalSpeed, self.cableLenTest2)
        # Check the BUFFER_PROFILE_TABLE and BUFFER_PG_TABLE
        self.app_db.wait_for_entry("BUFFER_PROFILE_TABLE", expectedProfile)
        self.app_db.wait_for_field_match("BUFFER_PG_TABLE", "Ethernet0:3-4", {"profile": "[BUFFER_PROFILE_TABLE:" + expectedProfile + "]"})

        # Revert the cable length
        dvs.runcmd("config interface cable-length Ethernet0 " + self.originalCableLen)
        # Check the BUFFER_PROFILE_TABLE and BUFFER_PG_TABLE
        self.app_db.wait_for_deleted_entry("BUFFER_PROFILE_TABLE", expectedProfile)
        expectedProfile = self.make_lossless_profile_name(self.originalSpeed, self.originalCableLen)
        self.app_db.wait_for_field_match("BUFFER_PG_TABLE", "Ethernet0:3-4", {"profile": "[BUFFER_PROFILE_TABLE:" + expectedProfile + "]"})

        # remove lossless PG 3-4 on interface
        dvs.runcmd("config interface buffer priority-group lossless remove Ethernet0 3-4")

    @pytest.mark.skip(reason="Traditional buffer model runs on VS image currently.")
    def test_MultipleLosslessPg(self, dvs, testlog):
        self.setup_db(dvs)

        # configure lossless PG 3-4 on interface
        dvs.runcmd("config interface buffer priority-group lossless add Ethernet0 3-4")

        # Add another lossless PG
        dvs.runcmd("config interface buffer priority-group lossless add Ethernet0 6")
        expectedProfile = self.make_lossless_profile_name(self.originalSpeed, self.originalCableLen)
        self.app_db.wait_for_field_match("BUFFER_PG_TABLE", "Ethernet0:6", {"profile": "[BUFFER_PROFILE_TABLE:" + expectedProfile + "]"})

        # change speed and check
        dvs.runcmd("config interface speed Ethernet0 " + self.speedToTest1)
        expectedProfile = self.make_lossless_profile_name(self.speedToTest1, self.originalCableLen)
        self.app_db.wait_for_entry("BUFFER_PROFILE_TABLE", expectedProfile)
        self.app_db.wait_for_field_match("BUFFER_PG_TABLE", "Ethernet0:3-4", {"profile": "[BUFFER_PROFILE_TABLE:" + expectedProfile + "]"})
        self.app_db.wait_for_field_match("BUFFER_PG_TABLE", "Ethernet0:6", {"profile": "[BUFFER_PROFILE_TABLE:" + expectedProfile + "]"})

        # change cable length and check
        dvs.runcmd("config interface cable-length Ethernet0 " + self.cableLenTest1)
        self.app_db.wait_for_deleted_entry("BUFFER_PROFILE_TABLE", expectedProfile)
        expectedProfile = self.make_lossless_profile_name(self.speedToTest1, self.cableLenTest1)
        self.app_db.wait_for_entry("BUFFER_PROFILE_TABLE", expectedProfile)
        self.app_db.wait_for_field_match("BUFFER_PG_TABLE", "Ethernet0:3-4", {"profile": "[BUFFER_PROFILE_TABLE:" + expectedProfile + "]"})
        self.app_db.wait_for_field_match("BUFFER_PG_TABLE", "Ethernet0:6", {"profile": "[BUFFER_PROFILE_TABLE:" + expectedProfile + "]"})

        # revert the speed and cable length and check
        dvs.runcmd("config interface cable-length Ethernet0 " + self.originalCableLen)
        dvs.runcmd("config interface speed Ethernet0 " + self.originalSpeed)
        self.app_db.wait_for_deleted_entry("BUFFER_PROFILE_TABLE", expectedProfile)
        expectedProfile = self.make_lossless_profile_name(self.originalSpeed, self.originalCableLen)
        self.app_db.wait_for_entry("BUFFER_PROFILE_TABLE", expectedProfile)
        self.app_db.wait_for_field_match("BUFFER_PG_TABLE", "Ethernet0:3-4", {"profile": "[BUFFER_PROFILE_TABLE:" + expectedProfile + "]"})
        self.app_db.wait_for_field_match("BUFFER_PG_TABLE", "Ethernet0:6", {"profile": "[BUFFER_PROFILE_TABLE:" + expectedProfile + "]"})

        # remove lossless PG 3-4 and 6 on interface
        dvs.runcmd("config interface buffer priority-group lossless remove Ethernet0 3-4")
        dvs.runcmd("config interface buffer priority-group lossless remove Ethernet0 6")

    @pytest.mark.skip(reason="Traditional buffer model runs on VS image currently.")
    def test_headroomOverride(self, dvs, testlog):
        self.setup_db(dvs)

        # configure lossless PG 3-4 on interface
        dvs.runcmd("config interface buffer priority-group lossless add Ethernet0 3-4")

        # Configure static profile
        dvs.runcmd("config buffer profile add test --xon 18432 --xoff 16384")
        self.app_db.wait_for_exact_match("BUFFER_PROFILE_TABLE", "test",
                        { "pool" : "[BUFFER_POOL_TABLE:ingress_lossless_pool]",
                          "xon" : "18432",
                          "xoff" : "16384",
                          "size" : "34816",
                          "dynamic_th" : "0"
                          })

        dvs.runcmd("config interface cable-length Ethernet0 " + self.cableLenTest1)
        expectedProfile = self.make_lossless_profile_name(self.originalSpeed, self.cableLenTest1)
        self.app_db.wait_for_entry("BUFFER_PROFILE_TABLE", expectedProfile)
        self.app_db.wait_for_field_match("BUFFER_PG_TABLE", "Ethernet0:3-4", {"profile": "[BUFFER_PROFILE_TABLE:" + expectedProfile + "]"})

        dvs.runcmd("config interface buffer priority-group lossless set Ethernet0 3-4 test")
        self.app_db.wait_for_field_match("BUFFER_PG_TABLE", "Ethernet0:3-4", {"profile": "[BUFFER_PROFILE_TABLE:test]"})
        dvs.runcmd("config interface buffer priority-group lossless add Ethernet0 6 test")
        self.app_db.wait_for_field_match("BUFFER_PG_TABLE", "Ethernet0:6", {"profile": "[BUFFER_PROFILE_TABLE:test]"})

        dvs.runcmd("config interface buffer priority-group lossless remove Ethernet0")
        self.app_db.wait_for_deleted_entry("BUFFER_PG_TABLE", "Ethernet0:3-4")
        self.app_db.wait_for_deleted_entry("BUFFER_PG_TABLE", "Ethernet0:6")

        dvs.runcmd("config interface buffer priority-group lossless add Ethernet0 3-4")
        self.app_db.wait_for_field_match("BUFFER_PG_TABLE", "Ethernet0:3-4", {"profile": "[BUFFER_PROFILE_TABLE:" + expectedProfile + "]"})

        dvs.runcmd("config interface cable-length Ethernet0 " + self.originalCableLen)
        self.app_db.wait_for_deleted_entry("BUFFER_PROFILE_TABLE", expectedProfile)
        expectedProfile = self.make_lossless_profile_name(self.originalSpeed, self.originalCableLen)
        self.app_db.wait_for_entry("BUFFER_PROFILE_TABLE", expectedProfile)
        self.app_db.wait_for_field_match("BUFFER_PG_TABLE", "Ethernet0:3-4", {"profile": "[BUFFER_PROFILE_TABLE:" + expectedProfile + "]"})

        # remove lossless PG 3-4 on interface
        dvs.runcmd("config interface buffer priority-group lossless remove Ethernet0 3-4")

    def test_bufferModel(self, dvs, testlog):
        self.setup_db(dvs)

        metadata = self.config_db.get_entry("DEVICE_METADATA", "localhost")
        assert metadata["buffer_model"] == "traditional"
