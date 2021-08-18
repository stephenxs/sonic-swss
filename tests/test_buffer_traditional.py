import pytest
import time


class TestBuffer(object):
    LOSSLESS_PGS = [3, 4]
    INTF = "Ethernet0"

    def setup_db(self, dvs):
        self.app_db = dvs.get_app_db()
        self.asic_db = dvs.get_asic_db()
        self.config_db = dvs.get_config_db()
        self.counter_db = dvs.get_counters_db()

        # enable PG watermark
        self.set_pg_wm_status('enable')

    def get_pg_oid(self, pg):
        fvs = dict()
        fvs = self.counter_db.get_entry("COUNTERS_PG_NAME_MAP", "")
        return fvs[pg]

    def set_pg_wm_status(self, state):
        fvs = {'FLEX_COUNTER_STATUS': state}
        self.config_db.update_entry("FLEX_COUNTER_TABLE", "PG_WATERMARK", fvs)
        time.sleep(1)

    def teardown(self):
        # disable PG watermark
        self.set_pg_wm_status('disable')

    def get_asic_buf_profile(self):
        return set(self.asic_db.get_keys("ASIC_STATE:SAI_OBJECT_TYPE_BUFFER_PROFILE"))

    def check_new_profile_in_asic_db(self, profile):
        retry_count = 0
        self.new_profile = None
        while retry_count < 5:
            retry_count += 1
            diff = set(self.asic_db.get_keys("ASIC_STATE:SAI_OBJECT_TYPE_BUFFER_PROFILE")) - self.orig_profiles
            if len(diff) == 1:
                self.new_profile = diff.pop()
                break
            else:
                time.sleep(1)
        assert self.new_profile, "Can't get SAI OID for newly created profile {} after retry {} times".format(profile, retry_count)

    def get_asic_buf_pg_profiles(self):
        self.buf_pg_profile = dict()
        for pg in self.pg_name_map:
            buf_pg_entries = self.asic_db.get_entry("ASIC_STATE:SAI_OBJECT_TYPE_INGRESS_PRIORITY_GROUP", self.pg_name_map[pg])
            self.buf_pg_profile[pg] = buf_pg_entries["SAI_INGRESS_PRIORITY_GROUP_ATTR_BUFFER_PROFILE"]

    def change_cable_len(self, cable_len):
        fvs = dict()
        fvs[self.INTF] = cable_len
        self.config_db.update_entry("CABLE_LENGTH", "AZURE", fvs)

    @pytest.fixture
    def setup_teardown_test(self, dvs):
        try:
            self.setup_db(dvs)
            pg_name_map = dict()
            for pg in self.LOSSLESS_PGS:
                pg_name = "{}:{}".format(self.INTF, pg)
                pg_name_map[pg_name] = self.get_pg_oid(pg_name)
            yield pg_name_map
        finally:
            self.teardown()

    @pytest.mark.skip(reason="Failing. Under investigation")
    def test_zero_cable_len_profile_update(self, dvs, setup_teardown_test):
        self.pg_name_map = setup_teardown_test
        orig_cable_len = None
        orig_speed = None
        try:
            # get orig cable length and speed
            fvs = self.config_db.get_entry("CABLE_LENGTH", "AZURE")
            orig_cable_len = fvs[self.INTF]
            if orig_cable_len == "0m":
                fvs[self.INTF] = "300m"
                cable_len_before_test = "300m"
                self.config_db.update_entry("CABLE_LENGTH", "AZURE", fvs)
            else:
                cable_len_before_test = orig_cable_len
            fvs = self.config_db.get_entry("PORT", self.INTF)
            orig_speed = fvs["speed"]

            if orig_speed == "100000":
                test_speed = "40000"
            elif orig_speed == "40000":
                test_speed = "100000"
            test_cable_len = "0m"

            dvs.runcmd("config interface startup {}".format(self.INTF))
            # Make sure the buffer PG has been created
            orig_lossless_profile = "pg_lossless_{}_{}_profile".format(orig_speed, cable_len_before_test)
            self.app_db.wait_for_entry("BUFFER_PROFILE_TABLE", orig_lossless_profile)
            self.orig_profiles = self.get_asic_buf_profile()

            # check if the lossless profile for the test speed is already present
            fvs = dict()
            new_lossless_profile = "pg_lossless_{}_{}_profile".format(test_speed, cable_len_before_test)
            fvs = self.app_db.get_entry("BUFFER_PROFILE_TABLE", new_lossless_profile)
            if len(fvs):
                profile_exp_cnt_diff = 0
            else:
                profile_exp_cnt_diff = 1

            # get the orig buf profiles attached to the pgs
            self.get_asic_buf_pg_profiles()

            # change cable length to 'test_cable_len'
            self.change_cable_len(test_cable_len)

            # change intf speed to 'test_speed'
            dvs.runcmd("config interface speed {} {}".format(self.INTF, test_speed))
            test_lossless_profile = "pg_lossless_{}_{}_profile".format(test_speed, test_cable_len)
            # buffer profile should not get created
            self.app_db.wait_for_deleted_entry("BUFFER_PROFILE_TABLE", test_lossless_profile)

            # buffer pgs should still point to the original buffer profile
            self.app_db.wait_for_field_match("BUFFER_PG_TABLE", self.INTF + ":3-4", {"profile": "[BUFFER_PROFILE_TABLE:{}]".format(orig_lossless_profile)})
            fvs = dict()
            for pg in self.pg_name_map:
                fvs["SAI_INGRESS_PRIORITY_GROUP_ATTR_BUFFER_PROFILE"] = self.buf_pg_profile[pg]
                self.asic_db.wait_for_field_match("ASIC_STATE:SAI_OBJECT_TYPE_INGRESS_PRIORITY_GROUP", self.pg_name_map[pg], fvs)

            # change cable length to 'cable_len_before_test'
            self.change_cable_len(cable_len_before_test)

            # change intf speed to 'test_speed'
            dvs.runcmd("config interface speed {} {}".format(self.INTF, test_speed))
            if profile_exp_cnt_diff != 0:
                # new profile will get created
                self.app_db.wait_for_entry("BUFFER_PROFILE_TABLE", new_lossless_profile)
                self.check_new_profile_in_asic_db(new_lossless_profile)
                # verify that buffer pgs point to the new profile
                fvs = dict()
                for pg in self.pg_name_map:
                    fvs["SAI_INGRESS_PRIORITY_GROUP_ATTR_BUFFER_PROFILE"] = self.new_profile
                    self.asic_db.wait_for_field_match("ASIC_STATE:SAI_OBJECT_TYPE_INGRESS_PRIORITY_GROUP", self.pg_name_map[pg], fvs)

            else:
                # verify that buffer pgs do not point to the old profile since we cannot deduce the new profile oid
                fvs = dict()
                for pg in self.pg_name_map:
                    fvs["SAI_INGRESS_PRIORITY_GROUP_ATTR_BUFFER_PROFILE"] = self.buf_pg_profile[pg]
                    self.asic_db.wait_for_field_negative_match("ASIC_STATE:SAI_OBJECT_TYPE_INGRESS_PRIORITY_GROUP", self.pg_name_map[pg], fvs)
        finally:
            if orig_cable_len:
                self.change_cable_len(orig_cable_len)
            if orig_speed:
                dvs.runcmd("config interface speed {} {}".format(self.INTF, orig_speed))
            dvs.runcmd("config interface shutdown {}".format(self.INTF))
