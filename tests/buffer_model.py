import re
import time

lossless_profile_name_pattern = 'pg_lossless_([1-9][0-9]*000)_([1-9][0-9]*m)_profile'
def enable_dynamic_buffer(config_db, cmd_runner):
    # check whether it's already running dynamic mode
    device_meta = config_db.get_entry('DEVICE_METADATA', 'localhost')
    assert 'buffer_model' in device_meta, "'buffer_model' doesn't exist in DEVICE_METADATA|localhost"
    if device_meta['buffer_model'] == 'dynamic':
        return

    try:
        device_meta['buffer_model'] = 'dynamic'
        config_db.update_entry('DEVICE_METADATA', 'localhost', device_meta)

        # stop the buffermgrd in order to avoid it copying tables to appl db
        cmd_runner("supervisorctl stop buffermgrd")

        # update lossless PGs from BUFFER_PG table
        pgs = config_db.get_keys('BUFFER_PG')
        for key in pgs:
            pg = config_db.get_entry('BUFFER_PG', key)
            if re.search(lossless_profile_name_pattern, pg['profile']):
                pg['profile'] = 'NULL'
                config_db.update_entry('BUFFER_PG', key, pg)

        # Remove the dynamically generated profiles
        profiles = config_db.get_keys('BUFFER_PROFILE')
        for key in profiles:
            if re.search(lossless_profile_name_pattern, key):
                config_db.delete_entry('BUFFER_PROFILE', key)

        profiles = config_db.get_keys('BUFFER_PROFILE')
        for key in profiles:
            if re.search(lossless_profile_name_pattern, key):
                assert False, "dynamically generated profile still exists"

    finally:
        # restart daemon
        cmd_runner("supervisorctl restart buffermgrd")
        # make sure dynamic buffer manager fully starts
        time.sleep(20)


def disable_dynamic_buffer(config_db, cmd_runner):
    device_meta = config_db.get_entry('DEVICE_METADATA', 'localhost')
    assert 'buffer_model' in device_meta, "'buffer_model' doesn't exist in DEVICE_METADATA|localhost"
    if device_meta['buffer_model'] == 'traditional':
        return

    try:
        device_meta['buffer_model'] = 'traditional'
        config_db.update_entry('DEVICE_METADATA', 'localhost', device_meta)

        # Remove all the PGs referencing the lossless profiles
        pgs = config_db.get_keys('BUFFER_PG')
        for key in pgs:
            pg = config_db.get_entry('BUFFER_PG', key)
            if pg['profile'] != '[BUFFER_PROFILE|ingress_lossy_profile]':
                config_db.delete_entry('BUFFER_PG', key)

        # Remove all the non-default profiles
        profiles = config_db.get_keys('BUFFER_PROFILE')
        for key in profiles:
            if re.search(lossless_profile_name_pattern, key):
                config_db.delete_entry('BUFFER_PROFILE', key)

        # wait a sufficient amount of time to make sure all tables are removed from APPL_DB
        time.sleep(20)

    finally:
        # restart daemon
        cmd_runner("supervisorctl restart buffermgrd")

        time.sleep(20)
