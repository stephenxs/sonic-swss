#include <fstream>
#include <iostream>
#include <string.h>
#include "logger.h"
#include "dbconnector.h"
#include "producerstatetable.h"
#include "tokenize.h"
#include "ipprefix.h"
#include "timer.h"
#include "buffermgrdyn.h"
#include "bufferorch.h"
#include "exec.h"
#include "shellcmd.h"
#include "schema.h"
#include "warm_restart.h"

/*
 * Some Tips
 * 1. All keys in this file are in format of APPL_DB key.
 *    Key population:
 *        On receiving item update from CONFIG_DB: key has been transformed into the format of APPL_DB
 *        In intermal maps: table name removed from the index
 * 2. Maintain maps for pools, profiles and PGs in CONFIG_DB and APPL_DB
 * 3. Keys of maps in this file don't contain the TABLE_NAME
 * 3. 
 */
using namespace std;
using namespace swss;

BufferMgrDynamic::BufferMgrDynamic(DBConnector *cfgDb, DBConnector *stateDb, DBConnector *applDb, const vector<TableConnector> &tables, shared_ptr<vector<KeyOpFieldsValuesTuple>> gearboxInfo = nullptr) :
        Orch(tables),
        m_applDb(applDb),
        m_cfgPortTable(cfgDb, CFG_PORT_TABLE_NAME),
        m_cfgCableLenTable(cfgDb, CFG_PORT_CABLE_LEN_TABLE_NAME),
        m_cfgBufferProfileTable(cfgDb, CFG_BUFFER_PROFILE_TABLE_NAME),
        m_cfgBufferPgTable(cfgDb, CFG_BUFFER_PG_TABLE_NAME),
        m_cfgLosslessPgPoolTable(cfgDb, CFG_BUFFER_POOL_TABLE_NAME),
        m_cfgDefaultLosslessBufferParam(cfgDb, CFG_DEFAULT_LOSSLESS_BUFFER_PARAMETER),
        m_applBufferPoolTable(applDb, APP_BUFFER_POOL_TABLE_NAME),
        m_applBufferProfileTable(applDb, APP_BUFFER_PROFILE_TABLE_NAME),
        m_applBufferPgTable(applDb, APP_BUFFER_PG_TABLE_NAME),
        m_applBufferQueueTable(applDb, APP_BUFFER_QUEUE_TABLE_NAME),
        m_applBufferIngressProfileListTable(applDb, APP_BUFFER_PORT_INGRESS_PROFILE_LIST_NAME),
        m_applBufferEgressProfileListTable(applDb, APP_BUFFER_PORT_EGRESS_PROFILE_LIST_NAME),
        m_stateBufferMaximumTable(stateDb, STATE_BUFFER_MAXIMUM_VALUE_TABLE),
        m_stateBufferPoolTable(stateDb, STATE_BUFFER_POOL_TABLE_NAME),
        m_stateBufferProfileTable(stateDb, STATE_BUFFER_PROFILE_TABLE_NAME),
        m_applPortTable(applDb, APP_PORT_TABLE_NAME),
        m_portInitDone(false),
        m_firstTimeCalculateBufferPool(true)
{
    SWSS_LOG_ENTER();

    // Initialize the handler map
    initTableHandlerMap();
    parseGearboxInfo(gearboxInfo);

    string platform = getenv("ASIC_VENDOR") ? getenv("ASIC_VENDOR") : "";
    if (platform == "")
    {
        SWSS_LOG_ERROR("Platform environment variable is not defined");
    }

    string headroomSha, bufferpoolSha;
    string headroomPluginName = "buffer_headroom_" + platform + ".lua";
    string bufferpoolPluginName = "buffer_pool_" + platform + ".lua";
    string checkHeadroomPluginName = "buffer_check_headroom_" + platform + ".lua";

    try
    {
        string headroomLuaScript = swss::loadLuaScript(headroomPluginName);
        m_headroomSha = swss::loadRedisScript(applDb, headroomLuaScript);

        string bufferpoolLuaScript = swss::loadLuaScript(bufferpoolPluginName);
        m_bufferpoolSha = swss::loadRedisScript(applDb, bufferpoolLuaScript);

        string checkHeadroomLuaScript = swss::loadLuaScript(checkHeadroomPluginName);
        m_checkHeadroomSha = swss::loadRedisScript(applDb, checkHeadroomLuaScript);
    }
    catch (...)
    {
        SWSS_LOG_WARN("Lua scripts for buffer calculation were not loaded successfully");
    }

    // Init timer
    auto interv = timespec { .tv_sec = BUFFERMGR_TIMER_PERIOD, .tv_nsec = 0 };
    m_buffermgrPeriodtimer = new SelectableTimer(interv);
    auto executor = new ExecutableTimer(m_buffermgrPeriodtimer, this, "PORT_INIT_DONE_POLL_TIMER");
    Orch::addExecutor(executor);
    m_buffermgrPeriodtimer->start();

    // Try fetch mmu size from STATE_DB
    m_stateBufferMaximumTable.hget("global", "mmu_size", m_mmuSize);
}

void BufferMgrDynamic::parseGearboxInfo(shared_ptr<vector<KeyOpFieldsValuesTuple>> gearboxInfo)
{
    if (nullptr == gearboxInfo)
    {
        m_supportGearbox = false;
    }
    else
    {
        string gearboxModel;
        for (auto &kfv : *gearboxInfo)
        {
            auto table = parseObjectNameFromKey(kfvKey(kfv), 0);
            auto key = parseObjectNameFromKey(kfvKey(kfv), 1);

            if (table == STATE_PERIPHERAL_TABLE)
            {
                for (auto &fv: kfvFieldsValues(kfv))
                {
                    auto &field = fvField(fv);
                    auto &value = fvValue(fv);
                    SWSS_LOG_DEBUG("Processing table %s field:%s, value:%s", table.c_str(), field.c_str(), value.c_str());
                    if (field == "gearbox_delay")
                        m_gearboxDelay[key] = value;
                }
            }

            if (table == STATE_PORT_PERIPHERAL_TABLE)
            {
                if (key != "global")
                {
                    SWSS_LOG_ERROR("Port peripheral table: only global gearbox model is supported but got %s", key.c_str());
                    continue;
                }

                for (auto &fv: kfvFieldsValues(kfv))
                {
                    auto &field = fvField(fv);
                    auto &value = fvValue(fv);
                    SWSS_LOG_DEBUG("Processing table %s field:%s, value:%s", table.c_str(), field.c_str(), value.c_str());
                    if (fvField(fv) == "gearbox_model")
                        gearboxModel = fvValue(fv);
                }
            }
        }

        m_identifyGearboxDelay = m_gearboxDelay[gearboxModel];
        m_supportGearbox = false;
    }
}

void BufferMgrDynamic::initTableHandlerMap()
{
    m_bufferTableHandlerMap.insert(buffer_handler_pair(STATE_BUFFER_MAXIMUM_VALUE_TABLE, &BufferMgrDynamic::handleBufferMaxParam));
    m_bufferTableHandlerMap.insert(buffer_handler_pair(CFG_DEFAULT_LOSSLESS_BUFFER_PARAMETER, &BufferMgrDynamic::handleDefaultLossLessBufferParam));
    m_bufferTableHandlerMap.insert(buffer_handler_pair(CFG_BUFFER_POOL_TABLE_NAME, &BufferMgrDynamic::handleBufferPoolTable));
    m_bufferTableHandlerMap.insert(buffer_handler_pair(CFG_BUFFER_PROFILE_TABLE_NAME, &BufferMgrDynamic::handleBufferProfileTable));
    m_bufferTableHandlerMap.insert(buffer_handler_pair(CFG_BUFFER_QUEUE_TABLE_NAME, &BufferMgrDynamic::handleBufferQueueTable));
    m_bufferTableHandlerMap.insert(buffer_handler_pair(CFG_BUFFER_PG_TABLE_NAME, &BufferMgrDynamic::handleBufferPgTable));
    m_bufferTableHandlerMap.insert(buffer_handler_pair(CFG_BUFFER_PORT_INGRESS_PROFILE_LIST_NAME, &BufferMgrDynamic::handleBufferPortIngressProfileListTable));
    m_bufferTableHandlerMap.insert(buffer_handler_pair(CFG_BUFFER_PORT_EGRESS_PROFILE_LIST_NAME, &BufferMgrDynamic::handleBufferPortEgressProfileListTable));
    m_bufferTableHandlerMap.insert(buffer_handler_pair(CFG_PORT_TABLE_NAME, &BufferMgrDynamic::handlePortTable));
    m_bufferTableHandlerMap.insert(buffer_handler_pair(CFG_PORT_CABLE_LEN_TABLE_NAME, &BufferMgrDynamic::handleCableLenTable));
}

// APIs to handle variant kinds of keys

// Transform key from CONFIG_DB format to APPL_DB format
void BufferMgrDynamic::transformSeperator(string &name)
{
    size_t pos;
    while ((pos = name.find("|")) != string::npos)
        name.replace(pos, 1, ":");
}

// For string "TABLE_NAME|objectname", returns "objectname"
string BufferMgrDynamic::parseObjectNameFromKey(const string &key, size_t pos = 0)
{
    auto keys = tokenize(key, ':');
    if (pos >= keys.size())
    {
        SWSS_LOG_ERROR("Failed to fetch %lu-th sector of key %s", pos, key.c_str());
    }
    return keys[pos];
}

// For string "[foo]", returns "foo"
string BufferMgrDynamic::parseObjectNameFromReference(const string &reference)
{
    auto objName = reference.substr(1, reference.size() - 2);
    return parseObjectNameFromKey(objName, 1);
}

string BufferMgrDynamic::getDynamicProfileName(const string &speed, const string &cable, const string &gearbox_model)
{
    string buffer_profile_key;

    if (gearbox_model.empty())
    {
        buffer_profile_key = "pg_lossless_" + speed + "_" + cable + "_profile";
    }
    else
    {
        buffer_profile_key = "pg_lossless_" + speed + "_" + cable + "_" + gearbox_model + "_profile";
    }

    return buffer_profile_key;
}

string BufferMgrDynamic::getPgPoolMode()
{
    return m_bufferPoolLookup[INGRESS_LOSSLESS_PG_POOL_NAME].mode;
}

// Meta flows which are called by main flows
void BufferMgrDynamic::calculateHeadroomSize(const string &speed, const string &cable, const string &gearbox_model, buffer_profile_t &headroom)
{
    // Call vendor-specific lua plugin to calculate the xon, xoff, xon_offset, size and threshold
    vector<string> keys = {};
    vector<string> argv = {};

    keys.emplace_back(headroom.name);
    argv.emplace_back(speed);
    argv.emplace_back(cable);
    argv.emplace_back(m_identifyGearboxDelay);

    try
    {
        auto ret = swss::runRedisScript(*m_applDb, m_headroomSha, keys, argv);

        // The format of the result:
        // a list of strings containing key, value pairs with colon as separator
        // each is a field of the profile
        // "xon:18432"
        // "xoff:18432"
        // "size:36864"

        for ( auto i : ret)
        {
            auto pairs = tokenize(i, ':');
            if (pairs[0] == "xon")
                headroom.xon = pairs[1];
            if (pairs[0] == "xoff")
                headroom.xoff = pairs[1];
            if (pairs[0] == "size")
                headroom.size = pairs[1];
            if (pairs[0] == "xon_offset")
                headroom.xon_offset = pairs[1];
            if (pairs[0] == "threshold" && !headroom.static_configured)
                headroom.threshold = pairs[1];
        }
    }
    catch (...)
    {
        SWSS_LOG_WARN("Lua scripts for headroom calculation were not executed successfully");
    }
}

void BufferMgrDynamic::recalculateSharedBufferPool()
{
    try
    {
        vector<string> keys = {};
        vector<string> argv = {};

        auto ret = runRedisScript(*m_applDb, m_bufferpoolSha, keys, argv);

        // The format of the result:
        // a list of strings containing key, value pairs with colon as separator
        // each is the size of a buffer pool

        for ( auto i : ret)
        {
            auto pairs = tokenize(i, ':');
            auto poolName = pairs[0];

            if ("debug" != poolName)
            {
                auto &pool = m_bufferPoolLookup[pairs[0]];

                if (pool.total_size == pairs[1])
                    continue;

                pool.total_size = pairs[1];

                if (pool.initialized)
                {
                    updateBufferPoolToDb(poolName, pool, false);
                }
                else
                {
                    updateBufferPoolToDb(poolName, pool, true);
                    pool.initialized = true;
                }

                SWSS_LOG_NOTICE("Buffer pool %s had been updated with new size [%s]", poolName.c_str(), pool.total_size.c_str());
            }
            else
            {
                SWSS_LOG_INFO("Buffer pool debug info %s", i.c_str());
            }
        }
    }
    catch (...)
    {
        SWSS_LOG_WARN("Lua scripts for buffer calculation were not executed successfully");
    }
}

void BufferMgrDynamic::checkSharedBufferPoolSize()
{
    // PortInitDone indicates all steps of port initialization has been done
    // Only after that does the buffer pool size update starts
    if (!m_portInitDone)
    {
        vector<FieldValueTuple> values;
        if (m_applPortTable.get("PortInitDone", values))
        {
            SWSS_LOG_NOTICE("Buffer pools start to be updated");
            m_portInitDone = true;
        }
        else
        {
            if (m_firstTimeCalculateBufferPool)
            {
                recalculateSharedBufferPool();
                m_firstTimeCalculateBufferPool = false;
                SWSS_LOG_NOTICE("Buffer pool update defered because port is still under initialization, start polling timer");
            }

            return;
        }
    }

    if (!m_mmuSize.empty())
        recalculateSharedBufferPool();
}

// For buffer pool, only size can be updated on-the-fly
void BufferMgrDynamic::updateBufferPoolToDb(const string &name, const buffer_pool_t &pool, bool create)
{
    vector<FieldValueTuple> fvVector;

    if (create)
    {
        if (pool.ingress)
            fvVector.emplace_back(make_pair("type", "ingress"));
        else
            fvVector.emplace_back(make_pair("type", "egress"));

        fvVector.emplace_back(make_pair("mode", pool.mode));

        SWSS_LOG_INFO("Buffer pool %s is initialized", name.c_str());
    }

    fvVector.emplace_back(make_pair("size", pool.total_size));

    m_applBufferPoolTable.set(name, fvVector);

    if (!create)
    {
        if (pool.ingress)
            fvVector.emplace_back(make_pair("type", "ingress"));
        else
            fvVector.emplace_back(make_pair("type", "egress"));

        fvVector.emplace_back(make_pair("mode", pool.mode));
    }

    m_stateBufferPoolTable.set(name, fvVector);
}

void BufferMgrDynamic::updateBufferProfileToDb(const string &name, const buffer_profile_t &profile, bool state_db_only = false)
{
    vector<FieldValueTuple> fvVector;
    string mode = getPgPoolMode();

    if (mode.empty())
    {
        // this should never happen if switch initialized properly
        SWSS_LOG_ERROR("PG lossless pool is not yet created, creating profile %s failed", name.c_str());
        return;
    }

    // profile threshold field name
    mode += "_th";
    string pg_pool_reference = string(APP_BUFFER_POOL_TABLE_NAME) +
        m_applBufferProfileTable.getTableNameSeparator() +
        INGRESS_LOSSLESS_PG_POOL_NAME;

    fvVector.emplace_back(make_pair("xon", profile.xon));
    if (!profile.xon_offset.empty()) {
        fvVector.emplace_back(make_pair("xon_offset", profile.xon_offset));
    }
    fvVector.emplace_back(make_pair("xoff", profile.xoff));
    fvVector.emplace_back(make_pair("size", profile.size));
    fvVector.emplace_back(make_pair("pool", "[" + pg_pool_reference + "]"));
    fvVector.emplace_back(make_pair(mode, profile.threshold));

    if (!state_db_only)
    {
        m_applBufferProfileTable.set(name, fvVector);
    }

    if (profile.state == PROFILE_STALE)
    {
        fvVector.emplace_back(make_pair("gc_timeout", to_string(profile.stale_timeout*BUFFERMGR_TIMER_PERIOD)));
    }
    m_stateBufferProfileTable.set(name, fvVector);
}

// Database operation
// Set/remove BUFFER_PG table entry
void BufferMgrDynamic::updateBufferPgToDb(const string &key, const string &profile, bool add)
{
    if (add)
    {
        vector<FieldValueTuple> fvVector;

        fvVector.clear();

        string profile_ref = string("[") +
            APP_BUFFER_PROFILE_TABLE_NAME +
            m_applBufferPgTable.getTableNameSeparator() +
            profile +
            "]";

        fvVector.clear();
 
        fvVector.push_back(make_pair("profile", profile_ref));
        m_applBufferPgTable.set(key, fvVector);
    }
    else
    {
        m_applBufferPgTable.del(key);
    }
}

// We have to check the headroom ahead of applying them
// The passed-in headroom is calculated for the checking.
task_process_status BufferMgrDynamic::allocateProfile(const string &speed, const string &cable, const string &gearbox_model, string &profile_name)
{
    // Create record in BUFFER_PROFILE table
    // key format is pg_lossless_<speed>_<cable>_profile
    string buffer_profile_key = getDynamicProfileName(speed, cable, gearbox_model);

    SWSS_LOG_INFO("Allocating new BUFFER_PROFILE %s", buffer_profile_key.c_str());

    // check if profile already exists - if yes - skip creation
    auto profileRef = m_bufferProfileLookup.find(buffer_profile_key);
    if (profileRef == m_bufferProfileLookup.end()
        || PROFILE_PARTIAL_INITIALIZED == profileRef->second.state)
    {
        auto &profile = m_bufferProfileLookup[buffer_profile_key];
        SWSS_LOG_NOTICE("Creating new profile '%s'", buffer_profile_key.c_str());

        string mode = getPgPoolMode();
        if (mode.empty())
        {
            SWSS_LOG_NOTICE("BUFFER_PROFILE %s cannot be created because the buffer pool isn't ready", buffer_profile_key.c_str());
            return task_process_status::task_need_retry;
        }

        // Call vendor-specific lua plugin to calculate the xon, xoff, xon_offset, size
        // Pay attention, the threshold can contain valid value
        calculateHeadroomSize(speed, cable, gearbox_model, profile);

        if (PROFILE_PARTIAL_INITIALIZED != profile.state)
        {
            profile.dynamic_calculated = true;
            profile.static_configured = false;
        }
        profile.name = buffer_profile_key;
        profile.state = PROFILE_NORMAL;

        updateBufferProfileToDb(buffer_profile_key, profile);

        SWSS_LOG_NOTICE("BUFFER_PROFILE %s has been created successfully", buffer_profile_key.c_str());
        SWSS_LOG_DEBUG("New profile created %s according to (%s %s %s): xon %s xoff %s size %s",
                       buffer_profile_key.c_str(),
                       speed.c_str(), cable.c_str(), gearbox_model.c_str(),
                       profile.xon.c_str(), profile.xoff.c_str(), profile.size.c_str());
    }
    else if (profileRef->second.state == PROFILE_STALE)
    {
        auto &profile = profileRef->second;
        SWSS_LOG_NOTICE("BUFFER_PROFILE %s is in STALE state, reuse it", buffer_profile_key.c_str());
        profile.state = PROFILE_NORMAL;
        profile.stale_timeout = 0;
        updateBufferProfileToDb(buffer_profile_key, profile, true);
    }
    else
    {
        SWSS_LOG_NOTICE("Reusing existing profile '%s'", buffer_profile_key.c_str());
    }

    profile_name = buffer_profile_key;

    return task_process_status::task_success;
}

void BufferMgrDynamic::markProfileToBeReleased(const string &profile_name)
{
    // Crete record in BUFFER_PROFILE table
    // key format is pg_lossless_<speed>_<cable>_profile
    string buffer_profile_key;
    vector<FieldValueTuple> fvVector;
    auto &profile = m_bufferProfileLookup[profile_name];

    if (profile.static_configured)
    {
        // Check whether the profile is statically configured.
        // This means:
        // 1. It's a statically configured profile, headroom override
        // 2. It's dynamically calculated headroom with static threshold (alpha)
        // In this case we won't remove the entry from the local cache even if it's dynamically calculated
        // because the speed, cable length and cable model are fixed the headroom info will always be valid once calculated.
        SWSS_LOG_INFO("Unable to release profile %s because it's a static configured profile", profile_name.c_str());
        return;
    }

    if (profile.state == PROFILE_STALE)
    {
        SWSS_LOG_INFO("BUFFER_PROFILE %s is already in STALE state when markRelease", profile_name.c_str());
        return;
    }

    // Check whether it's referenced anymore by other PGs.
    if (!profile.port_pgs.empty())
    {
        for (auto &pg : profile.port_pgs)
        {
            SWSS_LOG_INFO("Unable to release profile %s because it's still referenced by %s (and others)",
                          profile_name.c_str(), pg.c_str());
            return;
        }
    }

    profile.state = PROFILE_STALE;
    profile.stale_timeout = PROFILE_STALE_TIMEOUT;
    SWSS_LOG_INFO("BUFFER_PROFILE %s enters STALE state", profile_name.c_str());

    updateBufferProfileToDb(profile_name, profile, true);
}

void BufferMgrDynamic::releaseProfile(buffer_profile_t &profile)
{
    const string &profile_name = profile.name;

    if (--profile.stale_timeout > 0)
    {
        SWSS_LOG_INFO("BUFFER_PROFILE %s is STALE %d", profile_name.c_str(), profile.stale_timeout);
        updateBufferProfileToDb(profile_name, profile, true);
        return;
    }

    profile.port_pgs.clear();

    m_applBufferProfileTable.del(profile_name);

    m_stateBufferProfileTable.del(profile_name);

    // Don't remove profile within iteration. Mark them instead, and then remove them outside the iteration
    m_bufferProfilePendingRemoved.insert(profile_name);

    SWSS_LOG_NOTICE("BUFFER_PROFILE %s has been released successfully", profile_name.c_str());
}

void BufferMgrDynamic::checkPendingRemovedProfiles()
{
    for (auto &it : m_bufferProfileLookup)
    {
        auto &profile = it.second;

        if (profile.state == PROFILE_STALE)
            releaseProfile(profile);
    }

    for (auto &profileName : m_bufferProfilePendingRemoved)
    {
        m_bufferProfileLookup.erase(profileName);
    }

    m_bufferProfilePendingRemoved.clear();
}

bool BufferMgrDynamic::isHeadroomResourceValid(const string &port, buffer_profile_t &profile_info, string lossy_pg_changed = "")
{
    //port: used to check whether its a split port
    //pg_changed: which pg's profile has been changed?
    //profile_info: the new profile

    // 1. Get all the pgs associated to the port
    auto portPgs = m_portPgLookup[port];

    // 2. Accumulate all the headroom sizes allocated for each pg
    // Iterate all the pgs, if
    // for each pg in m_portPgLookup[port] do
    //     if pg is lossless or pg == lossy_pg_changed, take profile_info
    //     else take m_bufferProfileLookup[pg.profile_name]

    vector<string> keys = {port};
    vector<string> argv = {};

    try
    {
        auto ret = runRedisScript(*m_applDb, m_checkHeadroomSha, keys, argv);

        // The format of the result:
        // a list of strings containing key, value pairs with colon as separator
        // each is the size of a buffer pool

        for ( auto i : ret)
        {
            auto pairs = tokenize(i, ':');
            if ("result" != pairs[0])
                continue;

            if ("true" != pairs[1])
            {
                SWSS_LOG_ERROR("Unable to update profile for port %s. Accumulative headroom size exceeds limit", port.c_str());
                return false;
            }
        }
    }
    catch (...)
    {
        SWSS_LOG_WARN("Lua scripts for buffer calculation were not executed successfully");
    }

    return true;
}

//Called when speed/cable length updated from CONFIG_DB
task_process_status BufferMgrDynamic::doSpeedOrCableLengthUpdateTask(const string &port, const string &speed, const string &cable_length)
{
    vector<FieldValueTuple> fvVector;
    string old_cable, old_speed;
    string separator = m_cfgBufferProfileTable.getTableNameSeparator();
    string key_pattern = CFG_BUFFER_PG_TABLE_NAME + separator + port + separator + "*";
    string value;
    string newProfile, oldProfile;
    port_info_t &portInfo = m_portInfoLookup[port];
    buffer_profile_t pgProfileNew;
    string &gearbox_model = portInfo.gearbox_model;
    bool isNewProfileCalculated = false, isHeadroomUpdated = false;
    buffer_pg_lookup_t &portPgs = m_portPgLookup[port];

    oldProfile = portInfo.profile_name;
    if (!oldProfile.empty())
    {
        old_cable = portInfo.cable_length;
        old_speed = portInfo.speed;
        portInfo.profile_name = "";
    }

    // Iterate all the lossless PGs configured on this port
    for (auto it = portPgs.begin(); it != portPgs.end(); ++it)
    {
        auto &key = it->first;
        auto &portPg = it->second;

        if (!portPg.dynamic_calculated)
        {
            SWSS_LOG_DEBUG("Skip no-dynamic pg %s (profile %s)", key.c_str(), portPg.profile_name.c_str());
            continue;
        }

        SWSS_LOG_DEBUG("Handling PG %s port %s", key.c_str(), port.c_str());

        if (!isNewProfileCalculated)
        {
            //Calculate new headroom size
            isNewProfileCalculated = true;

            auto rc = allocateProfile(speed, cable_length, gearbox_model, newProfile);
            if (task_process_status::task_success != rc)
                return rc;

            //Calculate whether accumulative headroom size exceeds the maximum value
            //Abort if it does
            if (!isHeadroomResourceValid(port, m_bufferProfileLookup[newProfile]))
            {
                SWSS_LOG_ERROR("Update speed (%s) and cable length (%s) for port %s failed, accumulative headroom size exceeds the limit",
                               speed.c_str(), cable_length.c_str(), port.c_str());
                return task_process_status::task_failed;
            }
        }

        if (newProfile != oldProfile)
        {
            // Need to remove the reference to the old profile
            // and create the reference to the new one
            m_bufferProfileLookup[oldProfile].port_pgs.erase(key);
            m_bufferProfileLookup[newProfile].port_pgs.insert(key);
            SWSS_LOG_DEBUG("Move profile reference for %s from [%s] to [%s]", key.c_str(), oldProfile.c_str(), newProfile.c_str());

            // buffer pg needs to be updated as well
            portPg.profile_name = newProfile;
        }

        //appl_db Database operation: set item BUFFER_PG|<port>|<pg>
        updateBufferPgToDb(key, newProfile, true);
        isHeadroomUpdated = true;
    }

    portInfo.speed = speed;
    portInfo.cable_length = cable_length;
    portInfo.gearbox_model = gearbox_model;

    if (isHeadroomUpdated)
    {
        checkSharedBufferPoolSize();

        //update internal map
        portInfo.profile_name = newProfile;
        portInfo.state = PORT_DYNAMIC_HEADROOM;
    }
    else
    {
        SWSS_LOG_DEBUG("Nothing to do for port %s since no PG configured on it", port.c_str());
        portInfo.state = PORT_READY;
    }

    //Remove the old profile which is probably not referenced anymore.
    if (!oldProfile.empty() && oldProfile != newProfile)
        markProfileToBeReleased(oldProfile);

    return task_process_status::task_success;
}

// Main flows

// Update lossless pg on a port after an PG has been installed on the port
// Called when pg updated from CONFIG_DB
// Key format: BUFFER_PG:<port>:<pg>
task_process_status BufferMgrDynamic::doUpdatePgTask(const string &pg_key, string &profile)
{
    auto port = parseObjectNameFromKey(pg_key);
    string value;
    port_info_t &portInfo = m_portInfoLookup[port];
    task_process_status task_status = task_process_status::task_success;

    switch (portInfo.state)
    {
    case PORT_DYNAMIC_HEADROOM:
        // Headroom information has already been deployed for the port, no need to recaluclate
        // but need to check accumulative headroom size limit.
        if (!isHeadroomResourceValid(port, m_bufferProfileLookup[portInfo.profile_name]))
        {
            SWSS_LOG_ERROR("Failed to set BUFFER_PG for %s to profile %s, accumulative headroom exceeds limit",
                           pg_key.c_str(), portInfo.profile_name.c_str());
            return task_process_status::task_failed;
        }

        SWSS_LOG_NOTICE("Set BUFFER_PG for %s to profile %s", pg_key.c_str(), portInfo.profile_name.c_str());
        updateBufferPgToDb(pg_key, portInfo.profile_name, true);

        m_bufferProfileLookup[portInfo.profile_name].port_pgs.insert(pg_key);

        // Recalculate pool size
        checkSharedBufferPoolSize();

        break;

    case PORT_READY:
        // Not having profile_name but both speed and cable length have been configured for that port
        // This is because the first PG on that port is configured after speed, cable length configured
        // Just regenerate the profile
        task_status = doSpeedOrCableLengthUpdateTask(port, portInfo.speed, portInfo.cable_length);
        if (task_status != task_process_status::task_success)
            return task_status;

        // On success, the portInfo.profile_name should be updated
        m_bufferProfileLookup[portInfo.profile_name].port_pgs.insert(pg_key);

        break;

    case PORT_HEADROOM_OVERRIDE:
        SWSS_LOG_ERROR("Failed to set BUFFER_PG for %s to dynamic profile, headroom override should be removed first",
                       pg_key.c_str());
        return task_process_status::task_failed;

	case PORT_INITIALIZING:
    default:
            // speed and cable length hasn't been configured
            // In that case, we just skip the this update and return success.
            // It will be handled after speed and cable length configured.
        SWSS_LOG_NOTICE("Skip setting BUFFER_PG for %s because its profile hasn't created", pg_key.c_str());
        return task_process_status::task_success;
    }

    profile = portInfo.profile_name;

    return task_process_status::task_success;
}

//Remove the currently configured lossless pg
task_process_status BufferMgrDynamic::doRemovePgTask(const string &pg_key)
{
    auto port = parseObjectNameFromKey(pg_key);
    auto pgs = parseObjectNameFromKey(pg_key, 1);
    port_info_t &portInfo = m_portInfoLookup[port];

    // Remove the PG from APPL_DB
    string null_str("");
    updateBufferPgToDb(pg_key, null_str, false);

    SWSS_LOG_NOTICE("Remove BUFFER_PG %s (profile %s)", pg_key.c_str(), portInfo.profile_name.c_str());

    if (PORT_DYNAMIC_HEADROOM != portInfo.state)
    {
        SWSS_LOG_INFO("Port state isn't DYNAMIC_HEADROOM, no further action");
        return task_process_status::task_success;
    }

    // recalculate pool size
    checkSharedBufferPoolSize();

    // Update port state
    bool dynamic = false;
    for ( auto &it: m_portPgLookup[port])
    {
        auto &pg = it.second;
        auto &key = it.first;

        if (key == pg_key)
            continue;

        if (pg.dynamic_calculated)
        {
            dynamic = true;
            SWSS_LOG_DEBUG("BUFFER_PG: Dynamic profile %s pg %s is still on port %s",pg.profile_name.c_str(), key.c_str(), port.c_str());
            break;
        }
    }

    if (!dynamic)
    {
        portInfo.state = PORT_READY;
        SWSS_LOG_NOTICE("No lossless PG configured on port %s anymore, try removing the original profile %s",
                        port.c_str(), portInfo.profile_name.c_str());
        markProfileToBeReleased(portInfo.profile_name);
        portInfo.profile_name = "";
    }

    return task_process_status::task_success;
}

// Update headroom override
task_process_status BufferMgrDynamic::doUpdateHeadroomOverrideTask(const string &pg_key, const string &profile)
{
    string port = parseObjectNameFromKey(pg_key);
    auto &portInfo = m_portInfoLookup[port];

    if (PORT_DYNAMIC_HEADROOM == portInfo.state)
    {
        SWSS_LOG_ERROR("Unable to configure profile %s on %s, dynamically calculated profile should be removed first",
                       profile.c_str(), pg_key.c_str());
        return task_process_status::task_failed;
    }

    auto searchRef = m_bufferProfileLookup.find(profile);
    if (searchRef == m_bufferProfileLookup.end())
    {
        SWSS_LOG_WARN("Unable to configure profile %s on %s, profile hasn't been configured yet",
                       profile.c_str(), pg_key.c_str());
        return task_process_status::task_need_retry;
    }

    buffer_profile_t &pgProfile = searchRef->second;

    //check whether the accumulative headroom exceeds the limit
    if (!isHeadroomResourceValid(port, pgProfile, pg_key))
    {
        SWSS_LOG_ERROR("Unable to configure profile %s on %s, accumulative headroom size exceeds limit",
                       profile.c_str(), pg_key.c_str());
        return task_process_status::task_failed;
    }

    portInfo.profile_name = profile;
    portInfo.state = PORT_HEADROOM_OVERRIDE;

    updateBufferPgToDb(pg_key, profile, true);

    pgProfile.port_pgs.insert(pg_key);

    SWSS_LOG_NOTICE("Headroom override %s has been configured on %s", profile.c_str(), pg_key.c_str());

    checkSharedBufferPoolSize();

    return task_process_status::task_success;
}

task_process_status BufferMgrDynamic::doRemoveHeadroomOverrideTask(const string &pg_key, const string &profile)
{
    string port = parseObjectNameFromKey(pg_key);
    const buffer_pg_lookup_t &pgLookup = m_portPgLookup[port];
    bool noLossless = true;

    updateBufferPgToDb(pg_key, profile, false);

    SWSS_LOG_NOTICE("Headroom override %s has been removed on %s", profile.c_str(), pg_key.c_str());

    for (auto &it : pgLookup)
    {
        auto &portPg = it.second;
        auto &key = it.first;

        if (key == pg_key)
            continue;

        if (portPg.lossless && portPg.profile_name != profile)
        {
            SWSS_LOG_DEBUG("Lossless PG %s is configured on the port %s", it.first.c_str(), port.c_str());
            noLossless = false;
            break;
        }
    }

    if (noLossless)
    {
        auto &portInfo = m_portInfoLookup[port];
        portInfo.profile_name = "";
        if (!portInfo.speed.empty() && !portInfo.cable_length.empty())
            portInfo.state = PORT_READY;
        else
            portInfo.state = PORT_INITIALIZING;
        SWSS_LOG_DEBUG("Profile %s has been removed from %s", profile.c_str(), port.c_str());
    }

    checkSharedBufferPoolSize();

    return task_process_status::task_success;
}

task_process_status BufferMgrDynamic::doUpdateStaticProfileTask(const string &profileName, buffer_profile_t &profile)
{
    auto &profileToMap = profile.port_pgs;
    set<string> portsChecked;

    for (auto &key : profileToMap)
    {
        auto port = parseObjectNameFromKey(key);

        if (portsChecked.find(port) != portsChecked.end())
            continue;

        SWSS_LOG_DEBUG("Checking PG %s for profile %s", key.c_str(), profileName.c_str());

        if (!isHeadroomResourceValid(port, profile))
        {
            SWSS_LOG_ERROR("BUFFER_PROFILE %s cannot be updated because %s referencing it violates the resource limitation",
                           profileName.c_str(), key.c_str());
            return task_process_status::task_failed;
        }

        portsChecked.insert(port);
    }

    updateBufferProfileToDb(profileName, profile);

    m_bufferProfileLookup[profileName] = profile;

    checkSharedBufferPoolSize();

    return task_process_status::task_success;
}

task_process_status BufferMgrDynamic::handleBufferMaxParam(Consumer &consumer)
{
    auto it = consumer.m_toSync.begin();
    KeyOpFieldsValuesTuple t = it->second;
    string op = kfvOp(t);

    if (op == SET_COMMAND)
    {
        for (auto i : kfvFieldsValues(t))
        {
            if (fvField(i) == "mmu_size")
            {
                m_mmuSize = fvValue(i);
                SWSS_LOG_DEBUG("Handling Default Lossless Buffer Param table field mmu_size %s", m_mmuSize.c_str());
            }
        }
    }

    return task_process_status::task_success;
}

task_process_status BufferMgrDynamic::handleDefaultLossLessBufferParam(Consumer &consumer)
{
    auto it = consumer.m_toSync.begin();
    KeyOpFieldsValuesTuple t = it->second;
    string op = kfvOp(t);

    if (op == SET_COMMAND)
    {
        for (auto i : kfvFieldsValues(t))
        {
            if (fvField(i) == "default_dynamic_th")
            {
                m_defaultThreshold = fvValue(i);
                SWSS_LOG_DEBUG("Handling Buffer Maximum value table field default_dynamic_th value %s", m_defaultThreshold.c_str());
            }
        }
    }

    return task_process_status::task_success;
}

task_process_status BufferMgrDynamic::handleCableLenTable(Consumer &consumer)
{
    auto it = consumer.m_toSync.begin();
    KeyOpFieldsValuesTuple t = it->second;
    string op = kfvOp(t);

    task_process_status task_status = task_process_status::task_success;
    int failed_item_count = 0;
    if (op == SET_COMMAND)
    {
        for (auto i : kfvFieldsValues(t))
        {
            // receive and cache cable length table
            auto &port = fvField(i);
            auto &cable_length = fvValue(i);
            port_info_t &portInfo = m_portInfoLookup[port];
            string &speed = portInfo.speed;

            SWSS_LOG_DEBUG("Handling CABLE_LENGTH table field %s length %s", port.c_str(), cable_length.c_str());
            SWSS_LOG_DEBUG("Port Info for %s before handling %s %s %s %s",
                           port.c_str(),
                           portInfo.speed.c_str(), portInfo.cable_length.c_str(), portInfo.gearbox_model.c_str(), portInfo.profile_name.c_str());

            if (portInfo.cable_length == cable_length)
            {
                continue;
            }

            portInfo.cable_length = cable_length;
            if (speed.empty())
            {
                SWSS_LOG_WARN("Speed for %s hasn't configured yet, unable to calculate headroom", port.c_str());
                // We don't retry here because it doesn't make sense until the speed is configured.
                continue;
            }

            SWSS_LOG_INFO("Updating BUFFER_PG for port %s due to cable length updated", port.c_str());

            //Try updating the buffer information
            switch (portInfo.state)
            {
            case PORT_INITIALIZING:
                portInfo.state = PORT_READY;
                task_status = doSpeedOrCableLengthUpdateTask(port, speed, cable_length);
                break;

            case PORT_READY:
            case PORT_DYNAMIC_HEADROOM:
                task_status = doSpeedOrCableLengthUpdateTask(port, speed, cable_length);
                break;

            case PORT_HEADROOM_OVERRIDE:
                break;
            }

            switch (task_status)
            {
            case task_process_status::task_need_retry:
                return task_status;
            case task_process_status::task_failed:
                // We shouldn't return directly here. Because doing so will cause the following items lost
                failed_item_count++;
                break;
            default:
                break;
            }

            SWSS_LOG_DEBUG("Port Info for %s after handling speed %s cable %s gb %s profile %s",
                           port.c_str(),
                           portInfo.speed.c_str(), portInfo.cable_length.c_str(), portInfo.gearbox_model.c_str(), portInfo.profile_name.c_str());
        }
    }

    if (failed_item_count > 0)
    {
        return task_process_status::task_failed;
    }

    return task_process_status::task_success;
}

task_process_status BufferMgrDynamic::handlePortTable(Consumer &consumer)
{
    auto it = consumer.m_toSync.begin();
    KeyOpFieldsValuesTuple t = it->second;
    auto &port = kfvKey(t);
    string op = kfvOp(t);

    SWSS_LOG_DEBUG("processing command:%s PORT table key %s", op.c_str(), port.c_str());

    port_info_t &portInfo = m_portInfoLookup[port];

    SWSS_LOG_DEBUG("Port Info for %s before handling %s %s %s %s",
                   port.c_str(),
                   portInfo.speed.c_str(), portInfo.cable_length.c_str(), portInfo.gearbox_model.c_str(), portInfo.profile_name.c_str());

    task_process_status task_status = task_process_status::task_success;
    int failed_item_count = 0;
    if (op == SET_COMMAND)
    {
        for (auto i : kfvFieldsValues(t))
        {
            if (fvField(i) == "speed")
            {
                string &cable_length = portInfo.cable_length;
                string &speed = fvValue(i);

                if (cable_length.empty())
                {
                    portInfo.speed = speed;
                    SWSS_LOG_WARN("Cable length for %s hasn't configured yet, unable to calculate headroom", port.c_str());
                    // We don't retry here because it doesn't make sense until the cable length is configured.
                    return task_process_status::task_success;
                }

                SWSS_LOG_INFO("Updating BUFFER_PG for port %s due to speed updated", port.c_str());

                //Try updating the buffer information
                switch (portInfo.state)
                {
                case PORT_INITIALIZING:
                    portInfo.state = PORT_READY;
                    task_status = doSpeedOrCableLengthUpdateTask(port, speed, cable_length);
                    break;

                case PORT_READY:
                case PORT_DYNAMIC_HEADROOM:
                    task_status = doSpeedOrCableLengthUpdateTask(port, speed, cable_length);
                    break;

                case PORT_HEADROOM_OVERRIDE:
                    break;
                }

                switch (task_status)
                {
                case task_process_status::task_failed:
                    failed_item_count++;
                    break;
                case task_process_status::task_need_retry:
                    return task_status;
                default:
                    break;
                }

                SWSS_LOG_DEBUG("Port Info for %s after handling speed %s cable %s gb %s profile %s",
                               port.c_str(),
                               portInfo.speed.c_str(), portInfo.cable_length.c_str(), portInfo.gearbox_model.c_str(), portInfo.profile_name.c_str());
            }
            else if (fvField(i) == "admin_status")
            {
                if (!portInfo.profile_name.empty())
                {
                    SWSS_LOG_INFO("Recalculate shared buffer pool size due to port %s's admin_status updated", port.c_str());
                    checkSharedBufferPoolSize();
                }
            }
        }
    }

    if (failed_item_count > 0)
    {
        return task_process_status::task_failed;
    }

    return task_process_status::task_success;
}

task_process_status BufferMgrDynamic::handleBufferPoolTable(Consumer &consumer)
{
    SWSS_LOG_ENTER();
    auto it = consumer.m_toSync.begin();
    KeyOpFieldsValuesTuple tuple = it->second;
    string &pool = kfvKey(tuple);
    string op = kfvOp(tuple);
    vector<FieldValueTuple> fvVector;

    SWSS_LOG_DEBUG("processing command:%s table BUFFER_POOL key %s", op.c_str(), pool.c_str());
    if (op == SET_COMMAND)
    {
        // For set command:
        // 1. Create the corresponding table entries in APPL_DB
        // 2. Record the table in the internal cache m_bufferPoolLookup
        buffer_pool_t &bufferPool = m_bufferPoolLookup[pool];

        bufferPool.initialized = false;
        bufferPool.dynamic_size = true;
        for (auto i = kfvFieldsValues(tuple).begin(); i != kfvFieldsValues(tuple).end(); i++)
        {
            string &field = fvField(*i);
            string &value = fvValue(*i);

            SWSS_LOG_DEBUG("field:%s, value:%s", field.c_str(), value.c_str());
            if (field == buffer_size_field_name)
            {
                bufferPool.dynamic_size = false;
            }
            if (field == buffer_pool_xoff_field_name)
            {
                bufferPool.xoff = value;
            }
            if (field == buffer_pool_mode_field_name)
            {
                bufferPool.mode = value;
            }
            if (field == buffer_pool_type_field_name)
            {
                bufferPool.ingress = (value == buffer_value_ingress);
            }
            fvVector.emplace_back(FieldValueTuple(field, value));
            SWSS_LOG_INFO("Inserting BUFFER_POOL table field %s value %s", field.c_str(), value.c_str());
        }
        if (!bufferPool.dynamic_size)
        {
            bufferPool.initialized = true;
            m_applBufferPoolTable.set(pool, fvVector);
            m_stateBufferPoolTable.set(pool, fvVector);
        }
    }
    else if (op == DEL_COMMAND)
    {
        // How do we handle dependency?
        m_applBufferPoolTable.del(pool);
        m_stateBufferPoolTable.del(pool);
        m_bufferPoolLookup.erase(pool);
    }
    else
    {
        SWSS_LOG_ERROR("Unknown operation type %s", op.c_str());
        return task_process_status::task_invalid_entry;
    }
    return task_process_status::task_success;
}

task_process_status BufferMgrDynamic::handleBufferProfileTable(Consumer &consumer)
{
    SWSS_LOG_ENTER();
    auto it = consumer.m_toSync.begin();
    KeyOpFieldsValuesTuple tuple = it->second;
    string profileName = kfvKey(tuple);
    string op = kfvOp(tuple);
    vector<FieldValueTuple> fvVector;

    SWSS_LOG_DEBUG("processing command:%s BUFFER_PROFILE table key %s", op.c_str(), profileName.c_str());
    if (op == SET_COMMAND)
    {
        //For set command:
        //1. Create the corresponding table entries in APPL_DB
        //2. Record the table in the internal cache m_bufferProfileLookup
        buffer_profile_t profileApp;

        profileApp.static_configured = true;
        profileApp.dynamic_calculated = false;
        profileApp.lossless = false;
        profileApp.state = PROFILE_INITIALIZING;
        for (auto i = kfvFieldsValues(tuple).begin(); i != kfvFieldsValues(tuple).end(); i++)
        {
            string &field = fvField(*i);
            string &value = fvValue(*i);

            SWSS_LOG_DEBUG("field:%s, value:%s", field.c_str(), value.c_str());
            if (field == buffer_pool_field_name)
            {
                if (!value.empty())
                {
                    transformSeperator(value);
                    auto poolName = parseObjectNameFromReference(value);
                    auto poolRef = m_bufferPoolLookup.find(poolName);
                    if (poolRef == m_bufferPoolLookup.end())
                    {
                        SWSS_LOG_WARN("Pool %s hasn't been configured yet, skip", poolName.c_str());
                        return task_process_status::task_need_retry;
                    }
                    profileApp.pool_name = poolName;
                    profileApp.ingress = poolRef->second.ingress;
                }
                else
                {
                    SWSS_LOG_ERROR("Pool for BUFFER_PROFILE %s hasn't been specified", field.c_str());
                    return task_process_status::task_failed;
                }
            }
            if (field == buffer_xon_field_name)
            {
                profileApp.xon = value;
            }
            if (field == buffer_xoff_field_name)
            {
                profileApp.xoff = value;
                profileApp.lossless = true;
            }
            if (field == buffer_xon_offset_field_name)
            {
                profileApp.xon_offset = value;
            }
            if (field == buffer_size_field_name)
            {
                profileApp.size = value;
            }
            if (field == buffer_dynamic_th_field_name)
            {
                profileApp.threshold = value;
            }
            if (field == buffer_static_th_field_name)
            {
                profileApp.threshold = value;
            }
            if (field == buffer_headroom_type_field_name)
            {
                profileApp.dynamic_calculated = (value == "dynamic");
                if (profileApp.dynamic_calculated)
                {
                    profileApp.lossless = true;
                }
            }
            fvVector.emplace_back(FieldValueTuple(field, value));
            SWSS_LOG_INFO("Inserting BUFFER_PROFILE table field %s value %s", field.c_str(), value.c_str());
        }
        // don't insert dynamically calculated profiles into APPL_DB
        if (profileApp.lossless && profileApp.ingress)
        {
            if (profileApp.dynamic_calculated)
            {
                auto profileRef = m_bufferProfileLookup.find(profileName);
                if (profileRef == m_bufferProfileLookup.end())
                {
                    profileApp.state = PROFILE_PARTIAL_INITIALIZED;
                    m_bufferProfileLookup[profileName] = profileApp;
                    SWSS_LOG_NOTICE("Dynamically profile %s won't be deployed to APPL_DB until referenced by a port",
                                    profileName.c_str());
                }
                else
                {
                    auto &profile = profileRef->second;
                    profile.threshold = profileApp.threshold;
                    profile.static_configured = true;
                    if (PROFILE_NORMAL == profile.state)
                    {
                        updateBufferProfileToDb(profileName, profile);
                        SWSS_LOG_NOTICE("Non default threshold configured on dynamically profile %s, update APPL_DB",
                                        profileName.c_str());
                    }
                    // Don't push to db when profile is in other state like
                    // PARTIAL_INITIALIZED or STALE
                }
            }
            else
            {
                profileApp.state = PROFILE_NORMAL;
                doUpdateStaticProfileTask(profileName, profileApp);
                SWSS_LOG_NOTICE("BUFFER_PROFILE %s has been inserted into APPL_DB", profileName.c_str());
                SWSS_LOG_DEBUG("BUFFER_PROFILE %s for headroom override has been stored internally: [pool %s xon %s xoff %s size %s]",
                               profileName.c_str(),
                               profileApp.pool_name.c_str(), profileApp.xon.c_str(), profileApp.xoff.c_str(), profileApp.size.c_str());
            }
        }
        else
        {
            m_applBufferProfileTable.set(profileName, fvVector);
            SWSS_LOG_NOTICE("BUFFER_PROFILE %s has been inserted into APPL_DB directly", profileName.c_str());

            m_stateBufferProfileTable.set(profileName, fvVector);
            m_bufferProfileIgnored.insert(profileName);
        }
    }
    else if (op == DEL_COMMAND)
    {
        // For del command:
        // Check whether it is referenced by port. If yes, return "need retry" and exit
        // Typically, the referencing occurs when headroom override configured
        // Remove it from APPL_DB and internal cache

        auto profileRef = m_bufferProfileLookup.find(profileName);
        if (profileRef != m_bufferProfileLookup.end())
        {
            auto &profile = profileRef->second;
            if (!profile.port_pgs.empty())
            {
                // still being referenced
                if (profile.static_configured)
                {
                    if (profile.dynamic_calculated)
                    {
                        // For dynamic calculated profile, we transmit it into pure dynamic profile
                        vector<string> keys;
                        profile.static_configured = false;
                        m_cfgDefaultLosslessBufferParam.getKeys(keys);
                        if (!keys.empty())
                        {
                            m_cfgDefaultLosslessBufferParam.hget(keys[0], "default_dynamic_th", profile.threshold);
                            SWSS_LOG_WARN("BUFFER_PROFILE %s is updated from static configured to dynamic, threshold %s",
                                          profileName.c_str(), profile.threshold.c_str());
                            updateBufferProfileToDb(profileName, profile);
                            return task_process_status::task_success;
                        }
                        else
                        {
                            SWSS_LOG_ERROR("BUFFER_PROFILE %s has been updated from static to dynamic but no default threshold configured, threshold %s unchanged",
                                           profileName.c_str(), profile.threshold.c_str());
                            return task_process_status::task_failed;
                        }
                    }
                    else
                    {
                        // For headroom override, we just wait until all reference removed
                        SWSS_LOG_WARN("BUFFER_PROFILE %s for headroom override is referenced and cannot be removed for now", profileName.c_str());
                        return task_process_status::task_need_retry;
                    }
                }
                else
                {
                    SWSS_LOG_ERROR("Try to remove non-static-configured profile %s", profileName.c_str());
                    return task_process_status::task_invalid_entry;
                }
            }
        }

        m_applBufferProfileTable.del(profileName);
        m_stateBufferProfileTable.del(profileName);

        m_bufferProfileLookup.erase(profileName);
        m_bufferProfileIgnored.erase(profileName);
    }
    else
    {
        SWSS_LOG_ERROR("Unknown operation type %s", op.c_str());
        return task_process_status::task_invalid_entry;
    }
    return task_process_status::task_success;
}

task_process_status BufferMgrDynamic::handleOneBufferPgEntry(const string &key, const string &port, const string &op, const KeyOpFieldsValuesTuple &tuple)
{
    vector<FieldValueTuple> fvVector;
    buffer_pg_t &bufferPg = m_portPgLookup[port][key];

    SWSS_LOG_DEBUG("processing command:%s table BUFFER_PG key %s", op.c_str(), key.c_str());
    if (op == SET_COMMAND)
    {
        bool ignored = false;
        bool pureDynamic = true;
        // For set command:
        // 1. Create the corresponding table entries in APPL_DB
        // 2. Record the table in the internal cache m_portPgLookup
        // 3. Check whether the profile is ingress or egress
        // 4. Initialize "profile_name" of buffer_pg_t

        bufferPg.dynamic_calculated = true;
        for (auto i = kfvFieldsValues(tuple).begin(); i != kfvFieldsValues(tuple).end(); i++)
        {
            const string &field = fvField(*i);
            string value = fvValue(*i);

            SWSS_LOG_DEBUG("field:%s, value:%s", field.c_str(), value.c_str());
            if (field == buffer_profile_field_name)
            {
                // Headroom override
                pureDynamic = false;
                transformSeperator(value);
                string profileName = parseObjectNameFromReference(value);
                auto searchRef = m_bufferProfileLookup.find(profileName);
                if (searchRef == m_bufferProfileLookup.end())
                {
                    if (m_bufferProfileIgnored.find(profileName) != m_bufferProfileIgnored.end())
                    {
                        // Referencing an ignored profile, the PG should be ignored as well
                        ignored = true;
                        bufferPg.dynamic_calculated = false;
                        bufferPg.lossless = false;
                        bufferPg.profile_name = profileName;
                    }
                    else
                    {
                        // In this case, the dynamc calculated should not be true
                        // It will be updated when its profile configured.
                        bufferPg.dynamic_calculated = false;
                        SWSS_LOG_WARN("Profile %s hasn't been configured yet, skip", profileName.c_str());
                        return task_process_status::task_need_retry;
                    }
                }
                else
                {
                    buffer_profile_t &profileRef = searchRef->second;
                    bufferPg.dynamic_calculated = profileRef.dynamic_calculated;
                    bufferPg.profile_name = profileName;
                    bufferPg.lossless = profileRef.lossless;
                }
            }
            fvVector.emplace_back(FieldValueTuple(field, value));
            SWSS_LOG_INFO("Inserting BUFFER_PG table field %s value %s", field.c_str(), value.c_str());
        }

        if (pureDynamic)
        {
            // Generic dynamically calculated headroom
            bufferPg.dynamic_calculated = true;
            bufferPg.lossless = true;
        }

        if (!ignored && bufferPg.lossless)
        {
            // For ingress lossless, special handling is required
            if (bufferPg.dynamic_calculated)
            {
                // Case 1: no profile designated, pure dynamically calculated entry
                // Case 2: profile designated, headroom needs to be dynamically calculated
                //         while threshold (alpha) is statically configured
                //         In this case, the profile name should follow the name convention
                //         of dynamically calculated profiles.
                doUpdatePgTask(key, bufferPg.profile_name);
            }
            else
            {
                // Headroom override
                doUpdateHeadroomOverrideTask(key, bufferPg.profile_name);
            }
        }
        else
        {
            SWSS_LOG_NOTICE("Inserting BUFFER_PG table entry %s into APPL_DB directly", key.c_str());
            m_applBufferPgTable.set(key, fvVector);
        }
    }
    else if (op == DEL_COMMAND)
    {
        // For del command:
        // 1. Removing it from APPL_DB
        // 2. Update internal caches
        string profileName = bufferPg.profile_name;

        m_bufferProfileLookup[profileName].port_pgs.erase(key);

        if (bufferPg.lossless)
        {
            // For ingress lossless, special handling is required
            if (bufferPg.dynamic_calculated)
            {
                // Case 1: no profile designated, pure dynamically calculated entry
                // Case 2: profile designated, headroom needs to be dynamically calculated
                //         while threshold (alpha) is statically configured
                //         In this case, the profile name should follow the name convention
                //         of dynamically calculated profiles.
                doRemovePgTask(key);
            }
            else
            {
                // Headroom override
                doRemoveHeadroomOverrideTask(key, profileName);
            }
        }
        else
        {
            SWSS_LOG_NOTICE("Removing BUFFER_PG table entry %s from APPL_DB directly", key.c_str());
            m_applBufferPgTable.del(key);
        }

        m_portPgLookup[port].erase(key);
        SWSS_LOG_DEBUG("Profile %s has been removed from port %s PG %s", profileName.c_str(), port.c_str(), key.c_str());
        if (m_portPgLookup[port].empty())
        {
            m_portPgLookup.erase(port);
            SWSS_LOG_DEBUG("Profile %s has been removed from port %s on all lossless PG", profileName.c_str(), port.c_str());
        }
    }
    else
    {
        SWSS_LOG_ERROR("Unknown operation type %s", op.c_str());
        return task_process_status::task_invalid_entry;
    }

    return task_process_status::task_success;
}

task_process_status BufferMgrDynamic::handleBufferPgTable(Consumer &consumer)
{
    SWSS_LOG_ENTER();
    auto it = consumer.m_toSync.begin();
    KeyOpFieldsValuesTuple tuple = it->second;
    string key = kfvKey(tuple);
    string op = kfvOp(tuple);

    transformSeperator(key);
    string ports = parseObjectNameFromKey(key);
    string pgs = parseObjectNameFromKey(key, 1);
    auto portsList = tokenize(ports, ',');

    task_process_status rc = task_process_status::task_success;

    if (portsList.size() == 1)
    {
        rc = handleOneBufferPgEntry(key, ports, op, tuple);
    }
    else
    {
        for (auto port : portsList)
        {
            string singleKey = port + ':' + pgs;
            rc = handleOneBufferPgEntry(singleKey, port, op, tuple);
            if (rc == task_process_status::task_need_retry)
                return rc;
        }
    }

    return rc;
}

task_process_status BufferMgrDynamic::handleBufferQueueTable(Consumer &consumer)
{
    return doBufferTableTask(consumer, m_applBufferQueueTable);
}

task_process_status BufferMgrDynamic::handleBufferPortIngressProfileListTable(Consumer &consumer)
{
    return doBufferTableTask(consumer, m_applBufferIngressProfileListTable);
}

task_process_status BufferMgrDynamic::handleBufferPortEgressProfileListTable(Consumer &consumer)
{
    return doBufferTableTask(consumer, m_applBufferEgressProfileListTable);
}

/*
 * This function copies the data from tables in CONFIG_DB to APPL_DB.
 * With dynamically buffer calculation supported, the following tables
 * will be moved to APPL_DB from CONFIG_DB because the CONFIG_DB contains
 * confgured entries only while APPL_DB contains dynamically generated entries
 *  - BUFFER_POOL
 *  - BUFFER_PROFILE
 *  - BUFFER_PG
 * The following tables have to be moved to APPL_DB because they reference
 * some entries that have been moved to APPL_DB
 *  - BUFFER_QUEUE
 *  - BUFFER_PORT_INGRESS_PROFILE_LIST
 *  - BUFFER_PORT_EGRESS_PROFILE_LIST   
 * One thing we need to handle is to transform the separator from | to :
 * The following items contain separator:
 *  - keys of each item
 *  - pool in BUFFER_POOL
 *  - profile in BUFFER_PG
 */
task_process_status BufferMgrDynamic::doBufferTableTask(Consumer &consumer, ProducerStateTable &applTable)
{
    SWSS_LOG_ENTER();

    auto it = consumer.m_toSync.begin();
    KeyOpFieldsValuesTuple t = it->second;
    string key = kfvKey(t);
    const string &name = consumer.getTableName();

    //transform the separator in key from "|" to ":"
    transformSeperator(key);

    string op = kfvOp(t);
    if (op == SET_COMMAND)
    {
        vector<FieldValueTuple> fvVector;

        SWSS_LOG_INFO("Inserting entry %s|%s from CONFIG_DB to APPL_DB", name.c_str(), key.c_str());

        for (auto i : kfvFieldsValues(t))
        {
            //transform the separator in values from "|" to ":"
            if (fvField(i) == "pool")
                transformSeperator(fvValue(i));
            if (fvField(i) == "profile")
                transformSeperator(fvValue(i));
            if (fvField(i) == "profile_list")
                transformSeperator(fvValue(i));
            fvVector.emplace_back(FieldValueTuple(fvField(i), fvValue(i)));
            SWSS_LOG_INFO("Inserting field %s value %s", fvField(i).c_str(), fvValue(i).c_str());
        }
        applTable.set(key, fvVector);
    }
    else if (op == DEL_COMMAND)
    {
        SWSS_LOG_INFO("Removing entry %s from APPL_DB", key.c_str());
        applTable.del(key);
    }

    return task_process_status::task_success;
}

void BufferMgrDynamic::doTask(Consumer &consumer)
{
    SWSS_LOG_ENTER();
    string table_name = consumer.getTableName();
    auto it = consumer.m_toSync.begin();

    if (m_bufferTableHandlerMap.find(table_name) == m_bufferTableHandlerMap.end())
    {
        SWSS_LOG_ERROR("No handler for key:%s found.", table_name.c_str());
        while (it != consumer.m_toSync.end())
            it = consumer.m_toSync.erase(it);
        return;
    }

    while (it != consumer.m_toSync.end())
    {
        auto task_status = (this->*(m_bufferTableHandlerMap[table_name]))(consumer);
        switch (task_status)
        {
            case task_process_status::task_failed:
                SWSS_LOG_ERROR("Failed to process table update");
                return;
            case task_process_status::task_need_retry:
                SWSS_LOG_INFO("Unable to process table update. Will retry...");
                it++;
                break;
            case task_process_status::task_invalid_entry:
                SWSS_LOG_ERROR("Failed to process invalid entry, drop it");
                it = consumer.m_toSync.erase(it);
                break;
            default:
                it = consumer.m_toSync.erase(it);
                break;
        }
    }
}

void BufferMgrDynamic::doTask(SelectableTimer &timer)
{
    checkSharedBufferPoolSize();
    checkPendingRemovedProfiles();
}
