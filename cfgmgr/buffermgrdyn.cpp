#include <fstream>
#include <iostream>
#include <string.h>
#include "logger.h"
#include "dbconnector.h"
#include "producerstatetable.h"
#include "tokenize.h"
#include "ipprefix.h"
#include "buffermgrdyn.h"
#include "bufferorch.h"
#include "exec.h"
#include "shellcmd.h"

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

BufferMgrDynamic::BufferMgrDynamic(DBConnector *cfgDb, DBConnector *stateDb, DBConnector *applDb, string pg_lookup_file, const vector<string> &tableNames) :
        Orch(cfgDb, tableNames),
        m_applDb(applDb),
        m_cfgPortTable(cfgDb, CFG_PORT_TABLE_NAME),
        m_cfgCableLenTable(cfgDb, CFG_PORT_CABLE_LEN_TABLE_NAME),
        m_cfgBufferProfileTable(cfgDb, CFG_BUFFER_PROFILE_TABLE_NAME),
        m_cfgBufferPgTable(cfgDb, CFG_BUFFER_PG_TABLE_NAME),
        m_cfgLosslessPgPoolTable(cfgDb, CFG_BUFFER_POOL_TABLE_NAME),
        m_applBufferPoolTable(applDb, APP_BUFFER_POOL_TABLE_NAME),
        m_applBufferProfileTable(applDb, APP_BUFFER_PROFILE_TABLE_NAME),
        m_applBufferPgTable(applDb, APP_BUFFER_PG_TABLE_NAME),
        m_applBufferQueueTable(applDb, APP_BUFFER_QUEUE_TABLE_NAME),
        m_applBufferIngressProfileListTable(applDb, APP_BUFFER_PORT_INGRESS_PROFILE_LIST_NAME),
        m_applBufferEgressProfileListTable(applDb, APP_BUFFER_PORT_EGRESS_PROFILE_LIST_NAME)
{
    SWSS_LOG_ENTER();

    // Initialize the handler map
    InitTableHandlerMap();

    string platform = getenv("platform") ? getenv("platform") : "";
    if (platform == "")
    {
        SWSS_LOG_ERROR("Platform environment variable is not defined");
        return;
    }

    string headroomSha, bufferpoolSha;
    string headroomPluginName = "buffer_headroom_" + platform + ".lua";
    string bufferpoolPluginName = "buffer_pool_" + platform + ".lua";

    try
    {
        string headroomLuaScript = swss::loadLuaScript(headroomPluginName);
        m_headroomSha = swss::loadRedisScript(
                applDb,
                headroomLuaScript);

        string bufferpoolLuaScript = swss::loadLuaScript(bufferpoolPluginName);
        m_bufferpoolSha = swss::loadRedisScript(
                applDb,
                bufferpoolLuaScript);
    }
    catch (...)
    {
        SWSS_LOG_WARN("Lua scripts for buffer calculation were not loaded successfully");
    }

    //TODO: to initialize ASIC_TABLE

    //TODO: to initialize PERIPHERAL_TABLE and PORT_PERIPHERAL_TABLE if any

    //Post: ASIC_TABLE and PERIPHERIAL_TABLE has been initialized
}

void BufferMgrDynamic::InitTableHandlerMap()
{
    SWSS_LOG_ENTER();
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
string BufferMgrDynamic::parseObjectNameFromKey(string &key, size_t pos = 1)
{
    auto keys = tokenize(key, ':');
    return keys[pos];
}

// For string "[foo]", returns "foo"
string BufferMgrDynamic::parseObjectNameFromReference(string &reference)
{
    auto objName = reference.substr(1, reference.size() - 2);
    return parseObjectNameFromKey(objName);
}

string BufferMgrDynamic::getPgPoolMode()
{
    return m_bufferPoolLookup[INGRESS_LOSSLESS_PG_POOL_NAME].mode;
}

// Meta flows which are called by main flows
void BufferMgrDynamic::calculateHeadroomSize(string &speed, string &cable, string &gearbox_model, buffer_profile_t &headroom)
{
    // Call vendor-specific lua plugin to calculate the xon, xoff, xon_offset, size and threshold
    vector<string> keys = {};
    vector<string> argv = {};

    keys.emplace_back(headroom.name);
    argv.emplace_back(speed);
    argv.emplace_back(cable);
    argv.emplace_back("0");

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
        if (pairs[0] == "threshold")
            headroom.threshold = pairs[1];
    }
}

void BufferMgrDynamic::recalculateSharedBufferPool()
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
        auto pool = m_bufferPoolLookup[pairs[0]];

        pool.total_size = pairs[1];

        updateBufferPoolToDb(poolName, pool);
    }
}

// For buffer pool, only size can be updated on-the-fly
void BufferMgrDynamic::updateBufferPoolToDb(string &name, buffer_pool_t &pool)
{
    vector<FieldValueTuple> fvVector;

    fvVector.emplace_back(make_pair("size", pool.total_size));

    m_applBufferPoolTable.set(string(APP_BUFFER_POOL_TABLE_NAME) + ":" + name, fvVector);
}

void BufferMgrDynamic::updateBufferProfileToDb(string &name, buffer_profile_t &profile)
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

    fvVector.emplace_back(make_pair("pool", "[" + pg_pool_reference + "]"));
    fvVector.emplace_back(make_pair("xon", profile.xon));
    if (profile.xon_offset.empty()) {
        fvVector.emplace_back(make_pair("xon_offset", profile.xon_offset));
    }
    fvVector.emplace_back(make_pair("xoff", profile.xoff));
    fvVector.emplace_back(make_pair("size", profile.size));
    fvVector.emplace_back(make_pair(mode, profile.threshold));

    m_applBufferProfileTable.set(string(APP_BUFFER_PROFILE_TABLE_NAME) + ":" + name, fvVector);
}

// Database operation
// Set/remove BUFFER_PG table entry
void BufferMgrDynamic::updateBufferPgToDb(string &key, string &profile, bool add)
{
    string buffer_pg_key = key;

    if (add)
    {
        vector<FieldValueTuple> fvVector;

        fvVector.clear();

        string buffer_profile_key = profile;
        transformSeperator(buffer_profile_key);

        string profile_ref = string("[") +
            APP_BUFFER_PROFILE_TABLE_NAME +
            m_applBufferPgTable.getTableNameSeparator() +
            buffer_profile_key +
            "]";

        fvVector.clear();
 
        fvVector.push_back(make_pair("profile", profile_ref));
        m_applBufferPgTable.set(buffer_pg_key, fvVector);
    }
    else
    {
        m_applBufferPgTable.del(buffer_pg_key);
    }
}

// We have to check the headroom ahead of applying them
// The passed-in headroom is calculated for the checking.
task_process_status BufferMgrDynamic::allocateProfile(string &speed, string &cable, string &gearbox_model, string &profile_name, buffer_profile_t &headroom)
{
    // Create record in BUFFER_PROFILE table
    // key format is pg_lossless_<speed>_<cable>_profile
    string buffer_profile_key;

    if (gearbox_model.empty())
    {
        buffer_profile_key = "pg_lossless_" + speed + "_" + cable + "_profile";
        SWSS_LOG_INFO("Allocating new BUFFER_PROFILE for speed %s cable length %s",
                      speed.c_str(), cable.c_str());
    }
    else
    {
        buffer_profile_key = "pg_lossless_" + speed + "_" + cable + "_" + gearbox_model + "_profile";
        SWSS_LOG_INFO("Allocating new BUFFER_PROFILE for speed %s cable length %s gearbox model %s",
                      speed.c_str(), cable.c_str(), gearbox_model.c_str());
    }

    // check if profile already exists - if yes - skip creation
    auto profileRef = m_bufferProfileLookup.find(buffer_profile_key);
    if (profileRef == m_bufferProfileLookup.end()
        || profileRef->second.xoff.empty())
    {
        auto &profile = m_bufferProfileLookup[buffer_profile_key];
        SWSS_LOG_NOTICE("Creating new profile '%s'", buffer_profile_key.c_str());

        // Call vendor-specific lua plugin to calculate the xon, xoff, xon_offset, size
        // Pay attention, the threshold can contain valid value
        calculateHeadroomSize(speed, cable, gearbox_model, profile);

        // Store the headroom info in the internal cache
        headroom = m_bufferProfileLookup[buffer_profile_key];

        updateBufferProfileToDb(buffer_profile_key, profile);

        SWSS_LOG_NOTICE("BUFFER_PROFILE %s has been created successfully", buffer_profile_key.c_str());
    }
    else
    {
        SWSS_LOG_NOTICE("Reusing existing profile '%s'", buffer_profile_key.c_str());
    }

    profile_name = buffer_profile_key;

    return task_process_status::task_success;
}

void BufferMgrDynamic::releaseProfile(string &speed, string &cable_length, string &gearbox_model)
{
    // Crete record in BUFFER_PROFILE table
    // key format is pg_lossless_<speed>_<cable>_profile
    string buffer_profile_key;
    vector<FieldValueTuple> fvVector;

    if (gearbox_model.empty())
    {
        buffer_profile_key = "pg_lossless_" + speed + "_" + cable_length + "_profile";
        SWSS_LOG_INFO("Releasing BUFFER_PROFILE %s for speed %s cable length %s",
                      buffer_profile_key.c_str(), speed.c_str(), cable_length.c_str());
    }
    else
    {
        buffer_profile_key = "pg_lossless_" + speed + "_" + cable_length + "_" + gearbox_model + "_profile";
        SWSS_LOG_INFO("Releasing BUFFER_PROFILE %s for speed %s cable length %s gearbox model %s",
                      buffer_profile_key.c_str(), speed.c_str(), cable_length.c_str(), gearbox_model.c_str());
    }

    if (m_bufferProfileLookup[buffer_profile_key].static_configured)
    {
        // Check whether the profile is statically configured.
        // This means:
        // 1. It's a statically configured profile, headroom override
        // 2. It's dynamically calculated headroom with static threshold (alpha)
        // In this case we won't remove the entry from the local cache even if it's dynamically calculated
        // because the speed, cable length and cable model are fixed the headroom info will always be valid once calculated.
        SWSS_LOG_INFO("Unable to release profile %s because it's a static configured profile", buffer_profile_key.c_str());
        return;
    }

    // Check whether it's referenced anymore by other PGs.
    if (!m_profileToPortMap[buffer_profile_key].empty())
    {
        auto firstRef = m_profileToPortMap[buffer_profile_key].begin()->first;
        SWSS_LOG_INFO("Unable to release profile %s because it's still referenced by %s (and others)",
                      buffer_profile_key.c_str(), firstRef.c_str());
        return;
    }

    m_profileToPortMap.erase(buffer_profile_key);

    m_applBufferProfileTable.del(string(APP_BUFFER_PROFILE_TABLE_NAME) + string(":") + buffer_profile_key);

    // Remove the profile from the internal cache
    m_bufferProfileLookup.erase(buffer_profile_key);

    SWSS_LOG_NOTICE("BUFFER_PROFILE %s has been released successfully", buffer_profile_key.c_str());
}

bool BufferMgrDynamic::isHeadroomResourceValid(string &port, buffer_profile_t &profile_info, string lossy_pg_changed = "")
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
    SWSS_LOG_ERROR("Unable to update profile for port %s. Accumulative headroom size exceeds limit", port.c_str());
    return true;
}

//Called when speed/cable length updated from CONFIG_DB
task_process_status BufferMgrDynamic::doSpeedOrCableLengthUpdateTask(string &port, string &speed, string &cable_length)
{
    vector<FieldValueTuple> fvVector;
    string cable;
    string separator = m_cfgBufferProfileTable.getTableNameSeparator();
    string key_pattern = CFG_BUFFER_PG_TABLE_NAME + separator + port + separator + "*";
    string value;
    port_info_t &portInfo = m_portInfoLookup[port];
    buffer_profile_t pgProfileNew;
    string &gearbox_model = portInfo.gearbox_model;
    bool isNewProfileCalculated = false, isHeadroomUpdated = false;
    string profileName;
    buffer_pg_lookup_t &portPgs = m_portPgLookup[port];

    // Iterate all the lossless PGs configured on this port
    for (auto it = portPgs.begin(); it != portPgs.end(); ++it)
    {
        auto key = it->first;
        auto &portPg = it->second;

        if (!portPg.dynamic_calculated)
            continue;

        if (!isNewProfileCalculated)
        {
            //Calculate new headroom size
            isNewProfileCalculated = true;

            auto rc = allocateProfile(speed, cable_length, gearbox_model, profileName, pgProfileNew);
            if (task_process_status::task_success != rc)
                return rc;

            //Calculate whether accumulative headroom size exceeds the maximum value
            //Abort if it does
            if (!isHeadroomResourceValid(port, pgProfileNew))
            {
                SWSS_LOG_ERROR("Update speed (%s) and cable length (%s) for port %s failed, accumulative headroom size exceeds the limit",
                               speed.c_str(), cable_length.c_str(), port.c_str());
                return task_process_status::task_failed;
            }
        }

        //appl_db Database operation: set item BUFFER_PG|<port>|<pg>
        updateBufferPgToDb(key, profileName, true);
        isHeadroomUpdated = true;
    }

    if (isHeadroomUpdated)
    {
        recalculateSharedBufferPool();

        //Remove the old profile which is probably not referenced anymore.
        releaseProfile(portInfo.speed, portInfo.cable_length, portInfo.gearbox_model);
    }

    //update internal map
    portInfo.speed = speed;
    portInfo.cable_length = cable_length;
    portInfo.gearbox_model = gearbox_model;
    portInfo.profile_name = profileName;

    return task_process_status::task_success;
}

// Main flows

// Update lossless pg on a port after an PG has been installed on the port
// Called when pg updated from CONFIG_DB
// Key format: BUFFER_PG:<port>:<pg>
task_process_status BufferMgrDynamic::doUpdatePgTask(string &pg_key, string &profile)
{
    auto port = parseObjectNameFromKey(pg_key);
    string value;
    port_info_t &portInfo = m_portInfoLookup[port];

    if (!portInfo.profile_name.empty())
    {
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

        //TODO: recalculate pool size
        recalculateSharedBufferPool();
    }
    else
    {
        if (!portInfo.speed.empty() && !portInfo.cable_length.empty())
        {
            // Not having profile_name but both speed and cable length have been configured for that port
            // This is because the first PG on that port is configured after speed, cable length configured
            // Just regenerate the profile
            doSpeedOrCableLengthUpdateTask(port, portInfo.speed, portInfo.cable_length);
        }
        else
        {
            // speed and cable length hasn't been configured
            // In that case, we just skip the this update and return success.
            // It will be handled after speed and cable length configured.
            SWSS_LOG_NOTICE("Skip setting BUFFER_PG for %s because its profile hasn't created", pg_key.c_str());
        }
    }

    return task_process_status::task_success;
}

//Remove the currently configured lossless pg
task_process_status BufferMgrDynamic::doRemovePgTask(string &pg_key)
{
    auto port = parseObjectNameFromKey(pg_key);
    auto pgs = parseObjectNameFromKey(pg_key, 2);
    port_info_t &portInfo = m_portInfoLookup[port];
    buffer_pg_lookup_t &pgLookup = m_portPgLookup[port];

    // Remove the PG from APPL_DB
    string null_str("");
    updateBufferPgToDb(pg_key, null_str, false);

    SWSS_LOG_NOTICE("Remove BUFFER_PG for %s to profile %s", pg_key.c_str(), portInfo.profile_name.c_str());

    // TODO: recalculate pool size
    recalculateSharedBufferPool();

    // Remove reference
    pgLookup.erase(pgs);

    return task_process_status::task_success;
}

// Update headroom override
task_process_status BufferMgrDynamic::doUpdateHeadroomOverrideTask(string &pg_key, string &profile)
{
    string port = parseObjectNameFromKey(pg_key);
    auto portInfo = m_portInfoLookup[port];

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

    if (!portInfo.profile_name.empty())
    {
        SWSS_LOG_ERROR("Unable to configure profile %s on %s, dynamically calculated profile should be removed first",
                       profile.c_str(), pg_key.c_str());
        return task_process_status::task_failed;
    }

    transformSeperator(pg_key);
    updateBufferPgToDb(pg_key, profile, true);

    SWSS_LOG_NOTICE("Headroom override %s has been configured on %s", profile.c_str(), pg_key.c_str());

    recalculateSharedBufferPool();

    return task_process_status::task_success;
}

task_process_status BufferMgrDynamic::doRemoveHeadroomOverrideTask(string &pg_key, string &profile)
{
    string port = parseObjectNameFromKey(pg_key);    
    updateBufferPgToDb(pg_key, profile, false);

    SWSS_LOG_NOTICE("Headroom override %s has been removed on %s", profile.c_str(), pg_key.c_str());

    recalculateSharedBufferPool();

    return task_process_status::task_success;
}

task_process_status BufferMgrDynamic::doUpdateStaticProfileTask(string &profileName, buffer_profile_t &profile)
{
    auto profileToMap = m_profileToPortMap[profileName];
    map<string, bool> portsChecked;

    for (auto i : profileToMap)
    {
        auto key = i.first;
        auto port = parseObjectNameFromKey(key);

        if (portsChecked[port])
            continue;

        if (isHeadroomResourceValid(port, profile))
        {
            SWSS_LOG_ERROR("BUFFER_PROFILE %s cannot be updated because %s referencing it violates the resource limitation",
                           profileName.c_str(), key.c_str());
            return task_process_status::task_failed;
        }

        portsChecked[port] = true;
    }

    string mode = getPgPoolMode();

    if (mode.empty())
    {
        // this should never happen if switch initialized properly
        SWSS_LOG_WARN("PG lossless pool is not yet created, updating profile %s failed", profileName.c_str());
        return task_process_status::task_need_retry;
    }

    // profile threshold field name
    mode += "_th";
    string pg_pool_reference = string(APP_BUFFER_POOL_TABLE_NAME) +
        m_applBufferProfileTable.getTableNameSeparator() +
        INGRESS_LOSSLESS_PG_POOL_NAME;

    vector<FieldValueTuple> fvVector;
    fvVector.emplace_back(make_pair("pool", "[" + pg_pool_reference + "]"));
    fvVector.emplace_back(make_pair("xon", profile.xon));
    if (profile.xon_offset.empty()) {
        fvVector.emplace_back(make_pair("xon_offset", profile.xon_offset));
    }
    fvVector.emplace_back(make_pair("xoff", profile.xoff));
    fvVector.emplace_back(make_pair("size", profile.size));
    fvVector.emplace_back(make_pair(mode, profile.threshold));

    m_applBufferProfileTable.set(string(APP_BUFFER_PROFILE_TABLE_NAME) + ":" + profileName, fvVector);

    m_bufferProfileLookup[profileName] = profile;

    recalculateSharedBufferPool();

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

            if (speed.empty())
            {
                portInfo.cable_length = cable_length;
                SWSS_LOG_WARN("Speed for %s hasn't configured yet, unable to calculate headroom", port.c_str());
                // We don't retry here because it doesn't make sense until the speed is configured.
                continue;
            }

            SWSS_LOG_INFO("Updating BUFFER_PG for port %s due to cable length updated", port.c_str());

            //Try updating the buffer information
            task_status = doSpeedOrCableLengthUpdateTask(port, speed, cable_length);
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
    auto key = kfvKey(t);
    string op = kfvOp(t);

    transformSeperator(key);
    auto port = parseObjectNameFromKey(key);
    port_info_t &portInfo = m_portInfoLookup[port];

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
                task_status = doSpeedOrCableLengthUpdateTask(port, speed, cable_length);
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
            }
            else if (fvField(i) == "admin_status")
            {
                recalculateSharedBufferPool();
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
    string key = kfvKey(tuple);
    string op = kfvOp(tuple);
    vector<FieldValueTuple> fvVector;

    transformSeperator(key);
    string pool = parseObjectNameFromKey(key);

    SWSS_LOG_DEBUG("processing command:%s", op.c_str());
    if (op == SET_COMMAND)
    {
        // For set command:
        // 1. Create the corresponding table entries in APPL_DB
        // 2. Record the table in the internal cache m_bufferPoolLookup
        buffer_pool_t &bufferPool = m_bufferPoolLookup[pool];

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
            m_applBufferPoolTable.set(key, fvVector);
    }
    else if (op == DEL_COMMAND)
    {
        // How do we handle dependency?
        m_applBufferPoolTable.del(key);
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
    string key = kfvKey(tuple);
    string op = kfvOp(tuple);
    vector<FieldValueTuple> fvVector;

    transformSeperator(key);
    auto profileName = parseObjectNameFromKey(key);
    buffer_profile_t profileApp;

    SWSS_LOG_DEBUG("processing command:%s", op.c_str());
    if (op == SET_COMMAND)
    {
        //For set command:
        //1. Create the corresponding table entries in APPL_DB
        //2. Record the table in the internal cache m_bufferProfileLookup
        profileApp.static_configured = true;
        profileApp.dynamic_calculated = false;
        profileApp.lossless = false;
        for (auto i = kfvFieldsValues(tuple).begin(); i != kfvFieldsValues(tuple).end(); i++)
        {
            string &field = fvField(*i);
            string &value = fvValue(*i);

            SWSS_LOG_DEBUG("field:%s, value:%s", field.c_str(), value.c_str());
            if (field == buffer_pool_field_name)
            {
                transformSeperator(value);
                auto poolName = parseObjectNameFromKey(value);
                auto poolRef = m_bufferPoolLookup.find(poolName);
                if (poolRef == m_bufferPoolLookup.end())
                {
                    SWSS_LOG_WARN("Pool %s hasn't been configured yet, skip", poolName.c_str());
                    return task_process_status::task_need_retry;
                }
                profileApp.pool_name = poolName;
                profileApp.ingress = poolRef->second.ingress;
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
        if (profileApp.dynamic_calculated)
        {
            m_bufferProfileLookup[profileName] = profileApp;
            SWSS_LOG_NOTICE("Dynamically profile %s won't be deployed to APPL_DB until referenced by a port",
                            key.c_str());
        }
        else
        {
            doUpdateStaticProfileTask(profileName, profileApp);
            SWSS_LOG_NOTICE("BUFFER_PROFILE %s has been inserted into APPL_DB", key.c_str());
        }
    }
    else if (op == DEL_COMMAND)
    {
        // For del command:
        // Check whether it is referenced by port. If yes, return "need retry" and exit
        // Typically, the referencing occurs when headroom override configured
        // Remove it from APPL_DB and internal cache

        if (m_profileToPortMap[profileName].size() > 0)
        {
            SWSS_LOG_WARN("BUFFER_PROFILE %s is referenced and cannot be removed for now", key.c_str());
            return task_process_status::task_need_retry;
        }
        if (!profileApp.dynamic_calculated)
        {
            m_applBufferProfileTable.del(key);
        }
        m_bufferProfileLookup.erase(profileName);
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
    vector<FieldValueTuple> fvVector;

    transformSeperator(key);
    string port = parseObjectNameFromKey(key);
    string pgs = parseObjectNameFromKey(key, 2);
    buffer_pg_t &bufferPg = m_portPgLookup[port][pgs];

    SWSS_LOG_DEBUG("processing command:%s", op.c_str());
    if (op == SET_COMMAND)
    {
        // For set command:
        // 1. Create the corresponding table entries in APPL_DB
        // 2. Record the table in the internal cache m_portPgLookup
        // 3. Check whether the profile is ingress or egress
        // 4. Initialize "profile_name" of buffer_pg_t

        bufferPg.dynamic_calculated = true;
        for (auto i = kfvFieldsValues(tuple).begin(); i != kfvFieldsValues(tuple).end(); i++)
        {
            string &field = fvField(*i);
            string &value = fvValue(*i);

            SWSS_LOG_DEBUG("field:%s, value:%s", field.c_str(), value.c_str());
            if (field == buffer_profile_field_name)
            {
                transformSeperator(value);
                string profileName = parseObjectNameFromReference(value);
                auto searchRef = m_bufferProfileLookup.find(profileName);
                if (searchRef == m_bufferProfileLookup.end())
                {
                    SWSS_LOG_WARN("Profile %s hasn't been configured yet, skip", profileName.c_str());
                    return task_process_status::task_need_retry;
                }
                buffer_profile_t &profileRef = searchRef->second;
                bufferPg.dynamic_calculated = profileRef.dynamic_calculated;
                bufferPg.profile_name = profileName;
                bufferPg.ingress = profileRef.ingress;
                bufferPg.lossless = profileRef.lossless;
                m_profileToPortMap[profileName][key] = true;
            }
            fvVector.emplace_back(FieldValueTuple(field, value));
            SWSS_LOG_INFO("Inserting BUFFER_PG table field %s value %s", field.c_str(), value.c_str());
        }
        if (bufferPg.ingress && bufferPg.lossless)
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
        string &profileName = m_portPgLookup[port][pgs].profile_name;

        m_profileToPortMap[profileName].erase(key);

        if (bufferPg.ingress && bufferPg.lossless)
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
                doRemoveHeadroomOverrideTask(key, bufferPg.profile_name);
            }
        }
        else
        {
            SWSS_LOG_NOTICE("Removing BUFFER_PG table entry %s from APPL_DB directly", key.c_str());
            m_applBufferPgTable.del(key);
        }

        m_portPgLookup[port].erase(pgs);
        if (m_portPgLookup[port].empty())
        {
            m_portPgLookup.erase(port);
        }
    }
    else
    {
        SWSS_LOG_ERROR("Unknown operation type %s", op.c_str());
        return task_process_status::task_invalid_entry;
    }
    return task_process_status::task_success;
}

task_process_status BufferMgrDynamic::handleBufferQueueTable(Consumer &consumer)
{
    return doBufferTableTask(consumer, m_applBufferQueueTable);
}

task_process_status BufferMgrDynamic::handleBufferPortIngressProfileListTable(Consumer &consumer)
{
    return doBufferTableTask(consumer, m_applBufferEgressProfileListTable);
}

task_process_status BufferMgrDynamic::handleBufferPortEgressProfileListTable(Consumer &consumer)
{
    return doBufferTableTask(consumer, m_applBufferIngressProfileListTable);
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

    //transform the separator in key from "|" to ":"
    transformSeperator(key);

    string op = kfvOp(t);
    if (op == SET_COMMAND)
    {
        vector<FieldValueTuple> fvVector;

        SWSS_LOG_INFO("Inserting entry %s from CONFIG_DB to APPL_DB", key.c_str());

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
                ++it;
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
