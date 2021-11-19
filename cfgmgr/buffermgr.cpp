#include <fstream>
#include <iostream>
#include <string.h>
#include "logger.h"
#include "dbconnector.h"
#include "producerstatetable.h"
#include "tokenize.h"
#include "ipprefix.h"
#include "timer.h"
#include "buffermgr.h"
#include "exec.h"
#include "shellcmd.h"
#include "warm_restart.h"

using namespace std;
using namespace swss;

BufferMgr::BufferMgr(DBConnector *cfgDb, DBConnector *applDb, string pg_lookup_file, const vector<string> &tableNames) :
        Orch(cfgDb, tableNames),
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
    readPgProfileLookupFile(pg_lookup_file);
    dynamic_buffer_model = false;
}

//# speed, cable, size,    xon,  xoff, threshold,  xon_offset
//  40000   5m    34816   18432  16384 1           2496
void BufferMgr::readPgProfileLookupFile(string file)
{
    SWSS_LOG_NOTICE("Read lookup configuration file...");

    m_pgfile_processed = false;

    ifstream infile(file);
    if (!infile.is_open())
    {
        SWSS_LOG_WARN("PG profile lookup file: %s is not readable", file.c_str());
        return;
    }

    string line;
    while (getline(infile, line))
    {
        if (line.empty() || (line.at(0) == '#'))
        {
            continue;
        }

        istringstream iss(line);
        string speed, cable;

        iss >> speed;
        iss >> cable;
        iss >> m_pgProfileLookup[speed][cable].size;
        iss >> m_pgProfileLookup[speed][cable].xon;
        iss >> m_pgProfileLookup[speed][cable].xoff;
        iss >> m_pgProfileLookup[speed][cable].threshold;

        // Not all lookup table contains xon_offset values. Set default to empty
        m_pgProfileLookup[speed][cable].xon_offset = "";
        iss >> m_pgProfileLookup[speed][cable].xon_offset;

        SWSS_LOG_NOTICE("PG profile for speed %s and cable %s is: size:%s, xon:%s, xoff:%s, th:%s, xon_offset:%s",
                       speed.c_str(), cable.c_str(),
                       m_pgProfileLookup[speed][cable].size.c_str(),
                       m_pgProfileLookup[speed][cable].xon.c_str(),
                       m_pgProfileLookup[speed][cable].xoff.c_str(),
                       m_pgProfileLookup[speed][cable].threshold.c_str(),
                       m_pgProfileLookup[speed][cable].xon_offset.c_str()
                       );
    }

    m_pgfile_processed = true;
    infile.close();
}

task_process_status BufferMgr::doCableTask(string port, string cable_length)
{
    m_cableLenLookup[port] = cable_length;
    SWSS_LOG_INFO("Cable length set to %s for port %s", m_cableLenLookup[port].c_str(), port.c_str());
    return task_process_status::task_success;
}

string BufferMgr::getPgPoolMode()
{
    vector<FieldValueTuple> pool_properties;
    m_cfgLosslessPgPoolTable.get(INGRESS_LOSSLESS_PG_POOL_NAME, pool_properties);
    for (auto& prop : pool_properties)
    {
        if (fvField(prop) == "mode")
            return fvValue(prop);
    }
    return "";
}

/*
Create/update two tables: profile (in m_cfgBufferProfileTable) and port buffer (in m_cfgBufferPgTable):

    "BUFFER_PROFILE": {
        "pg_lossless_100G_300m_profile": {
            "pool":"[BUFFER_POOL_TABLE:ingress_lossless_pool]",
            "xon":"18432",
            "xon_offset":"2496",
            "xoff":"165888",
            "size":"184320",
            "dynamic_th":"1"
        }
    }
    "BUFFER_PG" :{
        Ethernet44|3-4": {
            "profile" : "[BUFFER_PROFILE:pg_lossless_100000_300m_profile]"
        }
    }
*/
task_process_status BufferMgr::doSpeedUpdateTask(string port)
{
    vector<FieldValueTuple> fvVector;
    string cable;
    string speed;

    if (m_cableLenLookup.count(port) == 0)
    {
        SWSS_LOG_INFO("Unable to create/update PG profile for port %s. Cable length is not set", port.c_str());
        return task_process_status::task_need_retry;
    }

    cable = m_cableLenLookup[port];
    if (cable == "0m")
    {
        SWSS_LOG_NOTICE("Not creating/updating PG profile for port %s. Cable length is set to %s", port.c_str(), cable.c_str());
        return task_process_status::task_success;
    }

    speed = m_speedLookup[port];
    if (m_pgProfileLookup.count(speed) == 0 || m_pgProfileLookup[speed].count(cable) == 0)
    {
        SWSS_LOG_ERROR("Unable to create/update PG profile for port %s. No PG profile configured for speed %s and cable length %s",
                       port.c_str(), speed.c_str(), cable.c_str());
        return task_process_status::task_invalid_entry;
    }

    // Crete record in BUFFER_PROFILE table
    // key format is pg_lossless_<speed>_<cable>_profile
    string buffer_profile_key = "pg_lossless_" + speed + "_" + cable + "_profile";

    // check if profile already exists - if yes - skip creation
    m_cfgBufferProfileTable.get(buffer_profile_key, fvVector);
    if (fvVector.size() == 0)
    {
        SWSS_LOG_NOTICE("Creating new profile '%s'", buffer_profile_key.c_str());

        string mode = getPgPoolMode();
        if (mode.empty())
        {
            // this should never happen if switch initialized properly
            SWSS_LOG_INFO("PG lossless pool is not yet created");
            return task_process_status::task_need_retry;
        }

        // profile threshold field name
        mode += "_th";
        string pg_pool_reference = string(CFG_BUFFER_POOL_TABLE_NAME) +
                                   m_cfgBufferProfileTable.getTableNameSeparator() +
                                   INGRESS_LOSSLESS_PG_POOL_NAME;

        fvVector.push_back(make_pair("pool", "[" + pg_pool_reference + "]"));
        fvVector.push_back(make_pair("xon", m_pgProfileLookup[speed][cable].xon));
        if (m_pgProfileLookup[speed][cable].xon_offset.length() > 0) {
            fvVector.push_back(make_pair("xon_offset",
                                         m_pgProfileLookup[speed][cable].xon_offset));
        }
        fvVector.push_back(make_pair("xoff", m_pgProfileLookup[speed][cable].xoff));
        fvVector.push_back(make_pair("size", m_pgProfileLookup[speed][cable].size));
        fvVector.push_back(make_pair(mode, m_pgProfileLookup[speed][cable].threshold));
        m_cfgBufferProfileTable.set(buffer_profile_key, fvVector);
    }
    else
    {
        SWSS_LOG_NOTICE("Reusing existing profile '%s'", buffer_profile_key.c_str());
    }

    fvVector.clear();

    string buffer_pg_key = port + m_cfgBufferPgTable.getTableNameSeparator() + LOSSLESS_PGS;

    string profile_ref = string("[") +
                         CFG_BUFFER_PROFILE_TABLE_NAME +
                         m_cfgBufferPgTable.getTableNameSeparator() +
                         buffer_profile_key +
                         "]";

    /* Check if PG Mapping is already then log message and return. */
    m_cfgBufferPgTable.get(buffer_pg_key, fvVector);

    for (auto& prop : fvVector)
    {
        if ((fvField(prop) == "profile") && (profile_ref == fvValue(prop)))
        {
            SWSS_LOG_NOTICE("PG to Buffer Profile Mapping %s already present", buffer_pg_key.c_str());
            return task_process_status::task_success;
        }
    }

    fvVector.clear();

    fvVector.push_back(make_pair("profile", profile_ref));
    m_cfgBufferPgTable.set(buffer_pg_key, fvVector);
    return task_process_status::task_success;
}

void BufferMgr::transformSeperator(string &name)
{
    size_t pos;
    while ((pos = name.find("|")) != string::npos)
        name.replace(pos, 1, ":");
}

void BufferMgr::transformReference(string &name)
{
    auto references = tokenize(name, list_item_delimiter);
    int ref_index = 0;

    name = "";

    for (auto &reference : references)
    {
        if (ref_index != 0)
            name += list_item_delimiter;
        ref_index ++;

        auto keys = tokenize(reference, config_db_key_delimiter);
        int key_index = 0;
        for (auto &key : keys)
        {
            if (key_index == 0)
                name += key + "_TABLE";
            else
                name += delimiter + key;
            key_index ++;
        }
    }
}

/*
 * This function copies the data from tables in CONFIG_DB to APPL_DB.
 * With dynamically buffer calculation supported, the following tables
 * will be moved to APPL_DB from CONFIG_DB because the CONFIG_DB contains
 * configured entries only while APPL_DB contains dynamically generated entries
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
 *  - pool in BUFFER_PROFILE
 *  - profile in BUFFER_PG
 *  - profile_list in BUFFER_PORT_INGRESS_PROFILE_LIST and BUFFER_PORT_EGRESS_PROFILE_LIST
 */
void BufferMgr::doBufferTableTask(Consumer &consumer, ProducerStateTable &applTable)
{
    SWSS_LOG_ENTER();

    auto it = consumer.m_toSync.begin();
    while (it != consumer.m_toSync.end())
    {
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
                SWSS_LOG_INFO("Inserting field %s value %s", fvField(i).c_str(), fvValue(i).c_str());
                //transform the separator in values from "|" to ":"
                if (fvField(i) == "pool")
                    transformReference(fvValue(i));
                if (fvField(i) == "profile")
                    transformReference(fvValue(i));
                if (fvField(i) == "profile_list")
                    transformReference(fvValue(i));
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
        it = consumer.m_toSync.erase(it);
    }
}

void BufferMgr::doBufferMetaTask(Consumer &consumer)
{
    SWSS_LOG_ENTER();

    auto it = consumer.m_toSync.begin();
    while (it != consumer.m_toSync.end())
    {
        KeyOpFieldsValuesTuple t = it->second;
        string key = kfvKey(t);

        string op = kfvOp(t);
        if (op == SET_COMMAND)
        {
            vector<FieldValueTuple> fvVector;

            for (auto i : kfvFieldsValues(t))
            {
                if (fvField(i) == "buffer_model")
                {
                    if (fvValue(i) == "dynamic")
                    {
                        dynamic_buffer_model = true;
                    }
                    else
                    {
                        dynamic_buffer_model = false;
                    }
                    break;
                }
            }
        }
        else if (op == DEL_COMMAND)
        {
            dynamic_buffer_model = false;
        }
        it = consumer.m_toSync.erase(it);
    }
}

void BufferMgr::doTask(Consumer &consumer)
{
    SWSS_LOG_ENTER();

    string table_name = consumer.getTableName();

    if (table_name == CFG_DEVICE_METADATA_TABLE_NAME)
    {
        doBufferMetaTask(consumer);
        return;
    }

    if (dynamic_buffer_model)
    {
         SWSS_LOG_DEBUG("Dynamic buffer model enabled. Skipping further processing");
         return;
    }
    if (table_name == CFG_BUFFER_POOL_TABLE_NAME)
    {
        doBufferTableTask(consumer, m_applBufferPoolTable);
        return;
    }

    if (table_name == CFG_BUFFER_PROFILE_TABLE_NAME)
    {
        doBufferTableTask(consumer, m_applBufferProfileTable);
        return;
    }

    if (table_name == CFG_BUFFER_PG_TABLE_NAME)
    {
        doBufferTableTask(consumer, m_applBufferPgTable);
        return;
    }

    if (table_name == CFG_BUFFER_QUEUE_TABLE_NAME)
    {
        doBufferTableTask(consumer, m_applBufferQueueTable);
        return;
    }

    if (table_name == CFG_BUFFER_PORT_INGRESS_PROFILE_LIST_NAME)
    {
        doBufferTableTask(consumer, m_applBufferIngressProfileListTable);
        return;
    }

    if (table_name == CFG_BUFFER_PORT_EGRESS_PROFILE_LIST_NAME)
    {
        doBufferTableTask(consumer, m_applBufferEgressProfileListTable);
        return;
    }

    auto it = consumer.m_toSync.begin();
    while (it != consumer.m_toSync.end())
    {
        KeyOpFieldsValuesTuple t = it->second;

        string keySeparator = CONFIGDB_KEY_SEPARATOR;
        vector<string> keys = tokenize(kfvKey(t), keySeparator[0]);
        string port(keys[0]);

        string op = kfvOp(t);
        task_process_status task_status = task_process_status::task_success;
        if (op == SET_COMMAND)
        {
            for (auto i : kfvFieldsValues(t))
            {
                if (table_name == CFG_PORT_CABLE_LEN_TABLE_NAME)
                {
                    // receive and cache cable length table
                    task_status = doCableTask(fvField(i), fvValue(i));
                }
                if (m_pgfile_processed && table_name == CFG_PORT_TABLE_NAME && (fvField(i) == "speed" || fvField(i) == "admin_status"))
                {
                    if (fvField(i) == "speed")
                    {
                        m_speedLookup[port] = fvValue(i);
                    }
                    
                    if (m_speedLookup.count(port) != 0)
                    {
                        // create/update profile for port
                        task_status = doSpeedUpdateTask(port);
                    }
                }
                if (task_status != task_process_status::task_success)
                {
                    break;
                }
            }
        }

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
