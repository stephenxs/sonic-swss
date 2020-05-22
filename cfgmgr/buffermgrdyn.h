#ifndef __BUFFMGRDYN__
#define __BUFFMGRDYN__

#include "dbconnector.h"
#include "producerstatetable.h"
#include "orch.h"

#include <map>
#include <string>

namespace swss {

#define INGRESS_LOSSLESS_PG_POOL_NAME "ingress_lossless_pool"
#define LOSSLESS_PGS "3-4"

typedef struct {
    bool ingress;
    bool dynamic_size;
    std::string total_size;
    std::string mode;
    std::string xoff;
} buffer_pool_t;

typedef struct {
    bool dynamic_calculated;
    bool static_configured;
    bool ingress;
    bool lossless;
    std::string name;
    std::string size;
    std::string xon;
    std::string xon_offset;
    std::string xoff;
    std::string threshold;
    std::string pool_name;
} buffer_profile_t;

typedef struct {
    bool ingress;
    bool lossless;
    bool dynamic_calculated;
    std::string profile_name;
} buffer_pg_t;

typedef struct {
    std::string speed;
    std::string cable_length;
    std::string gearbox_model;
    std::string profile_name;
} port_info_t;

//TODO:
//add map to store all configured PGs
//add map to store all configured profiles
//check whether the configure database update is for dynamically lossless,
//if yes, call further function to handle; if no, handle it directly in the callback

//map from port name to port info
typedef std::map<std::string, port_info_t> port_info_lookup_t;
//map from port info to profile
//for dynamically calculated profile
typedef std::map<std::string, buffer_profile_t> buffer_profile_lookup_t;
//map from name to pool
typedef std::map<std::string, buffer_pool_t> buffer_pool_lookup_t;
//profile reference table
typedef std::map<std::string, std::map<std::string, bool>> profile_port_lookup_t;
//port -> headroom override
typedef std::map<std::string, buffer_profile_t> headroom_override_t;
//map from pg to info
typedef std::map<std::string, buffer_pg_t> buffer_pg_lookup_t;
//map from port to all its pgs
typedef std::map<std::string, buffer_pg_lookup_t> port_pg_lookup_t;

class BufferMgrDynamic : public Orch
{
public:
    BufferMgrDynamic(DBConnector *cfgDb, DBConnector *stateDb, DBConnector *applDb, std::string pg_lookup_file, const std::vector<std::string> &tableNames);
    using Orch::doTask;

private:
    typedef task_process_status (BufferMgrDynamic::*buffer_table_handler)(Consumer& consumer);
    typedef std::map<std::string, buffer_table_handler> buffer_table_handler_map;
    typedef std::pair<std::string, buffer_table_handler> buffer_handler_pair;

    buffer_table_handler_map m_bufferTableHandlerMap;

    std::shared_ptr<DBConnector> m_applDb = nullptr;

    // CONFIG_DB tables
    Table m_cfgPortTable;
    Table m_cfgCableLenTable;
    Table m_cfgBufferProfileTable;
    Table m_cfgBufferPgTable;
    Table m_cfgLosslessPgPoolTable;

    // APPL_DB tables
    ProducerStateTable m_applBufferProfileTable;
    ProducerStateTable m_applBufferPgTable;
    ProducerStateTable m_applBufferPoolTable;
    ProducerStateTable m_applBufferQueueTable;
    ProducerStateTable m_applBufferIngressProfileListTable;
    ProducerStateTable m_applBufferEgressProfileListTable;

    // Internal maps
    // m_bufferPoolLookup - the cache for the buffer pool
    buffer_pool_lookup_t m_bufferPoolLookup;
    // m_bufferProfileLookup - the cache for the following set:
    // 1. CFG_BUFFER_PROFILE
    // 2. Dynamically calculated headroom info stored in APPL_BUFFER_PROFILE
    // key: profile name
    buffer_profile_lookup_t m_bufferProfileLookup;
    // m_portInfoLookup
    // key: port name
    // updated only when a port's speed and cable length updated
    port_info_lookup_t m_portInfoLookup;
    // m_profileToPortMap - a lookup table from profile name to pgs referencing it
    // key: profile name, value: a set of PGs.
    // An element will be added or removed when a PG added or removed
    // The set which is indexed by profile will be removed or added if the profile is removed or added
    profile_port_lookup_t m_profileToPortMap;
    // m_portPgLookup - the cache for CFG_BUFFER_PG and APPL_BUFFER_PG
    // 1st level key: port name, 2nd level key: PGs
    port_pg_lookup_t m_portPgLookup;

    // Vendor specific lua plugins for calculating headroom and buffer pool
    // Loaded when the buffer manager starts
    // Executed whenever the headroom and pool size need to be updated
    std::string m_headroomSha;
    std::string m_bufferpoolSha;

    // Initializers
    void InitTableHandlerMap();

    // Tool functions to parse keys and references
    std::string getPgPoolMode();
    void transformSeperator(std::string &name);
    std::string parseObjectNameFromKey(std::string &key, size_t pos/* = 1*/);
    std::string parseObjectNameFromReference(std::string &reference);

    // APPL_DB table operations
    void updateBufferPoolToDb(std::string &name, buffer_pool_t &pool);
    void updateBufferProfileToDb(std::string &name, buffer_profile_t &profile);
    void updateBufferPgToDb(std::string &key, std::string &profile, bool add);

    // Meta flows
    void calculateHeadroomSize(std::string &speed, std::string &cable, std::string &gearbox_model, buffer_profile_t &headroom);
    void recalculateSharedBufferPool();
    task_process_status allocateProfile(std::string &speed, std::string &cable, std::string &gearbox_model, std::string &profile_name, buffer_profile_t &headroom);
    void releaseProfile(std::string &speed, std::string &cable_length, std::string &gearbox_model);
    bool isHeadroomResourceValid(std::string &port, buffer_profile_t &profile_info, std::string lossy_pg_changed);

    // Main flows
    task_process_status doSpeedOrCableLengthUpdateTask(std::string &port, std::string &speed, std::string &cable_length);
    task_process_status doUpdatePgTask(std::string &pg_key, std::string &profile);
    task_process_status doRemovePgTask(std::string &pg_key);
    task_process_status doUpdateHeadroomOverrideTask(std::string &pg_key, std::string &profile);
    task_process_status doRemoveHeadroomOverrideTask(std::string &pg_key, std::string &profile);
    task_process_status doAdminStatusTask(std::string port, std::string adminStatus);
    task_process_status doUpdateStaticProfileTask(std::string &profileName, buffer_profile_t &profile);

    // Table update handlers
    task_process_status handleCableLenTable(Consumer &consumer);
    task_process_status handlePortTable(Consumer &consumer);
    task_process_status handleBufferPoolTable(Consumer &consumer);
    task_process_status handleBufferProfileTable(Consumer &consumer);
    task_process_status handleBufferPgTable(Consumer &consumer);
    task_process_status handleBufferQueueTable(Consumer &consumer);
    task_process_status handleBufferPortIngressProfileListTable(Consumer &consumer);
    task_process_status handleBufferPortEgressProfileListTable(Consumer &consumer);
    task_process_status doBufferTableTask(Consumer &consumer, ProducerStateTable &applTable);
    void doTask(Consumer &consumer);
};

}

#endif /* __BUFFMGRDYN__ */
