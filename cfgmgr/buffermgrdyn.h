#ifndef __BUFFMGRDYN__
#define __BUFFMGRDYN__

#include "dbconnector.h"
#include "producerstatetable.h"
#include "orch.h"

#include <map>
#include <set>
#include <string>

namespace swss {

#define INGRESS_LOSSLESS_PG_POOL_NAME "ingress_lossless_pool"
#define LOSSLESS_PGS "3-4"

#define BUFFERMGR_TIMER_PERIOD 10

typedef struct {
    bool ingress;
    bool dynamic_size;
    bool initialized;
    std::string total_size;
    std::string mode;
    std::string xoff;
} buffer_pool_t;

// State of the profile.
// We maintain state for dynamic profiles only
// INITIALIZING state indicates a profile is under initializing
// NORMAL state indicates that profile is working normally
// A profile's state will be STALE When it is no longer referenced
// It will be removed after having been in this state for 1 minute
// This deferred mechanism yields two benefits
// 1. Avoid frequently removing/re-adding a profile
// 2. Avoid removing profile failure if orchagent handles BUFFER_PG
//    and BUFFER_PROFILE tables in an order which differs from which
//    buffermgrd calls
typedef enum {
    PROFILE_INITIALIZING,
    PROFILE_PARTIAL_INITIALIZED,
    PROFILE_NORMAL,
    PROFILE_STALE
} profile_state_t;

#define PROFILE_STALE_TIMEOUT 6

typedef std::set<std::string> port_pg_set_t;
typedef struct {
    profile_state_t state;
    int stale_timeout;
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
    // port_pgs - stores pgs referencing this profile
    // An element will be added or removed when a PG added or removed
    port_pg_set_t port_pgs;
} buffer_profile_t;

typedef struct {
    bool lossless;
    bool dynamic_calculated;
    std::string profile_name;
} buffer_pg_t;

typedef enum {
    // Port is under initializing, which means its info hasn't been comprehensive for calculating headroom
    PORT_INITIALIZING,
    // All necessary information for calculating headrom is ready
    PORT_READY,
    // Currently at least one of the PGs has been configured as dynamic headroom
    PORT_DYNAMIC_HEADROOM,
    // Currently at least one of the PGs has been configured as headroom override
    PORT_HEADROOM_OVERRIDE
} port_state_t;

typedef struct {
    port_state_t state;
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
//port -> headroom override
typedef std::map<std::string, buffer_profile_t> headroom_override_t;
//map from pg to info
typedef std::map<std::string, buffer_pg_t> buffer_pg_lookup_t;
//map from port to all its pgs
typedef std::map<std::string, buffer_pg_lookup_t> port_pg_lookup_t;
//map from gearbox model to gearbox delay
typedef std::map<std::string, std::string> gearbox_delay_t;

class BufferMgrDynamic : public Orch
{
public:
    BufferMgrDynamic(DBConnector *cfgDb, DBConnector *stateDb, DBConnector *applDb, const std::vector<TableConnector> &tables, std::shared_ptr<std::vector<KeyOpFieldsValuesTuple>> gearboxInfo);
    using Orch::doTask;

private:
    typedef task_process_status (BufferMgrDynamic::*buffer_table_handler)(Consumer& consumer);
    typedef std::map<std::string, buffer_table_handler> buffer_table_handler_map;
    typedef std::pair<std::string, buffer_table_handler> buffer_handler_pair;

    buffer_table_handler_map m_bufferTableHandlerMap;

    bool m_portInitDone;
    bool m_firstTimeCalculateBufferPool;

    std::shared_ptr<DBConnector> m_applDb = nullptr;
    SelectableTimer *m_buffermgrPeriodtimer = nullptr;

    // PORT and CABLE_LENGTH table and caches
    Table m_cfgPortTable;
    Table m_cfgCableLenTable;
    // m_portInfoLookup
    // key: port name
    // updated only when a port's speed and cable length updated
    port_info_lookup_t m_portInfoLookup;

    // BUFFER_POOL table and cache
    ProducerStateTable m_applBufferPoolTable;
    Table m_stateBufferPoolTable;
    buffer_pool_lookup_t m_bufferPoolLookup;

    // BUFFER_PROFILE table and caches
    ProducerStateTable m_applBufferProfileTable;
    Table m_cfgBufferProfileTable;
    Table m_stateBufferProfileTable;
    // m_bufferProfileLookup - the cache for the following set:
    // 1. CFG_BUFFER_PROFILE
    // 2. Dynamically calculated headroom info stored in APPL_BUFFER_PROFILE
    // key: profile name
    buffer_profile_lookup_t m_bufferProfileLookup;
    // A set where the ignored profiles are stored.
    // A PG that reference an ignored profile should also be ignored.
    std::set<std::string> m_bufferProfileIgnored;
    std::set<std::string> m_bufferProfilePendingRemoved;

    // BUFFER_PG table and caches
    ProducerStateTable m_applBufferPgTable;
    Table m_cfgBufferPgTable;
    // m_portPgLookup - the cache for CFG_BUFFER_PG and APPL_BUFFER_PG
    // 1st level key: port name, 2nd level key: PGs
    // Updated in:
    // 1. handleBufferPgTable, update from database
    // 2. doSpeedOrCableLengthUpdateTask, speed/cable length updated
    port_pg_lookup_t m_portPgLookup;

    // Other tables
    Table m_cfgLosslessPgPoolTable;
    Table m_cfgDefaultLosslessBufferParam;

    Table m_stateBufferMaximumTable;

    ProducerStateTable m_applBufferQueueTable;
    ProducerStateTable m_applBufferIngressProfileListTable;
    ProducerStateTable m_applBufferEgressProfileListTable;

    Table m_applPortTable;

    bool m_supportGearbox;
    gearbox_delay_t m_gearboxDelay;
    std::string m_identifyGearboxDelay;

    // Vendor specific lua plugins for calculating headroom and buffer pool
    // Loaded when the buffer manager starts
    // Executed whenever the headroom and pool size need to be updated
    std::string m_headroomSha;
    std::string m_bufferpoolSha;
    std::string m_checkHeadroomSha;

    // Parameters for headroom generation
    std::string m_mmuSize;
    std::string m_defaultThreshold;
    // Default reserved size for ingress profile
    // Vendor can chose not to provide it. In that case the size will be the sum of xon and xoff
    std::string m_defaultReservedSize;

    // Initializers
    void initTableHandlerMap();
    void parseGearboxInfo(std::shared_ptr<std::vector<KeyOpFieldsValuesTuple>> gearboxInfo);

    // Tool functions to parse keys and references
    std::string getPgPoolMode();
    void transformSeperator(std::string &name);
    std::string parseObjectNameFromKey(const std::string &key, size_t pos/* = 1*/);
    std::string parseObjectNameFromReference(const std::string &reference);
    std::string getDynamicProfileName(const std::string &speed, const std::string &cable, const std::string &gearbox_model);

    // APPL_DB table operations
    void updateBufferPoolToDb(const std::string &name, const buffer_pool_t &pool, bool create);
    void updateBufferProfileToDb(const std::string &name, const buffer_profile_t &profile, bool state_db_only);
    void updateBufferPgToDb(const std::string &key, const std::string &profile, bool add);

    // Meta flows
    void calculateHeadroomSize(const std::string &speed, const std::string &cable, const std::string &gearbox_model, buffer_profile_t &headroom);
    void checkSharedBufferPoolSize();
    void recalculateSharedBufferPool();
    task_process_status allocateProfile(const std::string &speed, const std::string &cable, const std::string &gearbox_model, std::string &profile_name);
    void markProfileToBeReleased(const std::string &profile_name);
    void releaseProfile(buffer_profile_t &profile);
    void checkPendingRemovedProfiles();
    bool isHeadroomResourceValid(const std::string &port, buffer_profile_t &profile_info, std::string lossy_pg_changed);

    // Main flows
    task_process_status doSpeedOrCableLengthUpdateTask(const std::string &port, const std::string &speed, const std::string &cable_length);
    task_process_status doUpdatePgTask(const std::string &pg_key, std::string &profile);
    task_process_status doRemovePgTask(const std::string &pg_key);
    task_process_status doUpdateHeadroomOverrideTask(const std::string &pg_key, const std::string &profile);
    task_process_status doRemoveHeadroomOverrideTask(const std::string &pg_key, const std::string &profile);
    task_process_status doAdminStatusTask(const std::string port, const std::string adminStatus);
    task_process_status doUpdateStaticProfileTask(const std::string &profileName, buffer_profile_t &profile);

    // Table update handlers
    task_process_status handleBufferMaxParam(Consumer &consumer);
    task_process_status handleDefaultLossLessBufferParam(Consumer &consumer);
    task_process_status handleCableLenTable(Consumer &consumer);
    task_process_status handlePortTable(Consumer &consumer);
    task_process_status handleBufferPoolTable(Consumer &consumer);
    task_process_status handleBufferProfileTable(Consumer &consumer);
    task_process_status handleOneBufferPgEntry(const std::string &key, const std::string &port, const std::string &op, const KeyOpFieldsValuesTuple &tuple);
    task_process_status handleBufferPgTable(Consumer &consumer);
    task_process_status handleBufferQueueTable(Consumer &consumer);
    task_process_status handleBufferPortIngressProfileListTable(Consumer &consumer);
    task_process_status handleBufferPortEgressProfileListTable(Consumer &consumer);
    task_process_status doBufferTableTask(Consumer &consumer, ProducerStateTable &applTable);
    void doTask(Consumer &consumer);
    void doTask(SelectableTimer &timer);
};

}

#endif /* __BUFFMGRDYN__ */
