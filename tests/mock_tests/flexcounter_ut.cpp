#define private public // make Directory::m_values available to clean it.
#include "directory.h"
#undef private

#include "json.h"
#include "ut_helper.h"
#include "mock_orchagent_main.h"
#include "mock_table.h"
#include "notifier.h"
#define private public
#include "pfcactionhandler.h"
#include "switchorch.h"
#include <sys/mman.h>
#undef private
#define private public
#include "warm_restart.h"
#undef private

#include <sstream>

extern bool gTraditionalFlexCounter;

namespace flexcounter_test
{
    using namespace std;

    // SAI default ports
    std::map<std::string, std::vector<swss::FieldValueTuple>> defaultPortList;

    shared_ptr<swss::DBConnector> mockFlexCounterDb;
    shared_ptr<swss::Table> mockFlexCounterGroupTable;
    shared_ptr<swss::Table> mockFlexCounterTable;
    sai_set_switch_attribute_fn mockOldSaiSetSwitchAttribute;

    void mock_counter_init(sai_set_switch_attribute_fn old)
    {
        mockFlexCounterDb = make_shared<swss::DBConnector>("FLEX_COUNTER_DB", 0);
        mockFlexCounterGroupTable = make_shared<swss::Table>(mockFlexCounterDb.get(), "FLEX_COUNTER_GROUP_TABLE");
        mockFlexCounterTable = make_shared<swss::Table>(mockFlexCounterDb.get(), "FLEX_COUNTER_TABLE");

        mockOldSaiSetSwitchAttribute = old;
    }

    sai_status_t mockFlexCounterOperation(sai_object_id_t objectId, const sai_attribute_t *attr)
    {
        if (objectId != gSwitchId)
        {
            return SAI_STATUS_FAILURE;
        }

        auto *param = reinterpret_cast<sai_redis_flex_counter_parameter_t*>(attr->value.ptr);
        std::vector<swss::FieldValueTuple> entries;
        auto serializedObjectId = sai_serialize_object_id(objectId);
        std::string key((const char*)param->counter_key.list);

        if (param->stats_mode.list != nullptr)
        {
            entries.push_back({STATS_MODE_FIELD, (const char*)param->stats_mode.list});
        }

        if (param->counter_ids.list != nullptr)
        {
            entries.push_back({(const char*)param->counter_field_name.list, (const char*)param->counter_ids.list});
            mockFlexCounterTable->set(key, entries);
        }
        else
        {
            mockFlexCounterTable->del(key);
        }

        return SAI_STATUS_SUCCESS;
    }

    sai_status_t mockFlexCounterGroupOperation(sai_object_id_t objectId, const sai_attribute_t *attr)
    {
        if (objectId != gSwitchId)
        {
            return SAI_STATUS_FAILURE;
        }

        std::vector<swss::FieldValueTuple> entries;
        sai_redis_flex_counter_group_parameter_t *flexCounterGroupParam = reinterpret_cast<sai_redis_flex_counter_group_parameter_t*>(attr->value.ptr);

        std::string key((const char*)flexCounterGroupParam->counter_group_name.list);

        if (flexCounterGroupParam->poll_interval.list != nullptr)
        {
            entries.push_back({POLL_INTERVAL_FIELD, (const char*)flexCounterGroupParam->poll_interval.list});
        }

        if (flexCounterGroupParam->stats_mode.list != nullptr)
        {
            entries.push_back({STATS_MODE_FIELD, (const char*)flexCounterGroupParam->stats_mode.list});
        }

        if (flexCounterGroupParam->plugin_name.list != nullptr)
        {
            entries.push_back({(const char*)flexCounterGroupParam->plugin_name.list, ""});
        }

        if (flexCounterGroupParam->operation.list != nullptr)
        {
            entries.push_back({FLEX_COUNTER_STATUS_FIELD, (const char*)flexCounterGroupParam->operation.list});
        }

        mockFlexCounterGroupTable->set(key, entries);

        return SAI_STATUS_SUCCESS;
    }

    bool _checkFlexCounterTableContent(std::shared_ptr<swss::Table> table, const std::string key, std::vector<swss::FieldValueTuple> entries)
    {
        vector<FieldValueTuple> fieldValues;
        if (table->get(key, fieldValues))
        {
            set<FieldValueTuple> fvSet(fieldValues.begin(), fieldValues.end());
            set<FieldValueTuple> expectedSet(entries.begin(), entries.end());

            return (fvSet == expectedSet);
        }

        return false;
    }

    bool checkFlexCounterGroup(const std::string group, std::vector<swss::FieldValueTuple> entries)
    {
        return _checkFlexCounterTableContent(mockFlexCounterGroupTable, group, entries);
    }

    bool checkFlexCounter(const std::string group, sai_object_id_t oid, const std::string counter_field_name, const std::string mode="")
    {
        std::vector<swss::FieldValueTuple> entries;

        if (!mockFlexCounterTable->get(group + ":" + sai_serialize_object_id(oid), entries))
        {
            return false;
        }

        if (fvField(entries[0]) == counter_field_name)
        {
            if (mode == "")
            {
                // only 1 item: counter IDs
                return true;
            }
            else
            {
                // 1st item: counter ID, 2nd item: mode
                return (fvField(entries[1]) == "mode") && (fvValue(entries[1]) == mode);
            }
        }
        else if (mode != "")
        {
            // 1st item: mode, 2nd item: counter ID
            return (fvField(entries[0]) == "mode") && (fvValue(entries[0]) == mode) && (fvField(entries[1]) == counter_field_name);
        }

        return false;
    }

    bool checkFlexCounter(const std::string group, sai_object_id_t oid, std::vector<swss::FieldValueTuple> entries)
    {
        return _checkFlexCounterTableContent(mockFlexCounterTable, group + ":" + sai_serialize_object_id(oid), entries);
    }

    sai_switch_api_t ut_sai_switch_api;
    sai_switch_api_t *pold_sai_switch_api;

    sai_status_t _ut_stub_sai_set_switch_attribute(
        _In_ sai_object_id_t switch_id,
        _In_ const sai_attribute_t *attr)
    {
        if (attr[0].id == SAI_REDIS_SWITCH_ATTR_FLEX_COUNTER_GROUP)
        {
            mockFlexCounterGroupOperation(switch_id, attr);
        }
        else if (attr[0].id == SAI_REDIS_SWITCH_ATTR_FLEX_COUNTER)
        {
            mockFlexCounterOperation(switch_id, attr);
        }
        return pold_sai_switch_api->set_switch_attribute(switch_id, attr);
    }

    void _hook_sai_switch_api()
    {
        ut_sai_switch_api = *sai_switch_api;
        pold_sai_switch_api = sai_switch_api;
        ut_sai_switch_api.set_switch_attribute = _ut_stub_sai_set_switch_attribute;
        sai_switch_api = &ut_sai_switch_api;
        mock_counter_init(nullptr);
    }

    void _unhook_sai_switch_api()
    {
        sai_switch_api = pold_sai_switch_api;
    }

    struct FlexCounterTest : public ::testing::Test
    {
        shared_ptr<swss::DBConnector> m_app_db;
        shared_ptr<swss::DBConnector> m_config_db;
        shared_ptr<swss::DBConnector> m_state_db;
        shared_ptr<swss::DBConnector> m_counters_db;
        shared_ptr<swss::DBConnector> m_chassis_app_db;
        shared_ptr<swss::DBConnector> m_asic_db;
        shared_ptr<swss::DBConnector> m_flex_counter_db;

        FlexCounterTest()
        {
            // FIXME: move out from constructor
            m_app_db = make_shared<swss::DBConnector>(
                "APPL_DB", 0);
            m_counters_db = make_shared<swss::DBConnector>(
                "COUNTERS_DB", 0);
            m_config_db = make_shared<swss::DBConnector>(
                "CONFIG_DB", 0);
            m_state_db = make_shared<swss::DBConnector>(
                "STATE_DB", 0);
            m_chassis_app_db = make_shared<swss::DBConnector>(
                "CHASSIS_APP_DB", 0);
            m_asic_db = make_shared<swss::DBConnector>(
                "ASIC_DB", 0);
            m_flex_counter_db = make_shared<swss::DBConnector>(
                "FLEX_COUNTER_DB", 0);
        }

        virtual void SetUp() override
        {
            ::testing_db::reset();

            gTraditionalFlexCounter = false;

            _hook_sai_switch_api();

            // Create dependencies ...
            TableConnector stateDbSwitchTable(m_state_db.get(), "SWITCH_CAPABILITY");
            TableConnector app_switch_table(m_app_db.get(), APP_SWITCH_TABLE_NAME);
            TableConnector conf_asic_sensors(m_config_db.get(), CFG_ASIC_SENSORS_TABLE_NAME);

            vector<TableConnector> switch_tables = {
                conf_asic_sensors,
                app_switch_table
            };

            ASSERT_EQ(gSwitchOrch, nullptr);
            gSwitchOrch = new SwitchOrch(m_app_db.get(), switch_tables, stateDbSwitchTable);

            const int portsorch_base_pri = 40;

            vector<table_name_with_pri_t> ports_tables = {
                { APP_PORT_TABLE_NAME, portsorch_base_pri + 5 },
                { APP_SEND_TO_INGRESS_PORT_TABLE_NAME, portsorch_base_pri + 5 },
                { APP_VLAN_TABLE_NAME, portsorch_base_pri + 2 },
                { APP_VLAN_MEMBER_TABLE_NAME, portsorch_base_pri },
                { APP_LAG_TABLE_NAME, portsorch_base_pri + 4 },
                { APP_LAG_MEMBER_TABLE_NAME, portsorch_base_pri }
            };

            ASSERT_EQ(gPortsOrch, nullptr);

            gPortsOrch = new PortsOrch(m_app_db.get(), m_state_db.get(), ports_tables, m_chassis_app_db.get());

            vector<string> flex_counter_tables = {
                CFG_FLEX_COUNTER_TABLE_NAME
            };
            auto* flexCounterOrch = new FlexCounterOrch(m_config_db.get(), flex_counter_tables);
            gDirectory.set(flexCounterOrch);

            vector<string> buffer_tables = { APP_BUFFER_POOL_TABLE_NAME,
                                             APP_BUFFER_PROFILE_TABLE_NAME,
                                             APP_BUFFER_QUEUE_TABLE_NAME,
                                             APP_BUFFER_PG_TABLE_NAME,
                                             APP_BUFFER_PORT_INGRESS_PROFILE_LIST_NAME,
                                             APP_BUFFER_PORT_EGRESS_PROFILE_LIST_NAME };

            ASSERT_EQ(gBufferOrch, nullptr);
            gBufferOrch = new BufferOrch(m_app_db.get(), m_config_db.get(), m_state_db.get(), buffer_tables);

            Table portTable = Table(m_app_db.get(), APP_PORT_TABLE_NAME);

            // Get SAI default ports to populate DB
            auto ports = ut_helper::getInitialSaiPorts();

            // Populate pot table with SAI ports
            for (const auto &it : ports)
            {
                portTable.set(it.first, it.second);
            }

            // Set PortConfigDone
            portTable.set("PortConfigDone", { { "count", to_string(ports.size()) } });
            gPortsOrch->addExistingData(&portTable);
            static_cast<Orch *>(gPortsOrch)->doTask();

            portTable.set("PortInitDone", { { "lanes", "0" } });
            gPortsOrch->addExistingData(&portTable);
            static_cast<Orch *>(gPortsOrch)->doTask();

            ASSERT_EQ(gIntfsOrch, nullptr);
            gIntfsOrch = new IntfsOrch(m_app_db.get(), APP_INTF_TABLE_NAME, gVrfOrch, m_chassis_app_db.get());
        }

        virtual void TearDown() override
        {
            ::testing_db::reset();

            auto buffer_maps = BufferOrch::m_buffer_type_maps;
            for (auto &i : buffer_maps)
            {
                i.second->clear();
            }

            delete gNeighOrch;
            gNeighOrch = nullptr;
            delete gFdbOrch;
            gFdbOrch = nullptr;
            delete gIntfsOrch;
            gIntfsOrch = nullptr;
            delete gPortsOrch;
            gPortsOrch = nullptr;
            delete gBufferOrch;
            gBufferOrch = nullptr;
            delete gPfcwdOrch<PfcWdDlrHandler, PfcWdDlrHandler>;
            gPfcwdOrch<PfcWdDlrHandler, PfcWdDlrHandler> = nullptr;
            delete gQosOrch;
            gQosOrch = nullptr;
            delete gSwitchOrch;
            gSwitchOrch = nullptr;

            // clear orchs saved in directory
            gDirectory.m_values.clear();

            _unhook_sai_switch_api();
        }

        static void SetUpTestCase()
        {
            // Init switch and create dependencies

            map<string, string> profile = {
                { "SAI_VS_SWITCH_TYPE", "SAI_VS_SWITCH_TYPE_BCM56850" },
                { "KV_DEVICE_MAC_ADDRESS", "20:03:04:05:06:00" }
            };

            auto status = ut_helper::initSaiApi(profile);
            ASSERT_EQ(status, SAI_STATUS_SUCCESS);

            sai_attribute_t attr;

            attr.id = SAI_SWITCH_ATTR_INIT_SWITCH;
            attr.value.booldata = true;

            status = sai_switch_api->create_switch(&gSwitchId, 1, &attr);
            ASSERT_EQ(status, SAI_STATUS_SUCCESS);

            // Get switch source MAC address
            attr.id = SAI_SWITCH_ATTR_SRC_MAC_ADDRESS;
            status = sai_switch_api->get_switch_attribute(gSwitchId, 1, &attr);

            ASSERT_EQ(status, SAI_STATUS_SUCCESS);

            gMacAddress = attr.value.mac;

            // Get the default virtual router ID
            attr.id = SAI_SWITCH_ATTR_DEFAULT_VIRTUAL_ROUTER_ID;
            status = sai_switch_api->get_switch_attribute(gSwitchId, 1, &attr);

            ASSERT_EQ(status, SAI_STATUS_SUCCESS);

            gVirtualRouterId = attr.value.oid;

            // Get SAI default ports
            defaultPortList = ut_helper::getInitialSaiPorts();
            ASSERT_TRUE(!defaultPortList.empty());
        }

        static void TearDownTestCase()
        {
            auto status = sai_switch_api->remove_switch(gSwitchId);
            ASSERT_EQ(status, SAI_STATUS_SUCCESS);
            gSwitchId = 0;

            ut_helper::uninitSaiApi();
        }

    };

    TEST_F(FlexCounterTest, CounterTest)
    {
        // Check flex counter database after system initialization
        ASSERT_TRUE(checkFlexCounterGroup(QUEUE_WATERMARK_STAT_COUNTER_FLEX_COUNTER_GROUP,
                                          {
                                              {STATS_MODE_FIELD, STATS_MODE_READ_AND_CLEAR},
                                              {POLL_INTERVAL_FIELD, QUEUE_WATERMARK_FLEX_STAT_COUNTER_POLL_MSECS},
                                              {QUEUE_PLUGIN_FIELD, ""}
                                          }));
        ASSERT_TRUE(checkFlexCounterGroup(PG_WATERMARK_STAT_COUNTER_FLEX_COUNTER_GROUP,
                                          {
                                              {STATS_MODE_FIELD, STATS_MODE_READ_AND_CLEAR},
                                              {POLL_INTERVAL_FIELD, PG_WATERMARK_FLEX_STAT_COUNTER_POLL_MSECS},
                                              {PG_PLUGIN_FIELD, ""}
                                          }));
        ASSERT_TRUE(checkFlexCounterGroup(PORT_STAT_COUNTER_FLEX_COUNTER_GROUP,
                                          {
                                              {STATS_MODE_FIELD, STATS_MODE_READ},
                                              {POLL_INTERVAL_FIELD, PORT_RATE_FLEX_COUNTER_POLLING_INTERVAL_MS},
                                              {PORT_PLUGIN_FIELD, ""},
                                              {FLEX_COUNTER_STATUS_FIELD, "disable"}
                                          }));
        ASSERT_TRUE(checkFlexCounterGroup(PG_DROP_STAT_COUNTER_FLEX_COUNTER_GROUP,
                                          {
                                              {STATS_MODE_FIELD, STATS_MODE_READ},
                                              {POLL_INTERVAL_FIELD, PG_DROP_FLEX_STAT_COUNTER_POLL_MSECS},
                                          }));
        ASSERT_TRUE(checkFlexCounterGroup(RIF_STAT_COUNTER_FLEX_COUNTER_GROUP,
                                          {
                                              {STATS_MODE_FIELD, STATS_MODE_READ},
                                              {POLL_INTERVAL_FIELD, "1000"},
                                              {RIF_PLUGIN_FIELD, ""},
                                          }));

        Table portTable = Table(m_app_db.get(), APP_PORT_TABLE_NAME);
        Table sendToIngressPortTable = Table(m_app_db.get(), APP_SEND_TO_INGRESS_PORT_TABLE_NAME);
        Table pgTable = Table(m_app_db.get(), APP_BUFFER_PG_TABLE_NAME);
        Table pgTableCfg = Table(m_config_db.get(), CFG_BUFFER_PG_TABLE_NAME);
        Table profileTable = Table(m_app_db.get(), APP_BUFFER_PROFILE_TABLE_NAME);
        Table poolTable = Table(m_app_db.get(), APP_BUFFER_POOL_TABLE_NAME);
        Table flexCounterCfg = Table(m_config_db.get(), CFG_FLEX_COUNTER_TABLE_NAME);

        // Get SAI default ports to populate DB
        auto ports = ut_helper::getInitialSaiPorts();

        // Create test buffer pool
        poolTable.set(
            "test_pool",
            {
                { "type", "ingress" },
                { "mode", "dynamic" },
                { "size", "4200000" },
            });

        // Create test buffer profile
        profileTable.set("test_profile", { { "pool", "test_pool" },
                                           { "xon", "14832" },
                                           { "xoff", "14832" },
                                           { "size", "35000" },
                                           { "dynamic_th", "0" } });

        // Apply profile on PGs 3-4 all ports
        for (const auto &it : ports)
        {
            std::ostringstream ossAppl, ossCfg;
            ossAppl << it.first << ":3-4";
            pgTable.set(ossAppl.str(), { { "profile", "test_profile" } });
            ossCfg << it.first << "|3-4";
            pgTableCfg.set(ossCfg.str(), { { "profile", "test_profile" } });
        }

        // Populate pot table with SAI ports
        for (const auto &it : ports)
        {
            portTable.set(it.first, it.second);
        }

        // Set PortConfigDone
        portTable.set("PortConfigDone", { { "count", to_string(ports.size()) } });
        // Populate send to ingresss port table
        sendToIngressPortTable.set("SEND_TO_INGRESS", {{"NULL", "NULL"}});

        // refill consumer
        gPortsOrch->addExistingData(&portTable);
        gBufferOrch->addExistingData(&pgTable);
        gBufferOrch->addExistingData(&poolTable);
        gBufferOrch->addExistingData(&profileTable);

        // Apply configuration :
        //  create ports
        static_cast<Orch *>(gBufferOrch)->doTask();
        static_cast<Orch *>(gPortsOrch)->doTask();

        portTable.set("PortInitDone", { { "lanes", "0" } });
        gPortsOrch->addExistingData(&portTable);

        // Apply configuration
        //  configure buffers
        //          ports
        static_cast<Orch *>(gPortsOrch)->doTask();

        // Since init done is set now, apply buffers
        static_cast<Orch *>(gBufferOrch)->doTask();

        ASSERT_TRUE(gPortsOrch->allPortsReady());

        // Enable and check counters
        const std::vector<FieldValueTuple> values({ {FLEX_COUNTER_DELAY_STATUS_FIELD, "false"},
                                                    {FLEX_COUNTER_STATUS_FIELD, "enable"} });
        flexCounterCfg.set("PG_WATERMARK", values);
        flexCounterCfg.set("QUEUE_WATERMARK", values);
        flexCounterCfg.set("QUEUE", values);
        flexCounterCfg.set("PORT_BUFFER_DROP", values);
        flexCounterCfg.set("PG_DROP", values);
        flexCounterCfg.set("PORT", values);
        flexCounterCfg.set("BUFFER_POOL_WATERMARK", values);
        flexCounterCfg.set("PFCWD", values);

        auto flexCounterOrch = gDirectory.get<FlexCounterOrch*>();
        flexCounterOrch->addExistingData(&flexCounterCfg);
        static_cast<Orch *>(flexCounterOrch)->doTask();

        ASSERT_TRUE(checkFlexCounterGroup(BUFFER_POOL_WATERMARK_STAT_COUNTER_FLEX_COUNTER_GROUP,
                                          {
                                              {POLL_INTERVAL_FIELD, "60000"},
                                              {STATS_MODE_FIELD, STATS_MODE_READ_AND_CLEAR},
                                              {FLEX_COUNTER_STATUS_FIELD, "enable"},
                                              {BUFFER_POOL_PLUGIN_FIELD, ""}
                                          }));
        ASSERT_TRUE(checkFlexCounterGroup(QUEUE_WATERMARK_STAT_COUNTER_FLEX_COUNTER_GROUP,
                                          {
                                              {POLL_INTERVAL_FIELD, "60000"},
                                              {STATS_MODE_FIELD, STATS_MODE_READ_AND_CLEAR},
                                              {FLEX_COUNTER_STATUS_FIELD, "enable"},
                                              {QUEUE_PLUGIN_FIELD, ""}
                                          }));
        ASSERT_TRUE(checkFlexCounterGroup(PG_WATERMARK_STAT_COUNTER_FLEX_COUNTER_GROUP,
                                          {
                                              {POLL_INTERVAL_FIELD, "60000"},
                                              {STATS_MODE_FIELD, STATS_MODE_READ_AND_CLEAR},
                                              {FLEX_COUNTER_STATUS_FIELD, "enable"},
                                              {PG_PLUGIN_FIELD, ""}
                                          }));
        ASSERT_TRUE(checkFlexCounterGroup(PORT_BUFFER_DROP_STAT_FLEX_COUNTER_GROUP,
                                          {
                                              {POLL_INTERVAL_FIELD, "60000"},
                                              {STATS_MODE_FIELD, STATS_MODE_READ},
                                              {FLEX_COUNTER_STATUS_FIELD, "enable"}
                                          }));
        ASSERT_TRUE(checkFlexCounterGroup(PG_DROP_STAT_COUNTER_FLEX_COUNTER_GROUP,
                                          {
                                              {POLL_INTERVAL_FIELD, "10000"},
                                              {STATS_MODE_FIELD, STATS_MODE_READ},
                                              {FLEX_COUNTER_STATUS_FIELD, "enable"}
                                          }));
        ASSERT_TRUE(checkFlexCounterGroup(PORT_STAT_COUNTER_FLEX_COUNTER_GROUP,
                                          {
                                              {POLL_INTERVAL_FIELD, "1000"},
                                              {STATS_MODE_FIELD, STATS_MODE_READ},
                                              {FLEX_COUNTER_STATUS_FIELD, "enable"},
                                              {PORT_PLUGIN_FIELD, ""}
                                          }));
        ASSERT_TRUE(checkFlexCounterGroup(QUEUE_STAT_COUNTER_FLEX_COUNTER_GROUP,
                                          {
                                              {POLL_INTERVAL_FIELD, "10000"},
                                              {STATS_MODE_FIELD, STATS_MODE_READ},
                                              {FLEX_COUNTER_STATUS_FIELD, "enable"},
                                          }));

        sai_object_id_t oid;
        oid = (*BufferOrch::m_buffer_type_maps[APP_BUFFER_POOL_TABLE_NAME])["test_pool"].m_saiObjectId;
        ASSERT_TRUE(checkFlexCounter(BUFFER_POOL_WATERMARK_STAT_COUNTER_FLEX_COUNTER_GROUP, oid, BUFFER_POOL_COUNTER_ID_LIST));
        Port firstPort;
        gPortsOrch->getPort(ports.begin()->first, firstPort);
        oid = firstPort.m_priority_group_ids[3];
        ASSERT_TRUE(checkFlexCounter(PG_DROP_STAT_COUNTER_FLEX_COUNTER_GROUP, oid,
                                     {
                                         {PG_COUNTER_ID_LIST,
                                          "SAI_INGRESS_PRIORITY_GROUP_STAT_DROPPED_PACKETS"
                                         }
                                     }));
        ASSERT_TRUE(checkFlexCounter(PG_WATERMARK_STAT_COUNTER_FLEX_COUNTER_GROUP, oid,
                                     {
                                         {PG_COUNTER_ID_LIST,
                                          "SAI_INGRESS_PRIORITY_GROUP_STAT_XOFF_ROOM_WATERMARK_BYTES,"
                                          "SAI_INGRESS_PRIORITY_GROUP_STAT_SHARED_WATERMARK_BYTES"
                                         }
                                     }));
        oid = firstPort.m_queue_ids[3];
        ASSERT_TRUE(checkFlexCounter(QUEUE_WATERMARK_STAT_COUNTER_FLEX_COUNTER_GROUP, oid,
                                     {
                                         {QUEUE_COUNTER_ID_LIST,
                                          "SAI_QUEUE_STAT_SHARED_WATERMARK_BYTES"
                                         }
                                     }));
        ASSERT_TRUE(checkFlexCounter(QUEUE_STAT_COUNTER_FLEX_COUNTER_GROUP, oid,
                                     {
                                         {QUEUE_COUNTER_ID_LIST,
                                          "SAI_QUEUE_STAT_DROPPED_BYTES,SAI_QUEUE_STAT_DROPPED_PACKETS,"
                                          "SAI_QUEUE_STAT_BYTES,SAI_QUEUE_STAT_PACKETS"
                                         }
                                     }));
        oid = firstPort.m_port_id;
        ASSERT_TRUE(checkFlexCounter(PORT_BUFFER_DROP_STAT_FLEX_COUNTER_GROUP, oid,
                                     {
                                         {PORT_COUNTER_ID_LIST,
                                          "SAI_PORT_STAT_OUT_DROPPED_PKTS,SAI_PORT_STAT_IN_DROPPED_PKTS"
                                         }
                                     }));
        // Do not check the content of port counter since it's large and varies among platforms.
        ASSERT_TRUE(checkFlexCounter(PORT_STAT_COUNTER_FLEX_COUNTER_GROUP, oid, PORT_COUNTER_ID_LIST));

        // create a routing interface
        std::deque<KeyOpFieldsValuesTuple> entries;
        entries.push_back({firstPort.m_alias, "SET", { {"mtu", "9100"}}});
        auto consumer = dynamic_cast<Consumer *>(gIntfsOrch->getExecutor(APP_INTF_TABLE_NAME));
        consumer->addToSync(entries);
        static_cast<Orch *>(gIntfsOrch)->doTask();

        // Check flex counter database
        auto rifOid = gIntfsOrch->m_rifsToAdd[0].m_rif_id;
        (gIntfsOrch)->doTask(*gIntfsOrch->m_updateMapsTimer);
        ASSERT_TRUE(checkFlexCounter(RIF_STAT_COUNTER_FLEX_COUNTER_GROUP, rifOid,
                                     {
                                         {RIF_COUNTER_ID_LIST,
                                          "SAI_ROUTER_INTERFACE_STAT_IN_PACKETS,SAI_ROUTER_INTERFACE_STAT_IN_OCTETS,"
                                          "SAI_ROUTER_INTERFACE_STAT_IN_ERROR_PACKETS,SAI_ROUTER_INTERFACE_STAT_IN_ERROR_OCTETS,"
                                          "SAI_ROUTER_INTERFACE_STAT_OUT_PACKETS,SAI_ROUTER_INTERFACE_STAT_OUT_OCTETS,"
                                          "SAI_ROUTER_INTERFACE_STAT_OUT_ERROR_PACKETS,SAI_ROUTER_INTERFACE_STAT_OUT_ERROR_OCTETS,"
                                         }
                                     }));

        // remove the dependency, expect delete and create a new one
        entries.clear();
        entries.push_back({firstPort.m_alias, "DEL", { {} }});
        consumer->addToSync(entries);
        static_cast<Orch *>(gIntfsOrch)->doTask();

        // Check flex counter database
        ASSERT_TRUE(!checkFlexCounter(RIF_STAT_COUNTER_FLEX_COUNTER_GROUP, rifOid, RIF_COUNTER_ID_LIST));

        // PFC watchdog counter test
        vector<string> pfc_wd_tables = {
            CFG_PFC_WD_TABLE_NAME
        };

        static const vector<sai_port_stat_t> portStatIds =
        {
            SAI_PORT_STAT_PFC_0_RX_PAUSE_DURATION_US,
            SAI_PORT_STAT_PFC_1_RX_PAUSE_DURATION_US,
            SAI_PORT_STAT_PFC_2_RX_PAUSE_DURATION_US,
            SAI_PORT_STAT_PFC_3_RX_PAUSE_DURATION_US,
            SAI_PORT_STAT_PFC_4_RX_PAUSE_DURATION_US,
            SAI_PORT_STAT_PFC_5_RX_PAUSE_DURATION_US,
            SAI_PORT_STAT_PFC_6_RX_PAUSE_DURATION_US,
            SAI_PORT_STAT_PFC_7_RX_PAUSE_DURATION_US,
            SAI_PORT_STAT_PFC_0_RX_PKTS,
            SAI_PORT_STAT_PFC_1_RX_PKTS,
            SAI_PORT_STAT_PFC_2_RX_PKTS,
            SAI_PORT_STAT_PFC_3_RX_PKTS,
            SAI_PORT_STAT_PFC_4_RX_PKTS,
            SAI_PORT_STAT_PFC_5_RX_PKTS,
            SAI_PORT_STAT_PFC_6_RX_PKTS,
            SAI_PORT_STAT_PFC_7_RX_PKTS,
        };

        static const vector<sai_queue_stat_t> queueStatIds =
        {
            SAI_QUEUE_STAT_PACKETS,
            SAI_QUEUE_STAT_CURR_OCCUPANCY_BYTES,
        };

        static const vector<sai_queue_attr_t> queueAttrIds =
        {
            SAI_QUEUE_ATTR_PAUSE_STATUS,
        };

        gPfcwdOrch<PfcWdZeroBufferHandler, PfcWdLossyHandler> = new PfcWdSwOrch<PfcWdZeroBufferHandler, PfcWdLossyHandler>(
            m_config_db.get(),
            pfc_wd_tables,
            portStatIds,
            queueStatIds,
            queueAttrIds,
            100);
        gPfcwdOrch<PfcWdZeroBufferHandler, PfcWdLossyHandler>->m_platform = MLNX_PLATFORM_SUBSTRING;

        vector<string> qos_tables = {
            CFG_TC_TO_QUEUE_MAP_TABLE_NAME,
            CFG_SCHEDULER_TABLE_NAME,
            CFG_DSCP_TO_TC_MAP_TABLE_NAME,
            CFG_MPLS_TC_TO_TC_MAP_TABLE_NAME,
            CFG_DOT1P_TO_TC_MAP_TABLE_NAME,
            CFG_QUEUE_TABLE_NAME,
            CFG_PORT_QOS_MAP_TABLE_NAME,
            CFG_WRED_PROFILE_TABLE_NAME,
            CFG_TC_TO_PRIORITY_GROUP_MAP_TABLE_NAME,
            CFG_PFC_PRIORITY_TO_PRIORITY_GROUP_MAP_TABLE_NAME,
            CFG_PFC_PRIORITY_TO_QUEUE_MAP_TABLE_NAME,
            CFG_DSCP_TO_FC_MAP_TABLE_NAME,
            CFG_EXP_TO_FC_MAP_TABLE_NAME,
            CFG_TC_TO_DSCP_MAP_TABLE_NAME
        };
        gQosOrch = new QosOrch(m_config_db.get(), qos_tables);
        entries.clear();
	entries.push_back({firstPort.m_alias, "SET",
			    {
			      {"pfc_enable", "3,4"},
			      {"pfcwd_sw_enable", "3,4"}
			  }});
        auto portQosMapConsumer = dynamic_cast<Consumer *>(gQosOrch->getExecutor(CFG_PORT_QOS_MAP_TABLE_NAME));
        portQosMapConsumer->addToSync(entries);
        entries.clear();
	static_cast<Orch *>(gQosOrch)->doTask();

        // create pfcwd entry for first port with drop action
        entries.clear();
	entries.push_back({"GLOBAL", "SET",
			  {
			    {POLL_INTERVAL_FIELD, "200"},
			  }});
	entries.push_back({firstPort.m_alias, "SET",
			  {
			    {"action", "drop"},
			    {"detection_time", "200"},
			    {"restoration_time", "200"}
			  }});

        auto PfcwdConsumer = dynamic_cast<Consumer *>(gPfcwdOrch<PfcWdZeroBufferHandler, PfcWdLossyHandler>->getExecutor(CFG_PFC_WD_TABLE_NAME));
	PfcwdConsumer->addToSync(entries);
        entries.clear();

        static_cast<Orch *>(gPfcwdOrch<PfcWdZeroBufferHandler, PfcWdLossyHandler>)->doTask();

        ASSERT_TRUE(checkFlexCounterGroup(PFC_WD_FLEX_COUNTER_GROUP,
                                          {
                                              {POLL_INTERVAL_FIELD, "200"},
                                              {STATS_MODE_FIELD, STATS_MODE_READ},
                                              {FLEX_COUNTER_STATUS_FIELD, "enable"},
                                              {QUEUE_PLUGIN_FIELD, ""}
                                          }));

        ASSERT_TRUE(checkFlexCounter(PFC_WD_FLEX_COUNTER_GROUP, firstPort.m_port_id,
                                     {
                                         {PORT_COUNTER_ID_LIST, "SAI_PORT_STAT_PFC_3_RX_PAUSE_DURATION_US,SAI_PORT_STAT_PFC_4_RX_PAUSE_DURATION_US,SAI_PORT_STAT_PFC_3_RX_PKTS,SAI_PORT_STAT_PFC_4_RX_PKTS"}
                                     }));

        ASSERT_TRUE(checkFlexCounter(PFC_WD_FLEX_COUNTER_GROUP, firstPort.m_queue_ids[3],
                                     {
                                         {QUEUE_COUNTER_ID_LIST, "SAI_QUEUE_STAT_PACKETS,SAI_QUEUE_STAT_CURR_OCCUPANCY_BYTES"},
                                         {QUEUE_ATTR_ID_LIST, "SAI_QUEUE_ATTR_PAUSE_STATUS"}
                                     }));
    }
}
