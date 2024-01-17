#define private public // make Directory::m_values available to clean it.
#include "directory.h"
#undef private
#define protected public
#include "orch.h"
#undef protected
#include "ut_helper.h"
#include "mock_orchagent_main.h"
#include "mock_table.h"
#include "mock_response_publisher.h"

extern void on_switch_asic_sdk_health_event(sai_object_id_t switch_id,
                                            sai_switch_asic_sdk_health_severity_t severity,
                                            sai_timespec_t timestamp,
                                            sai_switch_asic_sdk_health_category_t category,
                                            sai_switch_health_data_t data,
                                            const sai_u8_list_t description);

namespace switchorch_test
{
    using namespace std;

    sai_switch_api_t ut_sai_switch_api;
    sai_switch_api_t *pold_sai_switch_api;

    shared_ptr<swss::DBConnector> m_app_db;
    shared_ptr<swss::DBConnector> m_config_db;
    shared_ptr<swss::DBConnector> m_state_db;

    sai_switch_attr_t _ut_stub_asic_sdk_health_event_attribute_to_check;
    set<sai_switch_asic_sdk_health_category_t> _ut_stub_asic_sdk_health_event_passed_categories;

    sai_status_t _ut_stub_sai_set_switch_attribute(
        _In_ sai_object_id_t switch_id,
        _In_ const sai_attribute_t *attr)
    {
        switch (attr[0].id)
        {
        case SAI_SWITCH_ATTR_REG_FATAL_SWITCH_ASIC_SDK_HEALTH_CATEGORY:
        case SAI_SWITCH_ATTR_REG_WARNING_SWITCH_ASIC_SDK_HEALTH_CATEGORY:
        case SAI_SWITCH_ATTR_REG_NOTICE_SWITCH_ASIC_SDK_HEALTH_CATEGORY:
            if (_ut_stub_asic_sdk_health_event_attribute_to_check == attr[0].id)
            {
                auto *passed_category_list = reinterpret_cast<sai_switch_asic_sdk_health_category_t*>(attr[0].value.s32list.list);
                _ut_stub_asic_sdk_health_event_passed_categories = set<sai_switch_asic_sdk_health_category_t>(passed_category_list, passed_category_list + attr[0].value.s32list.count);
            }
            return SAI_STATUS_SUCCESS;
        default:
            break;
        }
        return pold_sai_switch_api->set_switch_attribute(switch_id, attr);
    }

    void _hook_sai_apis()
    {
        ut_sai_switch_api = *sai_switch_api;
        pold_sai_switch_api = sai_switch_api;
        ut_sai_switch_api.set_switch_attribute = _ut_stub_sai_set_switch_attribute;
        sai_switch_api = &ut_sai_switch_api;
    }

    void _unhook_sai_apis()
    {
        sai_switch_api = pold_sai_switch_api;
    }

    struct SwitchOrchTest : public ::testing::Test
    {
        SwitchOrchTest()
        {
        }

        void SetUp() override
        {
            map<string, string> profile = {
                { "SAI_VS_SWITCH_TYPE", "SAI_VS_SWITCH_TYPE_BCM56850" },
                { "KV_DEVICE_MAC_ADDRESS", "20:03:04:05:06:00" }
            };

            ut_helper::initSaiApi(profile);

            // Init switch and create dependencies
            m_app_db = make_shared<swss::DBConnector>("APPL_DB", 0);
            m_config_db = make_shared<swss::DBConnector>("CONFIG_DB", 0);
            m_state_db = make_shared<swss::DBConnector>("STATE_DB", 0);

            sai_attribute_t attr;

            attr.id = SAI_SWITCH_ATTR_INIT_SWITCH;
            attr.value.booldata = true;

            auto status = sai_switch_api->create_switch(&gSwitchId, 1, &attr);
            ASSERT_EQ(status, SAI_STATUS_SUCCESS);

            TableConnector stateDbSwitchTable(m_state_db.get(), "SWITCH_CAPABILITY");
            TableConnector conf_asic_sensors(m_config_db.get(), CFG_ASIC_SENSORS_TABLE_NAME);
            TableConnector app_switch_table(m_app_db.get(),  APP_SWITCH_TABLE_NAME);
            TableConnector conf_suppress_asic_sdk_health_categories(m_config_db.get(), CFG_SUPPRESS_ASIC_SDK_HEALTH_EVENT_NAME);

            vector<TableConnector> switch_tables = {
                conf_asic_sensors,
                conf_suppress_asic_sdk_health_categories,
                app_switch_table
            };

            ASSERT_EQ(gSwitchOrch, nullptr);
            gSwitchOrch = new SwitchOrch(m_app_db.get(), switch_tables, stateDbSwitchTable);
        }

        void TearDown() override
        {
            gDirectory.m_values.clear();

            delete gSwitchOrch;
            gSwitchOrch = nullptr;

            ut_helper::uninitSaiApi();
        }
    };

    TEST_F(SwitchOrchTest, SwitchOrchTestSuppressCategories)
    {
        _hook_sai_apis();
        vector<string> ts;
        std::deque<KeyOpFieldsValuesTuple> entries;
        Table suppressAsicSdkHealthEventTable = Table(m_config_db.get(), CFG_SUPPRESS_ASIC_SDK_HEALTH_EVENT_NAME);
        set<sai_switch_asic_sdk_health_category_t> all_categories({
                SAI_SWITCH_ASIC_SDK_HEALTH_CATEGORY_SW,
                SAI_SWITCH_ASIC_SDK_HEALTH_CATEGORY_FW,
                SAI_SWITCH_ASIC_SDK_HEALTH_CATEGORY_CPU_HW,
                SAI_SWITCH_ASIC_SDK_HEALTH_CATEGORY_ASIC_HW});
        set<sai_switch_asic_sdk_health_category_t> empty_category;

        // case: severity: fatal, operation: suppress all categories
        entries.push_back({"fatal", "SET",
                           {
                               {"categories", "software,firmware,cpu_hw,asic_hw"}
                           }});
        auto consumer = dynamic_cast<Consumer *>(gSwitchOrch->getExecutor(CFG_SUPPRESS_ASIC_SDK_HEALTH_EVENT_NAME));
        consumer->addToSync(entries);
        entries.clear();
        _ut_stub_asic_sdk_health_event_attribute_to_check = SAI_SWITCH_ATTR_REG_FATAL_SWITCH_ASIC_SDK_HEALTH_CATEGORY;
        static_cast<Orch *>(gSwitchOrch)->doTask();
        ASSERT_EQ(_ut_stub_asic_sdk_health_event_passed_categories, empty_category);

        // case: severity: warning, operation: suppress partial categories
        entries.push_back({"warning", "SET",
                           {
                               {"categories", "software,cpu_hw"}
                           }});
        consumer->addToSync(entries);
        entries.clear();
        _ut_stub_asic_sdk_health_event_attribute_to_check = SAI_SWITCH_ATTR_REG_WARNING_SWITCH_ASIC_SDK_HEALTH_CATEGORY;
        static_cast<Orch *>(gSwitchOrch)->doTask();
        ASSERT_EQ(_ut_stub_asic_sdk_health_event_passed_categories, set<sai_switch_asic_sdk_health_category_t>({
                    SAI_SWITCH_ASIC_SDK_HEALTH_CATEGORY_FW,
                    SAI_SWITCH_ASIC_SDK_HEALTH_CATEGORY_ASIC_HW}));

        // case: severity: warning, operation: set max_events only, which means to remove suppress list
        entries.push_back({"warning", "SET",
                           {
                               {"max_events", "10"}
                           }});
        consumer->addToSync(entries);
        entries.clear();
        _ut_stub_asic_sdk_health_event_attribute_to_check = SAI_SWITCH_ATTR_REG_WARNING_SWITCH_ASIC_SDK_HEALTH_CATEGORY;
        static_cast<Orch *>(gSwitchOrch)->doTask();
        ASSERT_EQ(_ut_stub_asic_sdk_health_event_passed_categories, set<sai_switch_asic_sdk_health_category_t>({
                    SAI_SWITCH_ASIC_SDK_HEALTH_CATEGORY_SW,
                    SAI_SWITCH_ASIC_SDK_HEALTH_CATEGORY_FW,
                    SAI_SWITCH_ASIC_SDK_HEALTH_CATEGORY_CPU_HW,
                    SAI_SWITCH_ASIC_SDK_HEALTH_CATEGORY_ASIC_HW}));

        // case: severity: notice, operation: suppress no category
        entries.push_back({"notice", "DEL", {}});
        consumer->addToSync(entries);
        entries.clear();
        _ut_stub_asic_sdk_health_event_attribute_to_check = SAI_SWITCH_ATTR_REG_NOTICE_SWITCH_ASIC_SDK_HEALTH_CATEGORY;
        static_cast<Orch *>(gSwitchOrch)->doTask();
        ASSERT_EQ(_ut_stub_asic_sdk_health_event_passed_categories, all_categories);

        _unhook_sai_apis();
    }

    TEST_F(SwitchOrchTest, SwitchOrchTestCheckCapability)
    {
        string value;
        gSwitchOrch->m_switchTable.hget("switch", SWITCH_CAPABILITY_TABLE_ASIC_SDK_HEALTH_EVENT_CAPABLE, value);
        ASSERT_EQ(value, "true");
        gSwitchOrch->m_switchTable.hget("switch", SWITCH_CAPABILITY_TABLE_REG_FATAL_ASIC_SDK_HEALTH_CATEGORY, value);
        ASSERT_EQ(value, "true");
        gSwitchOrch->m_switchTable.hget("switch", SWITCH_CAPABILITY_TABLE_REG_WARNING_ASIC_SDK_HEALTH_CATEGORY, value);
        ASSERT_EQ(value, "true");
        gSwitchOrch->m_switchTable.hget("switch", SWITCH_CAPABILITY_TABLE_REG_NOTICE_ASIC_SDK_HEALTH_CATEGORY, value);
        ASSERT_EQ(value, "true");
    }

    TEST_F(SwitchOrchTest, SwitchOrchTestHandleEvent)
    {
        sai_timespec_t timestamp = {.tv_sec = 1701160447, .tv_nsec = 538710245};
        sai_switch_health_data_t data = {.data_type = SAI_HEALTH_DATA_TYPE_GENERAL};
        vector<uint8_t> data_from_sai({100, 101, 115, 99, 114, 105, 112, 116, 105, 245, 111, 110, 245, 10, 123, 125, 100, 100});
        sai_u8_list_t description;
        description.list = data_from_sai.data();
        description.count = (uint32_t)(data_from_sai.size() - 2);
        on_switch_asic_sdk_health_event(gSwitchId,
                                        SAI_SWITCH_ASIC_SDK_HEALTH_SEVERITY_FATAL,
                                        timestamp,
                                        SAI_SWITCH_ASIC_SDK_HEALTH_CATEGORY_FW,
                                        data,
                                        description);

        string key = "2023-11-28 08:34:07";
        string value;
        gSwitchOrch->m_asicSdkHealthEventTable->hget(key, "category", value);
        ASSERT_EQ(value, "firmware");
        gSwitchOrch->m_asicSdkHealthEventTable->hget(key, "severity", value);
        ASSERT_EQ(value, "fatal");
        gSwitchOrch->m_asicSdkHealthEventTable->hget(key, "description", value);
        ASSERT_EQ(value, "description\n{}");
    }
}
