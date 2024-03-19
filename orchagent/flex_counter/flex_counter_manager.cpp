#include "flex_counter_manager.h"

#include <vector>

#include "schema.h"
#include "rediscommand.h"
#include "logger.h"
#include "sai_serialize.h"

#include <macsecorch.h>

using std::shared_ptr;
using std::string;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using swss::DBConnector;
using swss::FieldValueTuple;
using swss::ProducerTable;

extern sai_switch_api_t *sai_switch_api;

extern sai_object_id_t gSwitchId;
extern bool gTraditionalFlexCounter;

const string FLEX_COUNTER_ENABLE("enable");
const string FLEX_COUNTER_DISABLE("disable");

const unordered_map<StatsMode, string> FlexCounterManager::stats_mode_lookup =
{
    { StatsMode::READ, STATS_MODE_READ },
};

const unordered_map<bool, string> FlexCounterManager::status_lookup =
{
    { false, FLEX_COUNTER_DISABLE },
    { true,  FLEX_COUNTER_ENABLE }
};

const unordered_map<CounterType, string> FlexCounterManager::counter_id_field_lookup =
{
    { CounterType::PORT_DEBUG,      PORT_DEBUG_COUNTER_ID_LIST },
    { CounterType::SWITCH_DEBUG,    SWITCH_DEBUG_COUNTER_ID_LIST },
    { CounterType::PORT,            PORT_COUNTER_ID_LIST },
    { CounterType::QUEUE,           QUEUE_COUNTER_ID_LIST },
    { CounterType::MACSEC_SA_ATTR,  MACSEC_SA_ATTR_ID_LIST },
    { CounterType::MACSEC_SA,       MACSEC_SA_COUNTER_ID_LIST },
    { CounterType::MACSEC_FLOW,     MACSEC_FLOW_COUNTER_ID_LIST },
    { CounterType::ACL_COUNTER,     ACL_COUNTER_ATTR_ID_LIST },
    { CounterType::TUNNEL,          TUNNEL_COUNTER_ID_LIST },
    { CounterType::HOSTIF_TRAP,     FLOW_COUNTER_ID_LIST },
    { CounterType::ROUTE,           FLOW_COUNTER_ID_LIST },
};

FlexManagerDirectory g_FlexManagerDirectory;

FlexCounterManager *FlexManagerDirectory::createFlexCounterManager(const string& group_name,
                                                                   const StatsMode stats_mode,
                                                                   const uint polling_interval,
                                                                   const bool enabled,
                                                                   FieldValueTuple fv_plugin)
{
    if (m_managers.find(group_name) != m_managers.end())
    {
        if (stats_mode != m_managers[group_name]->getStatsMode())
        {
            SWSS_LOG_ERROR("Stats mode mismatch with already created flex counter manager %s",
                          group_name.c_str());
            return NULL;
        }
        if (polling_interval != m_managers[group_name]->getPollingInterval())
        {
            SWSS_LOG_ERROR("Polling interval mismatch with already created flex counter manager %s",
                          group_name.c_str());
            return NULL;
        }
        if (enabled != m_managers[group_name]->getEnabled())
        {
            SWSS_LOG_ERROR("Enabled field mismatch with already created flex counter manager %s",
                          group_name.c_str());
            return NULL;
        }
        return m_managers[group_name];
    }
    FlexCounterManager *fc_manager = new FlexCounterManager(group_name, stats_mode, polling_interval,
                                                            enabled, fv_plugin);
    m_managers[group_name] = fc_manager;
    return fc_manager;
}

FlexCounterManager::FlexCounterManager(
        const string& group_name,
        const StatsMode stats_mode,
        const uint polling_interval,
        const bool enabled,
        FieldValueTuple fv_plugin) :
    FlexCounterManager("FLEX_COUNTER_DB", group_name, stats_mode,
            polling_interval, enabled, fv_plugin)
{
}

FlexCounterManager::FlexCounterManager(
        const string& db_name,
        const string& group_name,
        const StatsMode stats_mode,
        const uint polling_interval,
        const bool enabled,
        FieldValueTuple fv_plugin) :
    group_name(group_name),
    stats_mode(stats_mode),
    polling_interval(polling_interval),
    enabled(enabled),
    fv_plugin(fv_plugin),
    flex_counter_db(new DBConnector(db_name, 0)),
    flex_counter_group_table(new ProducerTable(flex_counter_db.get(),
                FLEX_COUNTER_GROUP_TABLE)),
    flex_counter_table(new ProducerTable(flex_counter_db.get(),
                FLEX_COUNTER_TABLE))
{
    SWSS_LOG_ENTER();

    applyGroupConfiguration();

    SWSS_LOG_DEBUG("Initialized flex counter group '%s'.", group_name.c_str());
}

FlexCounterManager::~FlexCounterManager()
{
    SWSS_LOG_ENTER();

    for (const auto& counter: installed_counters)
    {
        flex_counter_table->del(getFlexCounterTableKey(group_name, counter));
    }

    if (flex_counter_group_table != nullptr)
    {
        flex_counter_group_table->del(group_name);
    }

    SWSS_LOG_DEBUG("Deleted flex counter group '%s'.", group_name.c_str());
}

void FlexCounterManager::applyGroupConfiguration()
{
    SWSS_LOG_ENTER();

    if (!gTraditionalFlexCounter)
    {
        sai_redis_flex_counter_group_parameter_t flexCounterGroupParam;
        sai_attribute_t attr;

        flexCounterGroupParam.counter_group_name = group_name.c_str();
        flexCounterGroupParam.poll_interval = std::to_string(polling_interval).c_str();
        flexCounterGroupParam.stats_mode = stats_mode_lookup.at(stats_mode).c_str();
        flexCounterGroupParam.operation = status_lookup.at(enabled).c_str();
        if (!fvField(fv_plugin).empty())
        {
            flexCounterGroupParam.plugins = fvValue(fv_plugin).c_str();
            flexCounterGroupParam.plugin_name = fvField(fv_plugin).c_str();
        }
        else
        {
            flexCounterGroupParam.plugins = nullptr;
        }

        attr.id = SAI_REDIS_SWITCH_ATTR_FLEX_COUNTER_GROUP;
        attr.value.ptr = (void*)&flexCounterGroupParam;
        sai_switch_api->set_switch_attribute(gSwitchId, &attr);

        return;
    }

    vector<FieldValueTuple> field_values =
    {
        FieldValueTuple(STATS_MODE_FIELD, stats_mode_lookup.at(stats_mode)),
        FieldValueTuple(POLL_INTERVAL_FIELD, std::to_string(polling_interval)),
        FieldValueTuple(FLEX_COUNTER_STATUS_FIELD, status_lookup.at(enabled))
    };

    if (!fvField(fv_plugin).empty())
    {
        field_values.emplace_back(fv_plugin);
    }

    flex_counter_group_table->set(group_name, field_values);
}

void FlexCounterManager::updateGroupPollingInterval(
        const uint polling_interval)
{
    SWSS_LOG_ENTER();

    if (!gTraditionalFlexCounter)
    {
        sai_redis_flex_counter_group_parameter_t flexCounterGroupParam;
        sai_attribute_t attr;
        std::string pollingIntervalStr = std::to_string(polling_interval);

        flexCounterGroupParam.counter_group_name = group_name.c_str();
        flexCounterGroupParam.poll_interval = pollingIntervalStr.c_str();
        flexCounterGroupParam.stats_mode = nullptr;
        flexCounterGroupParam.operation = nullptr;
        flexCounterGroupParam.plugins = nullptr;

        attr.id = SAI_REDIS_SWITCH_ATTR_FLEX_COUNTER_GROUP;
        attr.value.ptr = (void*)&flexCounterGroupParam;
        sai_switch_api->set_switch_attribute(gSwitchId, &attr);

        return;
    }

    vector<FieldValueTuple> field_values =
    {
        FieldValueTuple(POLL_INTERVAL_FIELD, std::to_string(polling_interval))
    };
    flex_counter_group_table->set(group_name, field_values);

    SWSS_LOG_DEBUG("Set polling interval for flex counter group '%s' to %d ms.",
            group_name.c_str(), polling_interval);
}

// enableFlexCounterGroup will do nothing if the flex counter group is already
// enabled.
void FlexCounterManager::enableFlexCounterGroup()
{
    SWSS_LOG_ENTER();

    if (enabled)
    {
        return;
    }

    if (!gTraditionalFlexCounter)
    {
        sai_redis_flex_counter_group_parameter_t flexCounterGroupParam;
        sai_attribute_t attr;

        flexCounterGroupParam.counter_group_name = group_name.c_str();
        flexCounterGroupParam.poll_interval = nullptr;
        flexCounterGroupParam.stats_mode = nullptr;
        flexCounterGroupParam.operation = FLEX_COUNTER_ENABLE.c_str();
        flexCounterGroupParam.plugins = nullptr;

        attr.id = SAI_REDIS_SWITCH_ATTR_FLEX_COUNTER_GROUP;
        attr.value.ptr = (void*)&flexCounterGroupParam;
        sai_switch_api->set_switch_attribute(gSwitchId, &attr);

        return;
    }

    vector<FieldValueTuple> field_values =
    {
        FieldValueTuple(FLEX_COUNTER_STATUS_FIELD, FLEX_COUNTER_ENABLE)
    };
    flex_counter_group_table->set(group_name, field_values);
    enabled = true;

    SWSS_LOG_DEBUG("Enabling flex counters for group '%s'.",
            group_name.c_str());
}

// disableFlexCounterGroup will do nothing if the flex counter group has been
// disabled.
void FlexCounterManager::disableFlexCounterGroup()
{
    SWSS_LOG_ENTER();

    if (!enabled)
    {
        return;
    }

    vector<FieldValueTuple> field_values =
    {
        FieldValueTuple(FLEX_COUNTER_STATUS_FIELD, FLEX_COUNTER_DISABLE)
    };
    flex_counter_group_table->set(group_name, field_values);
    enabled = false;

    SWSS_LOG_DEBUG("Disabling flex counters for group '%s'.",
            group_name.c_str());
}

// setCounterIdList configures a flex counter to poll the set of provided stats
// that are associated with the given object.
void FlexCounterManager::setCounterIdList(
        const sai_object_id_t object_id,
        const CounterType counter_type,
        const unordered_set<string>& counter_stats)
{
    SWSS_LOG_ENTER();

    auto counter_type_it = counter_id_field_lookup.find(counter_type);
    if (counter_type_it == counter_id_field_lookup.end())
    {
        SWSS_LOG_ERROR("Could not update flex counter id list for group '%s': counter type not found.",
                group_name.c_str());
        return;
    }

    auto key = getFlexCounterTableKey(group_name, object_id);
    auto counter_ids = serializeCounterStats(counter_stats);

    if (gTraditionalFlexCounter)
    {
        std::vector<swss::FieldValueTuple> field_values =
            {
                FieldValueTuple(counter_type_it->second, counter_ids)
            };
        flex_counter_table->set(key, field_values);
    }
    else
    {
        sai_attribute_t attr;
        sai_redis_flex_counter_parameter_t counterParam = {nullptr, nullptr, nullptr, nullptr};

        attr.id = SAI_REDIS_SWITCH_ATTR_FLEX_COUNTER;
        counterParam.counter_key = key.c_str();
        counterParam.counter_ids = counter_ids.c_str();
        counterParam.counter_field_name = counter_type_it->second.c_str();
        attr.value.ptr = &counterParam;

        sai_switch_api->set_switch_attribute(gSwitchId, &attr);
    }
    installed_counters.insert(object_id);

    SWSS_LOG_DEBUG("Updated flex counter id list for object '%" PRIu64 "' in group '%s'.",
            object_id,
            group_name.c_str());
}

// clearCounterIdList clears all stats that are currently being polled from
// the given object.
void FlexCounterManager::clearCounterIdList(const sai_object_id_t object_id)
{
    SWSS_LOG_ENTER();

    auto counter_it = installed_counters.find(object_id);
    if (counter_it == installed_counters.end())
    {
        SWSS_LOG_WARN("No counters found on object '%" PRIu64 "' in group '%s'.",
                object_id,
                group_name.c_str());
        return;
    }

    auto key = getFlexCounterTableKey(group_name, object_id);
    if (gTraditionalFlexCounter)
    {
        flex_counter_table->del(key);
    }
    else
    {
        sai_attribute_t attr;
        sai_redis_flex_counter_parameter_t counterParam = {nullptr, nullptr, nullptr, nullptr};

        attr.id = SAI_REDIS_SWITCH_ATTR_FLEX_COUNTER;
        counterParam.counter_key = key.c_str();
        attr.value.ptr = &counterParam;

        sai_switch_api->set_switch_attribute(gSwitchId, &attr);
    }
    installed_counters.erase(counter_it);

    SWSS_LOG_DEBUG("Cleared flex counter id list for object '%" PRIu64 "' in group '%s'.",
            object_id,
            group_name.c_str());
}

string FlexCounterManager::getFlexCounterTableKey(
        const string& group_name,
        const sai_object_id_t object_id) const
{
    SWSS_LOG_ENTER();

    return group_name + flex_counter_table->getTableNameSeparator() + sai_serialize_object_id(object_id);
}

// serializeCounterStats turns a set of stats into a format suitable for FLEX_COUNTER_DB.
string FlexCounterManager::serializeCounterStats(
        const unordered_set<string>& counter_stats) const
{
    SWSS_LOG_ENTER();

    string stats_string;
    for (const auto& stat : counter_stats)
    {
        stats_string.append(stat);
        stats_string.append(",");
    }

    if (!stats_string.empty())
    {
        // Fence post: remove the trailing comma
        stats_string.pop_back();
    }

    return stats_string;
}
