#include <linux/if_ether.h>

#include <unordered_map>
#include <utility>
#include <exception>

#include "txmonorch.h"
#include "orch.h"
#include "port.h"
#include "logger.h"
#include "sai_serialize.h"
//#include "swssnet.h"
#include "converter.h"
#include "portsorch.h"
#include "select.h"
#include "timer.h"
#include <vector>

extern sai_port_api_t *sai_port_api;
//extern sai_object_id_t  gSwitchId;
extern PortsOrch*       gPortsOrch;

using namespace std::rel_ops;

TxMonOrch::TxMonOrch(TableConnector appDbConnector, 
					 TableConnector confDbConnector, 
					 TableConnector stateDbConnector) :
	Orch(confDbConnector.first, confDbConnector.second),
	m_TxErrorTable(appDbConnector.first, appDbConnector.second),
	m_stateTxErrorTable(stateDbConnector.first, stateDbConnector.second),
    m_countersDb(COUNTERS_DB, DBConnector::DEFAULT_UNIXSOCKET, 0),
    m_countersTable(&m_countersDb, COUNTERS_TABLE),
	m_PortsTxErrStat(),
	m_pollPeriod(0)
{
    auto interv = timespec { .tv_sec = 0, .tv_nsec = 0 };
    auto timer = new SelectableTimer(interv);
    auto executor = new ExecutableTimer(timer, this, TXMONORCH_SEL_TIMER);
    Orch::addExecutor(executor);

	SWSS_LOG_NOTICE("TxMonOrch initialized with table %s %s %s\n",
					appDbConnector.second.c_str(),
					stateDbConnector.second.c_str(),
					confDbConnector.second.c_str());
}

void TxMonOrch::startTimer(uint32_t interval)
{
    SWSS_LOG_ENTER();

	try
	{
		auto timer = dynamic_cast<ExecutableTimer*>(getExecutor(TXMONORCH_SEL_TIMER))->getSelectableTimer();
		auto interv = timespec { .tv_sec = interval, .tv_nsec = 0 };

		SWSS_LOG_INFO("startTimer,  find executor %p\n", timer);
		timer->setInterval(interv);
		//is it ok to stop without having it started?
		timer->stop();
		timer->start();
		m_pollPeriod = interval;
	}
	catch (...)
	{
		SWSS_LOG_ERROR("Failed to startTimer which might be due to failed to get timer\n");
	}
}

void TxMonOrch::handlePeriodUpdate(const vector<FieldValueTuple>& data)
{
	bool needStart = false;
	uint32_t periodToSet = 0;

    SWSS_LOG_ENTER();

	//is it possible for redis to combine multiple updates and notify once?
	//if so, we handle it in this way.
	//however, in case of that, does it respect the order in which multiple updates comming?
	//suppose it does.
    for (auto i : data)
    {
        try {
            if (fvField(i) == TXMONORCH_FIELD_CFG_PERIOD)
            {
				periodToSet = to_uint<uint32_t>(fvValue(i));

				needStart |= (periodToSet != m_pollPeriod);
				SWSS_LOG_INFO("TX_ERR handle cfg update period new %d\n", periodToSet);
			}
			else
			{
				SWSS_LOG_ERROR("Unknown field type %s\n", fvField(i).c_str());
			}
		}
		catch (...) {
			SWSS_LOG_ERROR("Failed to handle period update\n");
		}
	}

	if (needStart)
	{
		startTimer(periodToSet);
		SWSS_LOG_INFO("TX_ERR poll timer restarted with interval %d\n", periodToSet);
	}
}

void TxMonOrch::handleThresholdUpdate(const string &port, const vector<FieldValueTuple>& data, bool clear)
{
    SWSS_LOG_ENTER();

	try {
		if (clear)
		{
			//attention, for clear, no data is empty
			//tesThreshold(m_PortsTxErrStat[port]) = 0;
			m_PortsTxErrStat.erase(port);
			m_TxErrorTable.del(port);
			m_stateTxErrorTable.del(port);
			//todo, remove data from state_db and appl_db??
			SWSS_LOG_INFO("TX_ERR threshold cleared for port %s\n", port.c_str());
		}
		else
		{
			for (auto i : data)
			{
				if (TXMONORCH_FIELD_CFG_THRESHOLD == fvField(i))
				{
					TxErrorStatistics &tes = m_PortsTxErrStat[port];
					if (tesPortId(tes) == 0/*invalid id??*/)
					{
						Port saiport;
						//what if port doesn't stand for a valid port? 
						//that is, getPort returns false?
						//what if the interface is removed with threshold configured?
						if (gPortsOrch->getPort(port, saiport))
						{
							tesPortId(tes) = saiport.m_port_id;
						}
					}
					tesThreshold(tes) = to_uint<uint64_t>(fvValue(i));
					SWSS_LOG_INFO("TX_ERR threshold reset to %ld for port %s\n", 
								  tesThreshold(tes), port.c_str());
				}
				else
				{
					SWSS_LOG_ERROR("Unknown field type %s when handle threshold for %s\n", 
								   fvField(i).c_str(), port.c_str());
				}
			}
		}
	}
	catch (...) {
		SWSS_LOG_ERROR("Fail to startTimer handle periodic update\n");
	}
}

/*handle configuration update*/
void TxMonOrch::doTask(Consumer& consumer)
{
    SWSS_LOG_ENTER();
	SWSS_LOG_INFO("TxMonOrch doTask consumer\n");

    if (!gPortsOrch->isPortReady())
    {
		SWSS_LOG_INFO("Ports not ready\n");
        return;
    }

    auto it = consumer.m_toSync.begin();
    while (it != consumer.m_toSync.end())
    {
		KeyOpFieldsValuesTuple t = it->second;

		string key = kfvKey(t);
		string op = kfvOp(t);
		vector<FieldValueTuple> fvs = kfvFieldsValues(t);

		SWSS_LOG_INFO("TX_ERR %s operation %s set %s del %s\n", 
					  key.c_str(),
					  op.c_str(), SET_COMMAND, DEL_COMMAND);
		if (key == TXMONORCH_KEY_CFG_PERIOD)
		{
			if (op == SET_COMMAND)
			{
				handlePeriodUpdate(fvs);
			}
			else
			{
				SWSS_LOG_ERROR("Unknown operation type %s when set period\n", op.c_str());
			}
		}
		else //key should be the alias of interface
		{
			if (op == SET_COMMAND)
			{
				//fetch the value which reprsents threshold
				handleThresholdUpdate(key, fvs, false);
			}
			else if (op == DEL_COMMAND)
			{
				//reset to default
				handleThresholdUpdate(key, fvs, true);
			}
			else
			{
				SWSS_LOG_ERROR("Unknown operation type %s when set threshold\n", op.c_str());
			}
		}

        consumer.m_toSync.erase(it++);
    }
}

int TxMonOrch::pollOnePortErrorStatistics(const string &port, TxErrorStatistics &stat)
{
	uint64_t txErrStatistics = 0,
		txErrStatLasttime = tesStatistics(stat),
		txErrStatThreshold = tesThreshold(stat);
	bool tx_error_state,
		tx_error_state_lasttime = tesState(stat);

    SWSS_LOG_ENTER();

#if 0
	{
		//generate a random value instead :D
		static uint64_t seed = 1;
		txErrStatistics = seed;
		txErrStatistics = txErrStatistics * 991 % 997;
		seed = txErrStatistics;
		txErrStatistics += txErrStatLasttime;
		SWSS_LOG_INFO("TX_ERR_POLL: got port %s tx_err stati %d, lasttime %d threshold %d\n", 
					  port.c_str(), txErrStatistics, txErrStatLasttime, txErrStatThreshold);
	}
#else
#if 0
	{
		uint64_t tx_err = 0;
		SWSS_LOG_INFO("TX_ERR_POLL: got port %s %lx tx_err stati %ld, lasttime %ld threshold %ld\n", 
					  port.c_str(), tesPortId(stat),
					  txErrStatistics, txErrStatLasttime, txErrStatThreshold);
		vector<FieldValueTuple> fieldValues;

		if (m_countersTable.get(sai_serialize_object_id(tesPortId(stat)), fieldValues))
		{
			SWSS_LOG_INFO("TX_ERR_POLL: got port %s %lx statistics, parsing... \n", port.c_str(), tesPortId(stat));
			for (const auto& fv : fieldValues)
			{
				const auto field = fvField(fv);
				const auto value = fvValue(fv);


				if (field == "SAI_PORT_STAT_IF_OUT_ERRORS")
				{
					tx_err = stoul(value);
					SWSS_LOG_INFO("    TX_ERR_POLL: %s found %ld %s\n", 
								  field.c_str(), tx_err, value.c_str());
					break;
				}
			}
		}
		else
		{
			SWSS_LOG_INFO("TX_ERR_POLL: failed to get port %s %lx \n", port.c_str(), tesPortId(stat));
		}
		txErrStatistics = tx_err;
		SWSS_LOG_INFO("TX_ERR_POLL: got port %s tx_err stati %ld, lasttime %ld threshold %ld\n", 
					  port.c_str(), txErrStatistics, txErrStatLasttime, txErrStatThreshold);
	}
#endif
        {
                static const vector<sai_stat_id_t> txErrStatId = {SAI_PORT_STAT_IF_OUT_ERRORS};
                uint64_t tx_err = -1;
                //get statistics from hal
                //check FlexCounter::saiUpdateSupportedPortCounters in sai-redis for reference
                sai_port_api->get_port_stats(tesPortId(stat),
                                                                         static_cast<uint32_t>(txErrStatId.size()),
                                                                         txErrStatId.data(),
                                                                         &tx_err);
                txErrStatistics = tx_err;
                SWSS_LOG_INFO("TX_ERR_POLL: got port %s tx_err stati %ld, lasttime %ld threshold %ld\n", 
                                          port.c_str(), txErrStatistics, txErrStatLasttime, txErrStatThreshold);

        }

#endif

	tx_error_state = (txErrStatistics - txErrStatLasttime > txErrStatThreshold);
	if (tx_error_state != tx_error_state_lasttime)
	{
		tesState(stat) = tx_error_state;
		//set status in STATE_DB
		vector<FieldValueTuple> fvs;
		fvs.emplace_back(TXMONORCH_FIELD_STATE_TX_STATE, to_string(tx_error_state));
		m_stateTxErrorTable.set(port, fvs);
		SWSS_LOG_INFO("TX_ERR_CFG: port %s state changed to %d, push to db\n", port.c_str(), tx_error_state);
	}

	//refresh the local copy of last time statistics
	tesStatistics(stat) = txErrStatistics;

	return 0;
}

void TxMonOrch::pollErrorStatistics()
{
    SWSS_LOG_ENTER();

	KeyOpFieldsValuesTuple portEntry;

	for (auto i : m_PortsTxErrStat)
    {
		vector<FieldValueTuple> fields;

		SWSS_LOG_INFO("TX_ERR_APPL: port %s tx_err_stat %ld, before get\n", i.first.c_str(),
						tesStatistics(i.second));
		pollOnePortErrorStatistics(i.first, i.second);
		fields.emplace_back(TXMONORCH_FIELD_APPL_STATI, to_string(tesStatistics(i.second)));
		fields.emplace_back(TXMONORCH_FIELD_APPL_TIMESTAMP, "0");
		fields.emplace_back(TXMONORCH_FIELD_APPL_SAIPORTID, to_string(tesPortId(i.second)));
		m_TxErrorTable.set(i.first, fields);
		SWSS_LOG_INFO("TX_ERR_APPL: port %s tx_err_stat %ld, push to db\n", i.first.c_str(),
						tesStatistics(i.second));
    }

	m_TxErrorTable.flush();
	m_stateTxErrorTable.flush();
	SWSS_LOG_INFO("TX_ERR_APPL: flushing tables\n");
}

void TxMonOrch::doTask(SelectableTimer &timer)
{
	SWSS_LOG_INFO("TxMonOrch doTask selectable timer\n");
	//for each ports, check the statisticis 
	pollErrorStatistics();
}
