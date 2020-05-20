#include <unistd.h>
#include <getopt.h>
#include <vector>
#include <mutex>
#include "dbconnector.h"
#include "select.h"
#include "exec.h"
#include "schema.h"
#include "buffermgr.h"
#include "buffermgrdyn.h"
#include <fstream>
#include <iostream>
#include "json.h"

using namespace std;
using namespace swss;

/* select() function timeout retry time, in millisecond */
#define SELECT_TIMEOUT 1000

/*
 * Following global variables are defined here for the purpose of
 * using existing Orch class which is to be refactored soon to
 * eliminate the direct exposure of the global variables.
 *
 * Once Orch class refactoring is done, these global variables
 * should be removed from here.
 */
int gBatchSize = 0;
bool gSwssRecord = false;
bool gLogRotate = false;
ofstream gRecordOfs;
string gRecordFile;
/* Global database mutex */
mutex gDbMutex;

void usage()
{
    cout << "Usage: buffermgrd -l pg_lookup.ini" << endl;
    cout << "       -l pg_lookup.ini: PG profile look up table file (mandatory)" << endl;
    cout << "       format: csv" << endl;
    cout << "       values: 'speed, cable, size, xon,  xoff, dynamic_threshold, xon_offset'" << endl;
}

void test_json(std::string json)
{
    std::vector<FieldValueTuple> table;
    JSon::readJson(json, table);
    for (auto i : table)
        cout << "field" << fvField(i) << "value" << fvValue(i) << endl;
}

int main(int argc, char **argv)
{
    int opt;
    string pg_lookup_file = "";
    string switch_table_file = "";
    string peripherial_table_file = "";
    string json_file = "";
    Logger::linkToDbNative("buffermgrd");
    SWSS_LOG_ENTER();

    SWSS_LOG_NOTICE("--- Starting buffermgrd ---");

    while ((opt = getopt(argc, argv, "l:h:s:p:j")) != -1 )
    {
        switch (opt)
        {
        case 'l':
            pg_lookup_file = optarg;
            break;
        case 'h':
            usage();
            return 1;
        case 's':
            switch_table_file = optarg;
            break;
        case 'p':
            peripherial_table_file = optarg;
            break;
        case 'j':
            json_file = optarg;
            std::cout << "jsonfile: " << json_file << endl;
            test_json(json_file);
            return 0;
        default: /* '?' */
            usage();
            return EXIT_FAILURE;
        }
    }

    try
    {
        std::vector<Orch *> cfgOrchList;
        vector<string> cfg_buffer_tables = {
            CFG_PORT_TABLE_NAME,
            CFG_PORT_CABLE_LEN_TABLE_NAME,
            CFG_BUFFER_POOL_TABLE_NAME,
            CFG_BUFFER_PROFILE_TABLE_NAME,
            CFG_BUFFER_PG_TABLE_NAME,
            CFG_BUFFER_QUEUE_TABLE_NAME,
            CFG_BUFFER_PORT_INGRESS_PROFILE_LIST_NAME,
            CFG_BUFFER_PORT_EGRESS_PROFILE_LIST_NAME
        };

        DBConnector cfgDb("CONFIG_DB", 0);
        DBConnector stateDb("STATE_DB", 0);
        DBConnector applDb("APPL_DB", 0);

        if (!pg_lookup_file.empty())
        {
            cfgOrchList.emplace_back(new BufferMgr(&cfgDb, &stateDb, &applDb, pg_lookup_file, cfg_buffer_tables));
        }
        else if (!switch_table_file.empty())
        {
            // Load the json file containing the SWITCH_TABLE
            if (!peripherial_table_file.empty())
            {
                //Load the json file containing the PERIPHERIAL_TABLE
            }
            cfgOrchList.emplace_back(new BufferMgrDynamic(&cfgDb, &stateDb, &applDb, pg_lookup_file, cfg_buffer_tables));
        }
        else
        {
            usage();
            return EXIT_FAILURE;
        }

        auto buffmgr = cfgOrchList[0];

        swss::Select s;
        for (Orch *o : cfgOrchList)
        {
            s.addSelectables(o->getSelectables());
        }

        SWSS_LOG_NOTICE("starting main loop");
        while (true)
        {
            Selectable *sel;
            int ret;

            ret = s.select(&sel, SELECT_TIMEOUT);
            if (ret == Select::ERROR)
            {
                SWSS_LOG_NOTICE("Error: %s!", strerror(errno));
                continue;
            }
            if (ret == Select::TIMEOUT)
            {
                buffmgr->doTask();
                continue;
            }

            auto *c = (Executor *)sel;
            c->execute();
        }
    }
    catch(const std::exception &e)
    {
        SWSS_LOG_ERROR("Runtime error: %s", e.what());
    }
    return -1;
}
