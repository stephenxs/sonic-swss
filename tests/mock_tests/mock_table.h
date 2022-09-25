#pragma once

#include "table.h"

namespace testing_db
{
    void reset();
    std::shared_ptr<std::string> __DBConnector_get(swss::DBConnector &db, const std::string &tableKey);
}
