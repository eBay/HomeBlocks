#pragma once

#include <homeblks/volume_mgr.hpp>
#include "sisl/utility/enum.hpp"
#include <homestore/homestore.hpp>
#include <homestore/index/index_table.hpp>
#include <homestore/replication/repl_dev.h>

#if USE_FIXED_INDEX
#include "fixed_index_table.hpp"
#else
#include "prefix_index_table.hpp"
#endif
