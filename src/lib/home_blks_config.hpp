#pragma once

#include <sisl/settings/settings.hpp>
#include "generated/home_blks_config_generated.h"

SETTINGS_INIT(homeblkscfg::HomeBlksSettings, home_blks_config);

namespace homeblocks {
#define HB_DYNAMIC_CONFIG_WITH(...) SETTINGS(home_blks_config, __VA_ARGS__)
#define HB_DYNAMIC_CONFIG_THIS(...) SETTINGS_THIS(home_blks_config, __VA_ARGS__)
#define HB_DYNAMIC_CONFIG(...) SETTINGS_VALUE(home_blks_config, __VA_ARGS__)

#define HB_SETTINGS_FACTORY() SETTINGS_FACTORY(home_blks_config)

} // namespace homeblocks
