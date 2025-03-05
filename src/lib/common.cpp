#include <homeblks/common.hpp>
#include <boost/uuid/random_generator.hpp>

namespace homeblocks {

homestore::uuid_t hb_utils::gen_random_uuid() { return boost::uuids::random_generator()(); }

} // namespace homeblocks
