-- KEYS - None
-- ARGV - None

local appl_db = "0"
local config_db = "4"
local state_db = "6"

local lossypg_reserved = 18 * 1024
local egress_perport_reserve = 9 * 1024

local result = {}
local profiles = {}

local count_shutdown_port = 0
local num_ports = 0
local mmu_size = tonumber(ARGV[1])

local function find_profile(ref)
    -- Remove the surrounding square bracket and the find in the list
    local name = string.sub(ref, 2, -2)
    for i = 1, #profiles, 1 do
        if profiles[i][1] == name then
            return i
        end
    end
    return 0
end

local function iterate_all_items(all_items)
    table.sort(all_items)
    local prev_port = "None"
    local port
    local is_up
    local fvpairs
    local status
    local admin_down_ports = 0
    for i = 1, #all_items, 1 do
        -- Check whether the port on which pg or tc hosts is admin down
        port = string.match(all_items[i], "Ethernet%d+")
        if prev_port ~= port then
            status = redis.call('HGET', 'PORT_TABLE:'..port, 'admin_status')
            prev_port = port
            if status == "down" then
                is_up = false
            else
                is_up = true
            end
        end
        if is_up == true then
            local range = string.match(all_items[i], "Ethernet%d+:([^%s]+)$")
            local profile = redis.call('HGET', all_items[i], 'profile')
            local index = find_profile(profile)
            local size
            if string.len(range) == 1 then
                size = 1
            else
                size = 1 + tonumber(string.sub(range, -1)) - tonumber(string.sub(range, 1, 1))
            end
            profiles[index][2] = profiles[index][2] + size
        else
            admin_down_ports = admin_down_ports + 1
        end
    end
    return admin_down_ports
end

-- Connect to APPL_DB
redis.call('SELECT', appl_db)

-- Fetch names of all profiles and insert them into the look up table
local all_profiles = redis.call('KEYS', 'BUFFER_PROFILE*')
for i = 1, #all_profiles, 1 do
    table.insert(profiles, {all_profiles[i], 0})
end

-- Fetch all the PGs
local all_pgs = redis.call('KEYS', 'BUFFER_PG*')
local all_tcs = redis.call('KEYS', 'BUFFER_QUEUE*')

count_shutdown_port = iterate_all_items(all_pgs)
iterate_all_items(all_tcs)

-- Fetch sizes of all of the profiles, accumulate them
local accumulative_occupied_buffer = 0
for i = 1, #profiles, 1 do
    local size = tonumber(redis.call('HGET', profiles[i][1], 'size'))
    if profiles[i][1] == "BUFFER_PROFILE:ingress_lossy_profile" then
        size = size + lossypg_reserved
    end
    if size ~= nil then 
        accumulative_occupied_buffer = accumulative_occupied_buffer + size * profiles[i][2]
    end
end

-- Fetch mmu_size
redis.call('SELECT', state_db)
local asic_keys = redis.call('KEYS', 'BUFFER_MAX_PARAM*')
local mmu_size = tonumber(redis.call('HGET', asic_keys[1], 'mmu_size'))

-- Switch to CONFIG_DB
redis.call('SELECT', config_db)

local ports_table = redis.call('KEYS', 'PORT|*')
local num_ports = #ports_table

accumulative_occupied_buffer = accumulative_occupied_buffer + egress_perport_reserve * (num_ports - count_shutdown_port)
mmu_size = mmu_size - accumulative_occupied_buffer

-- Fetch all the pools that need update
local pools_need_update = {}
local ipools = redis.call('KEYS', 'BUFFER_POOL|ingress*')
local ingress_pool_count = 0
for i = 1, #ipools, 1 do
    local size = tonumber(redis.call('HGET', ipools[i], 'size'))
    if not size then
        table.insert(pools_need_update, ipools[i])
        ingress_pool_count = ingress_pool_count + 1
    end
end

local epools = redis.call('KEYS', 'BUFFER_POOL|egress*')
for i = 1, #epools, 1 do
    local size = redis.call('HGET', epools[i], 'size')
    if not size then
        table.insert(pools_need_update, epools[i])
    end
end

local pool_size
if ingress_pool_count == 1 then
    pool_size = mmu_size - accumulative_occupied_buffer
else
    pool_size = (mmu_size - accumulative_occupied_buffer) / 2
end

for i = 1, #pools_need_update, 1 do
    table.insert(result, pools_need_update[i] .. ":" .. math.ceil(pool_size))
end

return result
