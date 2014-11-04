-- General Error Codes -- this must line up with the Longevity LOOP code
local ERR_SUCCESS        =   0; -- everything ok
local ERR_READ_NOT_FOUND =   1; -- the expected value is not there
local ERR_READ_FAILURE   =  -1; -- the bin or structure is wrong
local ERR_WRITE_FAILURE  =  -2; -- problems with write or create
local ERR_DELETE_FAILURE  = -3; -- problems with delete
local ERR_UPDATE_FAILURE  = -4; -- problems with udpate


function Hello(r)
    return "Hello"
end

function echo_binName(topRec, binName)
    return binName 
end

-- String support functions -----------------------------

function strlen(topRec, binName)
    if aerospike:exists(topRec) then 
    	return string.len(topRec[binName]) 
    else 
    	return 0 
    end
end

function add(topRec, binName, value)
    if not aerospike:exists(topRec) then 
        aerospike:create(topRec) 
        topRec[binName] = 0
    end
    topRec[binName] = topRec[binName] + value
    aerospike:update(topRec)
    return topRec[binName]    
end

function incrbyfloat(topRec, binName, value)
    if not aerospike:exists(topRec) then 
        aerospike:create(topRec) 
        topRec[binName] = '0'
    end
    topRec[binName] = tostring(tonumber(topRec[binName]) + tonumber(value))
    aerospike:update(topRec)
    return tostring(topRec[binName])
end


function getrange(topRec, binName, i, j)
	if (i >= 0) then i = i + 1 end
	if (j >= 0) then j = j + 1 end
    if aerospike:exists(topRec) then 
    	return string.sub(topRec[binName], i, j) 
    else  
    	return '' 
    end
end

local function replace(s, offset, value)
    if offset > string.len(s) then
        -- s =  s ..  string.rep(string.char(0), offset - string.len(s))
        -- this is not supported, so add spaces instead
        s =  s ..  string.rep(' ', offset - string.len(s) - 1)
    end
    return string.sub(s, 1, offset - 1) .. value ..  string.sub(s, offset + string.len(value), string.len(s))
end

function setrange(topRec, binName, offset, value)
    if (offset >= 0) then offset = offset + 1 end
    if not aerospike:exists(topRec) then 
        aerospike:create(topRec) 
        topRec[binName] = ''
    end
    topRec[binName] = replace(topRec[binName], offset, value)
    aerospike:update(topRec)
    return string.len(topRec[binName])
end


-- List support functions -------------------------------

function lpush(topRec, binName, values)
    if not aerospike:exists(topRec) then 
        aerospike:create(topRec) 
        topRec[binName] = list()
    end
    local l = topRec[binName] 
    for value in list.iterator(values) do
        list.prepend(l, value)
    end    
    topRec[binName] = l
    aerospike:update(topRec)
    return list.size(topRec[binName])
end

function lpushx(topRec, binName, value)
    if not aerospike:exists(topRec) then 
        return 0
    else
        local l = topRec[binName]
        list.prepend(l, value)
        topRec[binName] = l
        aerospike:update(topRec)
        return list.size(topRec[binName])
    end
end

function lpop(topRec, binName)
    if not aerospike:exists(topRec) then 
        return nil
    else
        local l = topRec[binName] 
        if (list.size(l) == 0) then
            return nil
        else
            local val = l[1]
            topRec[binName] = list.drop(l, 1)
            aerospike:update(topRec)
            return val
        end
    end
end

function rpush(topRec, binName, values)
    if not aerospike:exists(topRec) then 
        aerospike:create(topRec) 
        topRec[binName] = list()
    end
    local l = topRec[binName] 
    for value in list.iterator(values) do
        list.append(l, value)
    end    
    topRec[binName] = l
    aerospike:update(topRec)
    return list.size(topRec[binName])
end

function rpushx(topRec, binName, value)
    if not aerospike:exists(topRec) then 
        return 0
    else
        local l = topRec[binName]
        list.append(l, value)
        topRec[binName] = l
        aerospike:update(topRec)
        return list.size(topRec[binName])
    end
end

function rpop(topRec, binName)
    if not aerospike:exists(topRec) then 
        return nil
    else
        local l = topRec[binName] 
        if (list.size(l) == 0) then
            return nil
        else
            local val = l[list.size(l)]
            topRec[binName] = list.take(l, list.size(l) - 1)
            aerospike:update(topRec)
            return val
        end
    end
end

local function adjust_list_index(lst, index)
    if (index >= 0) then
        index = index + 1 -- lua lists are 1-based
    else
        index = list.size(lst) + index + 1
    end
    return index
end

--E, [2014-10-25T22:55:20.409949 #9060] ERROR -- : uninitialized constant IO::WaitWriteable
--trying to access a list by a negative index or maybe there was an extra 'end' after  index = list.size(l) + index + 1
function lset(topRec, binName, index, value)
    if not aerospike:exists(topRec) then 
        error( ERR_READ_NOT_FOUND )
    else
        local l = topRec[binName]
        index = adjust_list_index(l, index)
        if (index > list.size(l) or index < 1) then
            error( ERR_UPDATE_FAILURE )
        else
            l[index] = value
            topRec[binName] = l
            aerospike:update(topRec)
            return 'OK'
        end
    end
end

function lindex(topRec, binName, index)
    if not aerospike:exists(topRec) then 
        error( ERR_READ_NOT_FOUND )
    else
        local l = topRec[binName]
        index = adjust_list_index(l, index)
        if (index > list.size(l)) or (index < 1) then
            return nil
        else
            return l[index]
        end
    end
end

-- List utility functions returning an empty list instead of nil
local function merge_lists(list1, list2)
    if not (list2 == nil) then
        for val in list.iterator(list2) do
            list.append(list1, val)
        end
    end
    return list1
end

local function list_take(l, n)
    if (n < 1) or (l == nil) then
        return list()
    else
        return list.take(l, n)
    end
end

local function list_drop(l, n)
    if (l == nil) then
        return list()
    elseif (n < 1) then
        return l
    else
        return list.drop(l, n)
    end
end
-----------------------------------------------------------------

function linsert(topRec, binName, placement, pivot, value)
    if not aerospike:exists(topRec) then 
        return nil
    else
        local l = topRec[binName]
        local i = 0
        for val in list.iterator(l) do
            i = i + 1
            if (val == pivot) then
                if (string.upper(placement) == 'BEFORE') then
                    i = i - 1
                end

                new_list = list_take(l, i)
                list.append(new_list, value)
                new_list = merge_lists(new_list, list_drop(l, i))

                topRec[binName] = new_list
                aerospike:update(topRec)
                return list.size(topRec[binName])
            end
        end    
        return -1 -- pivot not found
    end
end

function llen(topRec, binName)
    if not aerospike:exists(topRec) then 
        return 0
    else
        return list.size(topRec[binName])
    end
end

function lrange(topRec, binName, start, stop)
    if not aerospike:exists(topRec) then 
        return list()
    else
        local l = topRec[binName] 
        start = adjust_list_index(l, start)
        stop = adjust_list_index(l, stop)
        l = list.merge(l, list())
        l = list_drop(l, start - 1)
        l = list_take(l, stop - start + 1)
        return l
    end
end

function lrem(topRec, binName, count, value)
    if not aerospike:exists(topRec) then 
        return 0
    else
        local l = topRec[binName] 
        local new_list = list()
        local rem_count = 0

        if (count == 0) then -- remove all instances
            for val in list.iterator(l) do
                if (val == value) then
                    rem_count = rem_count + 1
                else
                    list.append(new_list, val)
                end
            end
        elseif (count > 0) then
            for val in list.iterator(l) do
                if (rem_count < count) and (val == value) then
                    rem_count = rem_count + 1
                else
                    list.append(new_list, val)
                end
            end
        else
            for i = list.size(l), 1, -1 do
                local val = l[i]
                if (rem_count < -1 * count) and (val == value) then
                    rem_count = rem_count + 1
                else
                    list.prepend(new_list, val)
                end
            end
        end

        topRec[binName] = new_list
        aerospike:update(topRec)
        return rem_count
    end
end

function ltrim(topRec, binName, start, stop)
    if aerospike:exists(topRec) then 
        local l = topRec[binName]
        start = adjust_list_index(l, start)
        stop = adjust_list_index(l, stop)
        l = list_drop(l, start - 1)
        l = list_take(l, stop - start + 1)
        if (list.size(l) == 0) then
            aerospike:remove(topRec)
        else
            topRec[binName] = l
            aerospike:update(topRec)
        end
    end
    return 'OK'
end


-- Set support functions --------------------------------

function sadd(topRec, binName, values)
    if not aerospike:exists(topRec) then 
        aerospike:create(topRec) 
        topRec[binName] = list()
    end
    l = topRec[binName] 
    for value in list.iterator(values) do
        list.prepend(l, value)
    end    
    topRec[binName] = l
    aerospike:update(topRec)
    return list.size(topRec[binName])
end




--TODO use list.merge and list.clone to speed up things


