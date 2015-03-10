local boundary = require("boundary")
local timer = require("timer")
local http = require("http")
local json = require("json")
local fs = require("fs")

local authKey, stats, conversions, requests, param

-- base64
-- encoding username and password for basic auth
function base64(data)
    local b='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/'
    return ((data:gsub('.', function(x)
        local r,b='',x:byte()
        for i=8,1,-1 do r=r..(b%2^i-b%2^(i-1)>0 and '1' or '0') end
        return r
    end)..'0000'):gsub('%d%d%d?%d?%d?%d?', function(x)
    if (#x < 6) then return '' end
        local c=0
        for i=1,6 do c=c+(x:sub(i,i)=='1' and 2^(6-i) or 0) end
        return b:sub(c+1,c+1)
    end)..({ '', '==', '=' })[#data%3+1])
end

-- lookup
-- use an array of attrs to drill down through nested tables to retrieve a value
function lookup(table, attrs)
    local v = table
    for i, a in ipairs(attrs) do
        if (type(v) == "table") then
            v = v[a]
        else
            return nil
        end
    end
    return v
end
 
-- tail
-- make a shallow copy of all but first element of array
function tail(a)
    local t = {}
    for i, v in ipairs(a) do
        if i > 1 then table.insert(t, v) end
    end
    return t
end

-- fnFactory returns a function(stats) that when called
-- will output the metric string ready for printing
--      name - name of metric
--      format - printf type format specifier for the result eg "%d"
function fnFactory(name, format, attrs)
    local mask = "RABBITMQ_" .. name .. " " .. format .. " %s\n"
    return function (c)
        local v = lookup(c, attrs)
        if (type(v) == "number") then
            return string.format(mask, v, c.name)
        else
            return string.format(mask, 0, c.name)
        end
    end
end

-- processRequest takes a table of requests processes them in order
-- (recursively) and finally calls the callback funtion
function processRequest(reqs, callback)
    if (#reqs == 0) then
        callback()
    else
        local data = ""
        local r = reqs[1]
        local req = http.request({
            host = param.hostname,
            port = param.port,
            headers = { Authorization = authKey },
            path = "/api/" .. r.name
        }, function (res)
            res:on("end", function ()
                for n, v in pairs(r.handler(json.parse(data))) do
                    stats[n] = v
                end
                processRequest(tail(reqs), callback)
            end)
            res:on("data", function (chunk)
                data = data .. chunk
            end)
        end)
        req:done()
    end
end

-- poll
-- function to poll tre server and print results
function poll()
    stats = {}
    processRequest(requests, function()
        local t = {}
        for i, f in ipairs(conversions) do
            table.insert(t, f(stats))
        end
        fs.writeSync(1, -1, table.concat(t))
    end)
end

-- setup

-- set default parameters if not found in param.json
param = boundary.param or {
    pollInterval = 5000,
    host = "127.0.0.1",
    port = "15672",
    user = "guest",
    password = "guest"
}

-- build a table of functions to convert metrics into strings
conversions = {}
for i, v in ipairs({
    {"OBJECT_TOTALS_QUEUES", "%d", {"object_totals", "queues"}},
    {"OBJECT_TOTALS_CHANNELS", "%d", {"object_totals", "channels"}},
    {"OBJECT_TOTALS_EXCHANGES", "%d", {"object_totals", "exchanges"}},
    {"OBJECT_TOTALS_CONSUMERS", "%d", {"object_totals", "consumers"}},
    {"OBJECT_TOTALS_CONNECTIONS", "%d", {"object_totals", "connections"}},
    {"MESSAGE_STATS_DELIVER", "%d", {"message_stats", "deliver"}},
    {"MESSAGE_STATS_DELIVER_DETAILS_RATE", "%d", {"message_stats", "deliver_details", "rate"}},
    {"MESSAGE_STATS_DELIVER_NO_ACK", "%d", {"message_stats", "deliver_no_ack"}},
    {"MESSAGE_STATS_DELIVER_NO_ACK_DETAILS_RATE", "%f", {"message_stats", "deliver_no_ack_details", "rate"}},
    {"MESSAGE_STATS_DELIVER_GET", "%d", {"message_stats", "deliver_get"}},
    {"MESSAGE_STATS_DELIVER_GET_DETAILS_RATE", "%f", {"message_stats", "deliver_get_details", "rate"}},
    {"MESSAGE_STATS_REDELIVER", "%d", {"message_stats", "redeliver"}},
    {"MESSAGE_STATS_REDELIVER_DETAILS_RATE", "%f", {"message_stats", "redeliver_details", "rate"}},
    {"MESSAGE_STATS_PUBLISH", "%d", {"message_stats", "publish"}},
    {"MESSAGE_STATS_PUBLISH_DETAILS_RATE", "%f", {"message_stats", "publish_details", "rate"}},
    {"QUEUE_TOTALS_MESSAGES", "%d", {"queue_totals", "messages"}},
    {"QUEUE_TOTALS_MESSAGES_DETAILS_RATE", "%f", {"queue_totals", "messages_details", "rate"}},
    {"QUEUE_TOTALS_MESSAGES_READY", "%d", {"queue_totals", "messages_ready"}},
    {"QUEUE_TOTALS_MESSAGES_READY_DETAILS_RATE", "%f", {"queue_totals", "messages_ready_details", "rate"}},
    {"QUEUE_TOTALS_MESSAGES_UNACKNOWLEDGED", "%d", {"queue_totals", "messages_unacknowledged"}},
    {"QUEUE_TOTALS_MESSAGES_UNACKNOWLEDGED_DETAILS_RATE", "%f", {"queue_totals", "messages_unacknowledged_details", "rate"}},
    {"CONNECTIONS", "%d", {"connections", "total"}},
    {"CONNECTIONS_NOTRUNNING", "%d", {"connections", "notrunning"}},
    {"MESSAGES", "%d", {"overview", "messages"}},
    {"MESSAGES_READY", "%d", {"overview", "messages_ready"}},
    {"MESSAGES_UNACKNOWLEDGED", "%d", {"overview", "messages_unacknowledged"}},
    {"MEM_USED", "%d", {"mem_used"}},
    {"PROC_USED", "%d", {"proc_used"}},
    {"FD_USED", "%d", {"fd_used"}},
    {"SOCKETS_USED", "%d", {"sockets_used"}},
    {"DISK_FREE", "%d", {"disk_free"}}
}) do
    table.insert(conversions, fnFactory(table.unpack(v)))
end

-- build table of requests and associated handler function
requests = {
    { name = "overview", handler = function (json)
        return { object_totals = json.object_totals,
                 message_stats = json.message_stats,
                 queue_totals  = json.queue_totals }
    end },
    { name = "nodes", handler = function (json)
        return { name = json[1].name,
                 mem_used = json[1].mem_used,
                 disk_free = json[1].disk_free }
    end }
}

-- create the basic auth key for the api http requests
authKey = "Basic " .. base64(param.user .. ":" .. param.password)

-- go

print("_bevent:Boundary RabbitMQ plugin up : version 1.0|t:info|tags:lua,rabbitmq,plugin")

poll()
timer.setInterval(param.pollInterval, poll)
