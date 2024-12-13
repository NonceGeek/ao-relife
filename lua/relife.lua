-- ProcessId:  Y2PZPXyGxSmLO-BJJsi_FSL1_mgkC2WO5LNpGa4P6zY
local json = require("json")
local sqlite3 = require("lsqlite3")

DL_TARGET = 'cO4thcoxO57AflN5hfXjce0_DydbMJclTU9kC3S75cg'
ACHIEVEMENT_TARGET = 't5n7Op0zPQsro-NZxIuGX4izLJF5-lixZvRsO4eLZoY'

DB = DB or sqlite3.open_memory()

-- Create table for achievements with unique constraint on address
DB:exec [[
  CREATE TABLE IF NOT EXISTS acct (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    address TEXT,
    lifeCount INT,
    lastUpdated INT
  );
]]

DB:exec [[
  CREATE TABLE IF NOT EXISTS life (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    acct_id INTEGER,
    lifeEvents TEXT,
    lifeNumber INT,
    lastUpdated INT,
    FOREIGN KEY(acct_id) REFERENCES acct(id)
  );
]]

DB:exec [[
  ALTER TABLE life ADD COLUMN position TEXT;
]]

-- Function to execute SQL queries and return results
local function query(stmt)
    local rows = {}
    for row in stmt:nrows() do
        table.insert(rows, row)
    end
    stmt:reset()
    return rows
end

-- Function to generate a random hexadecimal string
local function generateRandomHex(length)
    local chars = '0123456789abcdef'
    local hex = ''
    for i = 1, length do
        local randIndex = math.random(1, #chars)
        hex = hex .. chars:sub(randIndex, randIndex)
    end
    return hex
end

-- Function to getAcct by address
local function getAcct(data)

    local dataJson = json.decode(data)
    local address = dataJson.address
    local stmt = DB:prepare [[
    SELECT * FROM acct WHERE address = :address;
  ]]

    if not stmt then
        error("Failed to prepare SQL statement: " .. DB:errmsg())
    end

    stmt:bind_names({
        address = address
    })

    local rows = query(stmt)
    return rows
end

-- Function to getLatestLife by address
local function getLatestLife(data)
    local dataJson = json.decode(data)
    local address = dataJson.address
    local acct = getAcct(data)

    -- Check if account exists
    if #acct == 0 then
        print("Error: Account does not exist")
        Send({ Target = msg.From, Data = "Error: Account does not exist" })
        return
    end

    local stmt = DB:prepare [[
        SELECT * FROM life 
        WHERE acct_id = :acct_id 
        ORDER BY id DESC 
        LIMIT 1;
    ]]

    if not stmt then
        error("Failed to prepare SQL statement: " .. DB:errmsg())
    end

    -- Bind the account ID to the statement
    stmt:bind_names({
        acct_id = acct[1].id
    })

    local rows = query(stmt)
    return rows 
end

local function updateLifeWithNewEvent(id, newEvent)
    
    -- Retrieve the current life events
    local stmtSelect = DB:prepare [[
        SELECT lifeEvents FROM life WHERE id = :id;
    ]]
    stmtSelect:bind_names({ id = id })
    local currentEvents = query(stmtSelect)[1].lifeEvents

    -- Decode current life events data
    local currentEventsData = json.decode(currentEvents)
    table.insert(currentEventsData, newEvent)

    -- Encode the updated events back to JSON
    local updatedEvents = json.encode(currentEventsData)

    -- Update the life events in the database
    local stmtUpdate = DB:prepare [[
        UPDATE life SET lifeEvents = :lifeEvents WHERE id = :id;
    ]]
    stmtUpdate:bind_names({
        id = id,
        lifeEvents = updatedEvents
    })

    local result = stmtUpdate:step()
    stmtSelect:reset()
    stmtSelect:finalize()
    stmtUpdate:reset()
    stmtUpdate:finalize()
end

-- Function to getLifes by address
local function getLifes(data)

    local dataJson = json.decode(data)
    local address = dataJson.address

    -- Call getAcct function with the provided data
    local acct = getAcct(data)

    -- Check if account exists
    if #acct == 0 then
        print("Error: Account does not exist")
        Send({ Target = msg.From, Data = "Error: Account does not exist" })
        return
    end

    local stmt = DB:prepare [[
      SELECT * FROM life WHERE acct_id = :acct_id;
    ]]

    if not stmt then
        error("Failed to prepare SQL statement: " .. DB:errmsg())
    end

    -- Bind the account ID to the statement
    stmt:bind_names({
        acct_id = acct[1].id
    })

    local rows = query(stmt)
    return rows
end

-- Function to update lifeCount in the acct table
local function updateLifeCount(acctId)
    print("updateLifeCount")
    local stmt = DB:prepare [[
        UPDATE acct SET lifeCount = lifeCount + 1 WHERE id = :id;
    ]]

    if not stmt then
        local errorMsg = "Failed to prepare SQL statement: " .. DB:errmsg()
        error(errorMsg)
    end

    stmt:bind_names({
        id = acctId
    })

    local updateResult = stmt:step()
    if updateResult ~= sqlite3.DONE then
        local errorMsg = "Error: Failed to update life count"
        print(errorMsg)
    else
        print('Life count updated!')
    end

    stmt:reset()
    stmt:finalize()
end


-- Function to initLife
local function initLife(msg, data, timestamp)
    -- Update Level here.
    Send({
        Target = DL_TARGET,
        Tags = {
            Action = 'updateLevel'
        },
        Data = json.encode({
            address = address
        })
    })
    -- Decode the JSON data
    local dataJson = json.decode(data)
    local address = dataJson.address
    local x = dataJson.x
    local y = dataJson.y
    local z = dataJson.z

    -- Convert [x, y, z] to POSITION TEXT
    local position = string.format("[%s, %s, %s]", x, y, z)

    -- Retrieve account information
    local acct = getAcct(data)

    -- Check if account exists
    if #acct == 0 then
        local errorMsg = "Error: Account does not exist"
        print(errorMsg)
        Send({ Target = msg.From, Data = errorMsg })
        return
    end

    -- Proceed with further logic if account exists
    print("initLife")

    -- Prepare the SQL statement for inserting a new life event
    local stmt = DB:prepare [[
      INSERT INTO life (acct_id, lifeEvents, lifeNumber, lastUpdated, position)
      VALUES (:acct_id, :lifeEvents, :lifeNumber, :lastUpdated, :position);
    ]]

    if not stmt then
        local errorMsg = "Failed to prepare SQL statement: " .. DB:errmsg()
        error(errorMsg)
    end

    -- Bind values to the statement
    stmt:bind_names({
        acct_id = acct[1].id,
        lifeEvents = "[]", -- Replace with actual life event data
        lifeNumber = acct[1].lifeCount + 1,
        lastUpdated = timestamp,
        position = position
    })

    -- Execute the statement
    local result = stmt:step()
    if result ~= sqlite3.DONE then
        local errorMsg = "Error: Failed to add life event"
        print(errorMsg)
    else
        print('Life event added!')
        -- Call the new function to update lifeCount
    end

    stmt:reset()
    stmt:finalize()
    -- HINTS: DO NOT REMOVE THIS LINE
    -- lua only runs the func that defined before!
    updateLifeCount(acct[1].id)

    Send({
        Target = ACHIEVEMENT_TARGET,
        Tags = {
            Action = 'AppendAchievement'
        },
        Data = json.encode({
            title = '1st life',
            data = 'got ur 1st ao-life!',
            proven = '',
            address = address
        })
    })
end



-- Function to initAcct
local function initAcct(msg, data, timestamp)
    -- Decode the JSON data
    local dataJson = json.decode(data)
    local address = dataJson.address

    -- Check if the address already exists
    local checkAddressStmt = DB:prepare [[
    SELECT * FROM acct WHERE address = :address;
  ]]

    if not checkAddressStmt then
        error("Failed to prepare SQL statement: " .. DB:errmsg())
    end

    checkAddressStmt:bind_names({
        address = address
    })

    local existingAcct = query(checkAddressStmt)[1]

    if existingAcct then
        print("Error: Address already exists")
        Send({ Target = msg.From, Data = "Error: Address already exists" })
        return
    end

    -- Prepare the SQL statement
    local stmt = DB:prepare [[
    INSERT INTO acct (address, lifeCount, lastUpdated)
    VALUES (:address, :lifeCount, :lastUpdated);
  ]]

    if not stmt then
        error("Failed to prepare SQL statement: " .. DB:errmsg())
    end

    -- Bind values to the statement
    stmt:bind_names({
        address = address,
        lifeCount = 0,
        lastUpdated = timestamp
    })

    -- Execute the statement
    local result = stmt:step()
    if result ~= sqlite3.DONE then
        print("Error: Failed to add account")
        Send({ Target = msg.From, Data = "Error: Failed to add account" })
    else
        print('Account Added!')
        Send({ Target = msg.From, Data = "Account Added!" })
    end

    -- Reset and finalize the statements
    checkAddressStmt:reset()
    checkAddressStmt:finalize()
    stmt:reset()
    stmt:finalize()
end

-- Send({ Target = ao.id, Action = "initAcct", Data = '{"address": "0x1"}' })
-- Add initAcct Handler
Handlers.add("initAcct", Handlers.utils.hasMatchingTag("Action", "initAcct"), function(msg)
    initAcct(msg, msg.Data, msg.Timestamp)
end)

--  Send({ Target = ao.id, Action = "getAcct", Data = '{"address": "0x1"}' })
-- Add getAcct Handler
Handlers.add("getAcct", Handlers.utils.hasMatchingTag("Action", "getAcct"), function(msg)
    local acct = getAcct(msg.Data)
    local acctJson = json.encode(acct)
    print(acctJson)
    Send({ Target = msg.From, Data = acctJson })
end)

Handlers.add("ping",
  { Action = "ping" },
  function (msg)
    print('ping')
    Send({ Target = msg.From, Data = "pong" })
  end
)

Handlers.add("pingping", Handlers.utils.hasMatchingTag("Action", "pingping"), function(msg)
    Send({ Target = msg.From, Data = "pong" })
end)

Handlers.add("pingpingping", Handlers.utils.hasMatchingTag("Action", "pingpingping"), function(msg)
    Send({ Target = msg.From, Data = "pingpingping" })
end)

-- Send({ Target = ao.id, Action = "getAccts", Data = '{"address": "0x1"}' })
Handlers.add("getAccts", Handlers.utils.hasMatchingTag("Action", "getAccts"), function(msg)
    Send({ Target = msg.From, Data = "pong" })
end)

Handlers.add("initLife", Handlers.utils.hasMatchingTag("Action", "initLife"), function(msg)
    initLife(msg, msg.Data, msg.Timestamp)
end)

-- Send({ Target = ao.id, Action = "getLifes", Data = '{"address": "0x1"}' })
Handlers.add("getLifes", Handlers.utils.hasMatchingTag("Action", "getLifes"), function(msg)
    local lifes = getLifes(msg.Data)
    local lifesJson = json.encode(lifes)
    print(lifesJson)
    Send({ Target = msg.From, Data = lifesJson })
end)

Handlers.add("getLatestLife", Handlers.utils.hasMatchingTag("Action", "getLatestLife"), function(msg)
    local life = getLatestLife(msg.Data)
    local lifeJson = json.encode(life)
    print(lifeJson)
    Send({ Target = msg.From, Data = lifeJson })
end)

-- Send({ Target = ao.id, Action = "updateLife", Data = '{"address": "0x1"}' })
Handlers.add("updateLife", Handlers.utils.hasMatchingTag("Action", "updateLife"), function(msg)

    local dataJson = json.decode(msg.Data)
    local address = dataJson.address

    local life = getLatestLife(msg.Data)
    local lifeJson = json.encode(life)
    print("lifeJson:")
    print(lifeJson)
    local randomHex = generateRandomHex(2)
    print(randomHex)
    updateLifeWithNewEvent(life[1].id, randomHex)

    local lifeUpdated = getLatestLife(msg.Data)
    local lifeUpdatedJson = json.encode(lifeUpdated)
    print(lifeUpdatedJson)
    -- Send to DL after updateLife
    print(life[1].address)

    -- Send({
    --     Target = DL_TARGET,
    --     Tags = {
    --         Action = 'updateLevel'
    --     },
    --     Data = json.encode({
    --         address = address
    --     })
    -- })

    Send({ Target = msg.From, Data = lifeUpdatedJson })
end)

-- Add getCount Handler to get the count of all pets
-- Send({ Target = ao.id, Action = "getCount", Data = '{"address": "0x1"}' })
Handlers.add(
  "getCount",
  Handlers.utils.hasMatchingTag("Action", "getCount"),
  function (msg)
    local stmt = DB:prepare [[
      SELECT COUNT(*) AS count FROM life;
    ]]
  
    if not stmt then
      error("Failed to prepare SQL statement: " .. DB:errmsg())
    end
  
    local rows = query(stmt)
    print(rows[1].count)
    Send({ Target = msg.From, Data = tostring(rows[1].count) })
  end
)