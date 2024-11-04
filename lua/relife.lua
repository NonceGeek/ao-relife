-- ProcessId:  Y2PZPXyGxSmLO-BJJsi_FSL1_mgkC2WO5LNpGa4P6zY
local json = require("json")
local sqlite3 = require("lsqlite3")

DL_TARGET = 'cO4thcoxO57AflN5hfXjce0_DydbMJclTU9kC3S75cg'

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
    local stmt = DB:prepare [[
        SELECT * FROM life WHERE lifeNumber = :lifeNumber;
    ]]

    stmt:bind_names({
        lifeNumber = acct[1].lifeCount
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
        Handlers.utils.reply("Error: Account does not exist")
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
        -- Handlers.utils.reply(errorMsg)(msg)
    else
        print('Life count updated!')
        -- Handlers.utils.reply("Life initialized and count updated!")(msg)
    end

    stmt:reset()
    stmt:finalize()
end


-- Function to initLife
local function initLife(msg, data, timestamp)
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
        Handlers.utils.reply(errorMsg)(msg)
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
        -- Handlers.utils.reply(errorMsg)(msg)
    else
        print('Life event added!')
        -- Call the new function to update lifeCount
    end

    stmt:reset()
    stmt:finalize()
    -- HINTS: DO NOT REMOVE THIS LINE
    -- lua only runs the func that defined before!
    updateLifeCount(acct[1].id)
end



-- Function to initAcct
local function initAcct(data, timestamp)
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
        Handlers.utils.reply("Error: Address already exists")
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
        Handlers.utils.reply("Error: Failed to add account")
    else
        print('Account Added!')
        Handlers.utils.reply("Account Added!")
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
    initAcct(msg.Data, msg.Timestamp)
end)

--  Send({ Target = ao.id, Action = "getAcct", Data = '{"address": "0x1"}' })
-- Add getAcct Handler
Handlers.add("getAcct", Handlers.utils.hasMatchingTag("Action", "getAcct"), function(msg)
    local acct = getAcct(msg.Data)
    local acctJson = json.encode(acct)
    print(acctJson)
    Handlers.utils.reply(acctJson)(msg)
end)

Handlers.add("getAccts", Handlers.utils.hasMatchingTag("Action", "getAccts"), function(msg)
    Handlers.utils.reply("AHA")(msg)
end)

Handlers.add("initLife", Handlers.utils.hasMatchingTag("Action", "initLife"), function(msg)
    initLife(msg, msg.Data, msg.Timestamp)
end)

-- Send({ Target = ao.id, Action = "getLifes", Data = '{"address": "0x1"}' })
Handlers.add("getLifes", Handlers.utils.hasMatchingTag("Action", "getLifes"), function(msg)
    local lifes = getLifes(msg.Data)
    local lifesJson = json.encode(lifes)
    print(lifesJson)
    Handlers.utils.reply(lifesJson)(msg)
end)


-- Send({ Target = ao.id, Action = "getLifes", Data = '{"address": "0x1"}' })
Handlers.add("updateLife", Handlers.utils.hasMatchingTag("Action", "updateLife"), function(msg)

    local dataJson = json.decode(msg.Data)
    local address = dataJson.address

    local life = getLatestLife(msg.Data)
    local lifeJson = json.encode(life)
    print(lifeJson)
    local randomHex = generateRandomHex(2)
    updateLifeWithNewEvent(life[1].id, randomHex)

    local lifeUpdated = getLatestLife(msg.Data)
    local lifeUpdatedJson = json.encode(lifeUpdated)
    print(lifeUpdatedJson)
    -- Send to DL after updateLife
    print(life[1].address)
    Send({
        Target = DL_TARGET,
        Tags = {
            Action = 'updateLevel'
        },
        Data = json.encode({
            address = address
        })
    })

    Handlers.utils.reply(lifeUpdatedJson)(msg)
end)

-- Add getCount Handler to get the count of all pets
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
    Handlers.utils.reply(tostring(rows[1].count))(msg)
  end
)