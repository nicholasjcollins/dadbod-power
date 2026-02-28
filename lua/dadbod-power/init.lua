local config = require("dadbod-power.config")
local M = {}

config.options = config.options or {}
config.options.custom_queries = config.options.custom_queries or {}

local default_queries = {
    sqlserver = {
        tables     = "SELECT s.name+'.'+t.name FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id ORDER BY s.name,t.name",
        table_cols = "SELECT c.name, UPPER(tp.name), c.is_nullable FROM sys.columns c JOIN sys.tables t ON c.object_id=t.object_id JOIN sys.schemas s ON t.schema_id=s.schema_id JOIN sys.types tp ON c.user_type_id=tp.user_type_id WHERE s.name+'.'+t.name='%s' ORDER BY c.column_id",
        objects    = "SELECT s.name+'.'+o.name FROM sys.objects o JOIN sys.schemas s ON o.schema_id=s.schema_id WHERE o.type IN ('P','V','FN','IF','TF') ORDER BY o.type,s.name,o.name",
        definition = "SELECT OBJECT_DEFINITION(OBJECT_ID('%s'))",
    },
    mysql = {
        tables     = "SELECT CONCAT(TABLE_SCHEMA,'.',TABLE_NAME) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME",
        table_cols = "SELECT COLUMN_NAME, UPPER(COLUMN_TYPE), IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE CONCAT(TABLE_SCHEMA,'.',TABLE_NAME)='%s' ORDER BY ORDINAL_POSITION",
        objects    = "SELECT CONCAT(ROUTINE_SCHEMA,'.',ROUTINE_NAME) FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA=DATABASE() UNION ALL SELECT CONCAT(TABLE_SCHEMA,'.',TABLE_NAME) FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA=DATABASE() ORDER BY 1",
        definition = "SELECT ROUTINE_DEFINITION FROM INFORMATION_SCHEMA.ROUTINES WHERE CONCAT(ROUTINE_SCHEMA,'.',ROUTINE_NAME)='%s'",
    },
    postgresql = {
        tables     = "SELECT table_schema||'.'||table_name FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema') AND table_type='BASE TABLE' ORDER BY table_schema,table_name",
        table_cols = "SELECT column_name, UPPER(data_type), is_nullable FROM information_schema.columns WHERE table_schema||'.'||table_name='%s' ORDER BY ordinal_position",
        objects    = "SELECT n.nspname||'.'||p.proname FROM pg_proc p JOIN pg_namespace n ON p.pronamespace=n.oid WHERE n.nspname NOT IN ('pg_catalog','information_schema') AND p.prokind IN ('p','f') UNION ALL SELECT table_schema||'.'||table_name FROM information_schema.views WHERE table_schema NOT IN ('pg_catalog','information_schema') ORDER BY 1",
        definition = "SELECT pg_get_functiondef(p.oid) FROM pg_proc p JOIN pg_namespace n ON p.pronamespace=n.oid WHERE n.nspname||'.'||p.proname='%s' UNION ALL SELECT view_definition FROM information_schema.views WHERE table_schema||'.'||table_name='%s'",
    },
}

local function get_server_type(url)
    if not url then return nil end
    local header = url:match("^([^:]+)://") or url:match("^([^:]+):")
    return string.lower(header)
end

local function has_telescope()
    return pcall(require, 'telescope')
end

local function get_queries(server_type)
    local queries = config.options.custom_queries[server_type]
    if queries == nil then queries = default_queries[server_type] end
    return queries
end

local function get_query(queries, command_type, parameter)
    if command_type == "table" then return parameter == nil and queries["tables"] or queries["table_cols"] end
    if command_type == "object" then return parameter == nil and queries["objects"] or queries["definition"] end
    return nil
end

local function build_query(query, server_type, parameter)
    if not parameter then return query end
    if server_type == "postgresql" then
        return string.format(query, parameter, parameter)
    end
    return string.format(query, parameter)
end

local function get_rows(query)
    local url = vim.t.db or vim.g.db
    local result = vim.fn["db#execute"](url, query)

    if not result or result == "" then
        vim.notify("No results returned", vim.log.levels.WARN)
        return nil
    end

    local rows = {}
    for line in result:gmatch("[^\r\n]+") do
        local trimmed = line:match("^%s*(.-)%s*$")
        if trimmed and trimmed ~= "" then
            table.insert(rows, trimmed)
        end
    end
    return rows
end


local function get_rows_old(creds, query)
    local cmd
    if creds.type == "sqlserver" then
        cmd = string.format('sqlcmd -S %s -U %s -P "%s" -d %s -h -1 -W -Q "%s"',
            creds.server, creds.username or "", creds.password or "", creds.database, query)
    elseif creds.type == "mysql" then
        -- Set MYSQL_PWD environment variable for mysql
        vim.env.MYSQL_PWD = creds.password
        cmd = string.format('mysql -h %s -u %s -sN -e "%s"',
            creds.server, creds.username, query)
    elseif creds.type == "postgresql" then
        cmd = string.format('PGPASSWORD="%s" psql -h %s -U %s -d %s -t -A -c "%s"',
            creds.password, creds.server, creds.username, creds.database, query)
    end
    if not cmd then return nil end
    local handle = io.popen(cmd)
    if not handle then
        vim.notify("Failed to execute database query", vim.log.levels.ERROR)
        return nil
    end
    local result = handle:read("*a")
    handle:close()
    if not result or result == "" then
        vim.notify("No databases found", vim.log.levels.WARN)
        return nil
    end
    -- Parse results
    local rows = {}
    for line in result:gmatch("[^\r\n]+") do
        local trimmed = line:match("^%s*(.-)%s*$")
        if trimmed and trimmed ~= "" then
            table.insert(rows, trimmed)
        end
    end
    return rows
end

local function open_buffer(rows)
    local buf = vim.api.nvim_create_buf(false, true)
    vim.api.nvim_buf_set_lines(buf, 0, -1, false, rows)
    vim.api.nvim_set_current_buf(buf)
    vim.bo[buf].filetype = "sql"
end

local function display_picker(server_type, rows, command_type)
   if not has_telescope() then
        vim.notify("Telescope is required for picker, please install or specify an object name directly.", vim.log.levels.ERROR)
        return
    end
    local pickers = require('telescope.pickers')
    local finders = require('telescope.finders')
    local conf = require('telescope.config').values
    local actions = require('telescope.actions')
    local action_state = require('telescope.actions.state')
    if not rows or #rows == 0 then
        vim.notify("No " .. command_type .. "s found", vim.log.levels.WARN)
        return
    end
    pickers.new({}, {
        prompt_title = 'Select ' .. command_type,
        finder = finders.new_table({
            results = rows,
            entry_maker = function(entry)
                return {
                value = entry,
                display = entry,
                ordinal = entry,
            }
            end,
        }),
        sorter = conf.generic_sorter({}),
        attach_mappings = function(prompt_bufnr, _)
            actions.select_default:replace(function()
                actions.close(prompt_bufnr)
                local selection = action_state.get_selected_entry()
                if selection then
                    local query = get_query(get_queries(server_type), command_type, selection.value)
                    query = build_query(query, server_type, selection.value)
                    open_buffer(get_rows(query))
                end
            end)
            return true
        end,
    }):find()
end

function M.execute_command(command_type, parameter)
    if not vim.t.database_credentials then
        vim.notify("No data stored in vim.t.database_credentials, please authenticate with dadbod-auth or set", vim.log.levels.WARN)
        return
    end
    local connection_string = vim.t.db or vim.g.db
    local server_type = get_server_type(connection_string)
    local query = get_query(get_queries(server_type), command_type, parameter)
    query = build_query(query, server_type, parameter)
    local rows = get_rows(server_type, query)
    if not parameter then display_picker(server_type, rows, command_type)
    else open_buffer(rows) end
end

return M

