local config = require("dadbod-power.config")
local M = {}

config.options = config.options or {}
config.options.custom_queries = config.options.custom_queries or {}

local default_queries = {
    sqlserver = {
        tables = [[SET NOCOUNT ON;
        SELECT
            s.name+'.'+t.name,
            STRING_AGG(CAST(c.name+' '+
                CASE
                    WHEN tp.name IN ('varchar','nvarchar','char','nchar')
                        THEN LOWER(tp.name)+'('+CASE WHEN c.max_length=-1 THEN 'MAX' ELSE CAST(c.max_length AS VARCHAR) END+')'
                    WHEN tp.name IN ('decimal','numeric')
                        THEN LOWER(tp.name)+'('+CAST(c.precision AS VARCHAR)+','+CAST(c.scale AS VARCHAR)+')'
                    ELSE LOWER(tp.name)
                END AS nvarchar(max)), '\n') WITHIN GROUP (ORDER BY c.column_id)
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id=s.schema_id
            JOIN sys.columns c ON t.object_id=c.object_id
            JOIN sys.types tp ON c.user_type_id=tp.user_type_id
            GROUP BY s.name, t.name
            ORDER BY s.name, t.name]],
        table_cols = [[SET NOCOUNT ON;
        SELECT
            'SELECT TOP 100'+CHAR(10)+
            STRING_AGG('    '+c.name+', --'+
                CASE
                    WHEN tp.name IN ('varchar','nvarchar','char','nchar')
                        THEN LOWER(tp.name)+'('+CASE WHEN c.max_length=-1 THEN 'MAX' ELSE CAST(c.max_length AS VARCHAR) END+')'
                    WHEN tp.name IN ('decimal','numeric')
                        THEN LOWER(tp.name)+'('+CAST(c.precision AS VARCHAR)+','+CAST(c.scale AS VARCHAR)+')'
                    ELSE LOWER(tp.name)
                END+
                CASE WHEN c.is_nullable=1 THEN ' NULL' ELSE ' NOT NULL' END,
            '\n') WITHIN GROUP (ORDER BY c.column_id)+CHAR(10)+
            'FROM '+s.name+'.'+t.name
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id=s.schema_id
            JOIN sys.columns c ON t.object_id=c.object_id
            JOIN sys.types tp ON c.user_type_id=tp.user_type_id
            WHERE s.name+'.'+t.name='%s'
            GROUP BY s.name, t.name]],
        objects    = [[SET NOCOUNT ON;
        SELECT
            s.name+'.'+o.name
            FROM sys.objects o
            JOIN sys.schemas s ON o.schema_id=s.schema_id
            WHERE o.type IN ('P','V','FN','IF','TF')
            ORDER BY o.type, s.name, o.name]],
        definition = [[SET NOCOUNT ON;
            SELECT
            REPLACE(OBJECT_DEFINITION(o.object_id), CHAR(9), '    ')
            FROM sys.objects o
            JOIN sys.schemas s ON o.schema_id=s.schema_id
            WHERE s.name+'.'+o.name='%s']],
    },
    mysql = {
        tables = [[SELECT
            CONCAT(TABLE_SCHEMA,'.',TABLE_NAME),
            GROUP_CONCAT(CONCAT(COLUMN_NAME,' ',LOWER(COLUMN_TYPE)) ORDER BY ORDINAL_POSITION SEPARATOR '\n')
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA=DATABASE()
            GROUP BY TABLE_SCHEMA, TABLE_NAME
            ORDER BY TABLE_NAME]],
        table_cols = [[SELECT CONCAT(
            'SELECT\n',
            GROUP_CONCAT(
                CONCAT('    ',COLUMN_NAME,', --',LOWER(COLUMN_TYPE),
                    CASE WHEN IS_NULLABLE='YES' THEN ' NULL' ELSE ' NOT NULL' END)
                ORDER BY ORDINAL_POSITION SEPARATOR '\n'),
            '\nFROM ',TABLE_SCHEMA,'.',TABLE_NAME)
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE CONCAT(TABLE_SCHEMA,'.',TABLE_NAME)='%s'
            GROUP BY TABLE_SCHEMA, TABLE_NAME]],
        objects = [[SELECT
            CONCAT(ROUTINE_SCHEMA,'.',ROUTINE_NAME),
            NULL
            FROM INFORMATION_SCHEMA.ROUTINES
            WHERE ROUTINE_SCHEMA=DATABASE()
            UNION ALL
            SELECT
            CONCAT(TABLE_SCHEMA,'.',TABLE_NAME),
            NULL
            FROM INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_SCHEMA=DATABASE()
            ORDER BY 1]],
        definition = [[SELECT ROUTINE_DEFINITION 
            FROM INFORMATION_SCHEMA.ROUTINES WHERE CONCAT(ROUTINE_SCHEMA,'.',ROUTINE_NAME)='%s'
            UNION ALL
            SELECT VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE CONCAT(TABLE_SCHEMA,'.',TABLE_NAME)='%s']],
    },
    postgresql = {
        tables = [[SELECT
            table_schema||'.'||table_name,
            STRING_AGG(column_name||' '||LOWER(data_type), ', ' ORDER BY ordinal_position)
            FROM information_schema.columns
            WHERE table_schema NOT IN ('pg_catalog','information_schema')
            GROUP BY table_schema, table_name
            ORDER BY table_schema, table_name]],
        table_cols = [[SELECT
            'SELECT'||E'\n'||
            STRING_AGG(
                '    '||column_name||', --'||LOWER(data_type)||
                CASE WHEN is_nullable='YES' THEN ' NULL' ELSE ' NOT NULL' END,
            E'\n' ORDER BY ordinal_position)||
            E'\nFROM '||table_schema||'.'||table_name
            FROM information_schema.columns
            WHERE table_schema||'.'||table_name='%s'
            GROUP BY table_schema, table_name]],
        objects = [[SELECT
            n.nspname||'.'||p.proname,
            NULL
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace=n.oid
            WHERE n.nspname NOT IN ('pg_catalog','information_schema')
            AND p.prokind IN ('p','f')
            UNION ALL
            SELECT
            table_schema||'.'||table_name,
            NULL
            FROM information_schema.views
            WHERE table_schema NOT IN ('pg_catalog','information_schema')
            ORDER BY 1]],
        definition = [[SELECT
            pg_get_functiondef(p.oid)
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace=n.oid
            WHERE n.nspname||'.'||p.proname='%s'
            UNION ALL
            SELECT
            table_schema||'.'||table_name,
            view_definition
            FROM information_schema.views
            WHERE table_schema||'.'||table_name='%s']],
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
    if command_type == "table" then return (not parameter or parameter == "") and queries["tables"] or queries["table_cols"] end
    if command_type == "object" then return (not parameter or parameter == "") and queries["objects"] or queries["definition"] end
    return nil
end

local function build_query(query, server_type, parameter)
    if (not parameter or parameter == "") then return query end
    return string.format(query, parameter, parameter)
end


local function get_rows(query, server_type)
    local url = vim.t.db or vim.g.db
    if not url then
        vim.notify("No active dadbod connection", vim.log.levels.WARN)
        return {}
    end
    local conn = vim.fn["db#connect"](vim.fn["db#resolve"](url))
    local cmd
    local infile = nil

    if server_type == "sqlserver" then
        infile = vim.fn.tempname() .. ".sql"
        vim.fn.writefile(vim.split(query, "\n"), infile)
        cmd = vim.fn["db#adapter#dispatch"](conn, "input", infile)
        -- strip headers and whitespace
        for i, v in ipairs(cmd) do
            if v == "sqlcmd" then
                table.insert(cmd, "-h-1")
                table.insert(cmd, "-s")
                table.insert(cmd, "\t")
                table.insert(cmd, "-y")
                table.insert(cmd, "0")
                break
            end
        end
    elseif server_type == "mysql" then
        cmd = vim.fn["db#adapter#dispatch"](conn, "filter")
        for i = #cmd, 1, -1 do
            if cmd[i] == "--table" or cmd[i] == "-t" then
                table.remove(cmd, i)
            end
        end
        table.insert(cmd, "--batch")
        table.insert(cmd, "--silent")
        table.insert(cmd, "--skip-column-names")
    else
        cmd = vim.fn["db#adapter#dispatch"](conn, "filter")
    end

    local lines = vim.fn["db#systemlist"](cmd, infile and "" or query)
    vim.notify(vim.inspect(lines[1]), vim.log.levels.DEBUG)
    if infile then vim.fn.delete(infile) end

    if not lines or #lines == 0 then
        vim.notify("No results returned", vim.log.levels.WARN)
        return {}
    end

    if server_type == "mysql" and lines[1] and lines[1]:match("^mysql: %[Warning%]") then
        table.remove(lines, 1)
    end

    local rows = {}
    for _, line in ipairs(lines) do
        local trimmed = line:match("^(.-)%s*$")
        if trimmed and trimmed ~= "" then
            local name, preview = trimmed:match("^(.-)%s*\t%s*(.*)$")
            if name then
                table.insert(rows, { name = name, preview = preview ~= "" and preview or nil })
            else
                table.insert(rows, { name = trimmed, preview = nil })
            end
        end
    end
    return rows
end

local function open_buffer(rows)
    local buf = vim.api.nvim_create_buf(false, true)
    local lines = {}
    for i, row in ipairs(rows) do
        if row and row.name then
            local content = row.name:gsub("\\n", "\n")
            for _, line in ipairs(vim.split(content, "\n", { plain = true })) do
                table.insert(lines, line)
            end
        end
    end
    vim.api.nvim_buf_set_lines(buf, 0, -1, false, lines)
    vim.api.nvim_set_current_buf(buf)
    vim.bo[buf].filetype = "sql"
end

local function display_picker(server_type, rows, command_type)
    if not has_telescope() then
        vim.notify("Telescope is required for picker, please install or specify an object name directly.", vim.log.levels.ERROR)
        return
    end
    local pickers     = require('telescope.pickers')
    local finders     = require('telescope.finders')
    local previewers  = require('telescope.previewers')
    local conf        = require('telescope.config').values
    local actions     = require('telescope.actions')
    local action_state = require('telescope.actions.state')
    if not rows or #rows == 0 then
        vim.notify("No " .. command_type .. "s found", vim.log.levels.WARN)
        return
    end

    local has_preview = rows[1].preview ~= nil
    local previewer = nil
    if has_preview then
        previewer = previewers.new_buffer_previewer({
            title = "Preview",
            define_preview = function(self, entry)
                if entry.value.preview then
                    local content = entry.value.preview:gsub("\\n", "\n")
                    local lines = vim.split(content, "\n", {plain = true })
                    vim.api.nvim_buf_set_lines(self.state.bufnr, 0, -1, false, lines)
                end
            end,
        })
    end

    pickers.new({}, {
        prompt_title = 'Select ' .. command_type,
        previewer = previewer,
        finder = finders.new_table({
            results = rows,
            entry_maker = function(entry)
                local name = entry.name:match("^%s*(.-)%s*$")
                return { value = { name = name, preview = entry.preview }, display = name, ordinal = name }
            end
        }),
        sorter = conf.generic_sorter({}),
        attach_mappings = function(prompt_bufnr, _)
            actions.select_default:replace(function()
                actions.close(prompt_bufnr)
                local selection = action_state.get_selected_entry()
                if selection then
                    local query = get_query(get_queries(server_type), command_type, selection.value.name)
                    query = build_query(query, server_type, selection.value.name)
                    local result_rows = get_rows(query, server_type)
                    if result_rows then
                        open_buffer(result_rows)
                    end
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
    local rows = get_rows(query, server_type)
    if (not rows or #rows == 0) then return end
    if (not parameter or parameter == "") then display_picker(server_type, rows, command_type)
    else open_buffer(rows) end
end

return M
