local config = require("dadbod-power.config")
local M = {}

config.options = config.options or {}
config.options.custom_queries = config.options.custom_queries or {}

local default_queries = {
    sqlserver = {
        tables = [[SELECT
            s.name+'.'+t.name,
            STRING_AGG(c.name+' '+
                CASE
                    WHEN tp.name IN ('varchar','nvarchar','char','nchar')
                        THEN LOWER(tp.name)+'('+CASE WHEN c.max_length=-1 THEN 'MAX' ELSE CAST(c.max_length AS VARCHAR) END+')'
                    WHEN tp.name IN ('decimal','numeric')
                        THEN LOWER(tp.name)+'('+CAST(c.precision AS VARCHAR)+','+CAST(c.scale AS VARCHAR)+')'
                    ELSE LOWER(tp.name)
                END,
            ', ') WITHIN GROUP (ORDER BY c.column_id)
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id=s.schema_id
            JOIN sys.columns c ON t.object_id=c.object_id
            JOIN sys.types tp ON c.user_type_id=tp.user_type_id
            GROUP BY s.name, t.name
            ORDER BY s.name, t.name]],
        table_cols = [[SELECT
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
            CHAR(10)) WITHIN GROUP (ORDER BY c.column_id)+CHAR(10)+
            'FROM '+s.name+'.'+t.name
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id=s.schema_id
            JOIN sys.columns c ON t.object_id=c.object_id
            JOIN sys.types tp ON c.user_type_id=tp.user_type_id
            WHERE s.name+'.'+t.name='%s'
            GROUP BY s.name, t.name]],
        objects    = [[SELECT
            s.name+'.'+o.name,
            NULL
            FROM sys.objects o
            JOIN sys.schemas s ON o.schema_id=s.schema_id
            WHERE o.type IN ('P','V','FN','IF','TF')
            ORDER BY o.type, s.name, o.name]],
        definition = [[SELECT
            OBJECT_DEFINITION(o.object_id)
            FROM sys.objects o
            JOIN sys.schemas s ON o.schema_id=s.schema_id
            WHERE s.name+'.'+o.name='%s']],
    },
    mysql = {
        tables = [[SELECT
            CONCAT(TABLE_SCHEMA,'.',TABLE_NAME),
            GROUP_CONCAT(CONCAT(COLUMN_NAME,' ',LOWER(COLUMN_TYPE)) ORDER BY ORDINAL_POSITION SEPARATOR ', ')
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
        definition = [[SELECT
            ROUTINE_DEFINITION
            FROM INFORMATION_SCHEMA.ROUTINES
            WHERE CONCAT(ROUTINE_SCHEMA,'.',ROUTINE_NAME)='%s']],
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
    if not url then
        vim.notify("No active dadbod connection", vim.log.levels.WARN)
        return nil
    end
    local cmd = string.format("DB %s %s", url, query)
    local result = vim.api.nvim_exec2(cmd, { output = true }).output
    if not result or result == "" then
        vim.notify("No results returned", vim.log.levels.WARN)
        return nil
    end
    local rows = {}
    for line in result:gmatch("[^\r\n]+") do
        local trimmed = line:match("^%s*(.-)%s*$")
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
    for _, row in ipairs(rows) do
        for _, line in ipairs(vim.split(row.name, "\n")) do
            table.insert(lines, line)
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
                    local lines = vim.split(entry.value.preview, "\n")
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
                return { value = entry, display = entry.name, ordinal = entry.name }
            end,
        }),
        sorter = conf.generic_sorter({}),
        attach_mappings = function(prompt_bufnr, _)
            actions.select_default:replace(function()
                actions.close(prompt_bufnr)
                local selection = action_state.get_selected_entry()
                if selection then
                    local query = get_query(get_queries(server_type), command_type, selection.value.name)
                    query = build_query(query, server_type, selection.value.name)
                    local result_rows = get_rows(query)
                    if result_rows then
                        open_buffer(vim.tbl_map(function(r) return r.name end, result_rows))
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
    local rows = get_rows(server_type, query)
    if not parameter then display_picker(server_type, rows, command_type)
    else open_buffer(rows) end
end

return M

