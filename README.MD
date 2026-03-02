# dadbod-power
## Installation

```lua
return {
  'nicholasjcollins/dadbod-power',
  dependencies = {
    'tpope/vim-dadbod',
  },
  config = function()
    local dadbod_power = require 'dadbod-power.config'
    dadbod_power.setup({
      custom_queries = {
        -- header = { -- use sqlserver, mssql, postgres, etc to override the existing connection queries, or build one for any other connector
            -- tables = {}, -- to get list of tables
            -- table_cols = {}, -- to get the columns for a particular table 
            -- objects = {}, -- to get a list of objects (views, sps, functions)
            -- definition = {}, -- to get an object definition
      },
    })
  end
}
```

## Example Usage
dadbod-power surfaces two commands, DBT (Database Tables) and DBO (Database Objects), both accept an optional parameter, indicating a table or object name respectively. 

## Configuration
Should be good to go out of the box, but it does require vim.t.db or vim.g.db to be set with your connection string for vim-dadbod in order to function.

