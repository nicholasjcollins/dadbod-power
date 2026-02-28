local config = {}

config.options = {
    custom_queries = {},
}

function config.setup(user_opts)
	config.options = vim.tbl_deep_extend("force", config.options, user_opts or {})

	vim.api.nvim_create_user_command("DBT", function(opts)
		require("dadbod-power").execute_command("table", opts.args)
	end, {
		nargs = '?',
		desc = "Get tables or table data (if table name provided)",
	})

	vim.api.nvim_create_user_command("DBO", function(opts)
		require("dadbod-power").execute_command("object", opts.args)
	end, {
		nargs = '?',
		desc = "Get views / sps/ functions or definition (if object name provided)",
	})

end

return config
