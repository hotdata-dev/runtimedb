use anyhow::Result;
use clap::{Parser, ValueEnum};
use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::{Context, Editor, Helper};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use rivetdb::catalog::CatalogManager;
use rivetdb::datafusion::{HotDataEngine, QueryResponse};

#[derive(Parser)]
#[command(name = "hotdata", about = "HotData Query Engine", version)]
struct Cli {
    /// SQL query to execute (if provided, runs in non-interactive mode)
    #[arg(short, long)]
    query: Option<String>,

    /// Output format: table (default) or csv
    #[arg(short, long, default_value = "table")]
    output: OutputFormat,

    /// Base directory for metadata (catalog.db, cache, state) (default: ~/.hotdata/rivetdb)
    #[arg(long)]
    metadata: Option<PathBuf>,

    /// Open catalog in readonly mode (allows concurrent operations, sync will fail)
    #[arg(long)]
    readonly: bool,
}

#[derive(ValueEnum, Clone)]
enum OutputFormat {
    Table,
    Csv,
}

/// Get the default metadata directory path (~/.hotdata/rivetdb)
fn get_default_metadata_path() -> Result<PathBuf> {
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .map_err(|_| anyhow::anyhow!("Unable to determine home directory"))?;

    let mut path = PathBuf::from(home);
    path.push(".hotdata");
    path.push("rivetdb");

    // Create metadata directory if it doesn't exist
    if !path.exists() {
        std::fs::create_dir_all(&path)?;
    }

    Ok(path)
}

struct ReplState {
    engine: HotDataEngine,
    default_catalog: Option<String>,
}

impl ReplState {
    fn new(metadata_dir: PathBuf, readonly: bool) -> Result<Self> {
        // Construct paths for catalog, cache, and state within metadata directory
        let catalog_path = metadata_dir.join("catalog.db");
        let cache_path = metadata_dir.join("cache");
        let state_path = metadata_dir.join("state");

        let engine = HotDataEngine::new_with_paths(
            catalog_path
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("Invalid catalog path"))?,
            cache_path
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("Invalid cache path"))?,
            state_path
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("Invalid state path"))?,
            readonly,
        )?;

        Ok(Self {
            engine,
            default_catalog: None,
        })
    }
}

struct ReplCompleter {
    catalog: Arc<dyn CatalogManager>,
}

impl ReplCompleter {
    fn new(catalog: Arc<dyn CatalogManager>) -> Self {
        Self { catalog }
    }

    fn get_static_commands(&self) -> Vec<String> {
        vec![
            "connect".to_string(),
            "list-connections".to_string(),
            "list-tables".to_string(),
            "sync-connection".to_string(),
            "purge-connection".to_string(),
            "purge-table".to_string(),
            "remove-connection".to_string(),
            "USE".to_string(),
            "help".to_string(),
            "exit".to_string(),
            "quit".to_string(),
        ]
    }

    fn get_sql_keywords(&self) -> Vec<String> {
        vec![
            "SELECT".to_string(),
            "FROM".to_string(),
            "WHERE".to_string(),
            "JOIN".to_string(),
            "INNER".to_string(),
            "LEFT".to_string(),
            "RIGHT".to_string(),
            "OUTER".to_string(),
            "ON".to_string(),
            "AND".to_string(),
            "OR".to_string(),
            "NOT".to_string(),
            "IN".to_string(),
            "LIKE".to_string(),
            "GROUP BY".to_string(),
            "ORDER BY".to_string(),
            "HAVING".to_string(),
            "LIMIT".to_string(),
            "OFFSET".to_string(),
            "INSERT".to_string(),
            "UPDATE".to_string(),
            "DELETE".to_string(),
            "AS".to_string(),
            "DISTINCT".to_string(),
            "COUNT".to_string(),
            "SUM".to_string(),
            "AVG".to_string(),
            "MIN".to_string(),
            "MAX".to_string(),
            "WITH".to_string(),
            "EXPLAIN".to_string(),
        ]
    }

    fn get_connections(&self) -> Vec<String> {
        // Use shared catalog connection (no new connections opened)
        self.catalog
            .list_connections()
            .unwrap_or_default()
            .into_iter()
            .map(|c| c.name)
            .collect()
    }

    fn get_tables(&self) -> Vec<String> {
        // Use shared catalog connection (no new connections opened)
        self.catalog
            .list_tables(None)
            .unwrap_or_default()
            .into_iter()
            .map(|t| format!("{}.{}", t.schema_name, t.table_name))
            .collect()
    }
}

impl Completer for ReplCompleter {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let mut candidates = Vec::new();

        // Get the word being completed
        let start = line[..pos]
            .rfind(|c: char| c.is_whitespace())
            .map(|i| i + 1)
            .unwrap_or(0);
        let prefix = &line[start..pos];

        // Add all possible completions
        for cmd in self.get_static_commands() {
            if cmd.starts_with(prefix) || cmd.to_lowercase().starts_with(&prefix.to_lowercase()) {
                candidates.push(Pair {
                    display: cmd.clone(),
                    replacement: cmd,
                });
            }
        }

        for keyword in self.get_sql_keywords() {
            if keyword.starts_with(prefix)
                || keyword.to_lowercase().starts_with(&prefix.to_lowercase())
            {
                candidates.push(Pair {
                    display: keyword.clone(),
                    replacement: keyword,
                });
            }
        }

        for conn in self.get_connections() {
            if conn.starts_with(prefix) {
                candidates.push(Pair {
                    display: conn.clone(),
                    replacement: conn,
                });
            }
        }

        for table in self.get_tables() {
            if table.starts_with(prefix) {
                candidates.push(Pair {
                    display: table.clone(),
                    replacement: table,
                });
            }
        }

        Ok((start, candidates))
    }
}

impl Helper for ReplCompleter {}
impl Hinter for ReplCompleter {
    type Hint = String;
}
impl Highlighter for ReplCompleter {}
impl Validator for ReplCompleter {}

fn format_results(response: &QueryResponse, format: &OutputFormat) -> Result<()> {
    let results = &response.results;
    match format {
        OutputFormat::Table => {
            use datafusion::arrow::util::pretty;
            pretty::print_batches(results)?;
        }
        OutputFormat::Csv => {
            use arrow_csv::Writer;
            let mut writer = Writer::new(std::io::stdout());
            for batch in results {
                writer.write(batch)?;
            }
        }
    }
    print_duration_sec(response.execution_time);
    Ok(())
}

async fn run_query_mode(
    query: &str,
    format: OutputFormat,
    metadata_dir: PathBuf,
    readonly: bool,
) -> i32 {
    match execute_query_and_format(query, format, metadata_dir, readonly).await {
        Ok(_) => 0,
        Err(e) => {
            eprintln!("Error: {}", e);
            1
        }
    }
}

async fn execute_query_and_format(
    query: &str,
    format: OutputFormat,
    metadata_dir: PathBuf,
    readonly: bool,
) -> Result<()> {
    // Construct paths for catalog, cache, and state within metadata directory
    let catalog_path = metadata_dir.join("catalog.db");
    let cache_path = metadata_dir.join("cache");
    let state_path = metadata_dir.join("state");

    let engine = HotDataEngine::new_with_paths(
        catalog_path
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid catalog path"))?,
        cache_path
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid cache path"))?,
        state_path
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid state path"))?,
        readonly,
    )?;
    let results = engine.execute_query(query).await?;
    format_results(&results, &format)?;
    Ok(())
}

#[tokio::main]
async fn main() {
    // Initialize logger
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();

    let cli = Cli::parse();

    // Get metadata directory from CLI args or use default
    let metadata_dir = cli
        .metadata
        .unwrap_or_else(|| match get_default_metadata_path() {
            Ok(path) => path,
            Err(e) => {
                eprintln!("Error determining metadata directory: {}", e);
                std::process::exit(1);
            }
        });

    if let Some(query) = cli.query {
        // Non-interactive mode
        let exit_code = run_query_mode(&query, cli.output, metadata_dir, cli.readonly).await;
        std::process::exit(exit_code);
    } else {
        // Interactive REPL
        if let Err(e) = run_repl(metadata_dir, cli.readonly).await {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    }
}

async fn run_repl(metadata_dir: PathBuf, readonly: bool) -> Result<()> {
    println!("HotData Query Engine v0.1.0");
    if readonly {
        println!("Running in READONLY mode - sync operations will fail");
    }
    println!("Type 'help' for commands, 'exit' to quit\n");

    let mut state = ReplState::new(metadata_dir, readonly)?;

    // Create editor with completer and configure for list-style completion
    let completer = ReplCompleter::new(state.engine.catalog());
    let config = rustyline::Config::builder()
        .completion_type(rustyline::CompletionType::List)
        .build();
    let mut rl = Editor::with_config(config)?;
    rl.set_helper(Some(completer));

    let mut exit_request_count = 0;
    loop {
        let readline = rl.readline("hotdata> ");
        match readline {
            Ok(line) => {
                exit_request_count = 0;
                let line = line.trim();

                if line.is_empty() {
                    continue;
                }

                let _ = rl.add_history_entry(line);

                if let Err(e) = handle_command(&mut state, line).await {
                    if e.to_string() == "exit_requested" {
                        println!("Goodbye!");
                        break;
                    }
                    eprintln!("Error: {}", e);
                }
            }
            Err(ReadlineError::Interrupted) => {
                if exit_request_count == 1 {
                    println!("Goodbye!");
                    break;
                }
                println!("Press ^C again to exit");
                exit_request_count += 1;
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!("^D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }

    Ok(())
}

async fn handle_command(state: &mut ReplState, line: &str) -> Result<()> {
    match line {
        "exit" | "quit" => {
            // Signal to exit the REPL loop
            anyhow::bail!("exit_requested");
        }
        "help" => {
            print_help();
        }
        _ if line.starts_with("connect ") => {
            handle_connect(state, line).await?;
        }
        "list-connections" => {
            handle_list_connections(state)?;
        }
        _ if line.starts_with("list-tables") => {
            handle_list_tables(state, line)?;
        }
        _ if line.starts_with("sync-connection ") => {
            handle_sync_connection(state, line).await?;
        }
        _ if line.starts_with("purge-connection ") => {
            handle_purge_connection(state, line).await?;
        }
        _ if line.starts_with("purge-table ") => {
            handle_purge_table(state, line).await?;
        }
        _ if line.starts_with("remove-connection ") => {
            handle_remove_connection(state, line).await?;
        }
        _ if line.to_uppercase().starts_with("USE ") => {
            handle_use(state, line).await?;
        }
        _ if line.to_uppercase().starts_with("SELECT ")
            || line.to_uppercase().starts_with("WITH ")
            || line.to_uppercase().starts_with("EXPLAIN ") =>
        {
            handle_query(state, line).await?;
        }
        _ => {
            println!("Unknown command. Type 'help' for available commands.");
        }
    }

    Ok(())
}

async fn handle_query(state: &ReplState, sql: &str) -> Result<()> {
    use datafusion::arrow::util::pretty;

    // Execute query through the engine
    let results = state.engine.execute_query(sql).await?;

    // Print results
    pretty::print_batches(&results.results)?;
    print_duration_sec(results.execution_time);

    Ok(())
}

fn print_duration_sec(d: Duration) {
    // Convert to floating-point seconds with microsecond precision
    let secs = d.as_secs() as f64 + (d.subsec_micros() as f64 / 1_000_000.0);
    let time = format!("{:.6}", secs);
    println!("execution time {}", time);
}

async fn handle_connect(state: &mut ReplState, line: &str) -> Result<()> {
    // Parse: connect <type> <name> <key=value> ...
    let parts: Vec<&str> = line.split_whitespace().collect();

    if parts.len() < 3 {
        anyhow::bail!(
            "Usage: connect <type> <name> <params...>\n\
             Types: postgres, snowflake, motherduck\n\n\
             Examples:\n  \
             connect postgres mydb host=localhost port=5432 database=test user=postgres password=secret\n  \
             connect snowflake sf account=xxx user=user password=pass warehouse=wh database=db\n  \
             connect motherduck md token=xxx database=db"
        );
    }

    let source_type = parts[1];
    let name = parts[2];

    // Validate source type
    if !["postgres", "snowflake", "motherduck"].contains(&source_type) {
        anyhow::bail!(
            "Unsupported source type '{}'. Supported types: postgres, snowflake, motherduck",
            source_type
        );
    }

    // Parse key=value params
    let mut config = serde_json::Map::new();
    config.insert("type".to_string(), serde_json::json!(source_type));

    for part in &parts[3..] {
        let kv: Vec<&str> = part.splitn(2, '=').collect();
        if kv.len() != 2 {
            anyhow::bail!("Invalid parameter format: {}. Use key=value", part);
        }

        let key = kv[0];
        let value = kv[1];

        // Try to parse port as number for postgres
        if key == "port" {
            if let Ok(port) = value.parse::<u16>() {
                config.insert(key.to_string(), serde_json::json!(port));
            } else {
                anyhow::bail!("Port must be a number");
            }
        } else {
            config.insert(key.to_string(), serde_json::json!(value));
        }
    }

    // Validate required fields based on source type
    let required: &[&str] = match source_type {
        "postgres" => &["host", "database", "user", "password"],
        "snowflake" => &["account", "user", "password", "warehouse", "database"],
        "motherduck" => &["token", "database"],
        _ => unreachable!(),
    };

    for field in required {
        if !config.contains_key(*field) {
            anyhow::bail!("Missing required parameter for {}: {}", source_type, field);
        }
    }

    // Add defaults for postgres
    if source_type == "postgres" && !config.contains_key("port") {
        config.insert("port".to_string(), serde_json::json!(5432));
    }

    let config_value = serde_json::Value::Object(config);

    // Connect through the engine
    state.engine.connect(source_type, name, config_value).await?;

    Ok(())
}

fn handle_list_connections(state: &ReplState) -> Result<()> {
    let connections = state.engine.list_connections()?;

    if connections.is_empty() {
        println!("No connections configured.");
        return Ok(());
    }

    println!("Connections:");
    for conn in connections {
        println!("  {} ({})", conn.name, conn.source_type);
    }

    Ok(())
}

fn handle_list_tables(state: &ReplState, line: &str) -> Result<()> {
    let parts: Vec<&str> = line.split_whitespace().collect();

    let connection_name = if parts.len() > 1 {
        Some(parts[1])
    } else {
        None
    };

    if let Some(conn_name) = connection_name {
        // List tables for a specific connection
        let tables = state.engine.list_tables(Some(conn_name))?;

        if tables.is_empty() {
            println!("No tables found for connection '{}'.", conn_name);
            return Ok(());
        }

        println!("{}:", conn_name);
        for table in tables {
            let cached = if table.parquet_path.is_some() {
                "synced"
            } else {
                "un-synced"
            };
            println!(
                "  {}.{}.{} ({})",
                conn_name, table.schema_name, table.table_name, cached
            );
        }
    } else {
        // List tables for all connections, grouped by connection
        let connections = state.engine.list_connections()?;

        if connections.is_empty() {
            println!("No connections configured.");
            return Ok(());
        }

        let mut found_tables = false;
        for conn in connections {
            let tables = state.engine.list_tables(Some(&conn.name))?;

            if !tables.is_empty() {
                found_tables = true;
                println!("{}:", conn.name);
                for table in tables {
                    let cached = if table.parquet_path.is_some() {
                        "synced"
                    } else {
                        "un-synced"
                    };
                    println!(
                        "  {}.{}.{} ({})",
                        conn.name, table.schema_name, table.table_name, cached
                    );
                }
            }
        }

        if !found_tables {
            println!("No tables found.");
        }
    }

    Ok(())
}

async fn handle_sync_connection(state: &ReplState, line: &str) -> Result<()> {
    let parts: Vec<&str> = line.split_whitespace().collect();

    if parts.len() != 2 {
        anyhow::bail!("Usage: sync-connection <name>");
    }

    let name = parts[1];

    println!("Syncing all tables for connection '{}'...", name);
    state.engine.sync_connection(name).await?;
    println!("Sync complete for connection '{}'", name);

    Ok(())
}

async fn handle_use(state: &mut ReplState, line: &str) -> Result<()> {
    let parts: Vec<&str> = line.split_whitespace().collect();

    if parts.len() != 2 {
        anyhow::bail!("Usage: USE <connection_name>");
    }

    // Strip trailing semicolon and other punctuation
    let connection_name = parts[1].trim_end_matches(&[';', ','][..]);

    // Set default catalog through the engine
    state.engine.set_default_catalog(connection_name).await?;

    // Track default catalog in state for UX
    state.default_catalog = Some(connection_name.to_string());

    println!("Using connection: {}", connection_name);

    Ok(())
}

async fn handle_purge_connection(state: &mut ReplState, line: &str) -> Result<()> {
    let parts: Vec<&str> = line.split_whitespace().collect();

    if parts.len() != 2 {
        anyhow::bail!("Usage: purge-connection <name|*>");
    }

    let name = parts[1];

    // Handle wildcard for all connections
    if name == "*" {
        let connections = state.engine.list_connections()?;
        if connections.is_empty() {
            println!("No connections to purge.");
            return Ok(());
        }

        println!(
            "Purging cache for all {} connection(s)...",
            connections.len()
        );
        for conn in &connections {
            println!("  Purging '{}'...", conn.name);
            state.engine.purge_connection(&conn.name).await?;
        }
        println!("Cache purged successfully for all connections");
    } else {
        println!("Purging cache for connection '{}'...", name);
        state.engine.purge_connection(name).await?;
        println!("Cache purged successfully for connection '{}'", name);
    }

    Ok(())
}

async fn handle_purge_table(state: &mut ReplState, line: &str) -> Result<()> {
    let parts: Vec<&str> = line.split_whitespace().collect();

    if parts.len() != 2 {
        anyhow::bail!("Usage: purge-table <connection.schema.table>");
    }

    let full_path = parts[1];

    // Parse connection.schema.table
    let path_parts: Vec<&str> = full_path.split('.').collect();
    if path_parts.len() != 3 {
        anyhow::bail!("Invalid table path format. Use: connection.schema.table");
    }

    let connection_name = path_parts[0];
    let schema_name = path_parts[1];
    let table_name = path_parts[2];

    println!(
        "Purging cache for table '{}.{}.{}'...",
        connection_name, schema_name, table_name
    );
    state
        .engine
        .purge_table(connection_name, schema_name, table_name)
        .await?;
    println!(
        "Cache purged successfully for table '{}.{}.{}'",
        connection_name, schema_name, table_name
    );

    Ok(())
}

async fn handle_remove_connection(state: &mut ReplState, line: &str) -> Result<()> {
    let parts: Vec<&str> = line.split_whitespace().collect();

    if parts.len() != 2 {
        anyhow::bail!("Usage: remove-connection <name>");
    }

    let name = parts[1];

    // Prompt for confirmation
    println!("Remove connection '{}' and all cached data? (yes/no)", name);
    let mut response = String::new();
    std::io::stdin().read_line(&mut response)?;

    let response = response.trim().to_lowercase();
    if response != "yes" && response != "y" {
        println!("Operation cancelled");
        return Ok(());
    }

    println!("Removing connection '{}'...", name);
    state.engine.remove_connection(name).await?;
    println!("Connection '{}' removed successfully", name);

    Ok(())
}

fn print_help() {
    println!("Available commands:");
    println!("  connect <type> <name> <params>  - Connect to external data source");
    println!("    Types: postgres, snowflake, motherduck");
    println!("    Examples:");
    println!("      connect postgres mydb host=localhost port=5432 database=test user=postgres password=secret");
    println!(
        "      connect snowflake sf account=xxx user=user password=pass warehouse=wh database=db"
    );
    println!("      connect motherduck md token=xxx database=db");
    println!("  list-connections                - Show all connections");
    println!("  list-tables [connection]        - Show all tables");
    println!("  sync-connection <name>          - Sync all tables for a connection using DLT");
    println!("  purge-connection <name|*>       - Clear all cached data for a connection (use * for all)");
    println!("  purge-table <conn.schema.table> - Clear cached data for a single table");
    println!("  remove-connection <name>        - Remove a connection and all its data");
    println!("  USE <connection>                - Set default connection for queries");
    println!("  SELECT ...                      - Execute SQL query");
    println!("  help                            - Show this help");
    println!("  exit, quit                      - Exit the REPL");
}
