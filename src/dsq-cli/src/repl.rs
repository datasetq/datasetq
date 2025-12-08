//! Interactive REPL (Read-Eval-Print Loop) for dsq
//!
//! This module provides an interactive shell for experimenting with dsq filters
//! and data transformations in real-time.

use std::io::{self, Write};
use std::path::Path;

use dsq_core::error::{Error, Result};
use dsq_core::Value;

use crate::config::Config;
use crate::executor::Executor;
use crate::output::OutputWriter;

/// Interactive REPL for dsq
pub struct Repl {
    config: Config,
    executor: Executor,
    output_writer: OutputWriter,
    history: Vec<String>,
    current_data: Option<Value>,
}

impl Repl {
    /// Create a new REPL instance
    pub fn new(config: Config) -> Result<Self> {
        let executor = Executor::new(config.clone());
        let output_writer = OutputWriter::new(config.clone());

        Ok(Self {
            config,
            executor,
            output_writer,
            history: Vec::new(),
            current_data: None,
        })
    }

    /// Run the interactive REPL loop
    pub async fn run(&mut self) -> Result<()> {
        println!("Welcome to dsq interactive mode!");
        println!("Type 'help' for available commands, 'quit' to exit.");
        println!(
            "Current data: {}",
            self.current_data
                .as_ref()
                .map_or("none".to_string(), |_| "loaded".to_string())
        );
        println!();

        let stdin = io::stdin();
        let mut stdout = io::stdout();

        loop {
            print!("dsq> ");
            stdout.flush()?;

            let mut line = String::new();
            stdin.read_line(&mut line)?;
            let line = line.trim();

            if line.is_empty() {
                continue;
            }

            match self.process_command(line).await {
                Ok(CommandResult::Continue) => continue,
                Ok(CommandResult::Exit) => break,
                Err(e) => {
                    eprintln!("Error: {}", e);
                    continue;
                }
            }
        }

        println!("Goodbye!");
        Ok(())
    }

    /// Process a single command
    async fn process_command(&mut self, line: &str) -> Result<CommandResult> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        let command = parts.first().unwrap_or(&"");

        match *command {
            "quit" | "exit" | "q" => Ok(CommandResult::Exit),
            "help" | "h" => {
                self.show_help();
                Ok(CommandResult::Continue)
            }
            "load" => {
                if parts.len() < 2 {
                    eprintln!("Usage: load <file>");
                    return Ok(CommandResult::Continue);
                }
                self.load_file(parts[1]).await?;
                Ok(CommandResult::Continue)
            }
            "clear" => {
                self.current_data = None;
                println!("Data cleared.");
                Ok(CommandResult::Continue)
            }
            "show" => {
                self.show_current_data()?;
                Ok(CommandResult::Continue)
            }
            "history" => {
                self.show_history();
                Ok(CommandResult::Continue)
            }
            "explain" => {
                if parts.len() < 2 {
                    eprintln!("Usage: explain <filter>");
                    return Ok(CommandResult::Continue);
                }
                let filter = &parts[1..].join(" ");
                self.explain_filter(filter)?;
                Ok(CommandResult::Continue)
            }
            "validate" => {
                if parts.len() < 2 {
                    eprintln!("Usage: validate <filter>");
                    return Ok(CommandResult::Continue);
                }
                let filter = &parts[1..].join(" ");
                self.validate_filter(filter)?;
                Ok(CommandResult::Continue)
            }
            _ => {
                // Assume it's a filter to execute
                self.execute_filter(line)?;
                Ok(CommandResult::Continue)
            }
        }
    }

    /// Load data from a file
    async fn load_file(&mut self, path: &str) -> Result<()> {
        let path = Path::new(path);
        if !path.exists() {
            return Err(Error::operation(format!(
                "File does not exist: {}",
                path.display()
            )));
        }

        self.current_data = Some(self.executor.read_input(path).await?);
        println!("Loaded data from: {}", path.display());
        Ok(())
    }

    /// Show the current loaded data
    fn show_current_data(&self) -> Result<()> {
        match &self.current_data {
            Some(data) => {
                println!("Current data:");
                self.output_writer.write_to_stdout(data)?;
            }
            None => {
                println!("No data loaded. Use 'load <file>' to load data.");
            }
        }
        Ok(())
    }

    /// Execute a filter on the current data
    fn execute_filter(&mut self, filter: &str) -> Result<()> {
        let data = match &self.current_data {
            Some(d) => d,
            None => {
                eprintln!("No data loaded. Use 'load <file>' to load data first.");
                return Ok(());
            }
        };

        // For REPL, we'll create a temporary executor to avoid mutating the main one
        let mut temp_executor = Executor::new(self.config.clone());
        let result = temp_executor
            .filter_executor
            .execute_str(filter, data.clone())?;

        println!("Result:");
        self.output_writer.write_to_stdout(&result.value)?;

        // Add to history
        self.history.push(filter.to_string());

        // Print execution stats if verbose
        if self.config.debug.verbosity > 0 {
            eprintln!(
                "Execution time: {} ms",
                result
                    .stats
                    .as_ref()
                    .map(|s| s.execution_time.as_millis() as u64)
                    .unwrap_or(0)
            );
            eprintln!(
                "Operations: {}",
                result
                    .stats
                    .as_ref()
                    .map(|s| s.operations_executed)
                    .unwrap_or(0)
            );
        }

        Ok(())
    }

    /// Explain what a filter does
    fn explain_filter(&self, filter: &str) -> Result<()> {
        match self.executor.explain_filter(filter) {
            Ok(explanation) => {
                println!("Filter explanation:");
                println!("{}", explanation);
            }
            Err(e) => {
                eprintln!("Failed to explain filter: {}", e);
            }
        }
        Ok(())
    }

    /// Validate a filter
    fn validate_filter(&self, filter: &str) -> Result<()> {
        match self.executor.validate_filter(filter) {
            Ok(_) => {
                println!("Filter '{}' is valid.", filter);
            }
            Err(e) => {
                eprintln!("Filter '{}' is invalid: {}", filter, e);
            }
        }
        Ok(())
    }

    /// Show command history
    fn show_history(&self) {
        if self.history.is_empty() {
            println!("No commands in history.");
            return;
        }

        println!("Command history:");
        for (i, cmd) in self.history.iter().enumerate() {
            println!("  {}: {}", i + 1, cmd);
        }
    }

    /// Show help information
    fn show_help(&self) {
        println!("Available commands:");
        println!("  load <file>     - Load data from a file");
        println!("  clear           - Clear current data");
        println!("  show            - Display current data");
        println!("  history         - Show command history");
        println!("  explain <filter> - Explain what a filter does");
        println!("  validate <filter> - Check if a filter is valid");
        println!("  help            - Show this help message");
        println!("  quit            - Exit the REPL");
        println!();
        println!("Any other input is treated as a filter to execute on the current data.");
        println!("Examples:");
        println!("  .               - Identity filter (show all data)");
        println!("  .[]             - Iterate over array elements");
        println!("  .field          - Access object field");
        println!("  .[0]            - Access array element at index 0");
    }
}

/// Result of processing a REPL command
#[derive(Debug, PartialEq)]
enum CommandResult {
    Continue,
    Exit,
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use dsq_core::Value;

    use super::*;

    #[tokio::test]
    async fn test_repl_new() {
        let config = Config::default();
        let repl = Repl::new(config);
        assert!(repl.is_ok());
        let repl = repl.unwrap();
        assert!(repl.history.is_empty());
        assert!(repl.current_data.is_none());
    }

    #[tokio::test]
    async fn test_process_command_quit() {
        let config = Config::default();
        let mut repl = Repl::new(config).unwrap();

        let result = repl.process_command("quit").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CommandResult::Exit);

        let result = repl.process_command("exit").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CommandResult::Exit);

        let result = repl.process_command("q").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CommandResult::Exit);
    }

    #[tokio::test]
    async fn test_process_command_help() {
        let config = Config::default();
        let mut repl = Repl::new(config).unwrap();

        let result = repl.process_command("help").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CommandResult::Continue);

        let result = repl.process_command("h").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CommandResult::Continue);
    }

    #[tokio::test]
    async fn test_process_command_clear() {
        let config = Config::default();
        let mut repl = Repl::new(config).unwrap();

        // Set some data
        repl.current_data = Some(Value::String("test".to_string()));

        let result = repl.process_command("clear").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CommandResult::Continue);
        assert!(repl.current_data.is_none());
    }

    #[tokio::test]
    async fn test_process_command_show() {
        let config = Config::default();
        let mut repl = Repl::new(config).unwrap();

        // Test with no data
        let result = repl.process_command("show").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CommandResult::Continue);

        // Test with data
        repl.current_data = Some(Value::String("test data".to_string()));
        let result = repl.process_command("show").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CommandResult::Continue);
    }

    #[tokio::test]
    async fn test_process_command_history() {
        let config = Config::default();
        let mut repl = Repl::new(config).unwrap();

        let result = repl.process_command("history").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CommandResult::Continue);
    }

    #[tokio::test]
    async fn test_process_command_load_valid_file() {
        let config = Config::default();
        let mut repl = Repl::new(config).unwrap();

        // Create a temporary JSON file
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, r#"{{"name": "test", "value": 42}}"#).unwrap();
        let path = temp_file.path().to_str().unwrap();

        let result = repl.process_command(&format!("load {}", path)).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CommandResult::Continue);
        assert!(repl.current_data.is_some());
    }

    #[tokio::test]
    async fn test_process_command_load_invalid_file() {
        let config = Config::default();
        let mut repl = Repl::new(config).unwrap();

        let result = repl.process_command("load nonexistent.json").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_process_command_load_no_path() {
        let config = Config::default();
        let mut repl = Repl::new(config).unwrap();

        let result = repl.process_command("load").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CommandResult::Continue);
    }

    #[tokio::test]
    async fn test_process_command_explain() {
        let config = Config::default();
        let mut repl = Repl::new(config).unwrap();

        let result = repl.process_command("explain .").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CommandResult::Continue);

        let result = repl.process_command("explain .name").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CommandResult::Continue);
    }

    #[tokio::test]
    async fn test_process_command_explain_no_filter() {
        let config = Config::default();
        let mut repl = Repl::new(config).unwrap();

        let result = repl.process_command("explain").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CommandResult::Continue);
    }

    #[tokio::test]
    async fn test_process_command_validate() {
        let config = Config::default();
        let mut repl = Repl::new(config).unwrap();

        let result = repl.process_command("validate .").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CommandResult::Continue);

        let result = repl.process_command("validate invalid +++").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CommandResult::Continue);
    }

    #[tokio::test]
    async fn test_process_command_validate_no_filter() {
        let config = Config::default();
        let mut repl = Repl::new(config).unwrap();

        let result = repl.process_command("validate").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CommandResult::Continue);
    }

    #[tokio::test]
    async fn test_process_command_execute_filter() {
        let config = Config::default();
        let mut repl = Repl::new(config).unwrap();

        // Create test data
        let test_data = Value::from_json(serde_json::json!({"name": "test", "value": 42}));
        repl.current_data = Some(test_data);

        let result = repl.process_command(".").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CommandResult::Continue);
        assert_eq!(repl.history.len(), 1);
        assert_eq!(repl.history[0], ".");

        let result = repl.process_command(".name").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CommandResult::Continue);
        assert_eq!(repl.history.len(), 2);
        assert_eq!(repl.history[1], ".name");
    }

    #[tokio::test]
    async fn test_process_command_execute_filter_no_data() {
        let config = Config::default();
        let mut repl = Repl::new(config).unwrap();

        let result = repl.process_command(".").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CommandResult::Continue);
        // Should not add to history when no data
        assert!(repl.history.is_empty());
    }

    #[tokio::test]
    async fn test_process_command_invalid() {
        let config = Config::default();
        let mut repl = Repl::new(config).unwrap();

        let result = repl.process_command("invalid_command").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CommandResult::Continue);
    }

    #[tokio::test]
    async fn test_process_command_empty() {
        let config = Config::default();
        let mut repl = Repl::new(config).unwrap();

        let result = repl.process_command("").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CommandResult::Continue);
    }

    #[tokio::test]
    async fn test_load_file_nonexistent() {
        let config = Config::default();
        let mut repl = Repl::new(config).unwrap();

        let result = repl.load_file("nonexistent.json").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_load_file_valid_json() {
        let config = Config::default();
        let mut repl = Repl::new(config).unwrap();

        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, r#"{{"name": "test", "value": 42}}"#).unwrap();
        let path = temp_file.path();

        let result = repl.load_file(path.to_str().unwrap()).await;
        assert!(result.is_ok());
        assert!(repl.current_data.is_some());
    }

    #[tokio::test]
    async fn test_show_current_data_none() {
        let config = Config::default();
        let repl = Repl::new(config).unwrap();

        let result = repl.show_current_data();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_show_current_data_some() {
        let config = Config::default();
        let mut repl = Repl::new(config).unwrap();
        repl.current_data = Some(Value::String("test".to_string()));

        let result = repl.show_current_data();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_filter_no_data() {
        let config = Config::default();
        let mut repl = Repl::new(config).unwrap();

        let result = repl.execute_filter(".");
        assert!(result.is_ok());
        assert!(repl.history.is_empty());
    }

    #[tokio::test]
    async fn test_execute_filter_with_data() {
        let config = Config::default();
        let mut repl = Repl::new(config).unwrap();

        let test_data = Value::from_json(serde_json::json!({"name": "test"}));
        repl.current_data = Some(test_data);

        let result = repl.execute_filter(".");
        assert!(result.is_ok());
        assert_eq!(repl.history.len(), 1);
        assert_eq!(repl.history[0], ".");
    }

    #[tokio::test]
    async fn test_execute_filter_invalid() {
        let config = Config::default();
        let mut repl = Repl::new(config).unwrap();

        let test_data = Value::from_json(serde_json::json!({"name": "test"}));
        repl.current_data = Some(test_data);

        let result = repl.execute_filter("invalid +++");
        assert!(result.is_err());
        // Invalid filters should not be added to history
        assert!(repl.history.is_empty());
    }

    #[tokio::test]
    async fn test_explain_filter() {
        let config = Config::default();
        let repl = Repl::new(config).unwrap();

        let result = repl.explain_filter(".");
        assert!(result.is_ok());

        let result = repl.explain_filter("invalid");
        // explain_filter should not fail even for invalid filters
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_validate_filter() {
        let config = Config::default();
        let repl = Repl::new(config).unwrap();

        let result = repl.validate_filter(".");
        assert!(result.is_ok());

        let result = repl.validate_filter("invalid +++");
        assert!(result.is_ok()); // validate_filter should not fail
    }

    #[tokio::test]
    async fn test_show_history_empty() {
        let config = Config::default();
        let repl = Repl::new(config).unwrap();

        repl.show_history(); // Just ensure it doesn't panic
    }

    #[tokio::test]
    async fn test_show_history_with_commands() {
        let config = Config::default();
        let mut repl = Repl::new(config).unwrap();

        repl.history.push("command1".to_string());
        repl.history.push("command2".to_string());

        repl.show_history(); // Just ensure it doesn't panic
    }

    #[tokio::test]
    async fn test_show_help() {
        let config = Config::default();
        let repl = Repl::new(config).unwrap();

        repl.show_help(); // Just ensure it doesn't panic
    }

    #[tokio::test]
    async fn test_process_command_multi_word_filter() {
        let config = Config::default();
        let mut repl = Repl::new(config).unwrap();

        // Create test data
        let test_data = Value::from_json(serde_json::json!({"items": [{"name": "test"}]}));
        repl.current_data = Some(test_data);

        let result = repl.process_command(".items | .[0].name").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CommandResult::Continue);
        assert_eq!(repl.history.len(), 1);
        assert_eq!(repl.history[0], ".items | .[0].name");
    }

    #[tokio::test]
    async fn test_process_command_explain_multi_word() {
        let config = Config::default();
        let mut repl = Repl::new(config).unwrap();

        let result = repl.process_command("explain .items | .[0]").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CommandResult::Continue);
    }

    #[tokio::test]
    async fn test_process_command_validate_multi_word() {
        let config = Config::default();
        let mut repl = Repl::new(config).unwrap();

        let result = repl.process_command("validate .items | .[0]").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), CommandResult::Continue);
    }
}
