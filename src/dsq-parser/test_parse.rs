use dsq_parser::FilterParser;
use std::env;
use std::io::{self, Read};

fn main() {
    let parser = FilterParser::new();

    let input = if let Some(arg) = env::args().nth(1) {
        arg
    } else {
        let mut buffer = String::new();
        io::stdin()
            .read_to_string(&mut buffer)
            .expect("Failed to read from stdin");
        buffer.trim().to_string()
    };

    if input.is_empty() {
        eprintln!("Usage: dsq-parser <filter_expression> or pipe input via stdin");
        std::process::exit(1);
    }

    match parser.parse(&input) {
        Ok(filter) => {
            let json = serde_json::to_string_pretty(&filter).expect("Failed to serialize AST");
            println!("{}", json);
        }
        Err(e) => {
            eprintln!("Parse error: {}", e);
            std::process::exit(1);
        }
    }
}
