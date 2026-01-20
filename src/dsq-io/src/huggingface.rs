//! HuggingFace Hub support for dsq-io
//!
//! This module provides functionality for fetching files from HuggingFace Hub.

use crate::{Error, Result};
use hf_hub::api::tokio::{Api, ApiBuilder};
use std::env;
use std::path::PathBuf;

/// Fetch a file from HuggingFace Hub
///
/// # Format
///
/// The URL format should be one of:
/// - `hf://datasets/{owner}/{repo}/{file_path}` - for datasets
/// - `hf://models/{owner}/{repo}/{file_path}` - for models (default)
/// - `hf://{owner}/{repo}/{file_path}` - defaults to models
///
/// # Examples
///
/// ```rust,ignore
/// use dsq_io::huggingface::fetch_huggingface;
///
/// // Fetch a dataset file
/// let data = fetch_huggingface("hf://datasets/username/dataset-name/data.csv").await.unwrap();
///
/// // Fetch a model file
/// let data = fetch_huggingface("hf://models/username/model-name/config.json").await.unwrap();
///
/// // Short form (defaults to models)
/// let data = fetch_huggingface("hf://username/model-name/config.json").await.unwrap();
/// ```
pub async fn fetch_huggingface(url: &str) -> Result<Vec<u8>> {
    let url = url
        .strip_prefix("hf://")
        .ok_or_else(|| Error::Other("HuggingFace URL must start with 'hf://'".to_string()))?;

    // Parse the URL format
    let (repo_type, owner, repo, file_path) = parse_hf_url(url)?;

    // Configure API with custom cache directory
    let api = get_hf_api()?;

    let file_path_local = match repo_type {
        RepoType::Dataset => {
            let repo = api.dataset(format!("{owner}/{repo}"));
            repo.get(&file_path)
                .await
                .map_err(|e| Error::Other(format!("Failed to fetch from HuggingFace: {e}")))?
        }
        RepoType::Model => {
            let repo = api.model(format!("{owner}/{repo}"));
            repo.get(&file_path)
                .await
                .map_err(|e| Error::Other(format!("Failed to fetch from HuggingFace: {e}")))?
        }
    };

    // Read the downloaded file
    let bytes = tokio::fs::read(&file_path_local)
        .await
        .map_err(|e| Error::Other(format!("Failed to read downloaded file: {e}")))?;

    Ok(bytes)
}

#[derive(Debug, Clone, Copy)]
enum RepoType {
    Dataset,
    Model,
}

fn parse_hf_url(url: &str) -> Result<(RepoType, String, String, String)> {
    let parts: Vec<&str> = url.split('/').collect();

    if parts.len() < 3 {
        return Err(Error::Other(
            "Invalid HuggingFace URL format. Expected: hf://[datasets|models/]owner/repo/file_path"
                .to_string(),
        ));
    }

    // Check if first part is "datasets" or "models"
    let (repo_type, owner_idx) = if parts[0] == "datasets" {
        (RepoType::Dataset, 1)
    } else if parts[0] == "models" {
        (RepoType::Model, 1)
    } else {
        // Default to models if not specified
        (RepoType::Model, 0)
    };

    if parts.len() < owner_idx + 3 {
        return Err(Error::Other(
            "Invalid HuggingFace URL format. Expected: hf://[datasets|models/]owner/repo/file_path"
                .to_string(),
        ));
    }

    let owner = parts[owner_idx].to_string();
    let repo = parts[owner_idx + 1].to_string();
    let file_path = parts[owner_idx + 2..].join("/");

    if file_path.is_empty() {
        return Err(Error::Other(
            "File path cannot be empty in HuggingFace URL".to_string(),
        ));
    }

    Ok((repo_type, owner, repo, file_path))
}

/// Get the HuggingFace API instance with custom cache directory
///
/// Uses cache directory in the following priority:
/// 1. `DATASETQ_HF_CACHE` environment variable
/// 2. `~/.local/durable/datasetq/cache` (default)
///
/// Caching can be disabled by setting `DATASETQ_HF_NO_CACHE=1`
fn get_hf_api() -> Result<Api> {
    // Check if caching is disabled
    if env::var("DATASETQ_HF_NO_CACHE").unwrap_or_default() == "1" {
        // Use default cache location (hf-hub default)
        return Api::new().map_err(|e| Error::Other(format!("Failed to create HF API: {e}")));
    }

    // Determine cache directory
    let cache_dir = if let Ok(custom_cache) = env::var("DATASETQ_HF_CACHE") {
        PathBuf::from(custom_cache)
    } else {
        // Default: ~/.local/durable/datasetq/cache
        dirs::home_dir()
            .ok_or_else(|| Error::Other("Could not determine home directory".to_string()))?
            .join(".local")
            .join("durable")
            .join("datasetq")
            .join("cache")
    };

    // Create cache directory if it doesn't exist
    std::fs::create_dir_all(&cache_dir)
        .map_err(|e| Error::Other(format!("Failed to create cache directory: {e}")))?;

    // Build API with custom cache directory
    ApiBuilder::new()
        .with_cache_dir(cache_dir)
        .build()
        .map_err(|e| Error::Other(format!("Failed to create HF API: {e}")))
}

/// Check if a string is a HuggingFace URL
pub fn is_huggingface_url(s: &str) -> bool {
    s.starts_with("hf://")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_huggingface_url() {
        assert!(is_huggingface_url("hf://datasets/user/repo/file.csv"));
        assert!(is_huggingface_url("hf://models/user/repo/file.csv"));
        assert!(is_huggingface_url("hf://user/repo/file.csv"));
        assert!(!is_huggingface_url(
            "https://huggingface.co/datasets/user/repo"
        ));
        assert!(!is_huggingface_url("/path/to/file.csv"));
    }

    #[test]
    fn test_parse_hf_url() {
        // Dataset URL
        let (repo_type, owner, repo, file_path) =
            parse_hf_url("datasets/user/my-dataset/data.csv").unwrap();
        assert!(matches!(repo_type, RepoType::Dataset));
        assert_eq!(owner, "user");
        assert_eq!(repo, "my-dataset");
        assert_eq!(file_path, "data.csv");

        // Model URL
        let (repo_type, owner, repo, file_path) =
            parse_hf_url("models/user/my-model/config.json").unwrap();
        assert!(matches!(repo_type, RepoType::Model));
        assert_eq!(owner, "user");
        assert_eq!(repo, "my-model");
        assert_eq!(file_path, "config.json");

        // Short form (defaults to model)
        let (repo_type, owner, repo, file_path) = parse_hf_url("user/my-repo/file.txt").unwrap();
        assert!(matches!(repo_type, RepoType::Model));
        assert_eq!(owner, "user");
        assert_eq!(repo, "my-repo");
        assert_eq!(file_path, "file.txt");

        // Nested file path
        let (repo_type, owner, repo, file_path) =
            parse_hf_url("datasets/user/repo/dir1/dir2/file.csv").unwrap();
        assert!(matches!(repo_type, RepoType::Dataset));
        assert_eq!(owner, "user");
        assert_eq!(repo, "repo");
        assert_eq!(file_path, "dir1/dir2/file.csv");
    }

    #[test]
    fn test_parse_hf_url_invalid() {
        // Too few parts
        assert!(parse_hf_url("user/repo").is_err());
        assert!(parse_hf_url("user").is_err());

        // Empty file path
        assert!(parse_hf_url("datasets/user/repo/").is_err());
    }
}
