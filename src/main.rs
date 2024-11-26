use clap::Parser;
use glob_match::glob_match;
use chrono::DateTime;
use indicatif::ProgressBar;
use itertools::Itertools;
use serde::{ser::SerializeSeq, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::vec;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    repo_path: String,
    output_path: String,

    #[arg(long)]
    primary_key: String,

    #[arg(short, long, default_value = "**/*")]
    include: String,

    #[arg(short = 'a', long)]
    include_authors: Vec<String>,

    #[arg(long)]
    ignore_revs: Vec<String>,

    #[arg(long)]
    until: Option<String>,
}

#[derive(Debug, Serialize)]
struct ChangeInstant {
    commit: String,
    #[serde(serialize_with = "serialize_timestamp")]
    timestamp: i64,
}

#[derive(Serialize)]
struct ChangeRecord {
    #[serde(serialize_with = "serialize_change_instants")]
    added: Vec<Arc<ChangeInstant>>,
    #[serde(serialize_with = "serialize_change_instants")]
    removed: Vec<Arc<ChangeInstant>>,
    #[serde(serialize_with = "serialize_change_instants")]
    modified: Vec<Arc<ChangeInstant>>,
}

fn serialize_timestamp<S: serde::Serializer>(
    timestamp: &i64,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let dt = DateTime::from_timestamp(*timestamp, 0).unwrap();
    let s = dt.format("%+").to_string();
    serializer.serialize_str(&s)
}

fn serialize_change_instants<S>(
    instants: &Vec<Arc<ChangeInstant>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let mut seq = serializer.serialize_seq(Some(instants.len()))?;
    let mut reversed = instants.clone();
    reversed.reverse();
    for instant in reversed {
        seq.serialize_element(&*instant)?;
    }
    seq.end()
}

// TODO: return exactly what changed
fn deep_diff_json(old_json: &serde_json::Value, new_json: &serde_json::Value) -> bool {
    match (old_json, new_json) {
        (serde_json::Value::Object(old_obj), serde_json::Value::Object(new_obj)) => {
            let mut old_keys = old_obj.keys().collect::<Vec<&String>>();
            old_keys.sort();
            let mut new_keys = new_obj.keys().collect::<Vec<&String>>();
            new_keys.sort();
            if old_keys.len() != new_keys.len()
                || old_keys.iter().zip(new_keys.iter()).any(|(a, b)| a != b)
            {
                return true;
            }
            for (key, old_val) in old_obj {
                match new_obj.get(key) {
                    Some(new_val) => {
                        if deep_diff_json(old_val, new_val) {
                            return true;
                        }
                    }
                    None => return true,
                }
            }
        }
        (serde_json::Value::Array(old_arr), serde_json::Value::Array(new_arr)) => {
            if old_arr.len() != new_arr.len() {
                return true;
            }
            for (old_val, new_val) in old_arr.iter().zip(new_arr.iter()) {
                if deep_diff_json(old_val, new_val) {
                    return true;
                }
            }
        }
        (old_val, new_val) => {
            if old_val != new_val {
                return true;
            }
        }
    }
    return false;
}

fn get_json_data(
    repo: &git2::Repository,
    tree: &git2::Tree,
    path: &Path,
    primary_key: &str,
) -> HashMap<String, serde_json::Value> {
    let tree_entry = tree.get_path(path).expect("Failed to get tree entry");
    let object = match tree_entry.to_object(&repo) {
        Ok(object) => object,
        Err(_) => {
            // Fetch object from remote
            todo!()
        }
    };
    let blob = object.into_blob().expect("Failed to get blob");
    let content = blob.content();
    let content: Vec<serde_json::Value> =
        serde_json::from_slice(content).expect("Failed to parse json");
    let mut data: HashMap<String, serde_json::Value> = HashMap::new();
    for record in content {
        let primary_key_val = match &record[primary_key] {
            serde_json::Value::String(s) => s,
            _ => panic!("Primary key is not a string"),
        };
        data.insert(primary_key_val.to_string(), record);
    }
    data
}

enum ChangeType {
    Added,
    Removed,
    Modified,
}

fn update_change_record_entry(
    change_record_entry: &mut HashMap<String, ChangeRecord>,
    primary_key: String,
    change_instant: Arc<ChangeInstant>,
    change_type: ChangeType,
) {
    let change_record = change_record_entry
        .entry(primary_key)
        .or_insert(ChangeRecord {
            added: vec![],
            removed: vec![],
            modified: vec![],
        });
    match change_type {
        ChangeType::Added => {
            change_record.added.push(change_instant);
        }
        ChangeType::Removed => {
            change_record.removed.push(change_instant);
        }
        ChangeType::Modified => {
            change_record.modified.push(change_instant);
        }
    }
}

fn main() {
    let args = Args::parse();
    let repo = git2::Repository::open(&args.repo_path).expect("Failed to open repository");
    let mut revwalk = repo.revwalk().expect("Failed to create revwalk");
    revwalk.push_head().unwrap();
    let mut revwalk_count = repo.revwalk().expect("Failed to create revwalk");
    revwalk_count.push_head().expect("Failed to push HEAD");
    let commit_count = revwalk_count.count();
    let progress_bar = ProgressBar::new(commit_count as u64);
    progress_bar.println(format!("Found {} commits", commit_count));
    let mut change_records: HashMap<PathBuf, HashMap<String, ChangeRecord>> = HashMap::new();
    let until_commit = match &args.until {
        Some(until) => repo
            .revparse_single(until)
            .expect(format!("Failed to find commit {}", until).as_str())
            .id(),
        None => git2::Oid::zero(),
    };
    let mut cached_data: HashMap<PathBuf, HashMap<String, serde_json::Value>> = HashMap::new();
    let mut prev_oid = git2::Oid::zero();
    revwalk.set_sorting(git2::Sort::TIME).unwrap();

    for oid in revwalk {
        let oid = oid.expect("Failed to get oid");
        if oid != prev_oid {
            cached_data.clear();
        }
        // Now cached_data is ready to be used. We've guaranteed that it contains data for the
        // current commit (which was previously the parent). At the end of the loop, we'll
        // overwrite it with the data from the parent.
        let mut next_cached_data: HashMap<PathBuf, HashMap<String, serde_json::Value>> =
            HashMap::new();
        if args.ignore_revs.contains(&oid.to_string()) {
            continue;
        }
        if oid == until_commit {
            progress_bar.println("Reached until commit");
            break;
        }
        let commit = repo
            .find_commit(oid)
            .expect(format!("Failed to find commit {oid}").as_str());
        if !args.include_authors.is_empty()
            && !args
                .include_authors
                .contains(&commit.author().name().unwrap().to_string())
            && !args
                .include_authors
                .contains(&commit.author().email().unwrap().to_string())
        {
            continue;
        }
        let parent_commit = match commit.parent(0) {
            Ok(parent) => parent,
            Err(_) => {
                progress_bar.println(format!("Commit {} with no parent", commit.id()));
                break;
            }
        };
        progress_bar.println(format!(
            "Diffing {} '{}' with {} '{}'",
            parent_commit.id(),
            parent_commit.message().unwrap().trim(),
            commit.id(),
            commit.message().unwrap().trim(),
        ));
        let parent_tree = &parent_commit.tree().expect("Failed to get parent tree");
        let commit_tree = &commit.tree().expect("Failed to get commit tree");
        let diff = repo
            .diff_tree_to_tree(Some(parent_tree), Some(commit_tree), None)
            .unwrap();
        let changed_files = diff.deltas();
        progress_bar.println(format!("Changed {} files", changed_files.len()));
        for delta in changed_files {
            let old_path = delta.old_file().path().unwrap();
            let new_path = delta.new_file().path().unwrap();
            if old_path != new_path {
                panic!(
                    "Old path {} does not match new path {}",
                    old_path.to_string_lossy(),
                    new_path.to_string_lossy()
                );
            }
            if !glob_match(args.include.as_str(), old_path.to_str().unwrap()) {
                continue;
            }
            progress_bar.println(format!("Diffing: {}", old_path.to_string_lossy()));
            let change_instant = Arc::new(ChangeInstant {
                commit: commit.id().to_string(),
                timestamp: commit.time().seconds(),
            });
            let change_record_entry = change_records
                .entry(new_path.to_path_buf())
                .or_insert(HashMap::new());
            match &delta.status() {
                git2::Delta::Added => {
                    let new_content = cached_data
                        .remove(new_path)
                        .or_else(|| {
                            Some(get_json_data(
                                &repo,
                                &commit_tree,
                                new_path,
                                &args.primary_key,
                            ))
                        })
                        .unwrap();
                    for (pk, _) in new_content {
                        update_change_record_entry(
                            change_record_entry,
                            pk,
                            change_instant.clone(),
                            ChangeType::Added,
                        );
                    }
                }
                git2::Delta::Deleted => {
                    let old_content =
                        get_json_data(&repo, &parent_tree, old_path, &args.primary_key);
                    for (pk, _) in &old_content {
                        update_change_record_entry(
                            change_record_entry,
                            pk.to_string(),
                            change_instant.clone(),
                            ChangeType::Removed,
                        );
                    }
                    next_cached_data.insert(old_path.to_path_buf(), old_content);
                }
                git2::Delta::Modified => {
                    let new_content = cached_data
                        .remove(new_path)
                        .or_else(|| {
                            Some(get_json_data(
                                &repo,
                                &commit_tree,
                                new_path,
                                &args.primary_key,
                            ))
                        })
                        .unwrap();
                    let old_content =
                        get_json_data(&repo, &parent_tree, old_path, &args.primary_key);
                    let mut unseen_new_pks: HashSet<String> =
                        new_content.keys().map(|s| s.clone()).collect();
                    for (pk, old_val) in &old_content {
                        unseen_new_pks.remove(pk);
                        let new_val = match new_content.get(pk) {
                            Some(new_val) => new_val,
                            None => {
                                update_change_record_entry(
                                    change_record_entry,
                                    pk.to_string(),
                                    change_instant.clone(),
                                    ChangeType::Removed,
                                );
                                continue;
                            }
                        };
                        if !deep_diff_json(&old_val, &new_val) {
                            continue;
                        }
                        update_change_record_entry(
                            change_record_entry,
                            pk.to_string(),
                            change_instant.clone(),
                            ChangeType::Modified,
                        );
                    }
                    for pk in unseen_new_pks {
                        update_change_record_entry(
                            change_record_entry,
                            pk,
                            change_instant.clone(),
                            ChangeType::Added,
                        );
                    }
                }
                _ => panic!("Unknown delta type {:?}", delta.status()),
            }
        }
        progress_bar.inc(1);
        cached_data = next_cached_data;
        prev_oid = oid;
    }
    progress_bar.finish();
    for (path, change_record) in change_records {
        let output_path = Path::join(Path::new(&args.output_path), &path);
        fs::create_dir_all(output_path.parent().unwrap()).expect("Failed to create directory");
        let file = File::create(output_path).unwrap();
        let sorted_map = change_record
            .iter()
            .sorted_by_key(|v| v.0)
            .collect::<BTreeMap<_, _>>();
        serde_json::to_writer_pretty(file, &sorted_map).expect("Failed to write json");
    }
}
