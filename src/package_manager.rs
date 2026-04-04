use crate::error::DgmError;
use semver::{Version, VersionReq};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Cursor, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const MANIFEST_MAX_BYTES: usize = 1024 * 1024;
const METADATA_MAX_BYTES: usize = 1024 * 1024;
const PACKAGE_MAX_BYTES: usize = 10 * 1024 * 1024;
const DOWNLOAD_TIMEOUT_MS: u64 = 5_000;
const DOWNLOAD_RETRIES: usize = 3;
const LOCK_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProjectManifest {
    #[serde(default)]
    pub package: PackageSection,
    #[serde(default)]
    pub dependencies: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PackageSection {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub version: String,
    #[serde(default, alias = "entry")]
    pub main: Option<String>,
    #[serde(default)]
    pub test: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockFile {
    #[serde(default = "default_lock_version")]
    pub version: u32,
    #[serde(default)]
    pub root_dependencies: BTreeMap<String, String>,
    #[serde(default)]
    pub package: Vec<LockedPackage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockedPackage {
    pub name: String,
    pub version: String,
    pub hash: String,
    pub source: String,
    #[serde(default)]
    pub registry: Option<String>,
    #[serde(default)]
    pub fetched_at: Option<u64>,
    #[serde(default)]
    pub size: u64,
    #[serde(default)]
    pub dependencies: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct InstallManifest {
    #[serde(default)]
    dependencies: BTreeMap<String, InstalledDependency>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InstalledDependency {
    version: String,
    path: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RegistryMetadata {
    name: String,
    #[serde(default)]
    versions: Vec<RegistryPackageVersion>,
}

#[derive(Debug, Clone, Deserialize)]
struct RegistryPackageVersion {
    version: String,
    url: String,
    sha256: String,
    #[serde(default)]
    size: Option<u64>,
    #[serde(default)]
    dependencies: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
struct ResolvedPackage {
    name: String,
    version: String,
    source: String,
    hash: String,
    registry: String,
    fetched_at: u64,
    size: u64,
    dependencies: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct InstallReport {
    pub root: PathBuf,
    pub packages: usize,
    pub reused_lock: bool,
}

#[derive(Debug, Clone, Default)]
pub struct InstallOptions {
    pub offline: bool,
    pub frozen: bool,
}

#[derive(Debug, Clone, Default)]
pub struct UpdateOptions {
    pub targets: Vec<String>,
    pub offline: bool,
    pub dry_run: bool,
    pub allow_major: bool,
    pub run_tests: bool,
}

#[derive(Debug, Clone)]
pub struct DependencyChange {
    pub name: String,
    pub from: Option<String>,
    pub to: String,
    pub major_change: bool,
}

#[derive(Debug, Clone)]
pub struct UpdateReport {
    pub root: PathBuf,
    pub changes: Vec<DependencyChange>,
    pub packages: usize,
    pub dry_run: bool,
    pub tested: bool,
    pub rolled_back: bool,
}

#[derive(Debug, Clone)]
pub struct DoctorReport {
    pub root: PathBuf,
    pub issues: Vec<String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct CleanReport {
    pub root: PathBuf,
    pub removed_paths: Vec<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct OutdatedPackage {
    pub name: String,
    pub current: String,
    pub latest: String,
    pub major: bool,
}

#[derive(Debug, Clone)]
pub struct CachePruneReport {
    pub root: PathBuf,
    pub removed_files: Vec<PathBuf>,
}

struct ProjectLockGuard {
    path: PathBuf,
}

impl DoctorReport {
    pub fn is_healthy(&self) -> bool {
        self.issues.is_empty()
    }
}

impl Drop for ProjectLockGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn default_lock_version() -> u32 {
    LOCK_VERSION
}

pub fn find_project_root(start: &Path) -> Option<PathBuf> {
    let mut current = if start.is_dir() {
        start.to_path_buf()
    } else {
        start.parent()?.to_path_buf()
    };
    loop {
        if current.join("dgm.toml").exists() {
            return Some(current);
        }
        if !current.pop() {
            return None;
        }
    }
}

pub fn init_project(dir: &Path) -> Result<PathBuf, DgmError> {
    fs::create_dir_all(dir).map_err(|e| runtime_err(format!("cannot create project directory '{}': {}", dir.display(), e)))?;
    let root = dir.to_path_buf();
    fs::create_dir_all(root.join(".dgm")).map_err(|e| runtime_err(format!("cannot create '{}': {}", root.join(".dgm").display(), e)))?;
    let _guard = acquire_project_lock(&root)?;
    let manifest_path = root.join("dgm.toml");
    if manifest_path.exists() {
        return Ok(root);
    }
    let mut manifest = ProjectManifest::default();
    manifest.package.name = default_package_name(&root);
    manifest.package.version = "0.1.0".into();
    write_manifest(&manifest_path, &manifest)?;
    let main_path = root.join("main.dgm");
    if !main_path.exists() {
        fs::write(&main_path, "writ(\"Hello from DGM\")\n")
            .map_err(|e| runtime_err(format!("cannot write '{}': {}", main_path.display(), e)))?;
    }
    fs::create_dir_all(root.join(".dgm/packages"))
        .map_err(|e| runtime_err(format!("cannot create '.dgm/packages': {}", e)))?;
    write_install_manifest(&root.join(".dgm/manifest.toml"), &BTreeMap::new())?;
    Ok(root)
}

pub fn add_dependency(project_dir: &Path, spec: &str, offline: bool) -> Result<(PathBuf, String, String), DgmError> {
    let root = require_project_root(project_dir)?;
    let _guard = acquire_project_lock(&root)?;
    let manifest_path = root.join("dgm.toml");
    let mut manifest = read_manifest(&manifest_path)?;
    let (name, req) = parse_add_spec(spec)?;
    let version_req = match req {
        Some(req) => req,
        None => {
            let latest = resolve_latest_version(&name, offline)?;
            format!("^{}", latest)
        }
    };
    manifest.dependencies.insert(name.clone(), version_req.clone());
    write_manifest(&manifest_path, &manifest)?;
    Ok((root, name, version_req))
}

pub fn install_project(project_dir: &Path, offline: bool) -> Result<InstallReport, DgmError> {
    install_project_with_options(project_dir, &InstallOptions { offline, frozen: false })
}

pub fn install_project_with_options(project_dir: &Path, options: &InstallOptions) -> Result<InstallReport, DgmError> {
    let root = require_project_root(project_dir)?;
    let _guard = acquire_project_lock(&root)?;
    let manifest = read_manifest(&root.join("dgm.toml"))?;
    validate_manifest(&manifest)?;
    let lock_path = root.join("dgm.lock");
    let existing_lock = if lock_path.exists() {
        Some(read_lock_file(&lock_path)?)
    } else {
        None
    };
    let (resolved, reused_lock) = match &existing_lock {
        Some(lock) if lock.version == LOCK_VERSION && lock.root_dependencies == manifest.dependencies => {
            (resolved_from_lock(lock)?, true)
        }
        Some(lock) if options.frozen => {
            return Err(runtime_err(format!(
                "frozen install requires dgm.lock to match dgm.toml (lock has {} root deps, manifest has {})",
                lock.root_dependencies.len(),
                manifest.dependencies.len()
            )));
        }
        None if options.frozen => {
            return Err(runtime_err("frozen install requires an existing dgm.lock".into()));
        }
        _ => (resolve_manifest_dependencies(&manifest.dependencies, options.offline)?, false),
    };
    let lock = build_lock_file(&manifest.dependencies, &resolved);
    let lock_to_write = if options.frozen { None } else { Some(&lock) };
    apply_install_state(&root, &manifest.dependencies, &resolved, lock_to_write, false, options.offline)?;

    Ok(InstallReport {
        root,
        packages: resolved.len(),
        reused_lock,
    })
}

pub fn update_project<F>(project_dir: &Path, options: &UpdateOptions, mut test_hook: F) -> Result<UpdateReport, DgmError>
where
    F: FnMut(&Path) -> Result<(), DgmError>,
{
    let root = require_project_root(project_dir)?;
    let _guard = acquire_project_lock(&root)?;
    let manifest = read_manifest(&root.join("dgm.toml"))?;
    validate_manifest(&manifest)?;

    let targets = normalize_update_targets(&manifest.dependencies, &options.targets)?;
    let old_lock_path = root.join("dgm.lock");
    let old_lock = if old_lock_path.exists() {
        Some(read_lock_file(&old_lock_path)?)
    } else {
        None
    };
    let old_resolved = match &old_lock {
        Some(lock) => Some(resolved_from_lock(lock)?),
        None => None,
    };
    let old_root_versions = match (&old_lock, &old_resolved) {
        (Some(lock), Some(resolved)) => resolve_root_versions(&lock.root_dependencies, resolved)?,
        _ => BTreeMap::new(),
    };
    let effective_root_specs = build_effective_update_specs(&manifest.dependencies, &old_root_versions, &targets);
    let resolved = resolve_manifest_dependencies(&effective_root_specs, options.offline)?;
    let new_root_versions = resolve_root_versions(&effective_root_specs, &resolved)?;
    let changes = collect_dependency_changes(&manifest.dependencies, &old_root_versions, &new_root_versions);

    if changes.iter().any(|change| change.major_change) && !options.allow_major {
        let names = changes
            .iter()
            .filter(|change| change.major_change)
            .map(|change| format!("{} {} -> {}", change.name, change.from.clone().unwrap_or_else(|| "none".into()), change.to))
            .collect::<Vec<_>>()
            .join(", ");
        return Err(runtime_err(format!("major version update blocked: {} (rerun with --allow-major)", names)));
    }

    if options.dry_run || changes.is_empty() {
        return Ok(UpdateReport {
            root,
            changes,
            packages: resolved.len(),
            dry_run: options.dry_run,
            tested: false,
            rolled_back: false,
        });
    }

    let new_lock = build_lock_file(&manifest.dependencies, &resolved);
    apply_install_state(&root, &manifest.dependencies, &resolved, Some(&new_lock), true, options.offline)?;

    if options.run_tests {
        if let Err(err) = test_hook(&root) {
            restore_previous_state(&root, old_lock.as_ref(), old_resolved.as_deref())?;
            return Err(runtime_err(format!("update test failed, rolled back: {}", err)));
        }
    }

    Ok(UpdateReport {
        root,
        changes,
        packages: resolved.len(),
        dry_run: false,
        tested: options.run_tests,
        rolled_back: false,
    })
}

pub fn doctor_project(project_dir: &Path) -> Result<DoctorReport, DgmError> {
    let root = require_project_root(project_dir)?;
    let _guard = acquire_project_lock(&root)?;
    let mut issues = Vec::new();
    let mut warnings = Vec::new();
    let manifest = match read_manifest(&root.join("dgm.toml")) {
        Ok(manifest) => manifest,
        Err(err) => {
            issues.push(format!("{}", err));
            return Ok(DoctorReport { root, issues, warnings });
        }
    };
    if let Err(err) = validate_manifest(&manifest) {
        issues.push(format!("{}", err));
    }

    let lock_path = root.join("dgm.lock");
    let lock = if lock_path.exists() {
        match read_lock_file(&lock_path) {
            Ok(lock) => Some(lock),
            Err(err) => {
                issues.push(format!("{}", err));
                None
            }
        }
    } else {
        issues.push("missing dgm.lock".into());
        None
    };

    let packages_root = root.join(".dgm/packages");
    if !packages_root.exists() {
        issues.push(format!("missing installed package directory '{}'", packages_root.display()));
    }

    if let Some(lock) = &lock {
        if lock.root_dependencies != manifest.dependencies {
            issues.push("dgm.lock does not match dgm.toml dependencies".into());
        }
        for package in &lock.package {
            if package.registry.as_deref().unwrap_or("").is_empty() {
                warnings.push(format!("missing registry origin for '{}@{}'", package.name, package.version));
            }
            if package.fetched_at.unwrap_or(0) == 0 {
                warnings.push(format!("missing fetch timestamp for '{}@{}'", package.name, package.version));
            }
            let package_dir = packages_root.join(&package.name).join(&package.version);
            if !package_dir.exists() {
                issues.push(format!("missing installed package '{}@{}'", package.name, package.version));
            } else if let Err(err) = package_entry_path(&package_dir) {
                issues.push(format!("{}", err));
            }

            let cache_path = cached_archive_path(&package.name, &package.version, &package.hash)?;
            if !cache_path.exists() {
                warnings.push(format!("missing cache archive for '{}@{}'", package.name, package.version));
            } else {
                match read_file_bytes_with_limit(&cache_path, PACKAGE_MAX_BYTES, "package archive") {
                    Ok(bytes) => {
                        let actual_hash = sha256_hex(&bytes);
                        if !package.hash.eq_ignore_ascii_case(&actual_hash) {
                            issues.push(format!(
                                "hash mismatch for cached '{}@{}': expected {}, got {}",
                                package.name, package.version, package.hash, actual_hash
                            ));
                        }
                    }
                    Err(err) => issues.push(format!("{}", err)),
                }
            }
        }
    }

    let install_manifest_path = root.join(".dgm/manifest.toml");
    if !install_manifest_path.exists() {
        warnings.push("missing .dgm/manifest.toml".into());
    } else if let Err(err) = read_install_manifest(&install_manifest_path) {
        issues.push(format!("{}", err));
    }

    Ok(DoctorReport { root, issues, warnings })
}

pub fn clean_project(project_dir: &Path) -> Result<CleanReport, DgmError> {
    let root = require_project_root(project_dir)?;
    let _guard = acquire_project_lock(&root)?;
    let dgm_dir = root.join(".dgm");
    fs::create_dir_all(&dgm_dir).map_err(|e| runtime_err(format!("cannot create '{}': {}", dgm_dir.display(), e)))?;
    let mut removed_paths = Vec::new();
    for candidate in [dgm_dir.join("packages"), dgm_dir.join("packages_tmp")] {
        if candidate.exists() {
            fs::remove_dir_all(&candidate)
                .map_err(|e| runtime_err(format!("cannot remove '{}': {}", candidate.display(), e)))?;
            removed_paths.push(candidate);
        }
    }
    if let Ok(entries) = fs::read_dir(&dgm_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with(".tmp-") {
                if path.is_dir() {
                    fs::remove_dir_all(&path)
                        .map_err(|e| runtime_err(format!("cannot remove '{}': {}", path.display(), e)))?;
                } else {
                    fs::remove_file(&path)
                        .map_err(|e| runtime_err(format!("cannot remove '{}': {}", path.display(), e)))?;
                }
                removed_paths.push(path);
            }
        }
    }
    Ok(CleanReport { root, removed_paths })
}

pub fn prune_cache(project_dir: &Path) -> Result<CachePruneReport, DgmError> {
    let root = require_project_root(project_dir)?;
    let _guard = acquire_project_lock(&root)?;
    let lock = read_lock_file(&root.join("dgm.lock"))?;
    let cache_root = cache_root()?;
    let archives_dir = cache_root.join("archives");
    let metadata_dir = cache_root.join("metadata");
    let mut keep_archives = HashSet::new();
    let mut keep_metadata = HashSet::new();
    for package in &lock.package {
        keep_archives.insert(format!(
            "{}-{}-{}.tar.gz",
            package.name,
            package.version,
            &package.hash[..package.hash.len().min(16)]
        ));
        keep_metadata.insert(format!("{}.json", package.name));
    }
    let mut removed_files = Vec::new();
    for (dir, keep) in [(&archives_dir, &keep_archives), (&metadata_dir, &keep_metadata)] {
        if !dir.exists() {
            continue;
        }
        for entry in fs::read_dir(dir)
            .map_err(|e| runtime_err(format!("cannot read cache directory '{}': {}", dir.display(), e)))?
        {
            let entry = entry.map_err(|e| runtime_err(format!("cannot read cache entry in '{}': {}", dir.display(), e)))?;
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            if keep.contains(&name) {
                continue;
            }
            if path.is_dir() {
                fs::remove_dir_all(&path)
                    .map_err(|e| runtime_err(format!("cannot remove '{}': {}", path.display(), e)))?;
            } else {
                fs::remove_file(&path)
                    .map_err(|e| runtime_err(format!("cannot remove '{}': {}", path.display(), e)))?;
            }
            removed_files.push(path);
        }
    }
    removed_files.sort();
    Ok(CachePruneReport { root, removed_files })
}

pub fn outdated_project(project_dir: &Path, offline: bool) -> Result<Vec<OutdatedPackage>, DgmError> {
    let root = require_project_root(project_dir)?;
    let _guard = acquire_project_lock(&root)?;
    let manifest = read_manifest(&root.join("dgm.toml"))?;
    validate_manifest(&manifest)?;
    let lock = read_lock_file(&root.join("dgm.lock"))?;
    let resolved = resolved_from_lock(&lock)?;
    let current_versions = resolve_root_versions(&lock.root_dependencies, &resolved)?;
    let mut outdated = Vec::new();
    for name in manifest.dependencies.keys() {
        let current = current_versions
            .get(name)
            .cloned()
            .ok_or_else(|| runtime_err(format!("root dependency '{}' is missing from dgm.lock", name)))?;
        let latest = resolve_latest_version(name, offline)?;
        if latest != current {
            let current_version = Version::parse(&current)
                .map_err(|e| runtime_err(format!("invalid locked version '{}': {}", current, e)))?;
            let latest_version = Version::parse(&latest)
                .map_err(|e| runtime_err(format!("invalid registry version '{}': {}", latest, e)))?;
            outdated.push(OutdatedPackage {
                name: name.clone(),
                current,
                latest,
                major: current_version.major != latest_version.major,
            });
        }
    }
    outdated.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(outdated)
}

pub fn why_dependency(project_dir: &Path, target: &str) -> Result<Vec<String>, DgmError> {
    let root = require_project_root(project_dir)?;
    let _guard = acquire_project_lock(&root)?;
    let lock = read_lock_file(&root.join("dgm.lock"))?;
    let mut graph = HashMap::<String, Vec<String>>::new();
    graph.insert("app".into(), lock.root_dependencies.keys().cloned().collect());
    for package in &lock.package {
        graph.insert(package.name.clone(), package.dependencies.keys().cloned().collect());
    }
    let mut queue = vec![vec!["app".to_string()]];
    let mut visited = HashSet::new();
    while let Some(path) = queue.pop() {
        let Some(node) = path.last() else {
            continue;
        };
        if node == target {
            return Ok(path);
        }
        if !visited.insert(node.clone()) {
            continue;
        }
        if let Some(deps) = graph.get(node) {
            for dep in deps.iter().rev() {
                let mut next = path.clone();
                next.push(dep.clone());
                queue.push(next);
            }
        }
    }
    Err(runtime_err(format!("dependency '{}' is not required by this project", target)))
}

pub fn resolve_project_entry(project_dir: &Path) -> Result<PathBuf, DgmError> {
    let root = require_project_root(project_dir)?;
    let manifest = read_manifest(&root.join("dgm.toml"))?;
    let entry_name = manifest
        .package
        .main
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or("main.dgm");
    let entry = root.join(entry_name);
    if !entry.exists() {
        return Err(runtime_err(format!("project entry '{}' does not exist", entry.display())));
    }
    Ok(entry)
}

pub fn resolve_project_test_entry(project_dir: &Path) -> Result<PathBuf, DgmError> {
    let root = require_project_root(project_dir)?;
    let manifest = read_manifest(&root.join("dgm.toml"))?;
    let candidates = [
        manifest.package.test.as_deref(),
        Some("tests.dgm"),
        Some("test.dgm"),
    ];
    for candidate in candidates.into_iter().flatten() {
        let path = root.join(candidate);
        if path.exists() {
            return Ok(path);
        }
    }
    Err(runtime_err("project test entry not found".into()))
}

pub fn resolve_installed_package(base_dir: &Path, name: &str) -> Result<Option<PathBuf>, DgmError> {
    if !is_valid_package_name(name) {
        return Ok(None);
    }
    let Some(manifest_path) = find_nearest_install_manifest(base_dir) else {
        return Ok(None);
    };
    let install_manifest = read_install_manifest(&manifest_path)?;
    let Some(dep) = install_manifest.dependencies.get(name) else {
        return Ok(None);
    };
    let manifest_dir = manifest_path.parent().unwrap_or_else(|| Path::new("."));
    let package_root = manifest_dir.join(&dep.path);
    let package_root = if package_root.exists() {
        fs::canonicalize(&package_root).unwrap_or(package_root)
    } else {
        package_root
    };
    let entry = package_entry_path(&package_root)?;
    Ok(Some(entry))
}

fn require_project_root(path: &Path) -> Result<PathBuf, DgmError> {
    find_project_root(path).ok_or_else(|| runtime_err("dgm.toml not found in current project".into()))
}

fn acquire_project_lock(root: &Path) -> Result<ProjectLockGuard, DgmError> {
    let dgm_dir = root.join(".dgm");
    fs::create_dir_all(&dgm_dir).map_err(|e| runtime_err(format!("cannot create '{}': {}", dgm_dir.display(), e)))?;
    let lock_path = dgm_dir.join(".lock");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&lock_path)
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                runtime_err(format!("another DGM package command is already running in '{}'", root.display()))
            } else {
                runtime_err(format!("cannot create project lock '{}': {}", lock_path.display(), e))
            }
        })?;
    writeln!(
        file,
        "pid={}\nstarted_at={}",
        std::process::id(),
        unix_timestamp_now()
    )
    .map_err(|e| runtime_err(format!("cannot write project lock '{}': {}", lock_path.display(), e)))?;
    file.sync_all()
        .map_err(|e| runtime_err(format!("cannot sync project lock '{}': {}", lock_path.display(), e)))?;
    Ok(ProjectLockGuard { path: lock_path })
}

fn default_package_name(dir: &Path) -> String {
    let raw = dir
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("my_app")
        .to_ascii_lowercase()
        .replace('-', "_");
    if is_valid_package_name(&raw) {
        raw
    } else {
        "my_app".into()
    }
}

fn validate_manifest(manifest: &ProjectManifest) -> Result<(), DgmError> {
    if manifest.package.name.is_empty() {
        return Err(runtime_err("dgm.toml missing [package].name".into()));
    }
    if !is_valid_package_name(&manifest.package.name) {
        return Err(runtime_err(format!("invalid package name '{}'", manifest.package.name)));
    }
    if manifest.package.version.is_empty() {
        return Err(runtime_err("dgm.toml missing [package].version".into()));
    }
    Version::parse(&manifest.package.version)
        .map_err(|e| runtime_err(format!("invalid package version '{}': {}", manifest.package.version, e)))?;
    for (name, req) in &manifest.dependencies {
        validate_dependency_spec(name, req)?;
    }
    Ok(())
}

fn parse_add_spec(spec: &str) -> Result<(String, Option<String>), DgmError> {
    let trimmed = spec.trim();
    if trimmed.is_empty() {
        return Err(runtime_err("package name is required".into()));
    }
    let (name, req) = match trimmed.split_once('@') {
        Some((name, req)) => (name.trim(), Some(req.trim().to_string())),
        None => (trimmed, None),
    };
    if !is_valid_package_name(name) {
        return Err(runtime_err(format!("invalid package name '{}'", name)));
    }
    if let Some(req) = &req {
        validate_dependency_spec(name, req)?;
    }
    Ok((name.to_string(), req))
}

fn validate_dependency_spec(name: &str, req: &str) -> Result<(), DgmError> {
    if !is_valid_package_name(name) {
        return Err(runtime_err(format!("invalid package name '{}'", name)));
    }
    if req.starts_with('^') {
        VersionReq::parse(req).map_err(|e| runtime_err(format!("invalid version requirement '{}': {}", req, e)))?;
    } else {
        Version::parse(req).map_err(|e| runtime_err(format!("invalid version '{}': {}", req, e)))?;
    }
    Ok(())
}

fn is_valid_package_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn read_manifest(path: &Path) -> Result<ProjectManifest, DgmError> {
    let content = read_file_with_limit(path, MANIFEST_MAX_BYTES, "dgm.toml")?;
    toml::from_str(&content).map_err(|e| runtime_err(format!("invalid dgm.toml: {}", e)))
}

fn write_manifest(path: &Path, manifest: &ProjectManifest) -> Result<(), DgmError> {
    let text = toml::to_string_pretty(manifest).map_err(|e| runtime_err(format!("cannot encode dgm.toml: {}", e)))?;
    write_atomic_bytes(path, text.as_bytes())
}

fn read_lock_file(path: &Path) -> Result<LockFile, DgmError> {
    let content = read_file_with_limit(path, MANIFEST_MAX_BYTES, "dgm.lock")?;
    toml::from_str(&content).map_err(|e| runtime_err(format!("invalid dgm.lock: {}", e)))
}

fn write_lock_file(path: &Path, lock: &LockFile) -> Result<(), DgmError> {
    let text = toml::to_string_pretty(lock).map_err(|e| runtime_err(format!("cannot encode dgm.lock: {}", e)))?;
    write_atomic_bytes(path, text.as_bytes())
}

fn read_install_manifest(path: &Path) -> Result<InstallManifest, DgmError> {
    let content = read_file_with_limit(path, MANIFEST_MAX_BYTES, "install manifest")?;
    toml::from_str(&content).map_err(|e| runtime_err(format!("invalid install manifest '{}': {}", path.display(), e)))
}

fn write_install_manifest(path: &Path, deps: &BTreeMap<String, InstalledDependency>) -> Result<(), DgmError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| runtime_err(format!("cannot create '{}': {}", parent.display(), e)))?;
    }
    let text = toml::to_string_pretty(&InstallManifest { dependencies: deps.clone() })
        .map_err(|e| runtime_err(format!("cannot encode install manifest: {}", e)))?;
    write_atomic_bytes(path, text.as_bytes())
}

fn resolve_manifest_dependencies(
    dependencies: &BTreeMap<String, String>,
    offline: bool,
) -> Result<Vec<ResolvedPackage>, DgmError> {
    let mut resolved_by_key = HashMap::<String, ResolvedPackage>::new();
    let mut metadata_cache = HashMap::<String, RegistryMetadata>::new();
    let mut visiting = HashSet::<String>::new();
    let mut order = Vec::<String>::new();
    for (name, req) in dependencies {
        resolve_dependency(name, req, offline, &mut metadata_cache, &mut visiting, &mut resolved_by_key, &mut order)?;
    }
    Ok(order
        .into_iter()
        .filter_map(|key| resolved_by_key.get(&key).cloned())
        .collect())
}

fn resolve_dependency(
    name: &str,
    req: &str,
    offline: bool,
    metadata_cache: &mut HashMap<String, RegistryMetadata>,
    visiting: &mut HashSet<String>,
    resolved_by_key: &mut HashMap<String, ResolvedPackage>,
    order: &mut Vec<String>,
) -> Result<String, DgmError> {
    validate_dependency_spec(name, req)?;
    let registry = registry_base_url()?;
    let metadata = if let Some(metadata) = metadata_cache.get(name) {
        metadata.clone()
    } else {
        let metadata = fetch_registry_metadata(name, offline)?;
        metadata_cache.insert(name.to_string(), metadata.clone());
        metadata
    };
    if metadata.name != name {
        return Err(runtime_err(format!("registry metadata mismatch for '{}'", name)));
    }
    let package = pick_version(&metadata, req)?;
    let key = package_key(name, &package.version);
    if resolved_by_key.contains_key(&key) {
        return Ok(key);
    }
    if !visiting.insert(key.clone()) {
        return Err(runtime_err(format!("circular dependency detected at '{}@{}'", name, package.version)));
    }
    for (dep_name, dep_req) in &package.dependencies {
        resolve_dependency(dep_name, dep_req, offline, metadata_cache, visiting, resolved_by_key, order)?;
    }
    visiting.remove(&key);
    resolved_by_key.insert(
        key.clone(),
        ResolvedPackage {
            name: name.to_string(),
            version: package.version.clone(),
            source: package.url.clone(),
            hash: package.sha256.clone(),
            registry,
            fetched_at: unix_timestamp_now(),
            size: package.size.unwrap_or(0),
            dependencies: package.dependencies.clone(),
        },
    );
    order.push(key.clone());
    Ok(key)
}

fn pick_version(metadata: &RegistryMetadata, req: &str) -> Result<RegistryPackageVersion, DgmError> {
    let mut candidates = Vec::<(Version, RegistryPackageVersion)>::new();
    for version in &metadata.versions {
        let parsed = Version::parse(&version.version)
            .map_err(|e| runtime_err(format!("registry has invalid version '{}': {}", version.version, e)))?;
        if matches_version_req(&parsed, req)? {
            candidates.push((parsed, version.clone()));
        }
    }
    candidates.sort_by(|a, b| a.0.cmp(&b.0));
    candidates
        .into_iter()
        .last()
        .map(|(_, version)| version)
        .ok_or_else(|| runtime_err(format!("no version of '{}' matches '{}'", metadata.name, req)))
}

fn resolve_latest_version(name: &str, offline: bool) -> Result<String, DgmError> {
    let metadata = fetch_registry_metadata(name, offline)?;
    let mut versions = metadata
        .versions
        .iter()
        .map(|entry| Version::parse(&entry.version).map(|v| (v, entry.version.clone())))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| runtime_err(format!("registry has invalid version for '{}': {}", name, e)))?;
    versions.sort_by(|a, b| a.0.cmp(&b.0));
    versions
        .pop()
        .map(|(_, raw)| raw)
        .ok_or_else(|| runtime_err(format!("registry has no versions for '{}'", name)))
}

fn matches_version_req(version: &Version, req: &str) -> Result<bool, DgmError> {
    if req.starts_with('^') {
        let parsed = VersionReq::parse(req)
            .map_err(|e| runtime_err(format!("invalid version requirement '{}': {}", req, e)))?;
        Ok(parsed.matches(version))
    } else {
        Ok(version == &Version::parse(req).map_err(|e| runtime_err(format!("invalid version '{}': {}", req, e)))?)
    }
}

fn resolved_from_lock(lock: &LockFile) -> Result<Vec<ResolvedPackage>, DgmError> {
    let mut resolved = Vec::with_capacity(lock.package.len());
    for package in &lock.package {
        validate_dependency_spec(&package.name, &package.version)?;
        for (dep_name, dep_version) in &package.dependencies {
            validate_dependency_spec(dep_name, dep_version)?;
        }
        resolved.push(ResolvedPackage {
            name: package.name.clone(),
            version: package.version.clone(),
            source: package.source.clone(),
            hash: package.hash.clone(),
            registry: package.registry.clone().unwrap_or_else(|| "unknown".into()),
            fetched_at: package.fetched_at.unwrap_or(0),
            size: package.size,
            dependencies: package.dependencies.clone(),
        });
    }
    Ok(resolved)
}

fn build_lock_file(root_dependencies: &BTreeMap<String, String>, packages: &[ResolvedPackage]) -> LockFile {
    LockFile {
        version: LOCK_VERSION,
        root_dependencies: root_dependencies.clone(),
        package: packages
            .iter()
            .map(|pkg| LockedPackage {
                name: pkg.name.clone(),
                version: pkg.version.clone(),
                hash: pkg.hash.clone(),
                source: pkg.source.clone(),
                registry: Some(pkg.registry.clone()),
                fetched_at: Some(pkg.fetched_at),
                size: pkg.size,
                dependencies: pkg.dependencies.clone(),
            })
            .collect(),
    }
}

fn apply_install_state(
    root: &Path,
    root_dependencies: &BTreeMap<String, String>,
    resolved: &[ResolvedPackage],
    lock: Option<&LockFile>,
    clean_packages: bool,
    offline: bool,
) -> Result<(), DgmError> {
    let install_root = root.join(".dgm");
    let packages_root = install_root.join("packages");
    let staging_root = install_root.join("packages_tmp");
    if staging_root.exists() {
        fs::remove_dir_all(&staging_root)
            .map_err(|e| runtime_err(format!("cannot clean staging '{}': {}", staging_root.display(), e)))?;
    }
    fs::create_dir_all(&staging_root)
        .map_err(|e| runtime_err(format!("cannot create staging '{}': {}", staging_root.display(), e)))?;
    let stage_result = install_packages_into(&staging_root, resolved, offline);
    if let Err(err) = stage_result {
        let _ = fs::remove_dir_all(&staging_root);
        return Err(err);
    }
    if clean_packages && packages_root.exists() {
        fs::remove_dir_all(&packages_root)
            .map_err(|e| runtime_err(format!("cannot clean '{}': {}", packages_root.display(), e)))?;
    } else if packages_root.exists() {
        fs::remove_dir_all(&packages_root)
            .map_err(|e| runtime_err(format!("cannot replace '{}': {}", packages_root.display(), e)))?;
    }
    fs::rename(&staging_root, &packages_root)
        .map_err(|e| runtime_err(format!("cannot activate '{}': {}", packages_root.display(), e)))?;
    finalize_installed_packages(root, resolved, &packages_root)?;
    if let Some(lock) = lock {
        write_lock_file(&root.join("dgm.lock"), lock)?;
    }
    write_root_install_manifest(root, root_dependencies, resolved)?;
    Ok(())
}

fn restore_previous_state(root: &Path, old_lock: Option<&LockFile>, old_resolved: Option<&[ResolvedPackage]>) -> Result<(), DgmError> {
    match (old_lock, old_resolved) {
        (Some(lock), Some(resolved)) => apply_install_state(root, &lock.root_dependencies, resolved, Some(lock), true, true),
        _ => {
            let packages_root = root.join(".dgm/packages");
            if packages_root.exists() {
                fs::remove_dir_all(&packages_root)
                    .map_err(|e| runtime_err(format!("cannot clean '{}': {}", packages_root.display(), e)))?;
            }
            fs::create_dir_all(&packages_root)
                .map_err(|e| runtime_err(format!("cannot create '{}': {}", packages_root.display(), e)))?;
            let lock_path = root.join("dgm.lock");
            if lock_path.exists() {
                fs::remove_file(&lock_path)
                    .map_err(|e| runtime_err(format!("cannot remove '{}': {}", lock_path.display(), e)))?;
            }
            write_install_manifest(&root.join(".dgm/manifest.toml"), &BTreeMap::new())
        }
    }
}

fn normalize_update_targets(
    dependencies: &BTreeMap<String, String>,
    targets: &[String],
) -> Result<HashSet<String>, DgmError> {
    let mut set = HashSet::new();
    for target in targets {
        let name = target.trim();
        if name.is_empty() {
            continue;
        }
        if !dependencies.contains_key(name) {
            return Err(runtime_err(format!("dependency '{}' is not declared in dgm.toml", name)));
        }
        set.insert(name.to_string());
    }
    Ok(set)
}

fn build_effective_update_specs(
    manifest_dependencies: &BTreeMap<String, String>,
    old_root_versions: &BTreeMap<String, String>,
    targets: &HashSet<String>,
) -> BTreeMap<String, String> {
    let update_all = targets.is_empty();
    manifest_dependencies
        .iter()
        .map(|(name, spec)| {
            let effective = if update_all || targets.contains(name) {
                spec.clone()
            } else {
                old_root_versions.get(name).cloned().unwrap_or_else(|| spec.clone())
            };
            (name.clone(), effective)
        })
        .collect()
}

fn resolve_root_versions(
    root_dependencies: &BTreeMap<String, String>,
    packages: &[ResolvedPackage],
) -> Result<BTreeMap<String, String>, DgmError> {
    let mut versions = BTreeMap::new();
    for (name, spec) in root_dependencies {
        let package = resolve_root_dependency(name, spec, packages)?;
        versions.insert(name.clone(), package.version.clone());
    }
    Ok(versions)
}

fn collect_dependency_changes(
    manifest_dependencies: &BTreeMap<String, String>,
    old_versions: &BTreeMap<String, String>,
    new_versions: &BTreeMap<String, String>,
) -> Vec<DependencyChange> {
    let mut changes = Vec::new();
    for name in manifest_dependencies.keys() {
        let from = old_versions.get(name).cloned();
        let Some(to) = new_versions.get(name).cloned() else {
            continue;
        };
        if from.as_deref() == Some(to.as_str()) {
            continue;
        }
        let major_change = match (&from, Version::parse(&to)) {
            (Some(from), Ok(to_version)) => Version::parse(from)
                .map(|from_version| from_version.major != to_version.major)
                .unwrap_or(false),
            _ => false,
        };
        changes.push(DependencyChange { name: name.clone(), from, to, major_change });
    }
    changes
}

fn install_packages_into(packages_root: &Path, resolved: &[ResolvedPackage], offline: bool) -> Result<(), DgmError> {
    fs::create_dir_all(packages_root)
        .map_err(|e| runtime_err(format!("cannot create '{}': {}", packages_root.display(), e)))?;
    for package in resolved {
        install_single_package(packages_root, package, offline)?;
    }
    Ok(())
}

fn install_single_package(packages_root: &Path, package: &ResolvedPackage, offline: bool) -> Result<(), DgmError> {
    let archive_path = cached_archive_path(&package.name, &package.version, &package.hash)?;
    let bytes = if archive_path.exists() {
        read_file_bytes_with_limit(&archive_path, PACKAGE_MAX_BYTES, "package archive")?
    } else {
        let bytes = download_bytes(&package.source, PACKAGE_MAX_BYTES, "package archive", offline)?;
        let actual_hash = sha256_hex(&bytes);
        if !package.hash.eq_ignore_ascii_case(&actual_hash) {
            return Err(runtime_err(format!(
                "hash mismatch for '{}@{}': expected {}, got {}",
                package.name, package.version, package.hash, actual_hash
            )));
        }
        if package.size > 0 && package.size != bytes.len() as u64 {
            return Err(runtime_err(format!(
                "package size mismatch for '{}@{}': expected {}, got {}",
                package.name, package.version, package.size, bytes.len()
            )));
        }
        if let Some(parent) = archive_path.parent() {
            fs::create_dir_all(parent).map_err(|e| runtime_err(format!("cannot create '{}': {}", parent.display(), e)))?;
        }
        fs::write(&archive_path, &bytes).map_err(|e| runtime_err(format!("cannot write '{}': {}", archive_path.display(), e)))?;
        bytes
    };
    let actual_hash = sha256_hex(&bytes);
    if !package.hash.eq_ignore_ascii_case(&actual_hash) {
        return Err(runtime_err(format!(
            "hash mismatch for '{}@{}': expected {}, got {}",
            package.name, package.version, package.hash, actual_hash
        )));
    }
    let dest = packages_root.join(&package.name).join(&package.version);
    extract_package_archive(&bytes, &dest)?;
    Ok(())
}

fn write_root_install_manifest(
    root: &Path,
    root_dependencies: &BTreeMap<String, String>,
    packages: &[ResolvedPackage],
) -> Result<(), DgmError> {
    let root_manifest_path = root.join(".dgm/manifest.toml");
    let mut package_map = HashMap::<String, &ResolvedPackage>::new();
    for package in packages {
        package_map.insert(package_key(&package.name, &package.version), package);
    }
    let mut deps = BTreeMap::<String, InstalledDependency>::new();
    let manifest_dir = root.join(".dgm");
    for (name, spec) in root_dependencies {
        let resolved = resolve_root_dependency(name, spec, packages)?;
        let rel = relative_path(&manifest_dir, &root.join(".dgm/packages").join(&resolved.name).join(&resolved.version))?;
        deps.insert(
            name.clone(),
            InstalledDependency {
                version: resolved.version.clone(),
                path: rel,
            },
        );
    }
    write_install_manifest(&root_manifest_path, &deps)
}

fn finalize_installed_packages(root: &Path, resolved: &[ResolvedPackage], packages_root: &Path) -> Result<(), DgmError> {
    for package in resolved {
        let package_root = packages_root.join(&package.name).join(&package.version);
        write_package_install_manifest(&package_root, package, packages_root)?;
        let package_root = if package_root.exists() {
            fs::canonicalize(&package_root).unwrap_or(package_root)
        } else {
            package_root
        };
        if !package_root.starts_with(root.join(".dgm/packages")) {
            return Err(runtime_err(format!("package install escaped sandbox for '{}@{}'", package.name, package.version)));
        }
    }
    Ok(())
}

fn write_package_install_manifest(
    package_root: &Path,
    package: &ResolvedPackage,
    packages_root: &Path,
) -> Result<(), DgmError> {
    let manifest_path = package_root.join(".dgm/manifest.toml");
    let mut deps = BTreeMap::<String, InstalledDependency>::new();
    for (dep_name, dep_version) in &package.dependencies {
        let dep_root = packages_root.join(dep_name).join(dep_version);
        let rel = relative_path(&package_root.join(".dgm"), &dep_root)?;
        deps.insert(
            dep_name.clone(),
            InstalledDependency {
                version: dep_version.clone(),
                path: rel,
            },
        );
    }
    write_install_manifest(&manifest_path, &deps)?;
    let package_manifest = package_root.join("dgm.toml");
    if !package_manifest.exists() {
        let mut manifest = ProjectManifest::default();
        manifest.package.name = package.name.clone();
        manifest.package.version = package.version.clone();
        write_manifest(&package_manifest, &manifest)?;
    }
    if !package_entry_path(package_root)?.exists() {
        return Err(runtime_err(format!(
            "package '{}@{}' missing entry file",
            package.name, package.version
        )));
    }
    Ok(())
}

fn resolve_root_dependency<'a>(
    name: &str,
    spec: &str,
    packages: &'a [ResolvedPackage],
) -> Result<&'a ResolvedPackage, DgmError> {
    let mut matches = packages
        .iter()
        .filter(|pkg| pkg.name == name)
        .collect::<Vec<_>>();
    matches.sort_by(|a, b| Version::parse(&a.version).unwrap().cmp(&Version::parse(&b.version).unwrap()));
    matches
        .into_iter()
        .rev()
        .find(|pkg| matches_version_req(&Version::parse(&pkg.version).unwrap(), spec).unwrap_or(false))
        .ok_or_else(|| runtime_err(format!("root dependency '{}@{}' was not resolved", name, spec)))
}

fn fetch_registry_metadata(name: &str, offline: bool) -> Result<RegistryMetadata, DgmError> {
    let cache_path = cache_root()?.join("metadata").join(format!("{}.json", name));
    let bytes = if offline {
        if !cache_path.exists() {
            return Err(runtime_err(format!("offline metadata cache miss for '{}'", name)));
        }
        read_file_bytes_with_limit(&cache_path, METADATA_MAX_BYTES, "registry metadata")?
    } else {
        let url = format!("{}/packages/{}.json", registry_base_url()?, name);
        let bytes = download_bytes(&url, METADATA_MAX_BYTES, "registry metadata", false)?;
        if let Some(parent) = cache_path.parent() {
            fs::create_dir_all(parent).map_err(|e| runtime_err(format!("cannot create '{}': {}", parent.display(), e)))?;
        }
        fs::write(&cache_path, &bytes).map_err(|e| runtime_err(format!("cannot write '{}': {}", cache_path.display(), e)))?;
        bytes
    };
    let content = String::from_utf8(bytes).map_err(|e| runtime_err(format!("registry metadata is not valid utf-8: {}", e)))?;
    let json = crate::stdlib::json_mod::parse_json_limited(&content, "registry metadata")?;
    serde_json::from_value(json).map_err(|e| runtime_err(format!("invalid registry metadata for '{}': {}", name, e)))
}

fn download_bytes(url: &str, limit: usize, context: &str, offline: bool) -> Result<Vec<u8>, DgmError> {
    if offline {
        return Err(runtime_err(format!("offline mode cannot fetch {}", url)));
    }
    let agent = ureq::AgentBuilder::new()
        .timeout(Duration::from_millis(DOWNLOAD_TIMEOUT_MS))
        .build();
    let mut last_error = None;
    for _ in 0..DOWNLOAD_RETRIES {
        let response = match agent.get(url).call() {
            Ok(response) => response,
            Err(err) => {
                last_error = Some(err.to_string());
                continue;
            }
        };
        let mut reader = response.into_reader();
        let mut buf = Vec::new();
        let mut chunk = [0u8; 8192];
        loop {
            let read = reader.read(&mut chunk).map_err(|e| runtime_err(format!("{} read failed: {}", context, e)))?;
            if read == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..read]);
            if buf.len() > limit {
                return Err(runtime_err(format!("{} exceeds {} bytes", context, limit)));
            }
        }
        return Ok(buf);
    }
    Err(runtime_err(format!(
        "cannot fetch {}: {}",
        context,
        last_error.unwrap_or_else(|| "unknown error".into())
    )))
}

fn extract_package_archive(bytes: &[u8], dest: &Path) -> Result<(), DgmError> {
    let temp_parent = dest.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(temp_parent).map_err(|e| runtime_err(format!("cannot create '{}': {}", temp_parent.display(), e)))?;
    let temp_dir = temp_parent.join(format!(
        ".tmp-{}-{}",
        dest.file_name().and_then(|s| s.to_str()).unwrap_or("pkg"),
        std::process::id()
    ));
    if temp_dir.exists() {
        fs::remove_dir_all(&temp_dir).map_err(|e| runtime_err(format!("cannot clear '{}': {}", temp_dir.display(), e)))?;
    }
    fs::create_dir_all(&temp_dir).map_err(|e| runtime_err(format!("cannot create '{}': {}", temp_dir.display(), e)))?;

    let cursor = Cursor::new(bytes);
    let decoder = flate2::read::GzDecoder::new(cursor);
    let mut archive = tar::Archive::new(decoder);
    let mut extracted_bytes = 0usize;
    for entry_result in archive.entries().map_err(|e| runtime_err(format!("invalid archive: {}", e)))? {
        let mut entry = entry_result.map_err(|e| runtime_err(format!("invalid archive entry: {}", e)))?;
        let entry_type = entry.header().entry_type();
        if entry_type.is_symlink() || entry_type.is_hard_link() {
            return Err(runtime_err("package archive contains unsupported link entry".into()));
        }
        let raw_path = entry
            .path()
            .map_err(|e| runtime_err(format!("invalid archive path: {}", e)))?
            .into_owned();
        validate_archive_path(&raw_path)?;
        let output_path = temp_dir.join(&raw_path);
        if entry_type.is_dir() {
            fs::create_dir_all(&output_path).map_err(|e| runtime_err(format!("cannot create '{}': {}", output_path.display(), e)))?;
            continue;
        }
        if !entry_type.is_file() {
            return Err(runtime_err("package archive contains unsupported entry type".into()));
        }
        let size = entry.size() as usize;
        extracted_bytes = extracted_bytes.saturating_add(size);
        if extracted_bytes > PACKAGE_MAX_BYTES {
            return Err(runtime_err(format!("package archive exceeds {} bytes when extracted", PACKAGE_MAX_BYTES)));
        }
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent).map_err(|e| runtime_err(format!("cannot create '{}': {}", parent.display(), e)))?;
        }
        let mut file = fs::File::create(&output_path).map_err(|e| runtime_err(format!("cannot create '{}': {}", output_path.display(), e)))?;
        std::io::copy(&mut entry, &mut file).map_err(|e| runtime_err(format!("cannot unpack '{}': {}", output_path.display(), e)))?;
        file.flush().map_err(|e| runtime_err(format!("cannot flush '{}': {}", output_path.display(), e)))?;
    }

    if dest.exists() {
        fs::remove_dir_all(dest).map_err(|e| runtime_err(format!("cannot replace '{}': {}", dest.display(), e)))?;
    }
    let normalized_root = normalize_extracted_root(&temp_dir)?;
    fs::create_dir_all(dest.parent().unwrap_or_else(|| Path::new(".")))
        .map_err(|e| runtime_err(format!("cannot create '{}': {}", dest.parent().unwrap_or_else(|| Path::new(".")).display(), e)))?;
    fs::rename(&normalized_root, dest).map_err(|e| runtime_err(format!("cannot move package into '{}': {}", dest.display(), e)))?;
    if temp_dir.exists() {
        let _ = fs::remove_dir_all(&temp_dir);
    }
    Ok(())
}

fn validate_archive_path(path: &Path) -> Result<(), DgmError> {
    if path.is_absolute() {
        return Err(runtime_err("package archive contains absolute path".into()));
    }
    for component in path.components() {
        match component {
            Component::Normal(_) => {}
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(runtime_err("package archive contains unsafe path".into()))
            }
        }
    }
    Ok(())
}

fn normalize_extracted_root(temp_dir: &Path) -> Result<PathBuf, DgmError> {
    let entries = fs::read_dir(temp_dir)
        .map_err(|e| runtime_err(format!("cannot read '{}': {}", temp_dir.display(), e)))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| runtime_err(format!("cannot read '{}': {}", temp_dir.display(), e)))?;
    if entries.len() == 1 {
        let entry = &entries[0];
        let path = entry.path();
        if path.is_dir() {
            return Ok(path);
        }
    }
    Ok(temp_dir.to_path_buf())
}

fn package_entry_path(package_root: &Path) -> Result<PathBuf, DgmError> {
    let manifest_path = package_root.join("dgm.toml");
    let entry = if manifest_path.exists() {
        let manifest = read_manifest(&manifest_path)?;
        manifest.package.main.unwrap_or_else(|| "main.dgm".into())
    } else {
        "main.dgm".into()
    };
    let path = package_root.join(entry);
    if !path.exists() {
        return Err(runtime_err(format!("package entry '{}' does not exist", path.display())));
    }
    Ok(path)
}

fn registry_base_url() -> Result<String, DgmError> {
    let base = std::env::var("DGM_REGISTRY_URL").unwrap_or_else(|_| "https://registry.dgm-lang.org".into());
    let trimmed = base.trim_end_matches('/').to_string();
    if trimmed.is_empty() {
        return Err(runtime_err("DGM_REGISTRY_URL is empty".into()));
    }
    Ok(trimmed)
}

fn cache_root() -> Result<PathBuf, DgmError> {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::current_dir().ok())
        .ok_or_else(|| runtime_err("cannot determine home directory".into()))?;
    Ok(home.join(".dgm/cache"))
}

fn cached_archive_path(name: &str, version: &str, hash: &str) -> Result<PathBuf, DgmError> {
    Ok(cache_root()?.join("archives").join(format!("{}-{}-{}.tar.gz", name, version, &hash[..hash.len().min(16)])))
}

fn package_key(name: &str, version: &str) -> String {
    format!("{}@{}", name, version)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn unix_timestamp_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn read_file_with_limit(path: &Path, limit: usize, context: &str) -> Result<String, DgmError> {
    let bytes = read_file_bytes_with_limit(path, limit, context)?;
    String::from_utf8(bytes).map_err(|e| runtime_err(format!("{} '{}' is not valid utf-8: {}", context, path.display(), e)))
}

fn write_atomic_bytes(path: &Path, bytes: &[u8]) -> Result<(), DgmError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|e| runtime_err(format!("cannot create '{}': {}", parent.display(), e)))?;
    let temp_path = path.with_extension(format!(
        "{}.tmp{}",
        path.extension().and_then(|ext| ext.to_str()).map(|ext| format!(".{}", ext)).unwrap_or_default(),
        std::process::id()
    ));
    let mut file = File::create(&temp_path)
        .map_err(|e| runtime_err(format!("cannot write '{}': {}", temp_path.display(), e)))?;
    file.write_all(bytes)
        .map_err(|e| runtime_err(format!("cannot write '{}': {}", temp_path.display(), e)))?;
    file.sync_all()
        .map_err(|e| runtime_err(format!("cannot sync '{}': {}", temp_path.display(), e)))?;
    fs::rename(&temp_path, path)
        .map_err(|e| runtime_err(format!("cannot replace '{}': {}", path.display(), e)))?;
    if let Ok(dir) = File::open(parent) {
        let _ = dir.sync_all();
    }
    Ok(())
}

fn read_file_bytes_with_limit(path: &Path, limit: usize, context: &str) -> Result<Vec<u8>, DgmError> {
    let metadata = fs::metadata(path).map_err(|e| runtime_err(format!("cannot read '{}': {}", path.display(), e)))?;
    if metadata.len() as usize > limit {
        return Err(runtime_err(format!("{} '{}' exceeds {} bytes", context, path.display(), limit)));
    }
    fs::read(path).map_err(|e| runtime_err(format!("cannot read '{}': {}", path.display(), e)))
}

fn relative_path(from_dir: &Path, to_path: &Path) -> Result<String, DgmError> {
    let from_components = from_dir.components().collect::<Vec<_>>();
    let to_components = to_path.components().collect::<Vec<_>>();
    let mut same = 0usize;
    while same < from_components.len() && same < to_components.len() && from_components[same] == to_components[same] {
        same += 1;
    }
    let mut result = PathBuf::new();
    for _ in same..from_components.len() {
        result.push("..");
    }
    for component in &to_components[same..] {
        match component {
            Component::Normal(part) => result.push(part),
            _ => return Err(runtime_err("cannot compute relative install path".into())),
        }
    }
    Ok(result.to_string_lossy().to_string())
}

fn find_nearest_install_manifest(start: &Path) -> Option<PathBuf> {
    let mut current = if start.is_dir() {
        start.to_path_buf()
    } else {
        start.parent()?.to_path_buf()
    };
    loop {
        let candidate = current.join(".dgm/manifest.toml");
        if candidate.exists() {
            return Some(candidate);
        }
        if !current.pop() {
            return None;
        }
    }
}

fn runtime_err(msg: String) -> DgmError {
    DgmError::RuntimeError { msg }
}
