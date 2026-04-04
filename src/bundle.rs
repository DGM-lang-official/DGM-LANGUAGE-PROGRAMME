use crate::ast::{BindingPattern, Expr, MatchPattern, Stmt, StmtKind};
use crate::error::DgmError;
use crate::interpreter::{runtime_config, RuntimeConfig};
use crate::package_manager::{self, LockFile, ProjectManifest};
use crate::vm;
use memmap2::Mmap;
use semver::Version;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const BUNDLE_MAGIC: &[u8; 8] = b"DGMBNDL1";
const BUNDLE_FORMAT_VERSION_V1: u32 = 1;
const BUNDLE_FORMAT_VERSION: u32 = 2;
const BUNDLE_HEADER_SIZE_V1: usize = 60;
const BUNDLE_HEADER_SIZE: usize = 68;
const BUNDLE_MANIFEST_LIMIT: usize = 4 * 1024 * 1024;
const BUNDLE_INDEX_LIMIT: usize = 8 * 1024 * 1024;
const BUNDLE_MODULES_LIMIT: usize = 64 * 1024 * 1024;
const BUNDLE_TOTAL_LIMIT: u64 = 100 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildMode {
    Debug,
    Release,
}

impl BuildMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Release => "release",
        }
    }
}

#[derive(Debug, Clone)]
pub struct BuildOptions {
    pub mode: BuildMode,
    pub output: Option<PathBuf>,
}

impl Default for BuildOptions {
    fn default() -> Self {
        Self {
            mode: BuildMode::Debug,
            output: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BundleDebugSource {
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub line_count: usize,
    #[serde(default)]
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleRuntimeConfig {
    pub max_steps: u64,
    pub max_call_depth: usize,
    pub max_heap: usize,
    pub max_threads: usize,
    #[serde(default = "default_bundle_wall_time_ms")]
    pub max_wall_time_ms: u64,
    #[serde(default = "default_bundle_max_open_sockets")]
    pub max_open_sockets: usize,
    #[serde(default = "default_bundle_max_concurrent_requests")]
    pub max_concurrent_requests: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleManifest {
    #[serde(default = "default_bundle_format_version")]
    pub bundle_format_version: u32,
    pub entry: String,
    #[serde(default)]
    pub modules: BTreeMap<String, usize>,
    pub runtime: BundleRuntimeConfig,
    #[serde(default)]
    pub dgm_version: String,
    #[serde(default)]
    pub build_mode: String,
    #[serde(default)]
    pub vm_version: u32,
    #[serde(default)]
    pub debug_sources: BTreeMap<String, BundleDebugSource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleModule {
    #[serde(default)]
    pub id: usize,
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub logical_path: String,
    #[serde(default)]
    pub imports: BTreeMap<String, String>,
    #[serde(default)]
    pub stmts: Vec<Stmt>,
    #[serde(default)]
    pub bytecode: Option<vm::BytecodeModule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BundleBlobV1 {
    modules: Vec<BundleModule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleModuleIndex {
    #[serde(default)]
    pub id: usize,
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub logical_path: String,
    #[serde(default)]
    pub imports: BTreeMap<String, String>,
    #[serde(default)]
    pub offset: usize,
    #[serde(default)]
    pub len: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BundleIndexBlob {
    modules: Vec<BundleModuleIndex>,
}

#[derive(Debug, Clone, Default)]
pub struct BundleRuntimeStats {
    pub load_time_ns: u128,
    pub entry_decode_time_ns: u128,
    pub total_decode_time_ns: u128,
    pub loaded_module_count: usize,
    pub peak_loaded_module_bytes: usize,
    pub memory_mapped: bool,
}

#[derive(Debug)]
enum BundleBytes {
    Heap(Arc<Vec<u8>>),
    Mmap(Arc<Mmap>),
}

impl BundleBytes {
    fn len(&self) -> usize {
        match self {
            Self::Heap(bytes) => bytes.len(),
            Self::Mmap(bytes) => bytes.len(),
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn slice(&self, start: usize, end: usize) -> &[u8] {
        match self {
            Self::Heap(bytes) => &bytes[start..end],
            Self::Mmap(bytes) => &bytes[start..end],
        }
    }
}

#[derive(Debug)]
struct BundleStorage {
    module_bytes: BundleBytes,
    module_base_offset: usize,
    loaded_modules: Mutex<HashMap<String, BundleModule>>,
    stats: Mutex<BundleRuntimeStats>,
}

#[derive(Debug, Clone)]
pub struct BundleImage {
    manifest: BundleManifest,
    module_index: HashMap<String, BundleModuleIndex>,
    storage: Arc<BundleStorage>,
    checksum: String,
    bundle_size: u64,
}

#[derive(Debug, Clone)]
pub struct BundleInspection {
    pub manifest: BundleManifest,
    pub checksum: String,
    pub module_count: usize,
    pub bundle_size: u64,
}

impl BundleImage {
    pub fn manifest(&self) -> &BundleManifest {
        &self.manifest
    }

    pub fn entry_module(&self) -> Result<BundleModule, DgmError> {
        self.load_module(&self.manifest.entry)?
            .ok_or_else(|| runtime_err(format!("bundle entry '{}' is missing", self.manifest.entry)))
    }

    pub fn module_index(&self, key: &str) -> Option<&BundleModuleIndex> {
        self.module_index.get(key)
    }

    pub fn checksum(&self) -> &str {
        &self.checksum
    }

    pub fn bundle_size(&self) -> u64 {
        self.bundle_size
    }

    pub fn runtime_stats(&self) -> BundleRuntimeStats {
        self.storage.stats.lock().unwrap().clone()
    }

    pub fn resolve_import_key(&self, from_key: Option<&str>, raw_import: &str) -> Option<String> {
        if let Some(target_key) = from_key
            .and_then(|key| self.module_index.get(key))
            .and_then(|module| module.imports.get(raw_import))
        {
            return Some(target_key.clone());
        }
        self.module_index.get(raw_import).map(|module| module.key.clone())
    }

    pub fn load_module(&self, key: &str) -> Result<Option<BundleModule>, DgmError> {
        if let Some(module) = self.storage.loaded_modules.lock().unwrap().get(key).cloned() {
            return Ok(Some(module));
        }
        let Some(index) = self.module_index.get(key).cloned() else {
            return Ok(None);
        };
        if index.len == 0 && self.storage.module_bytes.is_empty() {
            return Ok(None);
        }
        let started = Instant::now();
        let end = index
            .offset
            .checked_add(index.len)
            .ok_or_else(|| runtime_err(format!("bundle module '{}' has invalid length", key)))?;
        if end > self.storage.module_bytes.len().saturating_sub(self.storage.module_base_offset) {
            return Err(runtime_err(format!("bundle module '{}' is out of bounds", key)));
        }
        let start = self.storage.module_base_offset + index.offset;
        let final_end = self.storage.module_base_offset + end;
        let module: BundleModule = serde_json::from_slice(self.storage.module_bytes.slice(start, final_end))
            .map_err(|e| runtime_err(format!("bundle module '{}' is invalid: {}", key, e)))?;
        self.storage.loaded_modules.lock().unwrap().insert(key.to_string(), module.clone());
        let mut stats = self.storage.stats.lock().unwrap();
        stats.loaded_module_count = self.storage.loaded_modules.lock().unwrap().len();
        let decode_ns = started.elapsed().as_nanos();
        stats.total_decode_time_ns = stats.total_decode_time_ns.saturating_add(decode_ns);
        if key == self.manifest.entry && stats.entry_decode_time_ns == 0 {
            stats.entry_decode_time_ns = decode_ns;
        }
        let current_loaded_bytes = self
            .storage
            .loaded_modules
            .lock()
            .unwrap()
            .keys()
            .filter_map(|loaded_key| self.module_index.get(loaded_key))
            .map(|loaded| loaded.len)
            .sum::<usize>();
        stats.peak_loaded_module_bytes = stats.peak_loaded_module_bytes.max(current_loaded_bytes);
        drop(stats);
        Ok(Some(module))
    }
}

struct BundleBuilder {
    root: PathBuf,
    modules: Vec<BundleModule>,
    debug_sources: BTreeMap<String, BundleDebugSource>,
    seen: HashMap<String, String>,
    visiting: HashSet<String>,
}

impl BundleBuilder {
    fn new(root: PathBuf) -> Self {
        Self {
            root,
            modules: Vec::new(),
            debug_sources: BTreeMap::new(),
            seen: HashMap::new(),
            visiting: HashSet::new(),
        }
    }

    fn collect_module(&mut self, path: &Path) -> Result<String, DgmError> {
        let canonical_path = canonical_or_original(path)?;
        let key = module_key(&self.root, &canonical_path)?;
        if let Some(existing) = self.seen.get(&key) {
            return Ok(existing.clone());
        }
        if !self.visiting.insert(key.clone()) {
            return Err(DgmError::ImportError {
                msg: format!("circular import detected while bundling '{}'", canonical_path.display()),
            });
        }
        let source = fs::read_to_string(&canonical_path).map_err(|e| {
            DgmError::RuntimeError {
                msg: format!("cannot read '{}' while building bundle: {}", canonical_path.display(), e),
            }
        })?;
        let stmts = crate::parse_source(&source)?;
        let mut imports = BTreeMap::new();
        for import_name in collect_imports(&stmts) {
            if is_stdlib_import(&import_name) {
                continue;
            }
            let target = resolve_build_import(&canonical_path, &import_name)?;
            let target_key = self.collect_module(&target)?;
            imports.insert(import_name, target_key);
        }
        self.visiting.remove(&key);
        let id = self.modules.len();
        let bytecode = vm::compile_module(&stmts, Some(key.clone()));
        let module = BundleModule {
            id,
            key: key.clone(),
            logical_path: logical_path(&self.root, &canonical_path)?,
            imports,
            stmts,
            bytecode: Some(bytecode),
        };
        self.debug_sources.insert(
            key.clone(),
            BundleDebugSource {
                path: module.logical_path.clone(),
                line_count: source.lines().count(),
                source: source.clone(),
            },
        );
        self.modules.push(module);
        self.seen.insert(key.clone(), key.clone());
        Ok(key)
    }
}

pub fn build_project(project_dir: &Path, mode: BuildMode) -> Result<PathBuf, DgmError> {
    build_project_with_options(project_dir, &BuildOptions { mode, output: None })
}

pub fn build_project_with_options(project_dir: &Path, options: &BuildOptions) -> Result<PathBuf, DgmError> {
    let root = package_manager::find_project_root(project_dir)
        .ok_or_else(|| runtime_err("dgm.toml not found in current project".into()))?;
    let manifest_path = root.join("dgm.toml");
    let lock_path = root.join("dgm.lock");
    let manifest = read_project_manifest(&manifest_path)?;
    if lock_path.exists() {
        let lock = read_lock_file(&lock_path)?;
        if lock.root_dependencies != manifest.dependencies {
            return Err(runtime_err("build requires dgm.lock to match dgm.toml; run dgm install --frozen first".into()));
        }
    } else if !manifest.dependencies.is_empty() {
        return Err(runtime_err("build requires dgm.lock for projects with dependencies".into()));
    }
    let entry_path = package_manager::resolve_project_entry(&root)?;
    let root = canonical_or_original(&root)?;
    let mut builder = BundleBuilder::new(root.clone());
    let entry_key = builder.collect_module(&entry_path)?;
    builder.modules.sort_by(|a, b| a.key.cmp(&b.key));
    for (id, module) in builder.modules.iter_mut().enumerate() {
        module.id = id;
    }
    let runtime = default_bundle_runtime(runtime_config());
    let manifest = BundleManifest {
        bundle_format_version: BUNDLE_FORMAT_VERSION,
        entry: entry_key,
        modules: builder.modules.iter().map(|module| (module.key.clone(), module.id)).collect(),
        runtime,
        dgm_version: env!("CARGO_PKG_VERSION").into(),
        build_mode: options.mode.as_str().into(),
        vm_version: 1,
        debug_sources: if options.mode == BuildMode::Debug {
            builder.debug_sources
        } else {
            BTreeMap::new()
        },
    };
    if options.mode == BuildMode::Release {
        for module in builder.modules.iter_mut() {
            module.stmts.clear();
        }
    }
    let (index, module_bytes) = encode_module_blob(&builder.modules)?;
    let output_path = options
        .output
        .as_ref()
        .map(|path| if path.is_absolute() { path.clone() } else { root.join(path) })
        .unwrap_or_else(|| root.join("build").join("bundle.dgm"));
    write_bundle_file(&output_path, &manifest, &index, &module_bytes)?;
    Ok(output_path)
}

pub fn load_bundle(path: &Path) -> Result<BundleImage, DgmError> {
    let started = Instant::now();
    let metadata = fs::metadata(path)
        .map_err(|e| runtime_err(format!("cannot read bundle '{}': {}", path.display(), e)))?;
    if metadata.len() > BUNDLE_TOTAL_LIMIT {
        return Err(runtime_err(format!(
            "bundle '{}' exceeds {} bytes",
            path.display(),
            BUNDLE_TOTAL_LIMIT
        )));
    }
    let file = File::open(path)
        .map_err(|e| runtime_err(format!("cannot read bundle '{}': {}", path.display(), e)))?;
    let mmap = unsafe { Mmap::map(&file).ok() };
    let heap_bytes;
    let bytes = if let Some(mapped) = mmap.as_ref() {
        mapped.as_ref()
    } else {
        heap_bytes = fs::read(path)
            .map_err(|e| runtime_err(format!("cannot read bundle '{}': {}", path.display(), e)))?;
        heap_bytes.as_slice()
    };
    let is_memory_mapped = mmap.is_some();
    if bytes.len() < BUNDLE_HEADER_SIZE_V1 {
        return Err(runtime_err(format!("bundle '{}' is truncated", path.display())));
    }
    if &bytes[..8] != BUNDLE_MAGIC {
        return Err(runtime_err(format!("bundle '{}' has invalid header", path.display())));
    }
    let header = read_bundle_header(path, &bytes)?;
    let manifest_len = header.manifest_len;
    let index_len = header.index_len;
    let modules_len = header.modules_len;
    if manifest_len > BUNDLE_MANIFEST_LIMIT {
        return Err(runtime_err(format!("bundle '{}' manifest exceeds limit", path.display())));
    }
    if index_len > BUNDLE_INDEX_LIMIT {
        return Err(runtime_err(format!("bundle '{}' index exceeds limit", path.display())));
    }
    if modules_len > BUNDLE_MODULES_LIMIT {
        return Err(runtime_err(format!("bundle '{}' module blob exceeds limit", path.display())));
    }
    let expected = header.header_size
        .checked_add(manifest_len)
        .and_then(|value| value.checked_add(index_len))
        .and_then(|value| value.checked_add(modules_len))
        .ok_or_else(|| runtime_err(format!("bundle '{}' lengths are invalid", path.display())))?;
    if bytes.len() != expected {
        return Err(runtime_err(format!("bundle '{}' is corrupt", path.display())));
    }
    let actual_checksum = bundle_checksum(&bytes)?;
    if actual_checksum.as_slice() != header.checksum.as_slice() {
        return Err(runtime_err(format!("bundle '{}' checksum mismatch", path.display())));
    }
    let manifest_start = header.header_size;
    let manifest_end = manifest_start + manifest_len;
    let index_end = manifest_end + index_len;
    let manifest_bytes = &bytes[manifest_start..manifest_end];
    let index_bytes = &bytes[manifest_end..index_end];
    let manifest: BundleManifest = serde_json::from_slice(manifest_bytes)
        .map_err(|e| runtime_err(format!("bundle '{}' manifest is invalid: {}", path.display(), e)))?;
    validate_bundle_manifest(path, &manifest)?;
    let module_slice = &bytes[index_end..expected];
    let (module_index, loaded_modules, peak_loaded_module_bytes) = match header.version {
        BUNDLE_FORMAT_VERSION_V1 => load_v1_modules(path, module_slice)?,
        BUNDLE_FORMAT_VERSION => load_v2_index(path, index_bytes, module_slice)?,
        _ => unreachable!(),
    };
    if !module_index.contains_key(&manifest.entry) {
        return Err(runtime_err(format!("bundle '{}' is missing entry module '{}'", path.display(), manifest.entry)));
    }
    for module_key in manifest.modules.keys() {
        if !module_index.contains_key(module_key) {
            return Err(runtime_err(format!("bundle '{}' is missing module '{}'", path.display(), module_key)));
        }
    }
    let initial_loaded_module_count = match header.version {
        BUNDLE_FORMAT_VERSION_V1 => manifest.modules.len(),
        _ => 0,
    };
    Ok(BundleImage {
        manifest,
        module_index,
        storage: Arc::new(BundleStorage {
            module_bytes: if let Some(mapped) = mmap {
                BundleBytes::Mmap(Arc::new(mapped))
            } else {
                BundleBytes::Heap(Arc::new(module_slice.to_vec()))
            },
            module_base_offset: if is_memory_mapped { index_end } else { 0 },
            loaded_modules: Mutex::new(loaded_modules),
            stats: Mutex::new(BundleRuntimeStats {
                load_time_ns: started.elapsed().as_nanos(),
                entry_decode_time_ns: 0,
                total_decode_time_ns: 0,
                loaded_module_count: initial_loaded_module_count,
                peak_loaded_module_bytes,
                memory_mapped: is_memory_mapped,
            }),
        }),
        checksum: hex_checksum(&header.checksum),
        bundle_size: metadata.len(),
    })
}

pub(crate) fn runtime_config_from_bundle(config: &BundleRuntimeConfig) -> RuntimeConfig {
    RuntimeConfig {
        max_steps: config.max_steps,
        max_call_depth: config.max_call_depth,
        max_heap_bytes: config.max_heap,
        max_threads: config.max_threads.max(1),
        max_wall_time_ms: config.max_wall_time_ms.max(1),
        max_open_sockets: config.max_open_sockets.max(1),
        max_concurrent_requests: config.max_concurrent_requests.max(1),
    }
}

fn write_bundle_file(path: &Path, manifest: &BundleManifest, index: &BundleIndexBlob, module_bytes: &[u8]) -> Result<(), DgmError> {
    let manifest_bytes = serde_json::to_vec(manifest)
        .map_err(|e| runtime_err(format!("cannot encode bundle manifest: {}", e)))?;
    let index_bytes = serde_json::to_vec(index)
        .map_err(|e| runtime_err(format!("cannot encode bundle index: {}", e)))?;
    let checksum = payload_checksum(
        BUNDLE_FORMAT_VERSION,
        &manifest_bytes,
        Some(&index_bytes),
        module_bytes,
    );
    let mut bytes = Vec::with_capacity(BUNDLE_HEADER_SIZE + manifest_bytes.len() + index_bytes.len() + module_bytes.len());
    bytes.extend_from_slice(BUNDLE_MAGIC);
    bytes.extend_from_slice(&BUNDLE_FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&(manifest_bytes.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&(index_bytes.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&(module_bytes.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&checksum);
    bytes.extend_from_slice(&manifest_bytes);
    bytes.extend_from_slice(&index_bytes);
    bytes.extend_from_slice(&module_bytes);
    write_atomic_bytes(path, &bytes)
}

pub fn inspect_bundle(path: &Path) -> Result<BundleInspection, DgmError> {
    let bundle = load_bundle(path)?;
    Ok(BundleInspection {
        manifest: bundle.manifest().clone(),
        checksum: bundle.checksum().into(),
        module_count: bundle.manifest().modules.len(),
        bundle_size: bundle.bundle_size(),
    })
}

pub fn is_bundle_file(path: &Path) -> Result<bool, DgmError> {
    let mut file = File::open(path)
        .map_err(|e| runtime_err(format!("cannot read '{}': {}", path.display(), e)))?;
    let mut header = [0u8; 8];
    match file.read_exact(&mut header) {
        Ok(()) => Ok(&header == BUNDLE_MAGIC),
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
        Err(err) => Err(runtime_err(format!("cannot read '{}': {}", path.display(), err))),
    }
}

fn write_atomic_bytes(path: &Path, bytes: &[u8]) -> Result<(), DgmError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|e| runtime_err(format!("cannot create '{}': {}", parent.display(), e)))?;
    let temp_path = path.with_extension(format!(
        "{}.tmp{}",
        path.extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| format!(".{}", ext))
            .unwrap_or_default(),
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

fn read_project_manifest(path: &Path) -> Result<ProjectManifest, DgmError> {
    let text = fs::read_to_string(path)
        .map_err(|e| runtime_err(format!("cannot read '{}': {}", path.display(), e)))?;
    toml::from_str(&text).map_err(|e| runtime_err(format!("invalid dgm.toml: {}", e)))
}

fn read_lock_file(path: &Path) -> Result<LockFile, DgmError> {
    let text = fs::read_to_string(path)
        .map_err(|e| runtime_err(format!("cannot read '{}': {}", path.display(), e)))?;
    toml::from_str(&text).map_err(|e| runtime_err(format!("invalid dgm.lock: {}", e)))
}

fn default_bundle_runtime(config: RuntimeConfig) -> BundleRuntimeConfig {
    BundleRuntimeConfig {
        max_steps: config.max_steps,
        max_call_depth: config.max_call_depth,
        max_heap: config.max_heap_bytes,
        max_threads: config.max_threads.max(1),
        max_wall_time_ms: config.max_wall_time_ms.max(1),
        max_open_sockets: config.max_open_sockets.max(1),
        max_concurrent_requests: config.max_concurrent_requests.max(1),
    }
}

fn default_bundle_wall_time_ms() -> u64 {
    30_000
}

fn default_bundle_max_open_sockets() -> usize {
    64
}

fn default_bundle_max_concurrent_requests() -> usize {
    16
}

#[derive(Debug, Clone)]
struct BundleHeader {
    version: u32,
    header_size: usize,
    manifest_len: usize,
    index_len: usize,
    modules_len: usize,
    checksum: [u8; 32],
}

fn encode_module_blob(modules: &[BundleModule]) -> Result<(BundleIndexBlob, Vec<u8>), DgmError> {
    let mut module_bytes = Vec::new();
    let mut entries = Vec::with_capacity(modules.len());
    for module in modules {
        let encoded = serde_json::to_vec(module)
            .map_err(|e| runtime_err(format!("cannot encode bundle module '{}': {}", module.key, e)))?;
        let offset = module_bytes.len();
        let len = encoded.len();
        module_bytes.extend_from_slice(&encoded);
        entries.push(BundleModuleIndex {
            id: module.id,
            key: module.key.clone(),
            logical_path: module.logical_path.clone(),
            imports: module.imports.clone(),
            offset,
            len,
        });
    }
    Ok((BundleIndexBlob { modules: entries }, module_bytes))
}

fn read_bundle_header(path: &Path, bytes: &[u8]) -> Result<BundleHeader, DgmError> {
    let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    match version {
        BUNDLE_FORMAT_VERSION_V1 => {
            if bytes.len() < BUNDLE_HEADER_SIZE_V1 {
                return Err(runtime_err(format!("bundle '{}' is truncated", path.display())));
            }
            let manifest_len = u64::from_le_bytes(bytes[12..20].try_into().unwrap()) as usize;
            let modules_len = u64::from_le_bytes(bytes[20..28].try_into().unwrap()) as usize;
            let mut checksum = [0u8; 32];
            checksum.copy_from_slice(&bytes[28..60]);
            Ok(BundleHeader {
                version,
                header_size: BUNDLE_HEADER_SIZE_V1,
                manifest_len,
                index_len: 0,
                modules_len,
                checksum,
            })
        }
        BUNDLE_FORMAT_VERSION => {
            if bytes.len() < BUNDLE_HEADER_SIZE {
                return Err(runtime_err(format!("bundle '{}' is truncated", path.display())));
            }
            let manifest_len = u64::from_le_bytes(bytes[12..20].try_into().unwrap()) as usize;
            let index_len = u64::from_le_bytes(bytes[20..28].try_into().unwrap()) as usize;
            let modules_len = u64::from_le_bytes(bytes[28..36].try_into().unwrap()) as usize;
            let mut checksum = [0u8; 32];
            checksum.copy_from_slice(&bytes[36..68]);
            Ok(BundleHeader {
                version,
                header_size: BUNDLE_HEADER_SIZE,
                manifest_len,
                index_len,
                modules_len,
                checksum,
            })
        }
        _ => Err(runtime_err(format!(
            "bundle '{}' uses unsupported format version {}",
            path.display(),
            version
        ))),
    }
}

fn payload_checksum(format_version: u32, manifest_bytes: &[u8], index_bytes: Option<&[u8]>, module_bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(BUNDLE_MAGIC);
    hasher.update(format_version.to_le_bytes());
    match format_version {
        BUNDLE_FORMAT_VERSION_V1 => {
            hasher.update((manifest_bytes.len() as u64).to_le_bytes());
            hasher.update((module_bytes.len() as u64).to_le_bytes());
            hasher.update(manifest_bytes);
            hasher.update(module_bytes);
        }
        BUNDLE_FORMAT_VERSION => {
            let index_bytes = index_bytes.unwrap_or(&[]);
            hasher.update((manifest_bytes.len() as u64).to_le_bytes());
            hasher.update((index_bytes.len() as u64).to_le_bytes());
            hasher.update((module_bytes.len() as u64).to_le_bytes());
            hasher.update(manifest_bytes);
            hasher.update(index_bytes);
            hasher.update(module_bytes);
        }
        _ => {}
    }
    hasher.finalize().into()
}

fn bundle_checksum(bytes: &[u8]) -> Result<[u8; 32], DgmError> {
    if bytes.len() < BUNDLE_HEADER_SIZE_V1 {
        return Err(runtime_err("bundle is truncated".into()));
    }
    let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    let header = match version {
        BUNDLE_FORMAT_VERSION_V1 => BundleHeader {
            version,
            header_size: BUNDLE_HEADER_SIZE_V1,
            manifest_len: u64::from_le_bytes(bytes[12..20].try_into().unwrap()) as usize,
            index_len: 0,
            modules_len: u64::from_le_bytes(bytes[20..28].try_into().unwrap()) as usize,
            checksum: [0u8; 32],
        },
        BUNDLE_FORMAT_VERSION => BundleHeader {
            version,
            header_size: BUNDLE_HEADER_SIZE,
            manifest_len: u64::from_le_bytes(bytes[12..20].try_into().unwrap()) as usize,
            index_len: u64::from_le_bytes(bytes[20..28].try_into().unwrap()) as usize,
            modules_len: u64::from_le_bytes(bytes[28..36].try_into().unwrap()) as usize,
            checksum: [0u8; 32],
        },
        _ => return Err(runtime_err(format!("bundle uses unsupported format version {}", version))),
    };
    let manifest_start = header.header_size;
    let manifest_end = manifest_start + header.manifest_len;
    let index_end = manifest_end + header.index_len;
    let modules_end = index_end + header.modules_len;
    Ok(payload_checksum(
        header.version,
        &bytes[manifest_start..manifest_end],
        if header.index_len > 0 { Some(&bytes[manifest_end..index_end]) } else { None },
        &bytes[index_end..modules_end],
    ))
}

fn load_v1_modules(
    path: &Path,
    module_bytes: &[u8],
) -> Result<(HashMap<String, BundleModuleIndex>, HashMap<String, BundleModule>, usize), DgmError> {
    let blob: BundleBlobV1 = serde_json::from_slice(module_bytes)
        .map_err(|e| runtime_err(format!("bundle '{}' modules are invalid: {}", path.display(), e)))?;
    let mut index = HashMap::new();
    let mut loaded = HashMap::new();
    for module in blob.modules {
        index.insert(
            module.key.clone(),
            BundleModuleIndex {
                id: module.id,
                key: module.key.clone(),
                logical_path: module.logical_path.clone(),
                imports: module.imports.clone(),
                offset: 0,
                len: 0,
            },
        );
        loaded.insert(module.key.clone(), module);
    }
    Ok((index, loaded, module_bytes.len()))
}

fn load_v2_index(
    path: &Path,
    index_bytes: &[u8],
    module_bytes: &[u8],
) -> Result<(HashMap<String, BundleModuleIndex>, HashMap<String, BundleModule>, usize), DgmError> {
    let blob: BundleIndexBlob = serde_json::from_slice(index_bytes)
        .map_err(|e| runtime_err(format!("bundle '{}' index is invalid: {}", path.display(), e)))?;
    let mut index = HashMap::new();
    for module in blob.modules {
        let end = module
            .offset
            .checked_add(module.len)
            .ok_or_else(|| runtime_err(format!("bundle '{}' has invalid module offsets", path.display())))?;
        if end > module_bytes.len() {
            return Err(runtime_err(format!("bundle '{}' module '{}' is out of bounds", path.display(), module.key)));
        }
        index.insert(module.key.clone(), module);
    }
    Ok((index, HashMap::new(), 0))
}

fn hex_checksum(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{:02x}", byte)).collect()
}

fn validate_bundle_manifest(path: &Path, manifest: &BundleManifest) -> Result<(), DgmError> {
    if manifest.bundle_format_version == 0 || manifest.bundle_format_version > BUNDLE_FORMAT_VERSION {
        return Err(runtime_err(format!(
            "bundle '{}' uses unsupported manifest format version {}",
            path.display(),
            manifest.bundle_format_version
        )));
    }
    let bundle_version = Version::parse(&manifest.dgm_version)
        .map_err(|e| runtime_err(format!("bundle '{}' has invalid dgm_version '{}': {}", path.display(), manifest.dgm_version, e)))?;
    let runtime_version = Version::parse(env!("CARGO_PKG_VERSION"))
        .map_err(|e| runtime_err(format!("current runtime version is invalid: {}", e)))?;
    if bundle_version.major != runtime_version.major {
        return Err(runtime_err(format!(
            "bundle '{}' targets DGM {} but runtime is {}",
            path.display(),
            manifest.dgm_version,
            env!("CARGO_PKG_VERSION")
        )));
    }
    if bundle_version.minor > runtime_version.minor {
        return Err(runtime_err(format!(
            "bundle '{}' was built for newer DGM {} than runtime {}",
            path.display(),
            manifest.dgm_version,
            env!("CARGO_PKG_VERSION")
        )));
    }
    Ok(())
}

fn canonical_or_original(path: &Path) -> Result<PathBuf, DgmError> {
    if path.exists() {
        fs::canonicalize(path)
            .map_err(|e| runtime_err(format!("cannot canonicalize '{}': {}", path.display(), e)))
    } else {
        Ok(path.to_path_buf())
    }
}

fn module_key(root: &Path, path: &Path) -> Result<String, DgmError> {
    let packages_root = root.join(".dgm/packages");
    if path.starts_with(&packages_root) {
        let relative = path
            .strip_prefix(&packages_root)
            .map_err(|_| runtime_err(format!("cannot map package path '{}'", path.display())))?;
        let mut components = relative.components();
        let name = component_text(components.next(), path)?;
        let version = component_text(components.next(), path)?;
        let rest = components.as_path();
        return Ok(format!("pkg:{}@{}:{}", name, version, normalize_path(rest)));
    }
    if path.starts_with(root) {
        let relative = path
            .strip_prefix(root)
            .map_err(|_| runtime_err(format!("cannot map project path '{}'", path.display())))?;
        return Ok(format!("project:{}", normalize_path(relative)));
    }
    Ok(format!("file:{}", normalize_path(path)))
}

fn logical_path(root: &Path, path: &Path) -> Result<String, DgmError> {
    let packages_root = root.join(".dgm/packages");
    if path.starts_with(&packages_root) {
        let relative = path
            .strip_prefix(&packages_root)
            .map_err(|_| runtime_err(format!("cannot map package path '{}'", path.display())))?;
        return Ok(format!("bundle/pkg/{}", normalize_path(relative)));
    }
    if path.starts_with(root) {
        let relative = path
            .strip_prefix(root)
            .map_err(|_| runtime_err(format!("cannot map project path '{}'", path.display())))?;
        return Ok(format!("bundle/project/{}", normalize_path(relative)));
    }
    Ok(format!("bundle/file/{}", normalize_path(path)))
}

fn component_text(component: Option<std::path::Component<'_>>, path: &Path) -> Result<String, DgmError> {
    component
        .and_then(|value| value.as_os_str().to_str())
        .map(|value| value.to_string())
        .ok_or_else(|| runtime_err(format!("cannot interpret module path '{}'", path.display())))
}

fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

pub fn debug_base_dir_for_module_key(module_key: &str, cwd: &Path) -> Option<PathBuf> {
    debug_path_for_module_key(module_key, cwd)?.parent().map(Path::to_path_buf)
}

fn debug_path_for_module_key(module_key: &str, cwd: &Path) -> Option<PathBuf> {
    if let Some(relative) = module_key.strip_prefix("project:") {
        return Some(cwd.join(relative));
    }
    if let Some(rest) = module_key.strip_prefix("pkg:") {
        let (package_ref, relative) = rest.split_once(':')?;
        let (name, version) = package_ref.split_once('@')?;
        return Some(cwd.join(".dgm").join("packages").join(name).join(version).join(relative));
    }
    if let Some(path) = module_key.strip_prefix("file:") {
        return Some(PathBuf::from(path));
    }
    None
}

fn default_bundle_format_version() -> u32 {
    BUNDLE_FORMAT_VERSION
}

fn resolve_build_import(current_path: &Path, raw_import: &str) -> Result<PathBuf, DgmError> {
    let trimmed = raw_import.trim();
    if trimmed.is_empty() {
        return Err(DgmError::ImportError {
            msg: "empty import in bundle build".into(),
        });
    }
    let raw = PathBuf::from(trimmed);
    let with_ext = if raw.extension().is_some() { raw } else { raw.with_extension("dgm") };
    let current_dir = current_path.parent().unwrap_or_else(|| Path::new("."));
    let candidate = if with_ext.is_absolute() {
        with_ext
    } else {
        current_dir.join(with_ext)
    };
    let candidate = canonical_or_original(&candidate)?;
    if candidate.exists() {
        return Ok(candidate);
    }
    if !trimmed.contains('/') && !trimmed.contains('\\') {
        if let Some(package_entry) = package_manager::resolve_installed_package(current_dir, trimmed)? {
            return Ok(canonical_or_original(&package_entry)?);
        }
    }
    Err(DgmError::ImportError {
        msg: format!("cannot resolve import '{}' while building bundle", raw_import),
    })
}

fn is_stdlib_import(import_name: &str) -> bool {
    !import_name.contains('/')
        && !import_name.contains('\\')
        && crate::stdlib::load_module(import_name).is_some()
}

fn collect_imports(stmts: &[Stmt]) -> BTreeSet<String> {
    let mut imports = BTreeSet::new();
    for stmt in stmts {
        collect_stmt_imports(stmt, &mut imports);
    }
    imports
}

fn collect_stmt_imports(stmt: &Stmt, imports: &mut BTreeSet<String>) {
    match &stmt.kind {
        StmtKind::Imprt(name) => {
            imports.insert(name.clone());
        }
        StmtKind::Expr(expr) | StmtKind::Writ(expr) | StmtKind::Throw(expr) => collect_expr_imports(expr, imports),
        StmtKind::Let { pattern, value } => {
            collect_binding_pattern_imports(pattern, imports);
            collect_expr_imports(value, imports);
        }
        StmtKind::If { condition, then_block, elseif_branches, else_block } => {
            collect_expr_imports(condition, imports);
            collect_stmt_block_imports(then_block, imports);
            for (expr, block) in elseif_branches {
                collect_expr_imports(expr, imports);
                collect_stmt_block_imports(block, imports);
            }
            if let Some(block) = else_block {
                collect_stmt_block_imports(block, imports);
            }
        }
        StmtKind::While { condition, body } => {
            collect_expr_imports(condition, imports);
            collect_stmt_block_imports(body, imports);
        }
        StmtKind::For { iterable, body, .. } => {
            collect_expr_imports(iterable, imports);
            collect_stmt_block_imports(body, imports);
        }
        StmtKind::FuncDef { params, body, .. } => {
            for param in params {
                if let Some(default) = &param.default {
                    collect_expr_imports(default, imports);
                }
            }
            collect_stmt_block_imports(body, imports);
        }
        StmtKind::ClassDef { methods: body, .. } => {
            collect_stmt_block_imports(body, imports);
        }
        StmtKind::Return(Some(expr)) => collect_expr_imports(expr, imports),
        StmtKind::TryCatch { try_block, catch_block, finally_block, .. } => {
            collect_stmt_block_imports(try_block, imports);
            collect_stmt_block_imports(catch_block, imports);
            if let Some(block) = finally_block {
                collect_stmt_block_imports(block, imports);
            }
        }
        StmtKind::Match { expr, arms } => {
            collect_expr_imports(expr, imports);
            for arm in arms {
                collect_match_pattern_imports(&arm.pattern, imports);
                if let Some(guard) = &arm.guard {
                    collect_expr_imports(guard, imports);
                }
                collect_stmt_block_imports(&arm.body, imports);
            }
        }
        StmtKind::Return(None) | StmtKind::Break | StmtKind::Continue => {}
    }
}

fn collect_stmt_block_imports(stmts: &[Stmt], imports: &mut BTreeSet<String>) {
    for stmt in stmts {
        collect_stmt_imports(stmt, imports);
    }
}

fn collect_binding_pattern_imports(pattern: &BindingPattern, imports: &mut BTreeSet<String>) {
    match pattern {
        BindingPattern::Ignore => {}
        BindingPattern::Name { default, .. } => {
            if let Some(default) = default {
                collect_expr_imports(default, imports);
            }
        }
        BindingPattern::List(items) => {
            for item in items {
                collect_binding_pattern_imports(item, imports);
            }
        }
        BindingPattern::Map(entries) => {
            for entry in entries {
                collect_binding_pattern_imports(&entry.pattern, imports);
            }
        }
    }
}

fn collect_match_pattern_imports(pattern: &MatchPattern, imports: &mut BTreeSet<String>) {
    if let MatchPattern::Expr(expr) = pattern {
        collect_expr_imports(expr, imports);
    }
}

fn collect_expr_imports(expr: &Expr, imports: &mut BTreeSet<String>) {
    match expr {
        Expr::BinOp { left, right, .. } => {
            collect_expr_imports(left, imports);
            collect_expr_imports(right, imports);
        }
        Expr::UnaryOp { operand, .. } | Expr::Await(operand) => collect_expr_imports(operand, imports),
        Expr::Call { callee, args } | Expr::OptionalCall { callee, args } | Expr::New { callee, args } => {
            collect_expr_imports(callee, imports);
            for arg in args {
                collect_expr_imports(&arg.value, imports);
            }
        }
        Expr::NullCoalesce { left, right } => {
            collect_expr_imports(left, imports);
            collect_expr_imports(right, imports);
        }
        Expr::Index { object, index } => {
            collect_expr_imports(object, imports);
            collect_expr_imports(index, imports);
        }
        Expr::FieldAccess { object, .. } | Expr::OptionalFieldAccess { object, .. } => collect_expr_imports(object, imports),
        Expr::List(items) | Expr::StringInterp(items) => {
            for item in items {
                collect_expr_imports(item, imports);
            }
        }
        Expr::Map(entries) => {
            for (key, value) in entries {
                collect_expr_imports(key, imports);
                collect_expr_imports(value, imports);
            }
        }
        Expr::Assign { target, value, .. } => {
            collect_expr_imports(target, imports);
            collect_expr_imports(value, imports);
        }
        Expr::Lambda { params, body } => {
            for param in params {
                if let Some(default) = &param.default {
                    collect_expr_imports(default, imports);
                }
            }
            collect_stmt_block_imports(body, imports);
        }
        Expr::Ternary { condition, then_expr, else_expr } => {
            collect_expr_imports(condition, imports);
            collect_expr_imports(then_expr, imports);
            collect_expr_imports(else_expr, imports);
        }
        Expr::Range { start, end } => {
            collect_expr_imports(start, imports);
            collect_expr_imports(end, imports);
        }
        Expr::IntLit(_)
        | Expr::FloatLit(_)
        | Expr::StringLit(_)
        | Expr::BoolLit(_)
        | Expr::NullLit
        | Expr::Ident(_)
        | Expr::This => {}
    }
}

fn runtime_err(msg: String) -> DgmError {
    DgmError::BundleError { msg }
}
