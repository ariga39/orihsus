//! Durable, client-session-aware key affinity.
//!
//! Only requests that carry a real client `x-opencode-session` value are
//! routed here. Requests without one keep the legacy fill-first pool behavior.
//!
//! Bindings live in one embedded SQLite database keyed by the full SHA-256 of
//! the client session bytes, so the raw session value is never persisted and
//! no truncation can merge distinct sessions. Each mutation is a synchronous
//! SQLite transaction executed on Tokio's blocking pool. Dropping the awaiting
//! future does not cancel the blocking transaction, so a committed bind/rebind
//! is never torn by request cancellation. A per-session generation makes the
//! rebind a compare-and-swap: an older report cannot overwrite a newer binding.

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use sha2::{Digest, Sha256};

use crate::pool::{KeyPool, PoolError};
use crate::usage::UsageSnapshotStore;

#[derive(Debug)]
pub enum RoutingError {
    Io(io::Error),
    Db(rusqlite::Error),
    InvalidSession,
    Pool(PoolError),
}

impl fmt::Display for RoutingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "session routing state I/O failed: {error}"),
            Self::Db(error) => write!(f, "session routing database failed: {error}"),
            Self::InvalidSession => f.write_str("invalid client session identity"),
            Self::Pool(error) => write!(f, "session routing key pool failed: {error}"),
        }
    }
}

impl std::error::Error for RoutingError {}

impl From<io::Error> for RoutingError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<rusqlite::Error> for RoutingError {
    fn from(value: rusqlite::Error) -> Self {
        Self::Db(value)
    }
}

impl From<PoolError> for RoutingError {
    fn from(value: PoolError) -> Self {
        Self::Pool(value)
    }
}

/// A durable binding plus the generation observed by a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    pub fingerprint: String,
    pub generation: u64,
}

#[derive(Debug, Clone)]
pub enum RouteDecision {
    /// Existing or newly committed affinity.
    Bound(Binding),
    /// No binding and no currently eligible key. The request attempt tracker
    /// waits using this fallback order; the selection is durably committed
    /// before it is sent upstream.
    New { order: Vec<String> },
}

#[derive(Debug)]
pub enum RebindOutcome {
    /// This report owned the current binding and its update or
    /// generation-preserving tombstone committed.
    Rebound(Option<Binding>),
    /// A concurrent report or first bind already changed the binding.
    Current(Option<Binding>),
}

#[derive(Clone)]
pub struct SessionRouting {
    inner: Arc<Inner>,
}

struct Inner {
    db: Arc<Mutex<Connection>>,
    usage: UsageSnapshotStore,
    /// Per-session version channels. Waiters subscribe before reading the DB,
    /// so a commit between resolve and await is not lost. Unrelated sessions
    /// never wake each other.
    versions: Arc<Mutex<HashMap<[u8; 32], tokio::sync::watch::Sender<u64>>>>,
}

/// A row always retains its generation, including while unbound
/// (`fingerprint == None`). Reusing the row avoids restarting the generation
/// at 1, which would let a stale exhaustion report for an older assignment of
/// the same key overwrite a newer binding.
#[derive(Debug, Clone)]
struct StoredBinding {
    fingerprint: Option<String>,
    generation: u64,
}

impl StoredBinding {
    fn public(&self) -> Option<Binding> {
        self.fingerprint.as_ref().map(|fingerprint| Binding {
            fingerprint: fingerprint.clone(),
            generation: self.generation,
        })
    }
}

impl SessionRouting {
    /// Open (creating if needed) the durable binding database. Every startup
    /// failure is explicit; there is no silent in-memory fallback.
    pub fn open(
        path: impl Into<PathBuf>,
        usage: UsageSnapshotStore,
    ) -> Result<SessionRouting, RoutingError> {
        let path = path.into();
        if path.as_os_str().is_empty() {
            return Err(RoutingError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "session routing state path is empty",
            )));
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let connection = Connection::open(path)?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS session_bindings (
                 session_hash BLOB PRIMARY KEY NOT NULL,
                 key_fingerprint TEXT,
                 generation INTEGER NOT NULL
             ) WITHOUT ROWID;",
        )?;
        Ok(SessionRouting {
            inner: Arc::new(Inner {
                db: Arc::new(Mutex::new(connection)),
                usage,
                versions: Arc::new(Mutex::new(HashMap::new())),
            }),
        })
    }

    /// Resolve the first-attempt route for a client session.
    ///
    /// An existing binding is always returned, even if its key was removed by
    /// a reload. It never rebinds without upstream evidence; the request then
    /// waits or reports hard unavailability under the pinned membership.
    pub async fn decide(
        &self,
        session: &[u8],
        candidates: &[String],
        model: &str,
        pool: &KeyPool,
    ) -> Result<RouteDecision, RoutingError> {
        let hash = session_hash(session)?;
        if let Some(binding) = self
            .load_stored_binding(hash)
            .await?
            .and_then(|stored| stored.public())
        {
            return Ok(RouteDecision::Bound(binding));
        }

        let order = self.new_session_order(candidates, model, pool)?;
        if let Some(fingerprint) = order
            .iter()
            .find(|fingerprint| self.is_new_eligible(fingerprint, model, pool))
        {
            let binding = self.bind_hash_if_absent(hash, fingerprint).await?;
            return Ok(RouteDecision::Bound(binding));
        }
        Ok(RouteDecision::New { order })
    }

    /// Commit the first selection made from a `New` order before it is sent
    /// upstream. If another request won the bind race, its binding is returned.
    pub async fn bind_if_absent(
        &self,
        session: &[u8],
        fingerprint: &str,
    ) -> Result<Binding, RoutingError> {
        let hash = session_hash(session)?;
        self.bind_hash_if_absent(hash, fingerprint).await
    }

    async fn bind_hash_if_absent(
        &self,
        hash: [u8; 32],
        fingerprint: &str,
    ) -> Result<Binding, RoutingError> {
        let fingerprint = fingerprint.to_string();
        let versions = Arc::clone(&self.inner.versions);
        self.blocking_db(move |connection| {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let stored = read_stored_binding(&transaction, hash)?;
            let (binding, changed) = match stored {
                Some(stored) => match stored.public() {
                    Some(binding) => (binding, false),
                    None => {
                        let generation = stored.generation.saturating_add(1);
                        transaction.execute(
                            "UPDATE session_bindings
                             SET key_fingerprint = ?2, generation = ?3
                             WHERE session_hash = ?1",
                            params![hash.as_slice(), fingerprint.as_str(), generation as i64],
                        )?;
                        (
                            Binding {
                                fingerprint,
                                generation,
                            },
                            true,
                        )
                    }
                },
                None => {
                    transaction.execute(
                        "INSERT INTO session_bindings
                             (session_hash, key_fingerprint, generation)
                         VALUES (?1, ?2, 1)",
                        params![hash.as_slice(), fingerprint.as_str()],
                    )?;
                    (
                        Binding {
                            fingerprint,
                            generation: 1,
                        },
                        true,
                    )
                }
            };
            transaction.commit()?;
            if changed {
                notify_version(&versions, hash);
            }
            Ok(binding)
        })
        .await
    }

    /// Rebind only when this request still owns the observed generation and
    /// key. A different current binding wins instead. If no alternate is
    /// eligible, the row is retained with no key and its generation advanced,
    /// so the next request (or a restart) can bind afresh without ever reusing
    /// an older generation.
    #[allow(clippy::too_many_arguments)]
    pub async fn rebind_after_usage_limit(
        &self,
        session: &[u8],
        from_fingerprint: &str,
        expected_generation: u64,
        candidates: &[String],
        model: &str,
        pool: &KeyPool,
        exclude: &[String],
    ) -> Result<RebindOutcome, RoutingError> {
        let hash = session_hash(session)?;
        let alternate = self
            .new_session_order(candidates, model, pool)?
            .into_iter()
            .find(|fingerprint| {
                fingerprint != from_fingerprint
                    && !exclude.iter().any(|used| used == fingerprint)
                    && self.is_new_eligible(fingerprint, model, pool)
            });
        let from_fingerprint = from_fingerprint.to_string();
        let versions = Arc::clone(&self.inner.versions);
        self.blocking_db(move |connection| {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let current = read_stored_binding(&transaction, hash)?;
            let owns = current.as_ref().is_some_and(|binding| {
                binding.fingerprint.as_deref() == Some(from_fingerprint.as_str())
                    && binding.generation == expected_generation
            });
            if !owns {
                let current = current.and_then(|binding| binding.public());
                transaction.commit()?;
                return Ok(RebindOutcome::Current(current));
            }
            let next_generation = expected_generation.saturating_add(1);
            let changed = transaction.execute(
                "UPDATE session_bindings
                 SET key_fingerprint = ?2, generation = ?3
                 WHERE session_hash = ?1
                   AND key_fingerprint = ?4
                   AND generation = ?5",
                params![
                    hash.as_slice(),
                    alternate.as_deref(),
                    next_generation as i64,
                    from_fingerprint.as_str(),
                    expected_generation as i64
                ],
            )?;
            if changed == 0 {
                let current =
                    read_stored_binding(&transaction, hash)?.and_then(|binding| binding.public());
                transaction.commit()?;
                return Ok(RebindOutcome::Current(current));
            }
            let outcome = RebindOutcome::Rebound(alternate.map(|fingerprint| Binding {
                fingerprint,
                generation: next_generation,
            }));
            transaction.commit()?;
            notify_version(&versions, hash);
            Ok(outcome)
        })
        .await
    }

    /// New first bindings prefer the eligible key with the earliest valid
    /// future monthly reset. Ties and missing/stale observations fall back to
    /// the pool's pinned fill-first order.
    pub fn new_session_order(
        &self,
        candidates: &[String],
        model: &str,
        pool: &KeyPool,
    ) -> Result<Vec<String>, RoutingError> {
        let now = Utc::now();
        let mut ranked: Vec<(usize, String, Option<DateTime<Utc>>, bool)> = candidates
            .iter()
            .enumerate()
            .map(|(index, fingerprint)| {
                let reset = self.monthly_reset(fingerprint, now);
                let eligible = self.is_new_eligible(fingerprint, model, pool);
                (index, fingerprint.clone(), reset, eligible)
            })
            .collect();
        ranked.sort_by(|left, right| {
            right.3.cmp(&left.3).then_with(|| match (left.2, right.2) {
                (Some(a), Some(b)) => a.cmp(&b).then_with(|| left.0.cmp(&right.0)),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => left.0.cmp(&right.0),
            })
        });
        Ok(ranked
            .into_iter()
            .map(|(_, fingerprint, _, _)| fingerprint)
            .collect())
    }

    fn is_new_eligible(&self, fingerprint: &str, model: &str, pool: &KeyPool) -> bool {
        // The pool owns both availability policies. Soft availability already
        // includes hard failures, breaker claims, and model liveness.
        pool.key_health(fingerprint, Some(model))
            .is_some_and(|health| health.soft_available)
    }

    fn monthly_reset(&self, fingerprint: &str, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.inner
            .usage
            .get(fingerprint)?
            .monthly
            .resets_at?
            .parse::<DateTime<Utc>>()
            .ok()
            .filter(|reset| *reset > now)
    }

    /// Subscribe to this session's binding-version changes before reading the
    /// current binding. A commit between the DB read and awaiting `changed()`
    /// advances the version, so the wakeup cannot be lost.
    pub fn subscribe(
        &self,
        session: &[u8],
    ) -> Result<tokio::sync::watch::Receiver<u64>, RoutingError> {
        let hash = session_hash(session)?;
        let mut versions = self
            .inner
            .versions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        versions.retain(|_, sender| sender.receiver_count() > 0);
        let sender = versions
            .entry(hash)
            .or_insert_with(|| tokio::sync::watch::channel(0_u64).0);
        Ok(sender.subscribe())
    }

    /// Current committed binding for a session, if any. A durable tombstone
    /// (retained generation with no key) reads as `None`.
    pub async fn current_binding(&self, session: &[u8]) -> Result<Option<Binding>, RoutingError> {
        let hash = session_hash(session)?;
        Ok(self
            .load_stored_binding(hash)
            .await?
            .and_then(|binding| binding.public()))
    }

    async fn load_stored_binding(
        &self,
        hash: [u8; 32],
    ) -> Result<Option<StoredBinding>, RoutingError> {
        self.blocking_db(move |connection| {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Deferred)?;
            let binding = read_stored_binding(&transaction, hash)?;
            transaction.commit()?;
            Ok(binding)
        })
        .await
    }

    /// Run one synchronous SQLite operation on the blocking pool. The blocking
    /// task is not cancelled when the awaiting request future is dropped, so a
    /// bind/rebind transaction always reaches commit or rollback as a unit.
    async fn blocking_db<T, F>(&self, operation: F) -> Result<T, RoutingError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> rusqlite::Result<T> + Send + 'static,
    {
        let db = Arc::clone(&self.inner.db);
        tokio::task::spawn_blocking(move || {
            let mut connection = db.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            operation(&mut connection)
        })
        .await
        .map_err(|error| RoutingError::Io(io::Error::other(error)))?
        .map_err(RoutingError::Db)
    }
}

fn read_stored_binding(
    transaction: &rusqlite::Transaction<'_>,
    hash: [u8; 32],
) -> rusqlite::Result<Option<StoredBinding>> {
    transaction
        .query_row(
            "SELECT key_fingerprint, generation
             FROM session_bindings WHERE session_hash = ?1",
            params![hash.as_slice()],
            |row| {
                Ok(StoredBinding {
                    fingerprint: row.get(0)?,
                    generation: row.get::<_, i64>(1)?.max(1) as u64,
                })
            },
        )
        .optional()
}

fn notify_version(
    versions: &Mutex<HashMap<[u8; 32], tokio::sync::watch::Sender<u64>>>,
    hash: [u8; 32],
) {
    let versions = versions
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(sender) = versions.get(&hash) {
        sender.send_modify(|version| *version = version.wrapping_add(1));
    }
}

fn session_hash(session: &[u8]) -> Result<[u8; 32], RoutingError> {
    if session.is_empty() {
        return Err(RoutingError::InvalidSession);
    }
    let digest = Sha256::digest(session);
    Ok(digest.into())
}
