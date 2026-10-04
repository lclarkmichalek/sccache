use std::io;
use std::process::Command;
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use futures::channel::mpsc;
use futures::channel::oneshot;
use tokio::sync::Notify;

use crate::errors::*;

// The execution model of sccache is that on the first run it spawns a server
// in the background and detaches it.
// When normally executing the rust compiler from either cargo or make, it
// will use cargo/make's jobserver and limit its resource usage accordingly.
// When executing the rust compiler through the sccache server, that jobserver
// is not available, and spawning as many rustc as there are CPUs can lead to
// a quadratic use of the CPU resources (each rustc spawning as many threads
// as there are CPUs).
// One way around this issue is to inherit the jobserver from cargo or make
// when the sccache server is spawned, but that means that in some cases, the
// cargo or make process can't terminate until the sccache server terminates
// after its idle timeout (which also never happens if SCCACHE_IDLE_TIMEOUT=0).
// Also, if the sccache server ends up shared between multiple runs of
// cargo/make, then which jobserver is used doesn't make sense anymore.
// Ideally, the sccache client would give a handle to the jobserver it has
// access to, so that the rust compiler would "just" use the jobserver it
// would have used if it had run without sccache, but that adds some extra
// complexity, and requires to use Unix domain sockets.
// What we do instead is to arbitrary use our own jobserver.
// Unfortunately, that doesn't absolve us from having to deal with the original
// jobserver, because make may give us file descriptors to its pipes, and the
// simple fact of keeping them open can block it. That is handled by closing
// every inherited descriptor when the server detaches; see
// `util::close_inherited_fds`.

#[derive(Clone)]
pub struct Client {
    shared: Arc<Shared>,
}

struct Shared {
    state: Mutex<State>,
    changed: Notify,
    limit: usize,
}

struct State {
    pool: Option<Arc<Pool>>,
    users: usize,
    needs_reset: bool,
}

struct Pool {
    helper: jobserver::HelperThread,
    tx: mpsc::UnboundedSender<oneshot::Sender<io::Result<jobserver::Acquired>>>,
    inner: jobserver::Client,
}

pub struct Acquired {
    // Return the token before allowing the pool to become idle.
    _token: jobserver::Acquired,
    reservation: Reservation,
}

struct Reservation {
    shared: Arc<Shared>,
    pool: Arc<Pool>,
    may_have_leaked: bool,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let retired = {
            let mut state = self.shared.state.lock().unwrap();
            state.needs_reset |= self.may_have_leaked;
            state.users -= 1;
            if state.needs_reset && state.users == 0 {
                debug!("Retiring jobserver after a shared child failed or was cancelled");
                state.needs_reset = false;
                let retired = state.pool.take();
                self.shared.changed.notify_waiters();
                retired
            } else {
                None
            }
        };
        drop(retired);
    }
}

impl Acquired {
    pub fn configure(&mut self, cmd: &mut Command) {
        // Cancellation also kills the child, so assume its borrowed tokens
        // are lost unless it completes successfully.
        self.reservation.may_have_leaked = true;
        self.reservation.pool.inner.configure(cmd);
    }

    pub fn completed(&mut self, status: std::process::ExitStatus) {
        if status.success() {
            self.reservation.may_have_leaked = false;
        }
    }
}

impl Pool {
    fn new(num: usize) -> Result<Self> {
        let inner = jobserver::Client::new(num).context("failed to create jobserver")?;
        let (tx, mut rx) = mpsc::unbounded::<oneshot::Sender<_>>();
        let helper = inner
            .clone()
            .into_helper_thread(move |token| {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    if let Some(sender) = rx.next().await {
                        drop(sender.send(token));
                    }
                });
            })
            .context("failed to spawn helper thread")?;
        Ok(Self { inner, helper, tx })
    }
}

impl Client {
    pub fn new() -> Client {
        Client::new_num(crate::util::num_cpus())
    }

    pub fn new_num(num: usize) -> Client {
        Client {
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    pool: Some(Arc::new(
                        Pool::new(num).expect("failed to create jobserver"),
                    )),
                    users: 0,
                    needs_reset: false,
                }),
                changed: Notify::new(),
                limit: num,
            }),
        }
    }

    /// Returns a future that represents an acquired jobserver token.
    ///
    /// This should be invoked before any "work" is spawned (for whatever the
    /// definition of "work" is) to ensure that the system is properly
    /// rate-limiting itself.
    pub async fn acquire(&self) -> Result<Acquired> {
        let reservation = loop {
            let changed = self.shared.changed.notified();
            let reservation = {
                let mut state = self.shared.state.lock().unwrap();
                if state.needs_reset {
                    None
                } else {
                    if state.pool.is_none() {
                        state.pool = Some(Arc::new(Pool::new(self.shared.limit)?));
                    }
                    state.users += 1;
                    Some(Reservation {
                        shared: self.shared.clone(),
                        pool: state.pool.as_ref().unwrap().clone(),
                        may_have_leaked: false,
                    })
                }
            };
            if let Some(reservation) = reservation {
                break reservation;
            }
            // Stop admitting work to a damaged pool so it can drain even
            // under continuous load. Pending acquisitions count as users too.
            changed.await;
        };
        let (mytx, myrx) = oneshot::channel();
        reservation.pool.tx.unbounded_send(mytx).unwrap();
        reservation.pool.helper.request_token();

        let acquired = myrx
            .await
            .context("jobserver helper panicked")?
            .context("failed to acquire jobserver token")?;

        Ok(Acquired {
            _token: acquired,
            reservation,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::mock_command::exit_status;
    use std::time::Duration;

    async fn acquire(client: &Client) -> Acquired {
        tokio::time::timeout(Duration::from_secs(5), client.acquire())
            .await
            .expect("jobserver acquisition timed out")
            .unwrap()
    }

    #[tokio::test]
    async fn successful_child_keeps_pool() {
        let client = Client::new_num(2);
        let mut token = acquire(&client).await;
        let pool = token.reservation.pool.clone();
        token.configure(&mut Command::new("rustc"));
        token.completed(exit_status(0));
        drop(token);
        let next = acquire(&client).await;
        assert!(Arc::ptr_eq(&pool, &next.reservation.pool));
        assert_eq!(pool.inner.available().unwrap(), 1);
    }

    #[tokio::test]
    async fn failed_unshared_child_keeps_pool() {
        let client = Client::new_num(2);
        let mut token = acquire(&client).await;
        let pool = token.reservation.pool.clone();
        token.completed(exit_status(1));
        drop(token);
        let next = acquire(&client).await;
        assert!(Arc::ptr_eq(&pool, &next.reservation.pool));
        assert_eq!(pool.inner.available().unwrap(), 1);
    }

    #[tokio::test]
    async fn failed_child_recovers_borrowed_tokens() {
        let client = Client::new_num(4);
        for _ in 0..3 {
            let mut token = acquire(&client).await;
            token.configure(&mut Command::new("rustc"));
            let pool = token.reservation.pool.clone();
            for _ in 0..3 {
                pool.inner.acquire().unwrap().drop_without_releasing();
            }
            token.completed(exit_status(1));
            drop(token);
            let next = acquire(&client).await;
            assert!(!Arc::ptr_eq(&pool, &next.reservation.pool));
            assert_eq!(next.reservation.pool.inner.available().unwrap(), 3);
        }
    }

    #[tokio::test]
    async fn cancelled_child_recovers_borrowed_tokens() {
        let client = Client::new_num(2);
        let mut token = acquire(&client).await;
        token.configure(&mut Command::new("rustc"));
        token
            .reservation
            .pool
            .inner
            .acquire()
            .unwrap()
            .drop_without_releasing();
        drop(token);
        let next = acquire(&client).await;
        assert_eq!(next.reservation.pool.inner.available().unwrap(), 1);
    }

    #[tokio::test]
    async fn recovery_waits_for_existing_users_and_blocks_new_work() {
        let client = Client::new_num(3);
        let mut failed = acquire(&client).await;
        failed.configure(&mut Command::new("rustc"));
        let peer = acquire(&client).await;
        let pool = peer.reservation.pool.clone();
        pool.inner.acquire().unwrap().drop_without_releasing();

        let mut queued = Box::pin(client.acquire());
        assert!(futures::poll!(&mut queued).is_pending());
        drop(failed);
        let queued = tokio::time::timeout(Duration::from_secs(5), queued)
            .await
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&pool, &queued.reservation.pool));

        let mut next = Box::pin(client.acquire());
        assert!(futures::poll!(&mut next).is_pending());
        drop(queued);
        assert!(futures::poll!(&mut next).is_pending());
        assert_eq!(pool.inner.available().unwrap(), 1);
        drop(peer);

        let next = tokio::time::timeout(Duration::from_secs(5), next)
            .await
            .unwrap()
            .unwrap();
        assert!(!Arc::ptr_eq(&pool, &next.reservation.pool));
        assert_eq!(next.reservation.pool.inner.available().unwrap(), 2);
    }

    #[tokio::test]
    async fn cancelled_acquisitions_do_not_prevent_recovery() {
        let client = Client::new_num(2);
        let mut failed = acquire(&client).await;
        failed.configure(&mut Command::new("rustc"));
        failed
            .reservation
            .pool
            .inner
            .acquire()
            .unwrap()
            .drop_without_releasing();
        let mut queued = Box::pin(client.acquire());
        assert!(futures::poll!(&mut queued).is_pending());
        drop(failed);
        let mut waiting_for_reset = Box::pin(client.acquire());
        assert!(futures::poll!(&mut waiting_for_reset).is_pending());
        drop(waiting_for_reset);
        drop(queued);
        let next = acquire(&client).await;
        assert_eq!(next.reservation.pool.inner.available().unwrap(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recovery_wakes_all_waiters_without_exceeding_capacity() {
        let client = Client::new_num(2);
        let peer = acquire(&client).await;
        let mut failed = acquire(&client).await;
        failed.configure(&mut Command::new("rustc"));
        drop(failed);
        let mut first = Box::pin(client.acquire());
        let mut second = Box::pin(client.acquire());
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut second).is_pending());
        drop(peer);
        let (first, second) = tokio::time::timeout(
            Duration::from_secs(5),
            futures::future::try_join(first, second),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(Arc::ptr_eq(
            &first.reservation.pool,
            &second.reservation.pool
        ));
        let mut excess = Box::pin(client.acquire());
        assert!(futures::poll!(&mut excess).is_pending());
        drop(first);
        tokio::time::timeout(Duration::from_secs(5), excess)
            .await
            .unwrap()
            .unwrap();
    }
}
