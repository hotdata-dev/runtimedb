use std::collections::HashMap;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::*;
use liquid_cache_client::PushdownOptimizer;
use object_store::ObjectStore;

type ObjectStoreMapper =
    Box<dyn Fn(Arc<dyn ObjectStore>, &ObjectStoreUrl) -> Arc<dyn ObjectStore> + Send + Sync>;

type SessionStateMapper = Box<dyn FnOnce(SessionStateBuilder) -> SessionStateBuilder + Send>;

// Local rewrite of `liquid_cache_client::LiquidCacheClientBuilder` to support
// object store and session state instrumentation hooks. Remove this once the
// upstream library exposes these extension points.
pub struct LiquidCacheClientBuilder {
    object_stores: Vec<(ObjectStoreUrl, HashMap<String, String>)>,
    cache_server: String,
    object_store_mapper: Option<ObjectStoreMapper>,
    session_state_mapper: Option<SessionStateMapper>,
}

impl LiquidCacheClientBuilder {
    /// Create a new builder for LiquidCache client state.
    pub fn new(cache_server: impl AsRef<str>) -> Self {
        Self {
            object_stores: vec![],
            cache_server: cache_server.as_ref().to_string(),
            object_store_mapper: None,
            session_state_mapper: None,
        }
    }

    /// Add an object store to the builder.
    /// Checkout <https://docs.rs/object_store/latest/object_store/fn.parse_url_opts.html> for available options.
    pub fn with_object_store(
        mut self,
        url: ObjectStoreUrl,
        object_store_options: Option<HashMap<String, String>>,
    ) -> Self {
        self.object_stores
            .push((url, object_store_options.unwrap_or_default()));
        self
    }

    /// Set a mapper function that transforms each object store before registration.
    ///
    /// This is useful for wrapping object stores with instrumentation or middleware.
    /// The function receives the object store and its URL, and should return the
    /// (possibly wrapped) object store.
    pub fn with_object_store_mapper(
        mut self,
        f: impl Fn(Arc<dyn ObjectStore>, &ObjectStoreUrl) -> Arc<dyn ObjectStore>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        self.object_store_mapper = Some(Box::new(f));
        self
    }

    /// Set a mapper function that transforms the `SessionStateBuilder` after the default
    /// configuration and optimizer rules have been applied.
    ///
    /// This is useful for adding additional physical optimizer rules (e.g., tracing instrumentation).
    pub fn with_session_state_mapper(
        mut self,
        f: impl FnOnce(SessionStateBuilder) -> SessionStateBuilder + Send + 'static,
    ) -> Self {
        self.session_state_mapper = Some(Box::new(f));
        self
    }

    /// Build the [SessionContext].
    pub fn build(self, config: SessionConfig) -> Result<SessionContext> {
        let mut session_config = config;
        session_config
            .options_mut()
            .execution
            .parquet
            .pushdown_filters = true;
        session_config
            .options_mut()
            .execution
            .parquet
            .schema_force_view_types = false;
        session_config
            .options_mut()
            .execution
            .parquet
            .binary_as_string = true;
        session_config.options_mut().execution.batch_size = 8192 * 2;

        let runtime_env = Arc::new(RuntimeEnv::default());

        // Register object stores
        for (object_store_url, options) in &self.object_stores {
            let (object_store, _path) =
                object_store::parse_url_opts(object_store_url.as_ref(), options.clone())
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let object_store: Arc<dyn ObjectStore> = Arc::new(object_store);
            let object_store = match &self.object_store_mapper {
                Some(mapper) => mapper(object_store, object_store_url),
                None => object_store,
            };
            runtime_env.register_object_store(object_store_url.as_ref(), object_store);
        }

        let mut state_builder = SessionStateBuilder::new()
            .with_config(session_config)
            .with_runtime_env(runtime_env)
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(PushdownOptimizer::new(
                self.cache_server.clone(),
                self.object_stores.clone(),
            )));

        if let Some(mapper) = self.session_state_mapper {
            state_builder = mapper(state_builder);
        }

        Ok(SessionContext::new_with_state(state_builder.build()))
    }
}
