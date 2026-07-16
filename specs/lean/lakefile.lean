import Lake
open Lake DSL

package «fanout-serializability» where

@[default_target]
lean_lib FanoutSerializability where
  roots := #[`FanoutSerializability, `WorkerSizing]
